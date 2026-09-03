// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use self::agg::AggKind;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::function::table::TableFunction;
use crate::expression::visitor::{walk_expr, ExprVisitor};
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::planner::operator::sort::SortField;
use crate::planner::{Explain, ExprRef, MetaArena, PlanArena};
use crate::types::evaluator::{
    binary_create, cast_create, unary_create, BinaryEvaluatorRef, CastEvaluatorRef,
    UnaryEvaluatorRef,
};
use crate::types::value::DataValue;
use crate::types::{CharLengthUnits, LogicalType};
use kite_sql_serde_macros::ReferenceSerialization;
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

pub mod agg;
mod eq_col;
mod evaluator;
pub mod function;
pub mod range_detacher;
pub mod simplify;
pub mod visitor;
pub mod visitor_mut;
pub mod window;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum AliasType {
    Name(String),
    Expr(ExprRef),
}

/// ScalarExpression represnet all scalar expression in SQL.
/// SELECT a+1, b FROM t1.
/// a+1 -> ScalarExpression::Unary(a + 1)
/// b   -> ScalarExpression::ColumnRef()
#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum ScalarExpression {
    Constant(DataValue),
    ColumnRef {
        column: ColumnRef,
        position: usize,
    },
    Alias {
        expr: ExprRef,
        alias: AliasType,
    },
    TypeCast {
        expr: ExprRef,
        ty: LogicalType,
        evaluator: Option<CastEvaluatorRef>,
    },
    IsNull {
        negated: bool,
        expr: ExprRef,
    },
    Unary {
        op: UnaryOperator,
        expr: ExprRef,
        evaluator: Option<UnaryEvaluatorRef>,
        ty: LogicalType,
    },
    Binary {
        op: BinaryOperator,
        left_expr: ExprRef,
        right_expr: ExprRef,
        evaluator: Option<BinaryEvaluatorRef>,
        ty: LogicalType,
    },
    AggCall {
        distinct: bool,
        kind: AggKind,
        args: Vec<ExprRef>,
        ty: LogicalType,
    },
    In {
        negated: bool,
        expr: ExprRef,
        args: Vec<ExprRef>,
    },
    Between {
        negated: bool,
        expr: ExprRef,
        left_expr: ExprRef,
        right_expr: ExprRef,
    },
    SubString {
        expr: ExprRef,
        for_expr: Option<ExprRef>,
        from_expr: Option<ExprRef>,
    },
    Position {
        expr: ExprRef,
        in_expr: ExprRef,
    },
    Trim {
        expr: ExprRef,
        trim_what_expr: Option<ExprRef>,
        trim_where: Option<TrimWhereField>,
    },
    // Temporary expression used for expression substitution
    Empty,
    Tuple(Vec<ExprRef>),
    ScalaFunction(ScalarFunction),
    TableFunction(TableFunction),
    If {
        condition: ExprRef,
        left_expr: ExprRef,
        right_expr: ExprRef,
        ty: LogicalType,
    },
    IfNull {
        left_expr: ExprRef,
        right_expr: ExprRef,
        ty: LogicalType,
    },
    NullIf {
        left_expr: ExprRef,
        right_expr: ExprRef,
        ty: LogicalType,
    },
    Coalesce {
        exprs: Vec<ExprRef>,
        ty: LogicalType,
    },
    CaseWhen {
        operand_expr: Option<ExprRef>,
        expr_pairs: Vec<(ExprRef, ExprRef)>,
        else_expr: Option<ExprRef>,
        ty: LogicalType,
    },
    WindowCall(window::WindowCall),
}

impl From<DataValue> for ScalarExpression {
    fn from(value: DataValue) -> Self {
        ScalarExpression::Constant(value)
    }
}

macro_rules! impl_scalar_expression_from_data_value {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for ScalarExpression {
                fn from(value: $ty) -> Self {
                    ScalarExpression::Constant(DataValue::from(value))
                }
            }
        )+
    };
}

impl_scalar_expression_from_data_value!(
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    String,
    Option<bool>,
    Option<i8>,
    Option<i16>,
    Option<i32>,
    Option<i64>,
    Option<u8>,
    Option<u16>,
    Option<u32>,
    Option<u64>,
    Option<f32>,
    Option<f64>,
    Option<String>,
);
#[cfg(feature = "decimal")]
impl_scalar_expression_from_data_value!(Decimal, Option<Decimal>);

impl From<&str> for ScalarExpression {
    fn from(value: &str) -> Self {
        ScalarExpression::Constant(DataValue::from(value.to_string()))
    }
}

impl From<Option<&str>> for ScalarExpression {
    fn from(value: Option<&str>) -> Self {
        ScalarExpression::Constant(value.map(str::to_string).into())
    }
}

impl From<Arc<str>> for ScalarExpression {
    fn from(value: Arc<str>) -> Self {
        ScalarExpression::Constant(DataValue::from(value.to_string()))
    }
}

impl From<Option<Arc<str>>> for ScalarExpression {
    fn from(value: Option<Arc<str>>) -> Self {
        ScalarExpression::Constant(value.map(|value| value.to_string()).into())
    }
}

#[cfg(feature = "time")]
mod chrono_scalar_expression {
    use super::ScalarExpression;
    use crate::types::value::DataValue;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    impl From<NaiveDate> for ScalarExpression {
        fn from(value: NaiveDate) -> Self {
            ScalarExpression::Constant(DataValue::from(&value))
        }
    }

    impl From<Option<NaiveDate>> for ScalarExpression {
        fn from(value: Option<NaiveDate>) -> Self {
            ScalarExpression::Constant(DataValue::from(value.as_ref()))
        }
    }

    impl From<NaiveDateTime> for ScalarExpression {
        fn from(value: NaiveDateTime) -> Self {
            ScalarExpression::Constant(DataValue::from(&value))
        }
    }

    impl From<Option<NaiveDateTime>> for ScalarExpression {
        fn from(value: Option<NaiveDateTime>) -> Self {
            ScalarExpression::Constant(DataValue::from(value.as_ref()))
        }
    }

    impl From<NaiveTime> for ScalarExpression {
        fn from(value: NaiveTime) -> Self {
            ScalarExpression::Constant(DataValue::from(&value))
        }
    }

    impl From<Option<NaiveTime>> for ScalarExpression {
        fn from(value: Option<NaiveTime>) -> Self {
            ScalarExpression::Constant(DataValue::from(value.as_ref()))
        }
    }
}

pub struct BindEvaluator;

impl ExprVisitorMut for BindEvaluator {
    fn visit_type_cast(
        &mut self,
        expr: &mut ExprRef,
        ty: &mut LogicalType,
        evaluator: &mut Option<CastEvaluatorRef>,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        let from = expr.return_type(arena);
        *evaluator = if from.as_ref() == ty {
            None
        } else {
            Some(cast_create(from.as_ref(), ty)?)
        };

        Ok(())
    }

    fn visit_unary(
        &mut self,
        op: &'_ mut UnaryOperator,
        expr: &mut ExprRef,
        evaluator: &mut Option<UnaryEvaluatorRef>,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;

        let ty = expr.return_type(arena);
        if ty.is_unsigned_numeric() {
            let target_ty = match ty.as_ref() {
                LogicalType::UTinyint => LogicalType::Tinyint,
                LogicalType::USmallint => LogicalType::Smallint,
                LogicalType::UInteger => LogicalType::Integer,
                LogicalType::UBigint => LogicalType::Bigint,
                _ => unreachable!(),
            };
            *expr = (*expr).type_cast(Cow::Owned(target_ty), arena)?;
        }
        *evaluator = Some(unary_create(expr.return_type(arena), *op)?);

        Ok(())
    }

    fn visit_binary(
        &mut self,
        op: &'_ mut BinaryOperator,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        evaluator: &mut Option<BinaryEvaluatorRef>,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr, arena)?;
        self.visit(right_expr, arena)?;

        let left_ty = left_expr.return_type(arena).into_owned();
        let right_ty = right_expr.return_type(arena).into_owned();
        let ty = LogicalType::max_logical_type(&left_ty, &right_ty)?.into_owned();
        *left_expr = left_expr.type_cast(Cow::Borrowed(&ty), arena)?;
        *right_expr = right_expr.type_cast(Cow::Borrowed(&ty), arena)?;

        *evaluator = Some(binary_create(Cow::Owned(ty), *op)?);

        Ok(())
    }
}

#[derive(Default)]
pub struct HasCountStar {
    pub value: bool,
}

impl ExprVisitor<PlanArena<'_>> for HasCountStar {
    fn visit_agg(
        &mut self,
        _distinct: bool,
        _kind: &AggKind,
        args: &[ExprRef],
        _ty: &LogicalType,
        arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if args.len() == 1 {
            if let ScalarExpression::Constant(value) = arena.expression(args[0]) {
                self.value = matches!(value.utf8(), Some("*"));
            }
        }
        Ok(())
    }

    fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
        if !self.value {
            walk_expr(self, expr, arena)?;
        }
        Ok(())
    }
}

pub trait TypeCast: Sized {
    fn return_type<'a>(&'a self, arena: &'a PlanArena<'_>) -> Cow<'a, LogicalType>;

    fn into_expr(
        self,
        ty: LogicalType,
        evaluator: CastEvaluatorRef,
        arena: &mut PlanArena<'_>,
    ) -> Self;

    fn type_cast(
        self,
        ty: Cow<'_, LogicalType>,
        arena: &mut PlanArena<'_>,
    ) -> Result<Self, DatabaseError> {
        let from = self.return_type(arena);
        if from.as_ref() == ty.as_ref() {
            return Ok(self);
        }
        let evaluator = cast_create(from.as_ref(), ty.as_ref())?;
        Ok(self.into_expr(ty.into_owned(), evaluator, arena))
    }
}

impl TypeCast for ScalarExpression {
    fn return_type<'a>(&'a self, arena: &'a PlanArena<'_>) -> Cow<'a, LogicalType> {
        match self {
            ScalarExpression::Constant(value) => Cow::Owned(value.logical_type()),
            ScalarExpression::ColumnRef { column, .. } => {
                Cow::Borrowed(arena.column(*column).datatype())
            }
            ScalarExpression::Binary { ty, .. }
            | ScalarExpression::Unary { ty, .. }
            | ScalarExpression::TypeCast { ty, .. }
            | ScalarExpression::AggCall { ty, .. }
            | ScalarExpression::If { ty, .. }
            | ScalarExpression::IfNull { ty, .. }
            | ScalarExpression::NullIf { ty, .. }
            | ScalarExpression::Coalesce { ty, .. }
            | ScalarExpression::CaseWhen { ty, .. }
            | ScalarExpression::WindowCall(window::WindowCall {
                function: window::WindowFunction { ty, .. },
                ..
            }) => Cow::Borrowed(ty),
            ScalarExpression::IsNull { .. }
            | ScalarExpression::In { .. }
            | ScalarExpression::Between { .. } => Cow::Owned(LogicalType::Boolean),
            ScalarExpression::SubString { .. } | ScalarExpression::Trim { .. } => {
                Cow::Owned(LogicalType::Varchar(None, CharLengthUnits::Characters))
            }
            ScalarExpression::Position { .. } => Cow::Owned(LogicalType::Integer),
            ScalarExpression::Alias { expr, .. } => expr.return_type(arena),
            ScalarExpression::Empty | ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::Tuple(exprs) => Cow::Owned(LogicalType::Tuple(
                exprs
                    .iter()
                    .map(|expr| expr.return_type(arena).into_owned())
                    .collect(),
            )),
            ScalarExpression::ScalaFunction(ScalarFunction { inner, .. }) => {
                Cow::Borrowed(inner.return_type())
            }
        }
    }

    fn into_expr(
        self,
        ty: LogicalType,
        evaluator: CastEvaluatorRef,
        arena: &mut PlanArena<'_>,
    ) -> Self {
        ScalarExpression::TypeCast {
            expr: arena.alloc_expression(self),
            ty,
            evaluator: Some(evaluator),
        }
    }
}

impl TypeCast for ExprRef {
    fn return_type<'a>(&'a self, arena: &'a PlanArena<'_>) -> Cow<'a, LogicalType> {
        arena.expression(*self).return_type(arena)
    }

    fn into_expr(
        self,
        ty: LogicalType,
        evaluator: CastEvaluatorRef,
        arena: &mut PlanArena<'_>,
    ) -> Self {
        arena.alloc_expression(ScalarExpression::TypeCast {
            expr: self,
            ty,
            evaluator: Some(evaluator),
        })
    }
}

impl ScalarExpression {
    pub fn column_expr(column: ColumnRef, position: usize) -> ScalarExpression {
        ScalarExpression::ColumnRef { column, position }
    }
}

impl Explain for ExprRef {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_exprs(
            exprs: &[ExprRef],
            arena: &PlanArena<'_>,
            f: &mut fmt::Formatter<'_>,
        ) -> fmt::Result {
            for (index, expr) in exprs.iter().enumerate() {
                if index > 0 {
                    f.write_str(", ")?;
                }
                write!(f, "{}", expr.explain(arena))?;
            }
            Ok(())
        }

        match arena.expression(*self) {
            ScalarExpression::Constant(value) => write!(f, "{value}"),
            ScalarExpression::ColumnRef { column, .. } => Explain::fmt(column, arena, f),
            ScalarExpression::Alias { alias, expr } => match alias {
                AliasType::Name(alias) => f.write_str(alias),
                AliasType::Expr(alias_expr) => write!(
                    f,
                    "({}) as ({})",
                    expr.explain(arena),
                    alias_expr.explain(arena)
                ),
            },
            ScalarExpression::TypeCast { expr, ty, .. } => {
                write!(f, "cast ({} as {ty})", expr.explain(arena))
            }
            ScalarExpression::IsNull { expr, negated } => write!(
                f,
                "{} {}",
                expr.explain(arena),
                if *negated { "is not null" } else { "is null" }
            ),
            ScalarExpression::Unary { expr, op, .. } => {
                write!(f, "{}{}", op, expr.explain(arena))
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => write!(
                f,
                "({} {op} {})",
                left_expr.explain(arena),
                right_expr.explain(arena)
            ),
            ScalarExpression::AggCall {
                args,
                kind,
                distinct,
                ..
            } => {
                write!(f, "{kind:?}(")?;
                if kind.allow_distinct() && *distinct {
                    f.write_str("distinct ")?;
                }
                write_exprs(args, arena, f)?;
                f.write_str(")")
            }
            ScalarExpression::WindowCall(window) => {
                write!(f, "{}(", window.function.kind.name())?;
                write_exprs(&window.function.args, arena, f)?;
                f.write_str(") over (")?;
                let mut has_spec = false;
                if !window.spec.partition_by.is_empty() {
                    f.write_str("partition by ")?;
                    write_exprs(&window.spec.partition_by, arena, f)?;
                    has_spec = true;
                }
                if !window.spec.order_by.is_empty() {
                    if has_spec {
                        f.write_str(" ")?;
                    }
                    f.write_str("order by ")?;
                    for (index, field) in window.spec.order_by.iter().enumerate() {
                        if index > 0 {
                            f.write_str(", ")?;
                        }
                        write!(f, "{}", field.explain(arena))?;
                    }
                }
                f.write_str(")")
            }
            ScalarExpression::In {
                args,
                negated,
                expr,
            } => {
                write!(
                    f,
                    "{} {} (",
                    expr.explain(arena),
                    if *negated { "not in" } else { "in" }
                )?;
                write_exprs(args, arena, f)?;
                f.write_str(")")
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                negated,
            } => write!(
                f,
                "{} {} [{}, {}]",
                expr.explain(arena),
                if *negated { "not between" } else { "between" },
                left_expr.explain(arena),
                right_expr.explain(arena)
            ),
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                write!(f, "substring({}", expr.explain(arena))?;
                if let Some(from_expr) = from_expr {
                    write!(f, ", from: {}", from_expr.explain(arena))?;
                }
                if let Some(for_expr) = for_expr {
                    write!(f, ", for: {}", for_expr.explain(arena))?;
                }
                f.write_str(")")
            }
            ScalarExpression::Position { expr, in_expr } => write!(
                f,
                "position({} in {})",
                expr.explain(arena),
                in_expr.explain(arena)
            ),
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                let trim_what = trim_what_expr
                    .as_ref()
                    .map(|expr| expr.explain(arena).to_string())
                    .unwrap_or_else(|| " ".to_string());

                f.write_str("trim(")?;
                match trim_where {
                    Some(TrimWhereField::Both) => write!(f, "both '{trim_what}' from")?,
                    Some(TrimWhereField::Leading) => write!(f, "leading '{trim_what}' from")?,
                    Some(TrimWhereField::Trailing) => write!(f, "trailing '{trim_what}' from")?,
                    None if !trim_what.is_empty() => write!(f, "'{trim_what}' from")?,
                    None => {}
                }
                write!(f, " {})", expr.explain(arena))
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Tuple(args) => {
                f.write_str("(")?;
                write_exprs(args, arena, f)?;
                f.write_str(")")
            }
            ScalarExpression::ScalaFunction(ScalarFunction { args, inner }) => {
                write!(f, "{}(", inner.summary().name)?;
                write_exprs(args, arena, f)?;
                f.write_str(")")
            }
            ScalarExpression::TableFunction(TableFunction { args, catalog }) => {
                write!(f, "{}(", catalog.inner.summary().name)?;
                write_exprs(args, arena, f)?;
                f.write_str(")")
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => write!(
                f,
                "if {} ({}, {})",
                condition.explain(arena),
                left_expr.explain(arena),
                right_expr.explain(arena)
            ),
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => write!(
                f,
                "ifnull({}, {})",
                left_expr.explain(arena),
                right_expr.explain(arena)
            ),
            ScalarExpression::Coalesce { exprs, .. } => {
                f.write_str("coalesce(")?;
                write_exprs(exprs, arena, f)?;
                f.write_str(")")
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                f.write_str("case ")?;
                if let Some(operand_expr) = operand_expr {
                    write!(f, "{} ", operand_expr.explain(arena))?;
                }
                for (index, (when_expr, then_expr)) in expr_pairs.iter().enumerate() {
                    if index > 0 {
                        f.write_str(" ")?;
                    }
                    write!(
                        f,
                        "when {} then {}",
                        when_expr.explain(arena),
                        then_expr.explain(arena)
                    )?;
                }
                f.write_str(" ")?;
                if let Some(else_expr) = else_expr {
                    write!(f, "else {} ", else_expr.explain(arena))?;
                }
                f.write_str("end")
            }
        }
    }
}

impl ExprRef {
    pub fn asc(self) -> SortField {
        SortField::from(self).asc()
    }

    pub fn desc(self) -> SortField {
        SortField::from(self).desc()
    }

    pub fn nulls_first(self) -> SortField {
        SortField::from(self).nulls_first()
    }

    pub fn nulls_last(self) -> SortField {
        SortField::from(self).nulls_last()
    }

    pub(crate) fn eq_ignore_colref_pos(self, other: ExprRef, arena: &PlanArena) -> bool {
        eq_col::eq_ignore_colref_pos(self, other, arena)
    }

    pub(crate) fn clone_expression(
        self,
        arena: &mut PlanArena<'_>,
    ) -> Result<ExprRef, DatabaseError> {
        let mut cloned = self;
        crate::expression::visitor_mut::ExprCloner.visit(&mut cloned, arena)?;
        Ok(cloned)
    }

    pub fn unpack_alias(self, arena: &impl MetaArena) -> ExprRef {
        if let ScalarExpression::Alias {
            alias: AliasType::Expr(expr),
            ..
        } = arena.expression(self)
        {
            expr.unpack_alias(arena)
        } else if let ScalarExpression::Alias { expr, .. } = arena.expression(self) {
            expr.unpack_alias(arena)
        } else {
            self
        }
    }

    pub fn unpack_alias_ref<'a, A: MetaArena>(self, arena: &'a A) -> &'a ScalarExpression {
        arena.expression(self.unpack_alias(arena))
    }

    pub fn any_referenced_column(
        self,
        arena: &PlanArena,
        mut predicate: impl FnMut(&PlanArena, &ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        struct ColumnRefVisitor<'a, 'arena, F> {
            f: &'a mut F,
            any: bool,
            arena: &'a PlanArena<'arena>,
        }

        impl<F: FnMut(&PlanArena, &ColumnRef) -> bool> ExprVisitor<PlanArena<'_>>
            for ColumnRefVisitor<'_, '_, F>
        {
            fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
                if !self.any {
                    walk_expr(self, expr, arena)?;
                }
                Ok(())
            }

            fn visit_column_ref(&mut self, col: &ColumnRef) -> Result<(), DatabaseError> {
                self.any = (self.f)(self.arena, col);
                Ok(())
            }
        }

        let mut visitor = ColumnRefVisitor {
            f: &mut predicate,
            any: false,
            arena,
        };
        visitor.visit(self, arena)?;
        Ok(visitor.any)
    }

    pub fn all_referenced_columns(
        self,
        arena: &PlanArena,
        mut predicate: impl FnMut(&PlanArena, &ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        struct ColumnRefVisitor<'a, 'arena, F> {
            f: &'a mut F,
            all: bool,
            arena: &'a PlanArena<'arena>,
        }

        impl<F: FnMut(&PlanArena, &ColumnRef) -> bool> ExprVisitor<PlanArena<'_>>
            for ColumnRefVisitor<'_, '_, F>
        {
            fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
                if self.all {
                    walk_expr(self, expr, arena)?;
                }
                Ok(())
            }

            fn visit_column_ref(&mut self, col: &ColumnRef) -> Result<(), DatabaseError> {
                self.all = (self.f)(self.arena, col);
                Ok(())
            }
        }

        let mut visitor = ColumnRefVisitor {
            f: &mut predicate,
            all: true,
            arena,
        };
        visitor.visit(self, arena)?;
        Ok(visitor.all)
    }

    pub fn has_agg_call(self, arena: &PlanArena<'_>) -> Result<bool, DatabaseError> {
        struct AggCallChecker {
            has_agg: bool,
        }
        impl ExprVisitor<PlanArena<'_>> for AggCallChecker {
            fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
                if self.has_agg {
                    return Ok(());
                }
                walk_expr(self, expr, arena)
            }
            fn visit_agg(
                &mut self,
                _distinct: bool,
                _kind: &AggKind,
                args: &[ExprRef],
                _ty: &LogicalType,
                arena: &PlanArena<'_>,
            ) -> Result<(), DatabaseError> {
                for arg in args {
                    self.visit(*arg, arena)?;
                }
                self.has_agg = true;
                Ok(())
            }
        }
        let mut checker = AggCallChecker { has_agg: false };
        checker.visit(self, arena)?;
        Ok(checker.has_agg)
    }

    pub fn has_window_call(self, arena: &PlanArena<'_>) -> Result<bool, DatabaseError> {
        struct WindowCallChecker(bool);

        impl ExprVisitor<PlanArena<'_>> for WindowCallChecker {
            fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
                if !self.0 {
                    walk_expr(self, expr, arena)?;
                }
                Ok(())
            }

            fn visit_window(
                &mut self,
                _window: &window::WindowCall,
                _arena: &PlanArena<'_>,
            ) -> Result<(), DatabaseError> {
                self.0 = true;
                Ok(())
            }
        }

        let mut checker = WindowCallChecker(false);
        checker.visit(self, arena)?;
        Ok(checker.0)
    }

    pub fn output_name(self, arena: &PlanArena) -> String {
        self.explain(arena).to_string()
    }

    pub fn output_column_ref(self, arena: &mut PlanArena) -> ColumnRef {
        match arena.expression(self) {
            ScalarExpression::ColumnRef { column, .. } => *column,
            ScalarExpression::Alias {
                alias: AliasType::Expr(expr),
                ..
            } => expr.output_column_ref(arena),
            _ => {
                let output_name = self.output_name(arena);
                let return_type = self.return_type(arena).into_owned();
                let column = ColumnCatalog::new(
                    output_name,
                    true,
                    // SAFETY: default expr must not be [`ScalarExpression::ColumnRef`]
                    ColumnDesc::new(return_type, None, false, None).unwrap(),
                );
                arena.alloc_column(column)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,

    Modulo,
    StringConcat,

    Gt,
    Lt,
    GtEq,
    LtEq,
    Spaceship,
    Eq,
    NotEq,
    Like(Option<char>),
    NotLike(Option<char>),

    And,
    Or,
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let like_op = |f: &mut Formatter, escape_char: &Option<char>| {
            if let Some(escape_char) = escape_char {
                write!(f, "(escape: {escape_char})")?;
            }
            Ok(())
        };

        match self {
            BinaryOperator::Plus => write!(f, "+"),
            BinaryOperator::Minus => write!(f, "-"),
            BinaryOperator::Multiply => write!(f, "*"),
            BinaryOperator::Divide => write!(f, "/"),
            BinaryOperator::Modulo => write!(f, "mod"),
            BinaryOperator::StringConcat => write!(f, "&"),
            BinaryOperator::Gt => write!(f, ">"),
            BinaryOperator::Lt => write!(f, "<"),
            BinaryOperator::GtEq => write!(f, ">="),
            BinaryOperator::LtEq => write!(f, "<="),
            BinaryOperator::Spaceship => write!(f, "<=>"),
            BinaryOperator::Eq => write!(f, "="),
            BinaryOperator::NotEq => write!(f, "!="),
            BinaryOperator::And => write!(f, "&&"),
            BinaryOperator::Or => write!(f, "||"),
            BinaryOperator::Like(escape_char) => {
                write!(f, "like")?;
                like_op(f, escape_char)
            }
            BinaryOperator::NotLike(escape_char) => {
                write!(f, "not like")?;
                like_op(f, escape_char)
            }
        }
    }
}

impl fmt::Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            UnaryOperator::Plus => write!(f, "+"),
            UnaryOperator::Minus => write!(f, "-"),
            UnaryOperator::Not => write!(f, "!"),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::test::build_table;
    use crate::db::{ScalaFunctions, TableFunctions};
    use crate::errors::DatabaseError;
    use crate::expression::agg::AggKind;
    use crate::expression::function::scala::{
        ArcScalarFunctionImpl, ScalarFunction, ScalarFunctionImpl,
    };
    use crate::expression::function::table::{
        ArcTableFunctionImpl, TableFunction, TableFunctionCatalog, TableFunctionImpl,
    };
    use crate::expression::window::{WindowCall, WindowFunction, WindowFunctionKind, WindowSpec};
    use crate::expression::TrimWhereField;
    use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::function::current_date::CurrentDate;
    use crate::function::numbers::Numbers;
    use crate::planner::{ExprRef, PlanArena, TableArenaCell};
    use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::{Storage, Transaction};
    use crate::types::evaluator::{binary_create, cast_create, unary_create};
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};
    use tempfile::TempDir;

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            expr: ScalarExpression,
            drive: Option<&ReferenceDecodeContext<'_, RocksTransaction>>,
            reference_tables: &mut ReferenceTables,
            arena: &mut PlanArena,
        ) -> Result<(), DatabaseError> {
            let expr = arena.alloc_expression(expr);
            expr.encode(cursor, false, reference_tables, arena)?;

            cursor.seek(SeekFrom::Start(0))?;
            let decoded = ExprRef::decode(cursor, drive, reference_tables, arena)?;
            assert!(
                decoded.eq_ignore_colref_pos(expr, arena),
                "decoded expression does not match: decoded={decoded:?}, expected={expr:?}",
            );
            cursor.seek(SeekFrom::Start(0))?;

            Ok(())
        }

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut scala_functions = ScalaFunctions::default();
        let current_date = CurrentDate::new();
        scala_functions.insert(current_date.summary().clone(), current_date);
        let mut table_functions = TableFunctions::default();
        let numbers = Numbers::new();
        let mut schema = Vec::new();
        numbers.output_schema_into(table_arena.borrow_mut(), &mut schema);
        table_functions.insert(
            numbers.summary().clone(),
            TableFunctionCatalog {
                schema,
                inner: ArcTableFunctionImpl(numbers),
            },
        );
        let mut plan_arena = PlanArena::new(&table_arena);
        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let mut plan_arena = PlanArena::new(&table_arena);

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let c3_column = {
            let table = transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            table.get_column_by_name("c3").unwrap()
        };
        let context = ReferenceDecodeContext::with_functions(
            Some((&transaction, &table_cache)),
            &scala_functions,
            &table_functions,
        );
        let empty = plan_arena.alloc_expression(ScalarExpression::Empty);

        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(DataValue::Null),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(DataValue::Int32(42)),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(DataValue::Utf8 {
                value: "hello".to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::column_expr(c3_column, 0),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::column_expr(
                plan_arena.alloc_column(ColumnCatalog::new(
                    "c4".to_string(),
                    false,
                    ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
                )),
                1,
            ),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: empty,
                alias: AliasType::Name("Hello".to_string()),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: empty,
                alias: AliasType::Expr(empty),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TypeCast {
                expr: empty,
                ty: LogicalType::Integer,
                evaluator: Some(cast_create(&LogicalType::Integer, &LogicalType::Integer)?),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IsNull {
                negated: true,
                expr: empty,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: empty,
                evaluator: Some(unary_create(
                    Cow::Owned(LogicalType::Boolean),
                    UnaryOperator::Not,
                )?),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: empty,
                evaluator: None,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: empty,
                right_expr: empty,
                evaluator: Some(
                    binary_create(Cow::Owned(LogicalType::Integer), BinaryOperator::Plus).unwrap(),
                ),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: empty,
                right_expr: empty,
                evaluator: None,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::AggCall {
                distinct: true,
                kind: AggKind::Avg,
                args: vec![empty],
                ty: LogicalType::Double,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::In {
                negated: true,
                expr: empty,
                args: vec![empty],
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Between {
                negated: true,
                expr: empty,
                left_expr: empty,
                right_expr: empty,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: empty,
                for_expr: Some(empty),
                from_expr: Some(empty),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: empty,
                for_expr: None,
                from_expr: Some(empty),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: empty,
                for_expr: None,
                from_expr: None,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Position {
                expr: empty,
                in_expr: empty,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: empty,
                trim_what_expr: Some(empty),
                trim_where: Some(TrimWhereField::Both),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: empty,
                trim_what_expr: None,
                trim_where: Some(TrimWhereField::Both),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: empty,
                trim_what_expr: None,
                trim_where: None,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Empty,
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Tuple(vec![empty]),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![empty],
                inner: ArcScalarFunctionImpl(CurrentDate::new()),
            }),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TableFunction(TableFunction {
                args: vec![empty],
                catalog: TableFunctionCatalog {
                    schema: Vec::new(),
                    inner: ArcTableFunctionImpl(Numbers::new()),
                },
            }),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::If {
                condition: empty,
                left_expr: empty,
                right_expr: empty,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IfNull {
                left_expr: empty,
                right_expr: empty,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::NullIf {
                left_expr: empty,
                right_expr: empty,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Coalesce {
                exprs: vec![empty],
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: Some(empty),
                expr_pairs: vec![(empty, empty)],
                else_expr: Some(empty),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        let one = plan_arena.alloc_expression(ScalarExpression::Constant(1.into()));
        let two = plan_arena.alloc_expression(ScalarExpression::Constant(2.into()));
        let three = plan_arena.alloc_expression(ScalarExpression::Constant(3.into()));
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(empty, empty)],
                else_expr: Some(empty),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(empty, empty)],
                else_expr: None,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::WindowCall(WindowCall {
                function: WindowFunction {
                    kind: WindowFunctionKind::Aggregate(AggKind::Sum),
                    args: vec![one],
                    ty: LogicalType::Integer,
                },
                spec: WindowSpec {
                    partition_by: vec![two],
                    order_by: vec![crate::planner::operator::sort::SortField::from(three).desc()],
                },
            }),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;

        Ok(())
    }
}
