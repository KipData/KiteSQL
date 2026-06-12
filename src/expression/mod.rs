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
use crate::expression::visitor::{walk_expr, Visitor};
use crate::expression::visitor_mut::VisitorMut;
use crate::planner::{MetaArena, PlanArena};
use crate::types::evaluator::{
    binary_create, cast_create, unary_create, BinaryEvaluatorRef, CastEvaluatorRef,
    UnaryEvaluatorRef,
};
use crate::types::value::DataValue;
use crate::types::{CharLengthUnits, LogicalType};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;
use std::{fmt, mem};

pub mod agg;
mod evaluator;
pub mod function;
pub mod range_detacher;
pub mod simplify;
pub mod visitor;
pub mod visitor_mut;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrimWhereField {
    Both,
    Leading,
    Trailing,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum AliasType {
    Name(String),
    Expr(Box<ScalarExpression>),
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
        expr: Box<ScalarExpression>,
        alias: AliasType,
    },
    TypeCast {
        expr: Box<ScalarExpression>,
        ty: LogicalType,
        evaluator: Option<CastEvaluatorRef>,
    },
    IsNull {
        negated: bool,
        expr: Box<ScalarExpression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ScalarExpression>,
        evaluator: Option<UnaryEvaluatorRef>,
        ty: LogicalType,
    },
    Binary {
        op: BinaryOperator,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        evaluator: Option<BinaryEvaluatorRef>,
        ty: LogicalType,
    },
    AggCall {
        distinct: bool,
        kind: AggKind,
        args: Vec<ScalarExpression>,
        ty: LogicalType,
    },
    In {
        negated: bool,
        expr: Box<ScalarExpression>,
        args: Vec<ScalarExpression>,
    },
    Between {
        negated: bool,
        expr: Box<ScalarExpression>,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
    },
    SubString {
        expr: Box<ScalarExpression>,
        for_expr: Option<Box<ScalarExpression>>,
        from_expr: Option<Box<ScalarExpression>>,
    },
    Position {
        expr: Box<ScalarExpression>,
        in_expr: Box<ScalarExpression>,
    },
    Trim {
        expr: Box<ScalarExpression>,
        trim_what_expr: Option<Box<ScalarExpression>>,
        trim_where: Option<TrimWhereField>,
    },
    // Temporary expression used for expression substitution
    Empty,
    Tuple(Vec<ScalarExpression>),
    ScalaFunction(ScalarFunction),
    TableFunction(TableFunction),
    If {
        condition: Box<ScalarExpression>,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    IfNull {
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    NullIf {
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    Coalesce {
        exprs: Vec<ScalarExpression>,
        ty: LogicalType,
    },
    CaseWhen {
        operand_expr: Option<Box<ScalarExpression>>,
        expr_pairs: Vec<(ScalarExpression, ScalarExpression)>,
        else_expr: Option<Box<ScalarExpression>>,
        ty: LogicalType,
    },
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

pub struct BindEvaluator<'a, 'p> {
    pub(crate) arena: &'a PlanArena<'p>,
}

impl VisitorMut<'_> for BindEvaluator<'_, '_> {
    fn visit_type_cast(
        &mut self,
        expr: &'_ mut ScalarExpression,
        ty: &'_ mut LogicalType,
        evaluator: &'_ mut Option<CastEvaluatorRef>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;
        let from = expr.return_type(self.arena);
        *evaluator = if from.as_ref() == ty {
            None
        } else {
            Some(cast_create(from, Cow::Borrowed(ty))?)
        };

        Ok(())
    }

    fn visit_unary(
        &mut self,
        op: &'_ mut UnaryOperator,
        expr: &'_ mut ScalarExpression,
        evaluator: &'_ mut Option<UnaryEvaluatorRef>,
        _ty: &'_ mut LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;

        let ty = expr.return_type(self.arena);
        if ty.is_unsigned_numeric() {
            let target_ty = match ty.as_ref() {
                LogicalType::UTinyint => LogicalType::Tinyint,
                LogicalType::USmallint => LogicalType::Smallint,
                LogicalType::UInteger => LogicalType::Integer,
                LogicalType::UBigint => LogicalType::Bigint,
                _ => unreachable!(),
            };
            *expr = ScalarExpression::type_cast(
                mem::replace(expr, ScalarExpression::Empty),
                Cow::Owned(target_ty),
                self.arena,
            )?;
        }
        *evaluator = Some(unary_create(expr.return_type(self.arena), *op)?);

        Ok(())
    }

    fn visit_binary(
        &mut self,
        op: &'_ mut BinaryOperator,
        left_expr: &'_ mut ScalarExpression,
        right_expr: &'_ mut ScalarExpression,
        evaluator: &'_ mut Option<BinaryEvaluatorRef>,
        _ty: &'_ mut LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr)?;
        self.visit(right_expr)?;

        let left_ty = left_expr.return_type(self.arena).into_owned();
        let right_ty = right_expr.return_type(self.arena).into_owned();
        let ty = LogicalType::max_logical_type(&left_ty, &right_ty)?;
        let fn_cast =
            |expr: &mut ScalarExpression, ty: &LogicalType| -> Result<(), DatabaseError> {
                *expr = ScalarExpression::type_cast(
                    mem::replace(expr, ScalarExpression::Empty),
                    Cow::Borrowed(ty),
                    self.arena,
                )?;
                Ok(())
            };
        fn_cast(left_expr, ty.as_ref())?;
        fn_cast(right_expr, ty.as_ref())?;

        *evaluator = Some(binary_create(ty, *op)?);

        Ok(())
    }
}

#[derive(Default)]
pub struct HasCountStar {
    pub value: bool,
}

impl Visitor<'_> for HasCountStar {
    fn visit_agg(
        &mut self,
        _distinct: bool,
        _kind: &'_ AggKind,
        args: &'_ [ScalarExpression],
        _ty: &'_ LogicalType,
    ) -> Result<(), DatabaseError> {
        if args.len() == 1 {
            if let ScalarExpression::Constant(value) = &args[0] {
                self.value = matches!(value.utf8(), Some("*"));
            }
        }
        Ok(())
    }

    fn visit(&mut self, expr: &'_ ScalarExpression) -> Result<(), DatabaseError> {
        if !self.value {
            walk_expr(self, expr)?;
        }
        Ok(())
    }
}

impl ScalarExpression {
    pub fn column_expr(column: ColumnRef, position: usize) -> ScalarExpression {
        ScalarExpression::ColumnRef { column, position }
    }

    pub fn type_cast(
        expr: ScalarExpression,
        ty: Cow<'_, LogicalType>,
        arena: &PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let from = expr.return_type(arena);
        if from.as_ref() == ty.as_ref() {
            return Ok(expr);
        }
        let evaluator = Some(cast_create(from, ty.clone())?);

        Ok(ScalarExpression::TypeCast {
            expr: Box::new(expr),
            ty: ty.into_owned(),
            evaluator,
        })
    }

    pub(crate) fn eq_ignore_colref_pos(&self, other: &ScalarExpression, arena: &PlanArena) -> bool {
        match (self.unpack_alias_ref(), other.unpack_alias_ref()) {
            (
                ScalarExpression::ColumnRef {
                    column: lhs_column, ..
                },
                ScalarExpression::ColumnRef {
                    column: rhs_column, ..
                },
            ) => arena.same_column(*lhs_column, *rhs_column),
            (lhs, rhs) => lhs == rhs,
        }
    }

    pub fn unpack_alias(self) -> ScalarExpression {
        if let ScalarExpression::Alias {
            alias: AliasType::Expr(expr),
            ..
        } = self
        {
            expr.unpack_alias()
        } else if let ScalarExpression::Alias { expr, .. } = self {
            expr.unpack_alias()
        } else {
            self
        }
    }

    pub fn unpack_alias_ref(&self) -> &ScalarExpression {
        if let ScalarExpression::Alias {
            alias: AliasType::Expr(expr),
            ..
        } = self
        {
            expr.unpack_alias_ref()
        } else if let ScalarExpression::Alias { expr, .. } = self {
            expr.unpack_alias_ref()
        } else {
            self
        }
    }

    pub fn return_type<'a>(&'a self, arena: &'a PlanArena<'_>) -> Cow<'a, LogicalType> {
        match self {
            ScalarExpression::Constant(v) => Cow::Owned(v.logical_type()),
            ScalarExpression::ColumnRef { column, .. } => {
                Cow::Borrowed(arena.column(*column).datatype())
            }
            ScalarExpression::Binary {
                ty: return_type, ..
            }
            | ScalarExpression::Unary {
                ty: return_type, ..
            }
            | ScalarExpression::TypeCast {
                ty: return_type, ..
            }
            | ScalarExpression::AggCall {
                ty: return_type, ..
            }
            | ScalarExpression::If {
                ty: return_type, ..
            }
            | ScalarExpression::IfNull {
                ty: return_type, ..
            }
            | ScalarExpression::NullIf {
                ty: return_type, ..
            }
            | ScalarExpression::Coalesce {
                ty: return_type, ..
            }
            | ScalarExpression::CaseWhen {
                ty: return_type, ..
            } => Cow::Borrowed(return_type),
            ScalarExpression::IsNull { .. }
            | ScalarExpression::In { .. }
            | ScalarExpression::Between { .. } => Cow::Owned(LogicalType::Boolean),
            ScalarExpression::SubString { .. } => {
                Cow::Owned(LogicalType::Varchar(None, CharLengthUnits::Characters))
            }
            ScalarExpression::Position { .. } => Cow::Owned(LogicalType::Integer),
            ScalarExpression::Trim { .. } => {
                Cow::Owned(LogicalType::Varchar(None, CharLengthUnits::Characters))
            }
            ScalarExpression::Alias { expr, .. } => expr.return_type(arena),
            ScalarExpression::Empty | ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::Tuple(exprs) => {
                let types = exprs
                    .iter()
                    .map(|expr| expr.return_type(arena).into_owned())
                    .collect_vec();

                Cow::Owned(LogicalType::Tuple(types))
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, .. }) => {
                Cow::Borrowed(inner.return_type())
            }
        }
    }

    pub fn visit_referenced_columns<A: MetaArena>(
        &self,
        arena: &mut A,
        f: &mut impl FnMut(&mut A, &ColumnRef) -> bool,
    ) -> bool {
        struct ColumnRefVisitor<'a, A, F> {
            f: &'a mut F,
            keep_going: bool,
            arena: &'a mut A,
        }

        impl<A, F> Visitor<'_> for ColumnRefVisitor<'_, A, F>
        where
            A: MetaArena,
            F: FnMut(&mut A, &ColumnRef) -> bool,
        {
            fn visit(&mut self, expr: &ScalarExpression) -> Result<(), DatabaseError> {
                if self.keep_going {
                    walk_expr(self, expr)?;
                }
                Ok(())
            }

            fn visit_column_ref(&mut self, col: &ColumnRef) -> Result<(), DatabaseError> {
                self.keep_going = (self.f)(self.arena, col);
                Ok(())
            }
        }

        let mut visitor = ColumnRefVisitor {
            f,
            keep_going: true,
            arena,
        };
        visitor.visit(self).unwrap();
        visitor.keep_going
    }

    pub fn any_referenced_column(
        &self,
        arena: &PlanArena,
        mut predicate: impl FnMut(&PlanArena, &ColumnRef) -> bool,
    ) -> bool {
        struct ColumnRefVisitor<'a, 'p, F> {
            f: &'a mut F,
            any: bool,
            arena: &'a PlanArena<'p>,
        }

        impl<F: FnMut(&PlanArena, &ColumnRef) -> bool> Visitor<'_> for ColumnRefVisitor<'_, '_, F> {
            fn visit(&mut self, expr: &ScalarExpression) -> Result<(), DatabaseError> {
                if !self.any {
                    walk_expr(self, expr)?;
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
        visitor.visit(self).unwrap();
        visitor.any
    }

    pub fn all_referenced_columns(
        &self,
        arena: &PlanArena,
        mut predicate: impl FnMut(&PlanArena, &ColumnRef) -> bool,
    ) -> bool {
        struct ColumnRefVisitor<'a, 'p, F> {
            f: &'a mut F,
            all: bool,
            arena: &'a PlanArena<'p>,
        }

        impl<F: FnMut(&PlanArena, &ColumnRef) -> bool> Visitor<'_> for ColumnRefVisitor<'_, '_, F> {
            fn visit(&mut self, expr: &ScalarExpression) -> Result<(), DatabaseError> {
                if self.all {
                    walk_expr(self, expr)?;
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
        visitor.visit(self).unwrap();
        visitor.all
    }

    pub fn has_table_ref_column(&self, arena: &PlanArena) -> bool {
        struct TableRefChecker<'arena, 'table> {
            found: bool,
            arena: &'arena PlanArena<'table>,
        }
        impl Visitor<'_> for TableRefChecker<'_, '_> {
            fn visit_column_ref(&mut self, col: &ColumnRef) -> Result<(), DatabaseError> {
                let col = self.arena.column(*col);
                if col.table_name().is_some() && col.id().is_some() {
                    self.found = true;
                }
                Ok(())
            }
        }
        let mut checker = TableRefChecker {
            found: false,
            arena,
        };
        checker.visit(self).unwrap();
        checker.found
    }

    pub fn has_agg_call(&self) -> bool {
        struct AggCallChecker {
            has_agg: bool,
        }
        impl<'a> Visitor<'a> for AggCallChecker {
            fn visit(&mut self, expr: &'a ScalarExpression) -> Result<(), DatabaseError> {
                if self.has_agg {
                    return Ok(());
                }
                walk_expr(self, expr)
            }
            fn visit_agg(
                &mut self,
                _distinct: bool,
                _kind: &'a AggKind,
                args: &'a [ScalarExpression],
                _ty: &'a LogicalType,
            ) -> Result<(), DatabaseError> {
                for arg in args {
                    self.visit(arg)?;
                }
                self.has_agg = true;
                Ok(())
            }
        }
        let mut checker = AggCallChecker { has_agg: false };
        checker.visit(self).unwrap();
        checker.has_agg
    }

    fn output_name_by<N: fmt::Display>(&self, fn_display: &impl Fn(ColumnRef) -> N) -> String {
        match self {
            ScalarExpression::Constant(value) => format!("{value}"),
            ScalarExpression::ColumnRef { column, .. } => format!("{}", fn_display(*column)),
            ScalarExpression::Alias { alias, expr } => match alias {
                AliasType::Name(alias) => alias.to_string(),
                AliasType::Expr(alias_expr) => {
                    format!(
                        "({}) as ({})",
                        expr.output_name_by(fn_display),
                        alias_expr.output_name_by(fn_display)
                    )
                }
            },
            ScalarExpression::TypeCast { expr, ty, .. } => {
                format!("cast ({} as {})", expr.output_name_by(fn_display), ty)
            }
            ScalarExpression::IsNull { expr, negated } => {
                let suffix = if *negated { "is not null" } else { "is null" };

                format!("{} {}", expr.output_name_by(fn_display), suffix)
            }
            ScalarExpression::Unary { expr, op, .. } => {
                format!("{}{}", op, expr.output_name_by(fn_display))
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => format!(
                "({} {} {})",
                left_expr.output_name_by(fn_display),
                op,
                right_expr.output_name_by(fn_display),
            ),
            ScalarExpression::AggCall {
                args,
                kind,
                distinct,
                ..
            } => {
                let args_str = args
                    .iter()
                    .map(|expr| expr.output_name_by(fn_display))
                    .join(", ");
                let op = |allow_distinct, distinct| {
                    if allow_distinct && distinct {
                        "distinct "
                    } else {
                        ""
                    }
                };
                format!(
                    "{:?}({}{})",
                    kind,
                    op(kind.allow_distinct(), *distinct),
                    args_str
                )
            }
            ScalarExpression::In {
                args,
                negated,
                expr,
            } => {
                let args_string = args
                    .iter()
                    .map(|arg| arg.output_name_by(fn_display))
                    .join(", ");
                let op_string = if *negated { "not in" } else { "in" };
                format!(
                    "{} {} ({})",
                    expr.output_name_by(fn_display),
                    op_string,
                    args_string
                )
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                negated,
            } => {
                let op_string = if *negated { "not between" } else { "between" };
                format!(
                    "{} {} [{}, {}]",
                    expr.output_name_by(fn_display),
                    op_string,
                    left_expr.output_name_by(fn_display),
                    right_expr.output_name_by(fn_display)
                )
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                let op = |tag: &str, num_expr: &Option<Box<ScalarExpression>>| {
                    num_expr
                        .as_ref()
                        .map(|expr| format!(", {}: {}", tag, expr.output_name_by(fn_display)))
                        .unwrap_or_default()
                };

                format!(
                    "substring({}{}{})",
                    expr.output_name_by(fn_display),
                    op("from", from_expr),
                    op("for", for_expr),
                )
            }
            ScalarExpression::Position { expr, in_expr } => {
                format!(
                    "position({} in {})",
                    expr.output_name_by(fn_display),
                    in_expr.output_name_by(fn_display)
                )
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                let trim_what_str = {
                    trim_what_expr
                        .as_ref()
                        .map(|expr| expr.output_name_by(fn_display))
                        .unwrap_or_else(|| " ".to_string())
                };
                let trim_where_str = match trim_where {
                    Some(TrimWhereField::Both) => format!("both '{trim_what_str}' from"),
                    Some(TrimWhereField::Leading) => format!("leading '{trim_what_str}' from"),
                    Some(TrimWhereField::Trailing) => format!("trailing '{trim_what_str}' from"),
                    None => {
                        if trim_what_str.is_empty() {
                            String::new()
                        } else {
                            format!("'{trim_what_str}' from")
                        }
                    }
                };
                format!(
                    "trim({} {})",
                    trim_where_str,
                    expr.output_name_by(fn_display)
                )
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Tuple(args) => {
                let args_str = args
                    .iter()
                    .map(|expr| expr.output_name_by(fn_display))
                    .join(", ");
                format!("({args_str})")
            }
            ScalarExpression::ScalaFunction(ScalarFunction { args, inner }) => {
                let args_str = args
                    .iter()
                    .map(|expr| expr.output_name_by(fn_display))
                    .join(", ");
                format!("{}({})", inner.summary().name, args_str)
            }
            ScalarExpression::TableFunction(TableFunction { args, catalog }) => {
                let args_str = args
                    .iter()
                    .map(|expr| expr.output_name_by(fn_display))
                    .join(", ");
                format!("{}({})", catalog.inner.summary().name, args_str)
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                format!(
                    "if {} ({}, {})",
                    condition.output_name_by(fn_display),
                    left_expr.output_name_by(fn_display),
                    right_expr.output_name_by(fn_display)
                )
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            } => {
                format!(
                    "ifnull({}, {})",
                    left_expr.output_name_by(fn_display),
                    right_expr.output_name_by(fn_display)
                )
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                format!(
                    "ifnull({}, {})",
                    left_expr.output_name_by(fn_display),
                    right_expr.output_name_by(fn_display)
                )
            }
            ScalarExpression::Coalesce { exprs, .. } => {
                let exprs_str = exprs
                    .iter()
                    .map(|expr| expr.output_name_by(fn_display))
                    .join(", ");
                format!("coalesce({exprs_str})")
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                let op = |tag: &str, expr: &Option<Box<ScalarExpression>>| {
                    expr.as_ref()
                        .map(|expr| format!("{}{} ", tag, expr.output_name_by(fn_display)))
                        .unwrap_or_default()
                };
                let expr_pairs_str = expr_pairs
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        format!(
                            "when {} then {}",
                            when_expr.output_name_by(fn_display),
                            then_expr.output_name_by(fn_display)
                        )
                    })
                    .join(" ");

                format!(
                    "case {}{} {}end",
                    op("", operand_expr),
                    expr_pairs_str,
                    op("else ", else_expr)
                )
            }
        }
    }

    pub fn output_name(&self, arena: &PlanArena) -> String {
        self.output_name_by(&|column| arena.column(column).full_name())
    }

    pub fn output_column_ref(&self, arena: &mut PlanArena) -> ColumnRef {
        match self {
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

impl fmt::Display for ScalarExpression {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.output_name_by(&|column| column))
    }
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
    use crate::expression::TrimWhereField;
    use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::function::current_date::CurrentDate;
    use crate::function::numbers::Numbers;
    use crate::planner::{PlanArena, TableArenaCell};
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
    fn test_eq_ignore_colref_pos() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = ScalarExpression::column_expr(
            arena.alloc_column(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
            )),
            0,
        );
        let right = ScalarExpression::column_expr(
            arena.alloc_column(ColumnCatalog::new(
                "c1".to_string(),
                true,
                ColumnDesc::new(LogicalType::Bigint, None, false, None)?,
            )),
            2,
        );
        let different = ScalarExpression::column_expr(
            arena.alloc_column(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
            )),
            0,
        );

        assert!(left.eq_ignore_colref_pos(&right, &arena));
        assert!(!left.eq_ignore_colref_pos(&different, &arena));
        Ok(())
    }

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            expr: ScalarExpression,
            drive: Option<&ReferenceDecodeContext<'_, RocksTransaction>>,
            reference_tables: &mut ReferenceTables,
            arena: &mut PlanArena,
        ) -> Result<(), DatabaseError> {
            expr.encode(cursor, false, reference_tables, arena)?;

            cursor.seek(SeekFrom::Start(0))?;
            let decoded = ScalarExpression::decode(cursor, drive, reference_tables, arena)?;
            assert!(
                decoded.eq_ignore_colref_pos(&expr, arena),
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
        numbers.output_schema_into(
            &numbers.summary().name,
            table_arena.borrow_mut(),
            &mut schema,
        );
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
                expr: Box::new(ScalarExpression::Empty),
                alias: AliasType::Name("Hello".to_string()),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::Empty),
                alias: AliasType::Expr(Box::new(ScalarExpression::Empty)),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TypeCast {
                expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
                evaluator: Some(cast_create(
                    Cow::Owned(LogicalType::Integer),
                    Cow::Owned(LogicalType::Integer),
                )?),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IsNull {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: Box::new(ScalarExpression::Empty),
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
                expr: Box::new(ScalarExpression::Empty),
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
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
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
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
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
                args: vec![ScalarExpression::Empty],
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
                expr: Box::new(ScalarExpression::Empty),
                args: vec![ScalarExpression::Empty],
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Between {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: Some(Box::new(ScalarExpression::Empty)),
                from_expr: Some(Box::new(ScalarExpression::Empty)),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: None,
                from_expr: Some(Box::new(ScalarExpression::Empty)),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
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
                expr: Box::new(ScalarExpression::Empty),
                in_expr: Box::new(ScalarExpression::Empty),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: Some(Box::new(ScalarExpression::Empty)),
                trim_where: Some(TrimWhereField::Both),
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
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
                expr: Box::new(ScalarExpression::Empty),
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
            ScalarExpression::Tuple(vec![ScalarExpression::Empty]),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![ScalarExpression::Empty],
                inner: ArcScalarFunctionImpl(CurrentDate::new()),
            }),
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TableFunction(TableFunction {
                args: vec![ScalarExpression::Empty],
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
                condition: Box::new(ScalarExpression::Empty),
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IfNull {
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::NullIf {
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Coalesce {
                exprs: vec![ScalarExpression::Empty],
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: Some(Box::new(ScalarExpression::Empty)),
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: Some(Box::new(ScalarExpression::Empty)),
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
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: Some(Box::new(ScalarExpression::Empty)),
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
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: None,
                ty: LogicalType::Integer,
            },
            Some(&context),
            &mut reference_tables,
            &mut plan_arena,
        )?;

        Ok(())
    }
}
