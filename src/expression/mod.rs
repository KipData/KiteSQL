use self::agg::AggKind;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::function::table::TableFunction;
use crate::types::evaluator::{BinaryEvaluatorBox, EvaluatorFactory, UnaryEvaluatorBox};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use sqlparser::ast::TrimWhereField;
use sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, CharLengthUnits, UnaryOperator as SqlUnaryOperator,
};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::{fmt, mem};

pub mod agg;
mod evaluator;
pub mod function;
pub mod range_detacher;
pub mod simplify;

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
    ColumnRef(ColumnRef),
    Alias {
        expr: Box<ScalarExpression>,
        alias: AliasType,
    },
    TypeCast {
        expr: Box<ScalarExpression>,
        ty: LogicalType,
    },
    IsNull {
        negated: bool,
        expr: Box<ScalarExpression>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ScalarExpression>,
        evaluator: Option<UnaryEvaluatorBox>,
        ty: LogicalType,
    },
    Binary {
        op: BinaryOperator,
        left_expr: Box<ScalarExpression>,
        right_expr: Box<ScalarExpression>,
        evaluator: Option<BinaryEvaluatorBox>,
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
    Reference {
        expr: Box<ScalarExpression>,
        pos: usize,
    },
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

impl ScalarExpression {
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

    pub fn try_reference(&mut self, output_exprs: &[ScalarExpression]) {
        let fn_output_column = |expr: &ScalarExpression| expr.output_column();
        let self_column = fn_output_column(self);
        if let Some((pos, _)) = output_exprs
            .iter()
            .find_position(|expr| self_column.summary() == fn_output_column(expr).summary())
        {
            let expr = Box::new(mem::replace(self, ScalarExpression::Empty));
            *self = ScalarExpression::Reference { expr, pos };
            return;
        }

        match self {
            ScalarExpression::Alias { expr, .. } => {
                expr.try_reference(output_exprs);
            }
            ScalarExpression::TypeCast { expr, .. } => {
                expr.try_reference(output_exprs);
            }
            ScalarExpression::IsNull { expr, .. } => {
                expr.try_reference(output_exprs);
            }
            ScalarExpression::Unary { expr, .. } => {
                expr.try_reference(output_exprs);
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                left_expr.try_reference(output_exprs);
                right_expr.try_reference(output_exprs);
            }
            ScalarExpression::AggCall { args, .. }
            | ScalarExpression::Coalesce { exprs: args, .. }
            | ScalarExpression::Tuple(args) => {
                for arg in args {
                    arg.try_reference(output_exprs);
                }
            }
            ScalarExpression::In { expr, args, .. } => {
                expr.try_reference(output_exprs);
                for arg in args {
                    arg.try_reference(output_exprs);
                }
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                expr.try_reference(output_exprs);
                left_expr.try_reference(output_exprs);
                right_expr.try_reference(output_exprs);
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                expr.try_reference(output_exprs);
                if let Some(expr) = for_expr {
                    expr.try_reference(output_exprs);
                }
                if let Some(expr) = from_expr {
                    expr.try_reference(output_exprs);
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.try_reference(output_exprs);
                in_expr.try_reference(output_exprs);
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                expr.try_reference(output_exprs);
                if let Some(trim_what_expr) = trim_what_expr {
                    trim_what_expr.try_reference(output_exprs);
                }
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Constant(_)
            | ScalarExpression::ColumnRef(_)
            | ScalarExpression::Reference { .. } => (),
            ScalarExpression::ScalaFunction(function) => {
                for expr in function.args.iter_mut() {
                    expr.try_reference(output_exprs);
                }
            }
            ScalarExpression::TableFunction(function) => {
                for expr in function.args.iter_mut() {
                    expr.try_reference(output_exprs);
                }
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                condition.try_reference(output_exprs);
                left_expr.try_reference(output_exprs);
                right_expr.try_reference(output_exprs);
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                left_expr.try_reference(output_exprs);
                right_expr.try_reference(output_exprs);
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                if let Some(expr) = operand_expr {
                    expr.try_reference(output_exprs);
                }
                for (expr_1, expr_2) in expr_pairs {
                    expr_1.try_reference(output_exprs);
                    expr_2.try_reference(output_exprs);
                }
                if let Some(expr) = else_expr {
                    expr.try_reference(output_exprs);
                }
            }
        }
    }

    pub fn bind_evaluator(&mut self) -> Result<(), DatabaseError> {
        match self {
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                evaluator,
                ..
            } => {
                left_expr.bind_evaluator()?;
                right_expr.bind_evaluator()?;

                let ty = LogicalType::max_logical_type(
                    &left_expr.return_type(),
                    &right_expr.return_type(),
                )?;
                let fn_cast = |expr: &mut ScalarExpression, ty: LogicalType| {
                    if expr.return_type() != ty {
                        *expr = ScalarExpression::TypeCast {
                            expr: Box::new(mem::replace(expr, ScalarExpression::Empty)),
                            ty,
                        }
                    }
                };
                fn_cast(left_expr, ty.clone());
                fn_cast(right_expr, ty.clone());

                *evaluator = Some(EvaluatorFactory::binary_create(ty, *op)?);
            }
            ScalarExpression::Unary {
                expr,
                op,
                evaluator,
                ..
            } => {
                expr.bind_evaluator()?;

                let ty = expr.return_type();
                if ty.is_unsigned_numeric() {
                    *expr.as_mut() = ScalarExpression::TypeCast {
                        expr: Box::new(mem::replace(expr, ScalarExpression::Empty)),
                        ty: match ty {
                            LogicalType::UTinyint => LogicalType::Tinyint,
                            LogicalType::USmallint => LogicalType::Smallint,
                            LogicalType::UInteger => LogicalType::Integer,
                            LogicalType::UBigint => LogicalType::Bigint,
                            _ => unreachable!(),
                        },
                    }
                }
                *evaluator = Some(EvaluatorFactory::unary_create(ty, *op)?);
            }
            ScalarExpression::Alias { expr, .. } => {
                expr.bind_evaluator()?;
            }
            ScalarExpression::TypeCast { expr, .. } => {
                expr.bind_evaluator()?;
            }
            ScalarExpression::IsNull { expr, .. } => {
                expr.bind_evaluator()?;
            }
            ScalarExpression::AggCall { args, .. }
            | ScalarExpression::Coalesce { exprs: args, .. }
            | ScalarExpression::Tuple(args) => {
                for arg in args {
                    arg.bind_evaluator()?;
                }
            }
            ScalarExpression::In { expr, args, .. } => {
                expr.bind_evaluator()?;
                for arg in args {
                    arg.bind_evaluator()?;
                }
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                expr.bind_evaluator()?;
                left_expr.bind_evaluator()?;
                right_expr.bind_evaluator()?;
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                expr.bind_evaluator()?;
                if let Some(expr) = for_expr {
                    expr.bind_evaluator()?;
                }
                if let Some(expr) = from_expr {
                    expr.bind_evaluator()?;
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.bind_evaluator()?;
                in_expr.bind_evaluator()?;
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                expr.bind_evaluator()?;
                if let Some(trim_what_expr) = trim_what_expr {
                    trim_what_expr.bind_evaluator()?;
                }
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Constant(_)
            | ScalarExpression::ColumnRef(_)
            | ScalarExpression::Reference { .. } => (),
            ScalarExpression::ScalaFunction(function) => {
                for expr in function.args.iter_mut() {
                    expr.bind_evaluator()?;
                }
            }
            ScalarExpression::TableFunction(function) => {
                for expr in function.args.iter_mut() {
                    expr.bind_evaluator()?;
                }
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                condition.bind_evaluator()?;
                left_expr.bind_evaluator()?;
                right_expr.bind_evaluator()?;
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                left_expr.bind_evaluator()?;
                right_expr.bind_evaluator()?;
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                if let Some(expr) = operand_expr {
                    expr.bind_evaluator()?;
                }
                for (expr_1, expr_2) in expr_pairs {
                    expr_1.bind_evaluator()?;
                    expr_2.bind_evaluator()?;
                }
                if let Some(expr) = else_expr {
                    expr.bind_evaluator()?;
                }
            }
        }

        Ok(())
    }

    pub fn has_count_star(&self) -> bool {
        match self {
            ScalarExpression::Alias { expr, .. } => expr.has_count_star(),
            ScalarExpression::TypeCast { expr, .. } => expr.has_count_star(),
            ScalarExpression::IsNull { expr, .. } => expr.has_count_star(),
            ScalarExpression::Unary { expr, .. } => expr.has_count_star(),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_count_star() || right_expr.has_count_star(),
            ScalarExpression::AggCall { args, .. }
            | ScalarExpression::ScalaFunction(ScalarFunction { args, .. })
            | ScalarExpression::Coalesce { exprs: args, .. } => {
                args.iter().any(Self::has_count_star)
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::Constant(_) | ScalarExpression::ColumnRef(_) => false,
            ScalarExpression::In { expr, args, .. } => {
                expr.has_count_star() || args.iter().any(Self::has_count_star)
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => expr.has_count_star() || left_expr.has_count_star() || right_expr.has_count_star(),
            ScalarExpression::SubString {
                expr,
                from_expr,
                for_expr,
            } => {
                expr.has_count_star()
                    || matches!(
                        from_expr.as_ref().map(|expr| expr.has_count_star()),
                        Some(true)
                    )
                    || matches!(
                        for_expr.as_ref().map(|expr| expr.has_count_star()),
                        Some(true)
                    )
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.has_count_star() || in_expr.has_count_star()
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                expr.has_count_star()
                    || trim_what_expr.as_ref().map(|expr| expr.has_count_star()) == Some(true)
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Reference { expr, .. } => expr.has_count_star(),
            ScalarExpression::Tuple(args) => args.iter().any(Self::has_count_star),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                condition.has_count_star()
                    || left_expr.has_count_star()
                    || right_expr.has_count_star()
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_count_star() || right_expr.has_count_star(),
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                matches!(
                    operand_expr.as_ref().map(|expr| expr.has_count_star()),
                    Some(true)
                ) || expr_pairs
                    .iter()
                    .any(|(expr_1, expr_2)| expr_1.has_count_star() || expr_2.has_count_star())
                    || matches!(
                        else_expr.as_ref().map(|expr| expr.has_count_star()),
                        Some(true)
                    )
            }
        }
    }

    pub fn return_type(&self) -> LogicalType {
        match self {
            ScalarExpression::Constant(v) => v.logical_type(),
            ScalarExpression::ColumnRef(col) => col.datatype().clone(),
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
            } => return_type.clone(),
            ScalarExpression::IsNull { .. }
            | ScalarExpression::In { .. }
            | ScalarExpression::Between { .. } => LogicalType::Boolean,
            ScalarExpression::SubString { .. } => {
                LogicalType::Varchar(None, CharLengthUnits::Characters)
            }
            ScalarExpression::Position { .. } => LogicalType::Integer,
            ScalarExpression::Trim { .. } => {
                LogicalType::Varchar(None, CharLengthUnits::Characters)
            }
            ScalarExpression::Alias { expr, .. } | ScalarExpression::Reference { expr, .. } => {
                expr.return_type()
            }
            ScalarExpression::Empty | ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::Tuple(exprs) => {
                let types = exprs.iter().map(|expr| expr.return_type()).collect_vec();

                LogicalType::Tuple(types)
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, .. }) => {
                inner.return_type().clone()
            }
        }
    }

    pub fn referenced_columns(&self, only_column_ref: bool) -> Vec<ColumnRef> {
        fn columns_collect(
            expr: &ScalarExpression,
            vec: &mut Vec<ColumnRef>,
            only_column_ref: bool,
        ) {
            // When `ScalarExpression` is a complex type, it itself is also a special Column
            if !only_column_ref {
                vec.push(expr.output_column());
            }
            match expr {
                ScalarExpression::ColumnRef(col) => {
                    vec.push(col.clone());
                }
                ScalarExpression::Alias { expr, .. } => {
                    columns_collect(expr, vec, only_column_ref);
                }
                ScalarExpression::TypeCast { expr, .. } => {
                    columns_collect(expr, vec, only_column_ref)
                }
                ScalarExpression::IsNull { expr, .. } => {
                    columns_collect(expr, vec, only_column_ref)
                }
                ScalarExpression::Unary { expr, .. } => columns_collect(expr, vec, only_column_ref),
                ScalarExpression::Binary {
                    left_expr,
                    right_expr,
                    ..
                } => {
                    columns_collect(left_expr, vec, only_column_ref);
                    columns_collect(right_expr, vec, only_column_ref);
                }
                ScalarExpression::AggCall { args, .. }
                | ScalarExpression::ScalaFunction(ScalarFunction { args, .. })
                | ScalarExpression::TableFunction(TableFunction { args, .. })
                | ScalarExpression::Tuple(args)
                | ScalarExpression::Coalesce { exprs: args, .. } => {
                    for expr in args {
                        columns_collect(expr, vec, only_column_ref)
                    }
                }
                ScalarExpression::In { expr, args, .. } => {
                    columns_collect(expr, vec, only_column_ref);
                    for arg in args {
                        columns_collect(arg, vec, only_column_ref)
                    }
                }
                ScalarExpression::Between {
                    expr,
                    left_expr,
                    right_expr,
                    ..
                } => {
                    columns_collect(expr, vec, only_column_ref);
                    columns_collect(left_expr, vec, only_column_ref);
                    columns_collect(right_expr, vec, only_column_ref);
                }
                ScalarExpression::SubString {
                    expr,
                    for_expr,
                    from_expr,
                } => {
                    columns_collect(expr, vec, only_column_ref);
                    if let Some(for_expr) = for_expr {
                        columns_collect(for_expr, vec, only_column_ref);
                    }
                    if let Some(from_expr) = from_expr {
                        columns_collect(from_expr, vec, only_column_ref);
                    }
                }
                ScalarExpression::Position { expr, in_expr } => {
                    columns_collect(expr, vec, only_column_ref);
                    columns_collect(in_expr, vec, only_column_ref);
                }
                ScalarExpression::Trim {
                    expr,
                    trim_what_expr,
                    ..
                } => {
                    columns_collect(expr, vec, only_column_ref);
                    if let Some(trim_what_expr) = trim_what_expr {
                        columns_collect(trim_what_expr, vec, only_column_ref);
                    }
                }
                ScalarExpression::Constant(_) => (),
                ScalarExpression::Reference { .. } | ScalarExpression::Empty => unreachable!(),
                ScalarExpression::If {
                    condition,
                    left_expr,
                    right_expr,
                    ..
                } => {
                    columns_collect(condition, vec, only_column_ref);
                    columns_collect(left_expr, vec, only_column_ref);
                    columns_collect(right_expr, vec, only_column_ref);
                }
                ScalarExpression::IfNull {
                    left_expr,
                    right_expr,
                    ..
                }
                | ScalarExpression::NullIf {
                    left_expr,
                    right_expr,
                    ..
                } => {
                    columns_collect(left_expr, vec, only_column_ref);
                    columns_collect(right_expr, vec, only_column_ref);
                }
                ScalarExpression::CaseWhen {
                    operand_expr,
                    expr_pairs,
                    else_expr,
                    ..
                } => {
                    if let Some(expr) = operand_expr {
                        columns_collect(expr, vec, only_column_ref);
                    }
                    for (expr_1, expr_2) in expr_pairs {
                        columns_collect(expr_1, vec, only_column_ref);
                        columns_collect(expr_2, vec, only_column_ref);
                    }
                    if let Some(expr) = else_expr {
                        columns_collect(expr, vec, only_column_ref);
                    }
                }
            }
        }
        let mut exprs = Vec::new();

        columns_collect(self, &mut exprs, only_column_ref);

        exprs
    }

    pub fn has_table_ref_column(&self) -> bool {
        match self {
            ScalarExpression::Constant(_) => false,
            ScalarExpression::ColumnRef(column) => {
                column.table_name().is_some() && column.id().is_some()
            }
            ScalarExpression::Alias { expr, .. } => expr.has_table_ref_column(),
            ScalarExpression::TypeCast { expr, .. } | ScalarExpression::IsNull { expr, .. } => {
                expr.has_table_ref_column()
            }
            ScalarExpression::Unary { expr, .. } => expr.has_table_ref_column(),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_table_ref_column() || right_expr.has_table_ref_column(),
            ScalarExpression::AggCall { args, .. } => {
                args.iter().any(ScalarExpression::has_table_ref_column)
            }
            ScalarExpression::In { expr, args, .. } => {
                expr.has_table_ref_column()
                    || args.iter().any(ScalarExpression::has_table_ref_column)
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                expr.has_table_ref_column()
                    || left_expr.has_table_ref_column()
                    || right_expr.has_table_ref_column()
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                expr.has_table_ref_column()
                    || for_expr
                        .as_deref()
                        .map(ScalarExpression::has_table_ref_column)
                        .unwrap_or(false)
                    || from_expr
                        .as_deref()
                        .map(ScalarExpression::has_table_ref_column)
                        .unwrap_or(false)
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.has_table_ref_column() || in_expr.has_table_ref_column()
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                expr.has_table_ref_column()
                    || trim_what_expr
                        .as_deref()
                        .map(ScalarExpression::has_table_ref_column)
                        .unwrap_or(false)
            }
            ScalarExpression::Empty => false,
            ScalarExpression::Reference { expr, .. } => expr.has_table_ref_column(),
            ScalarExpression::Tuple(exprs) => {
                exprs.iter().any(ScalarExpression::has_table_ref_column)
            }
            ScalarExpression::ScalaFunction(function) => function
                .args
                .iter()
                .any(ScalarExpression::has_table_ref_column),
            ScalarExpression::TableFunction(function) => function
                .args
                .iter()
                .any(ScalarExpression::has_table_ref_column),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                condition.has_table_ref_column()
                    || left_expr.has_table_ref_column()
                    || right_expr.has_table_ref_column()
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_table_ref_column() || right_expr.has_table_ref_column(),
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_table_ref_column() || right_expr.has_table_ref_column(),
            ScalarExpression::Coalesce { exprs, .. } => {
                exprs.iter().any(ScalarExpression::has_table_ref_column)
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                operand_expr
                    .as_deref()
                    .map(ScalarExpression::has_table_ref_column)
                    .unwrap_or(false)
                    || else_expr
                        .as_deref()
                        .map(ScalarExpression::has_table_ref_column)
                        .unwrap_or(false)
                    || expr_pairs.iter().any(|(left_expr, right_expr)| {
                        left_expr.has_table_ref_column() || right_expr.has_table_ref_column()
                    })
            }
        }
    }

    pub fn has_agg_call(&self) -> bool {
        match self {
            ScalarExpression::AggCall { .. } => true,
            ScalarExpression::Constant(_) => false,
            ScalarExpression::ColumnRef(_) => false,
            ScalarExpression::Alias { expr, .. } => expr.has_agg_call(),
            ScalarExpression::TypeCast { expr, .. } => expr.has_agg_call(),
            ScalarExpression::IsNull { expr, .. } => expr.has_agg_call(),
            ScalarExpression::Unary { expr, .. } => expr.has_agg_call(),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_agg_call() || right_expr.has_agg_call(),
            ScalarExpression::In { expr, args, .. } => {
                expr.has_agg_call() || args.iter().any(|arg| arg.has_agg_call())
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => expr.has_agg_call() || left_expr.has_agg_call() || right_expr.has_agg_call(),
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                expr.has_agg_call()
                    || matches!(
                        for_expr.as_ref().map(|expr| expr.has_agg_call()),
                        Some(true)
                    )
                    || matches!(
                        from_expr.as_ref().map(|expr| expr.has_agg_call()),
                        Some(true)
                    )
            }
            ScalarExpression::Position { expr, in_expr } => {
                expr.has_agg_call() || in_expr.has_agg_call()
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                expr.has_agg_call()
                    || trim_what_expr.as_ref().map(|expr| expr.has_agg_call()) == Some(true)
            }
            ScalarExpression::Reference { .. }
            | ScalarExpression::Empty
            | ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::Tuple(args)
            | ScalarExpression::ScalaFunction(ScalarFunction { args, .. })
            | ScalarExpression::Coalesce { exprs: args, .. } => args.iter().any(Self::has_agg_call),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => condition.has_agg_call() || left_expr.has_agg_call() || right_expr.has_agg_call(),
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => left_expr.has_agg_call() || right_expr.has_agg_call(),
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                matches!(
                    operand_expr.as_ref().map(|expr| expr.has_agg_call()),
                    Some(true)
                ) || expr_pairs
                    .iter()
                    .any(|(expr_1, expr_2)| expr_1.has_agg_call() || expr_2.has_agg_call())
                    || matches!(
                        else_expr.as_ref().map(|expr| expr.has_agg_call()),
                        Some(true)
                    )
            }
        }
    }

    pub fn output_name(&self) -> String {
        match self {
            ScalarExpression::Constant(value) => format!("{}", value),
            ScalarExpression::ColumnRef(col) => col.full_name(),
            ScalarExpression::Alias { alias, expr } => match alias {
                AliasType::Name(alias) => alias.to_string(),
                AliasType::Expr(alias_expr) => {
                    format!("({}) as ({})", expr, alias_expr.output_name())
                }
            },
            ScalarExpression::TypeCast { expr, ty } => {
                format!("cast ({} as {})", expr.output_name(), ty)
            }
            ScalarExpression::IsNull { expr, negated } => {
                let suffix = if *negated { "is not null" } else { "is null" };

                format!("{} {}", expr.output_name(), suffix)
            }
            ScalarExpression::Unary { expr, op, .. } => format!("{}{}", op, expr.output_name()),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ..
            } => format!(
                "({} {} {})",
                left_expr.output_name(),
                op,
                right_expr.output_name(),
            ),
            ScalarExpression::AggCall {
                args,
                kind,
                distinct,
                ..
            } => {
                let args_str = args.iter().map(|expr| expr.output_name()).join(", ");
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
                let args_string = args.iter().map(|arg| arg.output_name()).join(", ");
                let op_string = if *negated { "not in" } else { "in" };
                format!("{} {} ({})", expr.output_name(), op_string, args_string)
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
                    expr.output_name(),
                    op_string,
                    left_expr.output_name(),
                    right_expr.output_name()
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
                        .map(|expr| format!(", {}: {}", tag, expr.output_name()))
                        .unwrap_or_default()
                };

                format!(
                    "substring({}{}{})",
                    expr.output_name(),
                    op("from", from_expr),
                    op("for", for_expr),
                )
            }
            ScalarExpression::Position { expr, in_expr } => {
                format!(
                    "position({} in {})",
                    expr.output_name(),
                    in_expr.output_name()
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
                        .map(|expr| expr.output_name())
                        .unwrap_or(" ".to_string())
                };
                let trim_where_str = match trim_where {
                    Some(TrimWhereField::Both) => format!("both '{}' from", trim_what_str),
                    Some(TrimWhereField::Leading) => format!("leading '{}' from", trim_what_str),
                    Some(TrimWhereField::Trailing) => format!("trailing '{}' from", trim_what_str),
                    None => {
                        if trim_what_str.is_empty() {
                            String::new()
                        } else {
                            format!("'{}' from", trim_what_str)
                        }
                    }
                };
                format!("trim({} {})", trim_where_str, expr.output_name())
            }
            ScalarExpression::Reference { expr, .. } => expr.output_name(),
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Tuple(args) => {
                let args_str = args.iter().map(|expr| expr.output_name()).join(", ");
                format!("({})", args_str)
            }
            ScalarExpression::ScalaFunction(ScalarFunction { args, inner }) => {
                let args_str = args.iter().map(|expr| expr.output_name()).join(", ");
                format!("{}({})", inner.summary().name, args_str)
            }
            ScalarExpression::TableFunction(TableFunction { args, inner }) => {
                let args_str = args.iter().map(|expr| expr.output_name()).join(", ");
                format!("{}({})", inner.summary().name, args_str)
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                format!("if {} ({}, {})", condition, left_expr, right_expr)
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            } => {
                format!("ifnull({}, {})", left_expr, right_expr)
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                format!("ifnull({}, {})", left_expr, right_expr)
            }
            ScalarExpression::Coalesce { exprs, .. } => {
                let exprs_str = exprs.iter().map(|expr| expr.output_name()).join(", ");
                format!("coalesce({})", exprs_str)
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                let op = |tag: &str, expr: &Option<Box<ScalarExpression>>| {
                    expr.as_ref()
                        .map(|expr| format!("{}{} ", tag, expr.output_name()))
                        .unwrap_or_default()
                };
                let expr_pairs_str = expr_pairs
                    .iter()
                    .map(|(when_expr, then_expr)| format!("when {} then {}", when_expr, then_expr))
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

    pub fn output_column(&self) -> ColumnRef {
        match self {
            ScalarExpression::ColumnRef(col) => col.clone(),
            ScalarExpression::Alias {
                alias: AliasType::Expr(expr),
                ..
            }
            | ScalarExpression::Reference { expr, .. } => expr.output_column(),
            _ => ColumnRef::from(ColumnCatalog::new(
                self.output_name(),
                true,
                // SAFETY: default expr must not be [`ScalarExpression::ColumnRef`]
                ColumnDesc::new(self.return_type(), None, false, None).unwrap(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
}

impl TryFrom<SqlUnaryOperator> for UnaryOperator {
    type Error = DatabaseError;

    fn try_from(value: SqlUnaryOperator) -> Result<Self, Self::Error> {
        match value {
            SqlUnaryOperator::Plus => Ok(UnaryOperator::Plus),
            SqlUnaryOperator::Minus => Ok(UnaryOperator::Minus),
            SqlUnaryOperator::Not => Ok(UnaryOperator::Not),
            op => Err(DatabaseError::UnsupportedStmt(format!("{}", op))),
        }
    }
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
        write!(f, "{}", self.output_name())
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let like_op = |f: &mut Formatter, escape_char: &Option<char>| {
            if let Some(escape_char) = escape_char {
                write!(f, "(escape: {})", escape_char)?;
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

impl TryFrom<SqlBinaryOperator> for BinaryOperator {
    type Error = DatabaseError;

    fn try_from(value: SqlBinaryOperator) -> Result<Self, Self::Error> {
        match value {
            SqlBinaryOperator::Plus => Ok(BinaryOperator::Plus),
            SqlBinaryOperator::Minus => Ok(BinaryOperator::Minus),
            SqlBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
            SqlBinaryOperator::Divide => Ok(BinaryOperator::Divide),
            SqlBinaryOperator::Modulo => Ok(BinaryOperator::Modulo),
            SqlBinaryOperator::StringConcat => Ok(BinaryOperator::StringConcat),
            SqlBinaryOperator::Gt => Ok(BinaryOperator::Gt),
            SqlBinaryOperator::Lt => Ok(BinaryOperator::Lt),
            SqlBinaryOperator::GtEq => Ok(BinaryOperator::GtEq),
            SqlBinaryOperator::LtEq => Ok(BinaryOperator::LtEq),
            SqlBinaryOperator::Spaceship => Ok(BinaryOperator::Spaceship),
            SqlBinaryOperator::Eq => Ok(BinaryOperator::Eq),
            SqlBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
            SqlBinaryOperator::And => Ok(BinaryOperator::And),
            SqlBinaryOperator::Or => Ok(BinaryOperator::Or),
            op => Err(DatabaseError::UnsupportedStmt(format!("{}", op))),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::agg::AggKind;
    use crate::expression::function::scala::{ArcScalarFunctionImpl, ScalarFunction};
    use crate::expression::function::table::{ArcTableFunctionImpl, TableFunction};
    use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::function::current_date::CurrentDate;
    use crate::function::numbers::Numbers;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::{Storage, TableCache, Transaction};
    use crate::types::evaluator::boolean::BooleanNotUnaryEvaluator;
    use crate::types::evaluator::int32::Int32PlusBinaryEvaluator;
    use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use sqlparser::ast::{CharLengthUnits, TrimWhereField};
    use std::hash::RandomState;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        fn fn_assert(
            cursor: &mut Cursor<Vec<u8>>,
            expr: ScalarExpression,
            drive: Option<(&RocksTransaction, &TableCache)>,
            reference_tables: &mut ReferenceTables,
        ) -> Result<(), DatabaseError> {
            expr.encode(cursor, false, reference_tables)?;

            cursor.seek(SeekFrom::Start(0))?;
            assert_eq!(
                ScalarExpression::decode(cursor, drive, reference_tables)?,
                expr
            );
            cursor.seek(SeekFrom::Start(0))?;

            Ok(())
        }

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let c3_column_id = {
            let table = transaction
                .table(&table_cache, Arc::new("t1".to_string()))?
                .unwrap();
            *table.get_column_id_by_name("c3").unwrap()
        };

        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(DataValue::Null),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(DataValue::Int32(42)),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Constant(DataValue::Utf8 {
                value: "hello".to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ColumnRef(ColumnRef::from(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c3_column_id,
                        table_name: Arc::new("t1".to_string()),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
                false,
            ))),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ColumnRef(ColumnRef::from(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c4".to_string(),
                    relation: ColumnRelation::None,
                },
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
                false,
            ))),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::Empty),
                alias: AliasType::Name("Hello".to_string()),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::Empty),
                alias: AliasType::Expr(Box::new(ScalarExpression::Empty)),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TypeCast {
                expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IsNull {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: Box::new(ScalarExpression::Empty),
                evaluator: Some(UnaryEvaluatorBox(Arc::new(BooleanNotUnaryEvaluator))),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: Box::new(ScalarExpression::Empty),
                evaluator: None,
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                evaluator: Some(BinaryEvaluatorBox(Arc::new(Int32PlusBinaryEvaluator))),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
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
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::AggCall {
                distinct: true,
                kind: AggKind::Avg,
                args: vec![ScalarExpression::Empty],
                ty: LogicalType::Double,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::In {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
                args: vec![ScalarExpression::Empty],
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Between {
                negated: true,
                expr: Box::new(ScalarExpression::Empty),
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: Some(Box::new(ScalarExpression::Empty)),
                from_expr: Some(Box::new(ScalarExpression::Empty)),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: None,
                from_expr: Some(Box::new(ScalarExpression::Empty)),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::SubString {
                expr: Box::new(ScalarExpression::Empty),
                for_expr: None,
                from_expr: None,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Position {
                expr: Box::new(ScalarExpression::Empty),
                in_expr: Box::new(ScalarExpression::Empty),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: Some(Box::new(ScalarExpression::Empty)),
                trim_where: Some(TrimWhereField::Both),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: None,
                trim_where: Some(TrimWhereField::Both),
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Trim {
                expr: Box::new(ScalarExpression::Empty),
                trim_what_expr: None,
                trim_where: None,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Empty,
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Tuple(vec![ScalarExpression::Empty]),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![ScalarExpression::Empty],
                inner: ArcScalarFunctionImpl(CurrentDate::new()),
            }),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::TableFunction(TableFunction {
                args: vec![ScalarExpression::Empty],
                inner: ArcTableFunctionImpl(Numbers::new()),
            }),
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::If {
                condition: Box::new(ScalarExpression::Empty),
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::IfNull {
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::NullIf {
                left_expr: Box::new(ScalarExpression::Empty),
                right_expr: Box::new(ScalarExpression::Empty),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::Coalesce {
                exprs: vec![ScalarExpression::Empty],
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: Some(Box::new(ScalarExpression::Empty)),
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: Some(Box::new(ScalarExpression::Empty)),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: Some(Box::new(ScalarExpression::Empty)),
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;
        fn_assert(
            &mut cursor,
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(ScalarExpression::Empty, ScalarExpression::Empty)],
                else_expr: None,
                ty: LogicalType::Integer,
            },
            Some((&transaction, &table_cache)),
            &mut reference_tables,
        )?;

        Ok(())
    }
}
