#[cfg(test)]
use crate::planner::PlanArena;
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

use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::{AliasType, BinaryOperator, ScalarExpression, TrimWhereField};
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::evaluator::binary_create;
use crate::types::tuple::TupleLike;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{CharLengthUnits, LogicalType};
use std::borrow::Cow;
use std::cmp;
use std::cmp::Ordering;

macro_rules! eval_to_num {
    ($num_expr:expr, $arena:expr, $tuple:expr) => {
        if let Some(num_i32) = cast_cow(
            $arena.expression(*$num_expr).eval($arena, $tuple)?,
            &LogicalType::Integer,
        )?
        .i32()
        {
            num_i32
        } else {
            return Ok(Cow::Owned(DataValue::Null));
        }
    };
}

impl ScalarExpression {
    pub fn eval<'a>(
        &'a self,
        arena: &'a (dyn MetaArena + '_),
        tuple: Option<&'a dyn TupleLike>,
    ) -> Result<Cow<'a, DataValue>, DatabaseError> {
        match self {
            ScalarExpression::Constant(val) => match val {
                DataValue::Parameter { id, .. } => {
                    Err(DatabaseError::parameter_not_found(format!("${id}")))
                }
                val => Ok(Cow::Borrowed(val)),
            },
            ScalarExpression::ColumnRef { position, .. } => {
                let Some(tuple) = tuple else {
                    return Ok(Cow::Owned(DataValue::Null));
                };
                Ok(Cow::Borrowed(tuple.value_at(*position)))
            }
            ScalarExpression::Alias { expr, alias } => {
                let Some(tuple) = tuple else {
                    return Ok(Cow::Owned(DataValue::Null));
                };
                if let AliasType::Expr(inner_expr) = alias {
                    arena.expression(*inner_expr).eval(arena, Some(tuple))
                } else {
                    arena.expression(*expr).eval(arena, Some(tuple))
                }
            }
            ScalarExpression::TypeCast {
                expr, evaluator, ..
            } => {
                let value = arena.expression(*expr).eval(arena, tuple)?;
                if let Some(evaluator) = evaluator {
                    evaluator.eval(&value).map(Cow::Owned)
                } else {
                    Ok(value)
                }
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                evaluator,
                ..
            } => {
                let left = arena.expression(*left_expr).eval(arena, tuple)?;
                let right = arena.expression(*right_expr).eval(arena, tuple)?;

                evaluator
                    .as_ref()
                    .ok_or(DatabaseError::EvaluatorNotFound)?
                    .binary_eval(&left, &right)
                    .map(Cow::Owned)
            }
            ScalarExpression::IsNull { expr, negated } => {
                let mut is_null = arena.expression(*expr).eval(arena, tuple)?.is_null();
                if *negated {
                    is_null = !is_null;
                }
                Ok(Cow::Owned(DataValue::Boolean(is_null)))
            }
            ScalarExpression::In {
                expr,
                args,
                negated,
            } => {
                let value = arena.expression(*expr).eval(arena, tuple)?;
                if value.is_null() {
                    return Ok(Cow::Owned(DataValue::Null));
                }

                let mut matched = false;
                let mut saw_null = false;
                for arg in args {
                    let arg_value = arena.expression(*arg).eval(arena, tuple)?;

                    if arg_value.is_null() {
                        saw_null = true;
                        continue;
                    }
                    if arg_value == value {
                        matched = true;
                        break;
                    }
                }

                if matched {
                    Ok(Cow::Owned(DataValue::Boolean(!negated)))
                } else if saw_null {
                    Ok(Cow::Owned(DataValue::Null))
                } else {
                    Ok(Cow::Owned(DataValue::Boolean(*negated)))
                }
            }
            ScalarExpression::Unary {
                expr, evaluator, ..
            } => {
                let value = arena.expression(*expr).eval(arena, tuple)?;

                Ok(Cow::Owned(
                    evaluator
                        .as_ref()
                        .ok_or(DatabaseError::EvaluatorNotFound)?
                        .unary_eval(&value),
                ))
            }
            ScalarExpression::AggCall { .. } => {
                unreachable!("must use `NormalizationRuleImpl::ExpressionRemapper`")
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                negated,
            } => {
                let value = arena.expression(*expr).eval(arena, tuple)?;
                let left = arena.expression(*left_expr).eval(arena, tuple)?;
                let right = arena.expression(*right_expr).eval(arena, tuple)?;

                let mut is_between = match (
                    value.partial_cmp(&left).map(Ordering::is_ge),
                    value.partial_cmp(&right).map(Ordering::is_le),
                ) {
                    (Some(true), Some(true)) => true,
                    (None, _) | (_, None) => return Ok(Cow::Owned(DataValue::Null)),
                    _ => false,
                };
                if *negated {
                    is_between = !is_between;
                }
                Ok(Cow::Owned(DataValue::Boolean(is_between)))
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                let value = cast_cow(
                    arena.expression(*expr).eval(arena, tuple)?,
                    &LogicalType::Varchar(None, CharLengthUnits::Characters),
                )?;
                if let Some(mut string) = value.utf8().map(String::from) {
                    if let Some(from_expr) = from_expr {
                        let mut from = eval_to_num!(from_expr, arena, tuple).saturating_sub(1);
                        let len_i = string.len() as i32;

                        while from < 0 {
                            from += len_i + 1;
                        }
                        if from > len_i {
                            return Ok(Cow::Owned(DataValue::Null));
                        }
                        string = string.split_off(from as usize);
                    }
                    if let Some(for_expr) = for_expr {
                        let for_i =
                            cmp::min(eval_to_num!(for_expr, arena, tuple) as usize, string.len());
                        let _ = string.split_off(for_i);
                    }

                    Ok(Cow::Owned(DataValue::Utf8 {
                        value: string,
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }))
                } else {
                    Ok(Cow::Owned(DataValue::Null))
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                let varchar = LogicalType::Varchar(None, CharLengthUnits::Characters);
                let pattern = cast_cow(arena.expression(*expr).eval(arena, tuple)?, &varchar)?;
                let string = cast_cow(arena.expression(*in_expr).eval(arena, tuple)?, &varchar)?;
                Ok(Cow::Owned(DataValue::Int32(
                    string
                        .utf8()
                        .unwrap_or("")
                        .find(pattern.utf8().unwrap_or(""))
                        .map(|pos| pos as i32 + 1)
                        .unwrap_or(0),
                )))
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                let value = cast_cow(
                    arena.expression(*expr).eval(arena, tuple)?,
                    &LogicalType::Varchar(None, CharLengthUnits::Characters),
                )?;
                if let Some(string) = value.utf8() {
                    let mut trim_what = String::from(" ");
                    if let Some(trim_what_expr) = trim_what_expr {
                        let value = cast_cow(
                            arena.expression(*trim_what_expr).eval(arena, tuple)?,
                            &LogicalType::Varchar(None, CharLengthUnits::Characters),
                        )?;
                        trim_what = value.utf8().unwrap_or("").to_owned();
                    }
                    let string_trimmed = trim_string(string, &trim_what, *trim_where);

                    Ok(Cow::Owned(DataValue::Utf8 {
                        value: string_trimmed,
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    }))
                } else {
                    Ok(Cow::Owned(DataValue::Null))
                }
            }
            ScalarExpression::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    values.push(arena.expression(*expr).eval(arena, tuple)?.into_owned());
                }
                Ok(Cow::Owned(DataValue::Tuple(values)))
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, args, .. }) => {
                let value = match tuple {
                    Some(tuple) => inner.eval(args, arena, Some(tuple))?,
                    None => inner.eval(args, arena, None)?,
                };
                value.cast(inner.return_type()).map(Cow::Owned)
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ty,
            } => {
                if arena.expression(*condition).eval(arena, tuple)?.is_true()? {
                    cast_cow(arena.expression(*left_expr).eval(arena, tuple)?, ty)
                } else {
                    cast_cow(arena.expression(*right_expr).eval(arena, tuple)?, ty)
                }
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = arena.expression(*left_expr).eval(arena, tuple)?;

                if value.is_null() {
                    value = arena.expression(*right_expr).eval(arena, tuple)?;
                }
                cast_cow(value, ty)
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = arena.expression(*left_expr).eval(arena, tuple)?;

                if arena.expression(*right_expr).eval(arena, tuple)? == value {
                    value = Cow::Owned(DataValue::Null);
                }
                cast_cow(value, ty)
            }
            ScalarExpression::Coalesce { exprs, ty } => {
                let mut value = None;

                for expr in exprs {
                    let temp = arena.expression(*expr).eval(arena, tuple)?;

                    if !temp.is_null() {
                        value = Some(temp);
                        break;
                    }
                }
                cast_cow(value.unwrap_or(Cow::Owned(DataValue::Null)), ty)
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ty,
            } => {
                let mut operand_value = None;
                let mut result = None;

                if let Some(expr) = operand_expr {
                    operand_value = Some(arena.expression(*expr).eval(arena, tuple)?);
                }
                for (when_expr, result_expr) in expr_pairs {
                    let mut when_value = arena.expression(*when_expr).eval(arena, tuple)?;
                    let is_true = if let Some(operand_value) = &operand_value {
                        let ty = operand_value.logical_type();
                        when_value = cast_cow(when_value, &ty)?;
                        let evaluator = binary_create(Cow::Owned(ty), BinaryOperator::Eq)?;
                        evaluator
                            .binary_eval(operand_value, &when_value)?
                            .is_true()?
                    } else {
                        when_value.is_true()?
                    };
                    if is_true {
                        result = Some(arena.expression(*result_expr).eval(arena, tuple)?);
                        break;
                    }
                }
                if result.is_none() {
                    if let Some(expr) = else_expr {
                        result = Some(arena.expression(*expr).eval(arena, tuple)?);
                    }
                }
                cast_cow(result.unwrap_or(Cow::Owned(DataValue::Null)), ty)
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::WindowCall(_) => Err(DatabaseError::UnsupportedStmt(
                "window calls must be evaluated by the window executor".to_string(),
            )),
        }
    }
}

fn cast_cow<'a>(
    value: Cow<'a, DataValue>,
    ty: &LogicalType,
) -> Result<Cow<'a, DataValue>, DatabaseError> {
    if value.logical_type() == *ty {
        Ok(value)
    } else {
        value.into_owned().cast(ty).map(Cow::Owned)
    }
}

fn trim_string(value: &str, trim_what: &str, trim_where: Option<TrimWhereField>) -> String {
    if trim_what.is_empty() {
        return value.to_string();
    }

    let mut trimmed = value;
    if matches!(
        trim_where,
        Some(TrimWhereField::Leading | TrimWhereField::Both) | None
    ) {
        while let Some(rest) = trimmed.strip_prefix(trim_what) {
            trimmed = rest;
        }
    }
    if matches!(
        trim_where,
        Some(TrimWhereField::Trailing | TrimWhereField::Both) | None
    ) {
        while let Some(rest) = trimmed.strip_suffix(trim_what) {
            trimmed = rest;
        }
    }
    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::test::PlanArenaTestExt;

    fn const_in(
        arena: &mut PlanArena<'_>,
        expr: DataValue,
        args: Vec<DataValue>,
        negated: bool,
    ) -> ExprRef {
        let expr = arena.alloc_expression(expr.into());
        let args = arena.alloc_expressions(args);
        arena.alloc_expression(ScalarExpression::In {
            negated,
            expr,
            args,
        })
    }

    #[test]
    fn eval_borrows_leaf_values_and_owns_binary_results() -> Result<(), DatabaseError> {
        use crate::types::evaluator::binary_create;
        use crate::types::tuple::Tuple;

        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let value = DataValue::Int64(42);
        let constant = arena.alloc_expression(ScalarExpression::Constant(value.clone()));
        let column_ref = arena.alloc_column(crate::catalog::ColumnCatalog::new(
            "value".to_owned(),
            true,
            crate::catalog::ColumnDesc::new(LogicalType::Bigint, None, false, None)?,
        ));
        let column = arena.alloc_expression(ScalarExpression::ColumnRef {
            column: column_ref,
            position: 0,
        });
        let sum = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Plus,
            left_expr: constant,
            right_expr: column,
            evaluator: Some(binary_create(
                Cow::Owned(LogicalType::Bigint),
                BinaryOperator::Plus,
            )?),
            ty: LogicalType::Bigint,
        });
        let tuple = Tuple::new(None, vec![value.clone()]);
        assert!(matches!(
            arena.expression(constant).eval(&arena, Some(&tuple)),
            Ok(Cow::Borrowed(_))
        ));
        assert!(matches!(
            arena.expression(column).eval(&arena, Some(&tuple)),
            Ok(Cow::Borrowed(_))
        ));
        assert_eq!(
            arena.expression(sum).eval(&arena, Some(&tuple))?,
            Cow::Owned(DataValue::Int64(84))
        );

        Ok(())
    }

    #[test]
    fn eval_borrows_passthrough_branches_and_owns_casts() -> Result<(), DatabaseError> {
        use crate::types::evaluator::cast_create;
        use crate::types::tuple::Tuple;

        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let value = DataValue::Int32(7);
        let column_ref = arena.alloc_column(crate::catalog::ColumnCatalog::new(
            "value".to_owned(),
            true,
            crate::catalog::ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let column = arena.alloc_expression(ScalarExpression::ColumnRef {
            column: column_ref,
            position: 0,
        });
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: column,
            alias: AliasType::Name("alias".to_owned()),
        });
        let no_op_cast = arena.alloc_expression(ScalarExpression::TypeCast {
            expr: column,
            ty: LogicalType::Integer,
            evaluator: None,
        });
        let condition =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let null = arena.alloc_expression(ScalarExpression::Constant(DataValue::Null));
        let if_expr = arena.alloc_expression(ScalarExpression::If {
            condition,
            left_expr: column,
            right_expr: null,
            ty: LogicalType::Integer,
        });
        let if_null = arena.alloc_expression(ScalarExpression::IfNull {
            left_expr: null,
            right_expr: column,
            ty: LogicalType::Integer,
        });
        let coalesce = arena.alloc_expression(ScalarExpression::Coalesce {
            exprs: vec![null, column],
            ty: LogicalType::Integer,
        });
        let cast = arena.alloc_expression(ScalarExpression::TypeCast {
            expr: column,
            ty: LogicalType::Bigint,
            evaluator: Some(cast_create(&LogicalType::Integer, &LogicalType::Bigint)?),
        });
        let tuple = Tuple::new(None, vec![value.clone()]);
        for expr in [alias, no_op_cast, if_expr, if_null, coalesce] {
            match arena.expression(expr).eval(&arena, Some(&tuple))? {
                Cow::Borrowed(actual) => assert!(std::ptr::eq(actual, &tuple.values[0])),
                Cow::Owned(_) => panic!("pass-through expression must borrow the column"),
            }
        }
        assert_eq!(
            arena.expression(cast).eval(&arena, Some(&tuple))?,
            Cow::Owned(DataValue::Int64(7))
        );
        Ok(())
    }

    #[test]
    fn in_eval_matches_even_if_null_appears_first() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let expr = const_in(
            &mut arena,
            DataValue::Int32(1),
            vec![DataValue::Null, DataValue::Int32(1)],
            false,
        );

        assert_eq!(
            arena.expression(expr).eval(&arena, None)?.into_owned(),
            DataValue::Boolean(true)
        );
        Ok(())
    }

    #[test]
    fn in_eval_returns_null_when_only_null_blocks_non_match() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let expr = const_in(
            &mut arena,
            DataValue::Int32(2),
            vec![DataValue::Null, DataValue::Int32(1)],
            false,
        );

        assert_eq!(
            arena.expression(expr).eval(&arena, None)?.into_owned(),
            DataValue::Null
        );
        Ok(())
    }

    #[test]
    fn not_in_eval_matches_even_if_null_appears_first() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let expr = const_in(
            &mut arena,
            DataValue::Int32(1),
            vec![DataValue::Null, DataValue::Int32(1)],
            true,
        );

        assert_eq!(
            arena.expression(expr).eval(&arena, None)?.into_owned(),
            DataValue::Boolean(false)
        );
        Ok(())
    }

    #[test]
    fn trim_string_removes_requested_sides() {
        assert_eq!(trim_string("xxhelloxx", "x", None), "hello");
        assert_eq!(
            trim_string("xxhelloxx", "x", Some(TrimWhereField::Both)),
            "hello"
        );
        assert_eq!(
            trim_string("xxhelloxx", "x", Some(TrimWhereField::Leading)),
            "helloxx"
        );
        assert_eq!(
            trim_string("xxhelloxx", "x", Some(TrimWhereField::Trailing)),
            "xxhello"
        );
        assert_eq!(trim_string("ababhelloab", "ab", None), "hello");
        assert_eq!(trim_string("hello", "", None), "hello");
    }
}
