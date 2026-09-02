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
use crate::planner::{ExprRef, PlanArena};
use crate::types::evaluator::binary_create;
use crate::types::tuple::TupleLike;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{CharLengthUnits, LogicalType};
use std::borrow::Cow;
use std::cmp;
use std::cmp::Ordering;

macro_rules! eval_to_num {
    ($num_expr:expr, $arena:expr, $tuple:expr) => {
        if let Some(num_i32) = $arena
            .expression(*$num_expr)
            .eval($arena, $tuple)?
            .cast(&LogicalType::Integer)?
            .i32()
        {
            num_i32
        } else {
            return Ok(DataValue::Null);
        }
    };
}

impl ScalarExpression {
    pub fn eval<T: TupleLike + Copy>(
        &self,
        arena: &PlanArena<'_>,
        tuple: Option<T>,
    ) -> Result<DataValue, DatabaseError> {
        match self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef { position, .. } => {
                let Some(tuple) = tuple else {
                    return Ok(DataValue::Null);
                };
                Ok(tuple.value_at(*position).clone())
            }
            ScalarExpression::Alias { expr, alias } => {
                let Some(tuple) = tuple else {
                    return Ok(DataValue::Null);
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
                    evaluator.eval(&value)
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
            }
            ScalarExpression::IsNull { expr, negated } => {
                let mut is_null = arena.expression(*expr).eval(arena, tuple)?.is_null();
                if *negated {
                    is_null = !is_null;
                }
                Ok(DataValue::Boolean(is_null))
            }
            ScalarExpression::In {
                expr,
                args,
                negated,
            } => {
                let value = arena.expression(*expr).eval(arena, tuple)?;
                if value.is_null() {
                    return Ok(DataValue::Null);
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
                    Ok(DataValue::Boolean(!negated))
                } else if saw_null {
                    Ok(DataValue::Null)
                } else {
                    Ok(DataValue::Boolean(*negated))
                }
            }
            ScalarExpression::Unary {
                expr, evaluator, ..
            } => {
                let value = arena.expression(*expr).eval(arena, tuple)?;

                Ok(evaluator
                    .as_ref()
                    .ok_or(DatabaseError::EvaluatorNotFound)?
                    .unary_eval(&value))
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
                    (None, _) | (_, None) => return Ok(DataValue::Null),
                    _ => false,
                };
                if *negated {
                    is_between = !is_between;
                }
                Ok(DataValue::Boolean(is_between))
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                if let Some(mut string) = arena
                    .expression(*expr)
                    .eval(arena, tuple)?
                    .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                    .utf8()
                    .map(String::from)
                {
                    if let Some(from_expr) = from_expr {
                        let mut from = eval_to_num!(from_expr, arena, tuple).saturating_sub(1);
                        let len_i = string.len() as i32;

                        while from < 0 {
                            from += len_i + 1;
                        }
                        if from > len_i {
                            return Ok(DataValue::Null);
                        }
                        string = string.split_off(from as usize);
                    }
                    if let Some(for_expr) = for_expr {
                        let for_i =
                            cmp::min(eval_to_num!(for_expr, arena, tuple) as usize, string.len());
                        let _ = string.split_off(for_i);
                    }

                    Ok(DataValue::Utf8 {
                        value: string,
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    })
                } else {
                    Ok(DataValue::Null)
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                let unpack = |expr: ExprRef| -> Result<String, DatabaseError> {
                    Ok(arena
                        .expression(expr)
                        .eval(arena, tuple)?
                        .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                        .utf8()
                        .map(String::from)
                        .unwrap_or("".to_owned()))
                };
                let pattern = unpack(*expr)?;
                let str = unpack(*in_expr)?;
                Ok(DataValue::Int32(
                    str.find(&pattern).map(|pos| pos as i32 + 1).unwrap_or(0),
                ))
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                if let Some(string) = arena
                    .expression(*expr)
                    .eval(arena, tuple)?
                    .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                    .utf8()
                {
                    let mut trim_what = String::from(" ");
                    if let Some(trim_what_expr) = trim_what_expr {
                        trim_what = arena
                            .expression(*trim_what_expr)
                            .eval(arena, tuple)?
                            .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                            .utf8()
                            .map(String::from)
                            .unwrap_or_default();
                    }
                    let string_trimmed = trim_string(string, &trim_what, *trim_where);

                    Ok(DataValue::Utf8 {
                        value: string_trimmed,
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    })
                } else {
                    Ok(DataValue::Null)
                }
            }
            ScalarExpression::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    values.push(arena.expression(*expr).eval(arena, tuple)?);
                }
                Ok(DataValue::Tuple(values, false))
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, args, .. }) => {
                let value = match tuple {
                    Some(tuple) => inner.eval(args, arena, Some(&tuple as &dyn TupleLike))?,
                    None => inner.eval(args, arena, None)?,
                };
                value.cast(inner.return_type())
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ty,
            } => {
                if arena.expression(*condition).eval(arena, tuple)?.is_true()? {
                    arena.expression(*left_expr).eval(arena, tuple)?.cast(ty)
                } else {
                    arena.expression(*right_expr).eval(arena, tuple)?.cast(ty)
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
                value.cast(ty)
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = arena.expression(*left_expr).eval(arena, tuple)?;

                if arena.expression(*right_expr).eval(arena, tuple)? == value {
                    value = DataValue::Null;
                }
                value.cast(ty)
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
                value.unwrap_or(DataValue::Null).cast(ty)
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
                        when_value = when_value.cast(&ty)?;
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
                result.unwrap_or(DataValue::Null).cast(ty)
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
            ScalarExpression::WindowCall(_) => Err(DatabaseError::UnsupportedStmt(
                "window calls must be evaluated by the window executor".to_string(),
            )),
        }
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

    fn const_in(
        arena: &mut PlanArena,
        expr: DataValue,
        args: Vec<DataValue>,
        negated: bool,
    ) -> ExprRef {
        let expr = arena.alloc_expression(ScalarExpression::Constant(expr));
        let args = args
            .into_iter()
            .map(|value| arena.alloc_expression(ScalarExpression::Constant(value)))
            .collect();
        arena.alloc_expression(ScalarExpression::In {
            negated,
            expr,
            args,
        })
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
            arena.expression(expr).eval::<&[DataValue]>(&arena, None)?,
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
            arena.expression(expr).eval::<&[DataValue]>(&arena, None)?,
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
            arena.expression(expr).eval::<&[DataValue]>(&arena, None)?,
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
