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
use crate::expression::{AliasType, BinaryOperator, ScalarExpression};
use crate::types::evaluator::binary_create;
use crate::types::tuple::TupleLike;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use regex::Regex;
use sqlparser::ast::{CharLengthUnits, TrimWhereField};
use std::borrow::Cow;
use std::cmp;
use std::cmp::Ordering;

macro_rules! eval_to_num {
    ($num_expr:expr, $tuple:expr) => {
        if let Some(num_i32) = $num_expr.eval($tuple)?.cast(&LogicalType::Integer)?.i32() {
            num_i32
        } else {
            return Ok(DataValue::Null);
        }
    };
}

impl ScalarExpression {
    pub fn eval<T: TupleLike + Copy>(&self, tuple: Option<T>) -> Result<DataValue, DatabaseError> {
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
                    match inner_expr.eval(Some(tuple)) {
                        Err(DatabaseError::UnbindExpressionPosition(_)) => expr.eval(Some(tuple)),
                        res => res,
                    }
                } else {
                    expr.eval(Some(tuple))
                }
            }
            ScalarExpression::TypeCast {
                expr, evaluator, ..
            } => {
                let value = expr.eval(tuple)?;
                if let Some(evaluator) = evaluator {
                    evaluator.eval_cast(&value)
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
                let left = left_expr.eval(tuple)?;
                let right = right_expr.eval(tuple)?;

                evaluator
                    .as_ref()
                    .ok_or(DatabaseError::EvaluatorNotFound)?
                    .0
                    .binary_eval(&left, &right)
            }
            ScalarExpression::IsNull { expr, negated } => {
                let mut is_null = expr.eval(tuple)?.is_null();
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
                let value = expr.eval(tuple)?;
                if value.is_null() {
                    return Ok(DataValue::Null);
                }

                let mut matched = false;
                let mut saw_null = false;
                for arg in args {
                    let arg_value = arg.eval(tuple)?;

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
                let value = expr.eval(tuple)?;

                Ok(evaluator
                    .as_ref()
                    .ok_or(DatabaseError::EvaluatorNotFound)?
                    .0
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
                let value = expr.eval(tuple)?;
                let left = left_expr.eval(tuple)?;
                let right = right_expr.eval(tuple)?;

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
                if let Some(mut string) = expr
                    .eval(tuple)?
                    .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                    .utf8()
                    .map(String::from)
                {
                    if let Some(from_expr) = from_expr {
                        let mut from = eval_to_num!(from_expr, tuple).saturating_sub(1);
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
                        let for_i = cmp::min(eval_to_num!(for_expr, tuple) as usize, string.len());
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
                let unpack = |expr: &ScalarExpression| -> Result<String, DatabaseError> {
                    Ok(expr
                        .eval(tuple)?
                        .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                        .utf8()
                        .map(String::from)
                        .unwrap_or("".to_owned()))
                };
                let pattern = unpack(expr)?;
                let str = unpack(in_expr)?;
                Ok(DataValue::Int32(
                    str.find(&pattern).map(|pos| pos as i32 + 1).unwrap_or(0),
                ))
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                trim_where,
            } => {
                if let Some(string) = expr
                    .eval(tuple)?
                    .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                    .utf8()
                {
                    let mut trim_what = String::from(" ");
                    if let Some(trim_what_expr) = trim_what_expr {
                        trim_what = trim_what_expr
                            .eval(tuple)?
                            .cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?
                            .utf8()
                            .map(String::from)
                            .unwrap_or_default();
                    }
                    let trim_regex = match trim_where {
                        Some(TrimWhereField::Both) | None => Regex::new(&format!(
                            r"^(?:{0})*([\w\W]*?)(?:{0})*$",
                            regex::escape(&trim_what)
                        ))
                        .unwrap(),
                        Some(TrimWhereField::Leading) => {
                            Regex::new(&format!(r"^(?:{0})*([\w\W]*?)", regex::escape(&trim_what)))
                                .unwrap()
                        }
                        Some(TrimWhereField::Trailing) => {
                            Regex::new(&format!(r"([\w\W]*?)(?:{0})*$", regex::escape(&trim_what)))
                                .unwrap()
                        }
                    };
                    let string_trimmed = trim_regex.replace_all(string, "$1").to_string();

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
                    values.push(expr.eval(tuple)?);
                }
                Ok(DataValue::Tuple(values, false))
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, args, .. }) => {
                let value = match tuple {
                    Some(tuple) => inner.eval(args, Some(&tuple as &dyn TupleLike))?,
                    None => inner.eval(args, None)?,
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
                if condition.eval(tuple)?.is_true()? {
                    left_expr.eval(tuple)?.cast(ty)
                } else {
                    right_expr.eval(tuple)?.cast(ty)
                }
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = left_expr.eval(tuple)?;

                if value.is_null() {
                    value = right_expr.eval(tuple)?;
                }
                value.cast(ty)
            }
            ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ty,
            } => {
                let mut value = left_expr.eval(tuple)?;

                if right_expr.eval(tuple)? == value {
                    value = DataValue::Null;
                }
                value.cast(ty)
            }
            ScalarExpression::Coalesce { exprs, ty } => {
                let mut value = None;

                for expr in exprs {
                    let temp = expr.eval(tuple)?;

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
                    operand_value = Some(expr.eval(tuple)?);
                }
                for (when_expr, result_expr) in expr_pairs {
                    let mut when_value = when_expr.eval(tuple)?;
                    let is_true = if let Some(operand_value) = &operand_value {
                        let ty = operand_value.logical_type();
                        when_value = when_value.cast(&ty)?;
                        let evaluator =
                            binary_create(Cow::Owned(ty), BinaryOperator::Eq)?;
                        evaluator
                            .0
                            .binary_eval(operand_value, &when_value)?
                            .is_true()?
                    } else {
                        when_value.is_true()?
                    };
                    if is_true {
                        result = Some(result_expr.eval(tuple)?);
                        break;
                    }
                }
                if result.is_none() {
                    if let Some(expr) = else_expr {
                        result = Some(expr.eval(tuple)?);
                    }
                }
                result.unwrap_or(DataValue::Null).cast(ty)
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn const_in(expr: DataValue, args: Vec<DataValue>, negated: bool) -> ScalarExpression {
        ScalarExpression::In {
            negated,
            expr: Box::new(ScalarExpression::Constant(expr)),
            args: args.into_iter().map(ScalarExpression::Constant).collect(),
        }
    }

    #[test]
    fn in_eval_matches_even_if_null_appears_first() -> Result<(), DatabaseError> {
        let expr = const_in(
            DataValue::Int32(1),
            vec![DataValue::Null, DataValue::Int32(1)],
            false,
        );

        assert_eq!(expr.eval::<&[DataValue]>(None)?, DataValue::Boolean(true));
        Ok(())
    }

    #[test]
    fn in_eval_returns_null_when_only_null_blocks_non_match() -> Result<(), DatabaseError> {
        let expr = const_in(
            DataValue::Int32(2),
            vec![DataValue::Null, DataValue::Int32(1)],
            false,
        );

        assert_eq!(expr.eval::<&[DataValue]>(None)?, DataValue::Null);
        Ok(())
    }

    #[test]
    fn not_in_eval_matches_even_if_null_appears_first() -> Result<(), DatabaseError> {
        let expr = const_in(
            DataValue::Int32(1),
            vec![DataValue::Null, DataValue::Int32(1)],
            true,
        );

        assert_eq!(expr.eval::<&[DataValue]>(None)?, DataValue::Boolean(false));
        Ok(())
    }
}
