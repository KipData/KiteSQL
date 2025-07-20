use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::{AliasType, BinaryOperator, ScalarExpression};
use crate::types::evaluator::EvaluatorFactory;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use itertools::Itertools;
use regex::Regex;
use sqlparser::ast::{CharLengthUnits, TrimWhereField};
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
    pub fn eval(&self, tuple: Option<(&Tuple, &[ColumnRef])>) -> Result<DataValue, DatabaseError> {
        let check_cast = |value: DataValue, return_type: &LogicalType| {
            if value.logical_type() != *return_type {
                return value.cast(return_type);
            }
            Ok(value)
        };

        match self {
            ScalarExpression::Constant(val) => Ok(val.clone()),
            ScalarExpression::ColumnRef(col) => {
                let Some((tuple, schema)) = tuple else {
                    return Ok(DataValue::Null);
                };
                let value = schema
                    .iter()
                    .find_position(|tul_col| tul_col.summary() == col.summary())
                    .map(|(i, _)| tuple.values[i].clone())
                    .unwrap_or(DataValue::Null);

                Ok(value)
            }
            ScalarExpression::Alias { expr, alias } => {
                let Some((tuple, schema)) = tuple else {
                    return Ok(DataValue::Null);
                };
                if let Some(value) = schema
                    .iter()
                    .find_position(|tul_col| match alias {
                        AliasType::Name(alias) => {
                            tul_col.table_name().is_none() && tul_col.name() == alias
                        }
                        AliasType::Expr(alias_expr) => {
                            alias_expr.output_column().summary() == tul_col.summary()
                        }
                    })
                    .map(|(i, _)| tuple.values[i].clone())
                {
                    return Ok(value.clone());
                }

                expr.eval(Some((tuple, schema)))
            }
            ScalarExpression::TypeCast { expr, ty, .. } => Ok(expr.eval(tuple)?.cast(ty)?),
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
                let mut is_in = false;
                for arg in args {
                    let arg_value = arg.eval(tuple)?;

                    if arg_value.is_null() {
                        return Ok(DataValue::Null);
                    }
                    if arg_value == value {
                        is_in = true;
                        break;
                    }
                }
                if *negated {
                    is_in = !is_in;
                }
                Ok(DataValue::Boolean(is_in))
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
            ScalarExpression::Reference { pos, .. } => {
                let Some((tuple, _)) = tuple else {
                    return Ok(DataValue::Null);
                };
                Ok(tuple.values.get(*pos).cloned().unwrap_or(DataValue::Null))
            }
            ScalarExpression::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    values.push(expr.eval(tuple)?);
                }
                Ok(DataValue::Tuple(values, false))
            }
            ScalarExpression::ScalaFunction(ScalarFunction { inner, args, .. }) => {
                inner.eval(args, tuple)?.cast(inner.return_type())
            }
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ty,
            } => {
                if condition.eval(tuple)?.is_true()? {
                    check_cast(left_expr.eval(tuple)?, ty)
                } else {
                    check_cast(right_expr.eval(tuple)?, ty)
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
                check_cast(value, ty)
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
                check_cast(value, ty)
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
                check_cast(value.unwrap_or(DataValue::Null), ty)
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
                        let evaluator =
                            EvaluatorFactory::binary_create(ty.clone(), BinaryOperator::Eq)?;

                        if when_value.logical_type() != ty {
                            when_value = when_value.cast(&ty)?;
                        }
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
                check_cast(result.unwrap_or(DataValue::Null), ty)
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
        }
    }
}
