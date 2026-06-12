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
use crate::types::evaluator::CastEvaluatorRef;
use crate::types::evaluator::DataValue;
use std::cmp::Ordering;
use std::hint;

fn tuple_cmp(
    (v1, v1_is_upper): (&Vec<DataValue>, &bool),
    (v2, v2_is_upper): (&Vec<DataValue>, &bool),
) -> Option<Ordering> {
    let mut order = Ordering::Equal;
    let mut v1_iter = v1.iter();
    let mut v2_iter = v2.iter();

    while order == Ordering::Equal {
        order = match (v1_iter.next(), v2_iter.next()) {
            (Some(v1), Some(v2)) => v1.partial_cmp(v2)?,
            (Some(_), None) => {
                if *v2_is_upper {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (None, Some(_)) => {
                if *v1_is_upper {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (None, None) => break,
        }
    }
    Some(order)
}
pub fn tuple_eq_binary_eval(
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Tuple(v1, ..), DataValue::Tuple(v2, ..)) => DataValue::Boolean(*v1 == *v2),
        (DataValue::Null, DataValue::Boolean(_))
        | (DataValue::Boolean(_), DataValue::Null)
        | (DataValue::Null, DataValue::Null) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}
pub fn tuple_not_eq_binary_eval(
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Tuple(v1, ..), DataValue::Tuple(v2, ..)) => DataValue::Boolean(*v1 != *v2),
        (DataValue::Null, DataValue::Boolean(_))
        | (DataValue::Boolean(_), DataValue::Null)
        | (DataValue::Null, DataValue::Null) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}

macro_rules! tuple_order_binary {
    ($name:ident, $is_order:ident) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Tuple(v1, is_upper1), DataValue::Tuple(v2, is_upper2)) => {
                    tuple_cmp((v1, is_upper1), (v2, is_upper2))
                        .map(|order| DataValue::Boolean(order.$is_order()))
                        .unwrap_or(DataValue::Null)
                }
                (DataValue::Null, DataValue::Boolean(_))
                | (DataValue::Boolean(_), DataValue::Null)
                | (DataValue::Null, DataValue::Null) => DataValue::Null,
                _ => unsafe { hint::unreachable_unchecked() },
            })
        }
    };
}

tuple_order_binary!(tuple_gt_binary_eval, is_gt);
tuple_order_binary!(tuple_gt_eq_binary_eval, is_ge);
tuple_order_binary!(tuple_lt_binary_eval, is_lt);
tuple_order_binary!(tuple_lt_eq_binary_eval, is_le);

pub(crate) fn eval_tuple_cast(
    element_evaluators: &[CastEvaluatorRef],
    value: &DataValue,
) -> Result<DataValue, DatabaseError> {
    match value {
        DataValue::Null => Ok(DataValue::Null),
        DataValue::Tuple(values, is_upper) => {
            let mut casted = Vec::with_capacity(values.len());

            for (value, evaluator) in values.iter().zip(element_evaluators.iter()) {
                casted.push(evaluator.eval(value)?);
            }

            Ok(DataValue::Tuple(casted, *is_upper))
        }
        _ => unsafe { hint::unreachable_unchecked() },
    }
}
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::cast_create;
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use std::borrow::Cow;

    #[test]
    fn test_tuple_cast_eval() {
        let evaluator = cast_create(
            Cow::Owned(LogicalType::Tuple(vec![
                LogicalType::Integer,
                LogicalType::Varchar(None, CharLengthUnits::Characters),
            ])),
            Cow::Owned(LogicalType::Tuple(vec![
                LogicalType::Bigint,
                LogicalType::Integer,
            ])),
        )
        .unwrap();

        assert_eq!(
            evaluator
                .eval(&DataValue::Tuple(
                    vec![
                        DataValue::Int32(1),
                        DataValue::Utf8 {
                            value: "2".to_string(),
                            ty: crate::types::value::Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        },
                    ],
                    false,
                ))
                .unwrap(),
            DataValue::Tuple(vec![DataValue::Int64(1), DataValue::Int32(2)], false)
        );
    }
}
