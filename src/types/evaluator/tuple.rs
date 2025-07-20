use crate::errors::DatabaseError;
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleNotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TupleLtEqBinaryEvaluator;

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

#[typetag::serde]
impl BinaryEvaluator for TupleEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Tuple(v1, ..), DataValue::Tuple(v2, ..)) => DataValue::Boolean(*v1 == *v2),
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Tuple(v1, ..), DataValue::Tuple(v2, ..)) => DataValue::Boolean(*v1 != *v2),
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Tuple(v1, is_upper1), DataValue::Tuple(v2, is_upper2)) => {
                tuple_cmp((v1, is_upper1), (v2, is_upper2))
                    .map(|order| DataValue::Boolean(order.is_gt()))
                    .unwrap_or(DataValue::Null)
            }
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Tuple(v1, is_upper1), DataValue::Tuple(v2, is_upper2)) => {
                tuple_cmp((v1, is_upper1), (v2, is_upper2))
                    .map(|order| DataValue::Boolean(order.is_ge()))
                    .unwrap_or(DataValue::Null)
            }
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Tuple(v1, is_upper1), DataValue::Tuple(v2, is_upper2)) => {
                tuple_cmp((v1, is_upper1), (v2, is_upper2))
                    .map(|order| DataValue::Boolean(order.is_lt()))
                    .unwrap_or(DataValue::Null)
            }
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TupleLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Tuple(v1, is_upper1), DataValue::Tuple(v2, is_upper2)) => {
                tuple_cmp((v1, is_upper1), (v2, is_upper2))
                    .map(|order| DataValue::Boolean(order.is_le()))
                    .unwrap_or(DataValue::Null)
            }
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
