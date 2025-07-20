use crate::errors::DatabaseError;
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use crate::types::value::{ONE_DAY_TO_SEC, ONE_SEC_TO_NANO};
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimePlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeMinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeLtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct TimeNotEqBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for TimePlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2)) => {
                let (mut v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                let mut n = n1 + n2;
                while n > ONE_SEC_TO_NANO {
                    v1 += 1;
                    n -= ONE_SEC_TO_NANO;
                }
                let p = if p2 > p1 { *p2 } else { *p1 };
                if v1 + v2 > ONE_DAY_TO_SEC {
                    return Ok(DataValue::Null);
                }
                DataValue::Time32(DataValue::pack(v1 + v2, n, p), p)
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeMinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (mut v1, mut n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                while n1 < n2 {
                    v1 -= 1;
                    n1 += ONE_SEC_TO_NANO;
                }
                if v1 < v2 {
                    return Ok(DataValue::Null);
                }
                let p = if p2 > p1 { *p2 } else { *p1 };
                DataValue::Time32(DataValue::pack(v1 - v2, n1 - n2, p), p)
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

#[typetag::serde]
impl BinaryEvaluator for TimeGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                DataValue::Boolean(v1.cmp(&v2).then_with(|| n1.cmp(&n2)).is_gt())
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                DataValue::Boolean(!v1.cmp(&v2).then_with(|| n1.cmp(&n2)).is_lt())
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                DataValue::Boolean(v1.cmp(&v2).then_with(|| n1.cmp(&n2)).is_lt())
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                DataValue::Boolean(!v1.cmp(&v2).then_with(|| n1.cmp(&n2)).is_gt())
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                DataValue::Boolean(v1.cmp(&v2).then_with(|| n1.cmp(&n2)).is_eq())
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for TimeNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time32(v1, p1), DataValue::Time32(v2, p2, ..)) => {
                let (v1, n1) = DataValue::unpack(*v1, *p1);
                let (v2, n2) = DataValue::unpack(*v2, *p2);
                DataValue::Boolean(!v1.cmp(&v2).then_with(|| n1.cmp(&n2)).is_eq())
            }
            (DataValue::Time32(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time32(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
