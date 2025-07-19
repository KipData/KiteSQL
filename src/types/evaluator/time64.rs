use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64PlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64MinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Time64NotEqBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for Time64PlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, ..)) => {
                DataValue::Time64(v1 + v2, if p2 > p1 { *p2 } else { *p1 }, false)
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64MinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, ..)) => {
                DataValue::Time64(v1 - v2, if p2 > p1 { *p2 } else { *p1 }, false)
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

#[typetag::serde]
impl BinaryEvaluator for Time64GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, ..), DataValue::Time64(v2, ..)) => DataValue::Boolean(v1 > v2),
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, ..), DataValue::Time64(v2, ..)) => DataValue::Boolean(v1 >= v2),
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, ..), DataValue::Time64(v2, ..)) => DataValue::Boolean(v1 < v2),
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, ..), DataValue::Time64(v2, ..)) => DataValue::Boolean(v1 <= v2),
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, ..), DataValue::Time64(v2, ..)) => DataValue::Boolean(v1 == v2),
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        match (left, right) {
            (DataValue::Time64(v1, ..), DataValue::Time64(v2, ..)) => DataValue::Boolean(v1 != v2),
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
