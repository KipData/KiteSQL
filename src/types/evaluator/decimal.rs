use crate::errors::DatabaseError;
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalPlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalMinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalMultiplyBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalDivideBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalLtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalNotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalModBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for DecimalPlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 + v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalMinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 - v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalMultiplyBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 * v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalDivideBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 / v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 > v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 >= v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 < v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 <= v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 == v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 != v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalModBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 % v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
