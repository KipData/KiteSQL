use crate::errors::DatabaseError;
use crate::types::evaluator::DataValue;
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32PlusUnaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32MinusUnaryEvaluator;

#[typetag::serde]
impl UnaryEvaluator for Float32PlusUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        value.clone()
    }
}
#[typetag::serde]
impl UnaryEvaluator for Float32MinusUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        match value {
            DataValue::Float32(value) => DataValue::Float32(-value),
            DataValue::Null => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32PlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32MinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32MultiplyBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32DivideBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32NotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32ModBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for Float32PlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 + *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32MinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 - *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32MultiplyBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 * *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32DivideBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => {
                DataValue::Float64(ordered_float::OrderedFloat(**v1 as f64 / **v2 as f64))
            }
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 > v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 >= v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 < v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 <= v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 == v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 != v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32ModBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 % *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
