use crate::errors::DatabaseError;
use crate::types::evaluator::DataValue;
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanNotUnaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanAndBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanOrBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanNotEqBinaryEvaluator;

#[typetag::serde]
impl UnaryEvaluator for BooleanNotUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        match value {
            DataValue::Boolean(value) => DataValue::Boolean(!value),
            DataValue::Null => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for BooleanAndBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 && *v2),
            (DataValue::Boolean(false), DataValue::Null)
            | (DataValue::Null, DataValue::Boolean(false)) => DataValue::Boolean(false),
            (DataValue::Null, DataValue::Null)
            | (DataValue::Boolean(true), DataValue::Null)
            | (DataValue::Null, DataValue::Boolean(true)) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

#[typetag::serde]
impl BinaryEvaluator for BooleanOrBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 || *v2),
            (DataValue::Boolean(true), DataValue::Null)
            | (DataValue::Null, DataValue::Boolean(true)) => DataValue::Boolean(true),
            (DataValue::Null, DataValue::Null)
            | (DataValue::Boolean(false), DataValue::Null)
            | (DataValue::Null, DataValue::Boolean(false)) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

#[typetag::serde]
impl BinaryEvaluator for BooleanEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 == *v2),
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

#[typetag::serde]
impl BinaryEvaluator for BooleanNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 != *v2),
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
