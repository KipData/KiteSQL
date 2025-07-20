use crate::errors::DatabaseError;
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};

/// Tips:
/// - Null values operate as null values
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct NullBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for NullBinaryEvaluator {
    fn binary_eval(&self, _: &DataValue, _: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Null)
    }
}
