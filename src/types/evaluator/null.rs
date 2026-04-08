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
use crate::types::evaluator::DataValue;
use crate::types::evaluator::{BinaryEvaluator, CastEvaluator};
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

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct ToSqlNullCastEvaluator;

#[typetag::serde]
impl CastEvaluator for ToSqlNullCastEvaluator {
    fn eval_cast(&self, _value: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Null)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct NullCastEvaluator;

#[typetag::serde]
impl CastEvaluator for NullCastEvaluator {
    fn eval_cast(&self, _value: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Null)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;

    #[test]
    fn test_null_cast_evaluators() {
        assert_eq!(
            ToSqlNullCastEvaluator
                .eval_cast(&DataValue::Int32(1))
                .unwrap(),
            DataValue::Null
        );
        assert_eq!(
            NullCastEvaluator.eval_cast(&DataValue::Null).unwrap(),
            DataValue::Null
        );
    }
}
