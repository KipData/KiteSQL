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
