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
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use serde::{Deserialize, Serialize};
use std::hint;

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
impl BinaryEvaluator for Time64GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                if let (Some(v1), Some(v2)) = (
                    DataValue::from_timestamp_precision(*v1, *p1),
                    DataValue::from_timestamp_precision(*v2, *p2),
                ) {
                    let p = if p2 > p1 { *p2 } else { *p1 };
                    DataValue::Boolean(
                        DataValue::timestamp_precision(v1, p)
                            > DataValue::timestamp_precision(v2, p),
                    )
                } else {
                    DataValue::Null
                }
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                if let (Some(v1), Some(v2)) = (
                    DataValue::from_timestamp_precision(*v1, *p1),
                    DataValue::from_timestamp_precision(*v2, *p2),
                ) {
                    let p = if p2 > p1 { *p2 } else { *p1 };
                    DataValue::Boolean(
                        DataValue::timestamp_precision(v1, p)
                            >= DataValue::timestamp_precision(v2, p),
                    )
                } else {
                    DataValue::Null
                }
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                if let (Some(v1), Some(v2)) = (
                    DataValue::from_timestamp_precision(*v1, *p1),
                    DataValue::from_timestamp_precision(*v2, *p2),
                ) {
                    let p = if p2 > p1 { *p2 } else { *p1 };
                    DataValue::Boolean(
                        DataValue::timestamp_precision(v1, p)
                            < DataValue::timestamp_precision(v2, p),
                    )
                } else {
                    DataValue::Null
                }
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                if let (Some(v1), Some(v2)) = (
                    DataValue::from_timestamp_precision(*v1, *p1),
                    DataValue::from_timestamp_precision(*v2, *p2),
                ) {
                    let p = if p2 > p1 { *p2 } else { *p1 };
                    DataValue::Boolean(
                        DataValue::timestamp_precision(v1, p)
                            <= DataValue::timestamp_precision(v2, p),
                    )
                } else {
                    DataValue::Null
                }
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                if let (Some(v1), Some(v2)) = (
                    DataValue::from_timestamp_precision(*v1, *p1),
                    DataValue::from_timestamp_precision(*v2, *p2),
                ) {
                    let p = if p2 > p1 { *p2 } else { *p1 };
                    DataValue::Boolean(
                        DataValue::timestamp_precision(v1, p)
                            == DataValue::timestamp_precision(v2, p),
                    )
                } else {
                    DataValue::Null
                }
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Time64NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                if let (Some(v1), Some(v2)) = (
                    DataValue::from_timestamp_precision(*v1, *p1),
                    DataValue::from_timestamp_precision(*v2, *p2),
                ) {
                    let p = if p2 > p1 { *p2 } else { *p1 };
                    DataValue::Boolean(
                        DataValue::timestamp_precision(v1, p)
                            != DataValue::timestamp_precision(v2, p),
                    )
                } else {
                    DataValue::Null
                }
            }
            (DataValue::Time64(..), DataValue::Null)
            | (DataValue::Null, DataValue::Time64(..))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
