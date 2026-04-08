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
use crate::types::evaluator::cast::{cast_fail, to_char, to_varchar};
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use crate::types::value::{ONE_DAY_TO_SEC, ONE_SEC_TO_NANO};
use crate::types::LogicalType;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
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

crate::define_cast_evaluator!(
    Time32ToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits,
        to: LogicalType
    },
    DataValue::Time32(value, precision) => |this| {
        to_char(
            DataValue::format_time(*value, *precision).ok_or_else(|| {
                cast_fail(LogicalType::Time(Some(*precision)), this.to.clone())
            })?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    Time32ToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits,
        to: LogicalType
    },
    DataValue::Time32(value, precision) => |this| {
        to_varchar(
            DataValue::format_time(*value, *precision).ok_or_else(|| {
                cast_fail(LogicalType::Time(Some(*precision)), this.to.clone())
            })?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    Time32ToTimeCastEvaluator {
        precision: Option<u64>
    },
    DataValue::Time32(value, _precision) => |this| {
        Ok(DataValue::Time32(*value, this.precision.unwrap_or(0)))
    }
);

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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::{BinaryEvaluator, CastEvaluator};
    use crate::types::value::Utf8Type;

    #[test]
    fn test_time32_binary_evaluators() {
        assert_eq!(
            TimePlusBinaryEvaluator
                .binary_eval(
                    &DataValue::Time32(4_190_119_896, 3),
                    &DataValue::Time32(2_621_204_256, 4),
                )
                .unwrap(),
            DataValue::Time32(2_618_593_017, 4)
        );
        assert_eq!(
            TimeGtBinaryEvaluator
                .binary_eval(
                    &DataValue::Time32(2_621_204_256, 4),
                    &DataValue::Time32(4_190_119_896, 3),
                )
                .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_time32_cast_evaluators() {
        let value = DataValue::Time32(DataValue::pack(3 * 3600 + 4 * 60 + 5, 123_000_000, 3), 3);
        assert_eq!(
            Time32ToCharCastEvaluator {
                len: 12,
                unit: CharLengthUnits::Characters,
                to: LogicalType::Char(12, CharLengthUnits::Characters),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "03:04:05.123".to_string(),
                ty: Utf8Type::Fixed(12),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Time32ToVarcharCastEvaluator {
                len: Some(12),
                unit: CharLengthUnits::Characters,
                to: LogicalType::Varchar(Some(12), CharLengthUnits::Characters),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "03:04:05.123".to_string(),
                ty: Utf8Type::Variable(Some(12)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Time32ToTimeCastEvaluator { precision: Some(3) }
                .eval_cast(&value)
                .unwrap(),
            value
        );
    }
}
