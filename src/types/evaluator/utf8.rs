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
use crate::types::LogicalType;
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use ordered_float::OrderedFloat;
use crate::types::value::Utf8Type;
use rust_decimal::Decimal;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hint;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8NotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8StringConcatBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8LikeBinaryEvaluator {
    pub(crate) escape_char: Option<char>,
}
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Utf8NotLikeBinaryEvaluator {
    pub(crate) escape_char: Option<char>,
}

#[typetag::serde]
impl BinaryEvaluator for Utf8GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 > v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 >= v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 < v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

crate::define_cast_evaluator!(
    Utf8ToBooleanCastEvaluator {
        from: LogicalType
    },
    DataValue::Utf8 { value, .. } => {
        Ok(DataValue::Boolean(bool::from_str(value)?))
    }
);
crate::define_cast_evaluator!(Utf8ToTinyintCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int8(i8::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToUTinyintCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt8(u8::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToSmallintCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int16(i16::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToUSmallintCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt16(u16::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToIntegerCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int32(i32::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToUIntegerCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt32(u32::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToBigintCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int64(i64::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToUBigintCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt64(u64::from_str(value)?))
});
crate::define_cast_evaluator!(Utf8ToFloatCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Float32(OrderedFloat(f32::from_str(value)?)))
});
crate::define_cast_evaluator!(Utf8ToDoubleCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Float64(OrderedFloat(f64::from_str(value)?)))
});
crate::define_cast_evaluator!(
    Utf8ToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Utf8 { value, .. } => |this| to_char(value.clone(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    Utf8ToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Utf8 { value, .. } => |this| to_varchar(value.clone(), this.len, this.unit)
);
crate::define_cast_evaluator!(Utf8ToDateCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Date32(
        NaiveDate::parse_from_str(value, crate::types::value::DATE_FMT)?.num_days_from_ce(),
    ))
});
crate::define_cast_evaluator!(Utf8ToDatetimeCastEvaluator, DataValue::Utf8 { value, .. } => {
    let value = NaiveDateTime::parse_from_str(value, crate::types::value::DATE_TIME_FMT)
        .or_else(|_| {
            NaiveDate::parse_from_str(value, crate::types::value::DATE_FMT)
                .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
        })?
        .and_utc()
        .timestamp();

    Ok(DataValue::Date64(value))
});
crate::define_cast_evaluator!(
    Utf8ToTimeCastEvaluator {
        precision: Option<u64>
    },
    DataValue::Utf8 { value, .. } => |this| {
        let precision = this.precision.unwrap_or(0);
        let fmt = if precision == 0 {
            crate::types::value::TIME_FMT
        } else {
            crate::types::value::TIME_FMT_WITHOUT_ZONE
        };
        let (value, nano) = match precision {
            0 => (
                NaiveTime::parse_from_str(value, fmt)
                    .map(|time| time.num_seconds_from_midnight())?,
                0,
            ),
            _ => NaiveTime::parse_from_str(value, fmt)
                .map(|time| (time.num_seconds_from_midnight(), time.nanosecond()))?,
        };

        Ok(DataValue::Time32(DataValue::pack(value, nano, precision), precision))
    }
);
crate::define_cast_evaluator!(
    Utf8ToTimestampCastEvaluator {
        precision: Option<u64>,
        zone: bool,
        to: LogicalType
    },
    DataValue::Utf8 { value, .. } => |this| {
        let precision = this.precision.unwrap_or(0);
        let fmt = match (precision, this.zone) {
            (0, false) => crate::types::value::DATE_TIME_FMT,
            (0, true) => crate::types::value::TIME_STAMP_FMT_WITHOUT_PRECISION,
            (3 | 6 | 9, false) => crate::types::value::TIME_STAMP_FMT_WITHOUT_ZONE,
            _ => crate::types::value::TIME_STAMP_FMT_WITH_ZONE,
        };
        let complete_value = if this.zone {
            if value.contains("+") {
                value.clone()
            } else {
                format!("{value}+00:00")
            }
        } else {
            value.clone()
        };

        if !this.zone {
            let value = NaiveDateTime::parse_from_str(&complete_value, fmt)?.and_utc();
            let value = match precision {
                3 => value.timestamp_millis(),
                6 => value.timestamp_micros(),
                9 => value
                    .timestamp_nanos_opt()
                    .ok_or_else(|| cast_fail(this.to.clone(), this.to.clone()))?,
                0 => value.timestamp(),
                _ => unreachable!(),
            };

            return Ok(DataValue::Time64(value, precision, false));
        }

        let value = DateTime::parse_from_str(&complete_value, fmt);
        let value = match precision {
            3 => value.map(|date_time| date_time.timestamp_millis())?,
            6 => value.map(|date_time| date_time.timestamp_micros())?,
            9 => value
                .map(|date_time| date_time.timestamp_nanos_opt())?
                .ok_or_else(|| cast_fail(this.to.clone(), this.to.clone()))?,
            0 => value.map(|date_time| date_time.timestamp())?,
            _ => unreachable!(),
        };

        Ok(DataValue::Time64(value, precision, this.zone))
    }
);
crate::define_cast_evaluator!(Utf8ToDecimalCastEvaluator, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Decimal(Decimal::from_str(value)?))
});

#[typetag::serde]
impl BinaryEvaluator for Utf8LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 <= v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 == v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Boolean(v1 != v2)
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8StringConcatBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                DataValue::Utf8 {
                    value: v1.clone() + v2,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8LikeBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value, .. }, DataValue::Utf8 { value: pattern, .. }) => {
                DataValue::Boolean(string_like(value, pattern, self.escape_char))
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Utf8NotLikeBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Utf8 { value, .. }, DataValue::Utf8 { value: pattern, .. }) => {
                DataValue::Boolean(!string_like(value, pattern, self.escape_char))
            }
            (DataValue::Utf8 { .. }, DataValue::Null)
            | (DataValue::Null, DataValue::Utf8 { .. })
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

fn string_like(value: &str, pattern: &str, escape_char: Option<char>) -> bool {
    let mut regex_pattern = String::new();
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        if matches!(escape_char.map(|escape_c| escape_c == c), Some(true)) {
            if let Some(next_char) = chars.next() {
                regex_pattern.push(next_char);
            }
        } else if c == '%' {
            regex_pattern.push_str(".*");
        } else if c == '_' {
            regex_pattern.push('.');
        } else {
            regex_pattern.push(c);
        }
    }
    Regex::new(&regex_pattern).unwrap().is_match(value)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::{BinaryEvaluator, CastEvaluator};

    fn utf8(value: &str) -> DataValue {
        DataValue::Utf8 {
            value: value.to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }
    }

    #[test]
    fn test_utf8_binary_evaluators() {
        assert_eq!(
            Utf8LtBinaryEvaluator
                .binary_eval(&utf8("a"), &utf8("b"))
                .unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            Utf8StringConcatBinaryEvaluator
                .binary_eval(&utf8("ab"), &utf8("cd"))
                .unwrap(),
            utf8("abcd")
        );
        assert_eq!(
            Utf8LikeBinaryEvaluator { escape_char: None }
                .binary_eval(&utf8("kite"), &utf8("ki%"))
                .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_utf8_cast_evaluators() {
        assert_eq!(
            Utf8ToBooleanCastEvaluator {
                from: LogicalType::Varchar(None, CharLengthUnits::Characters),
            }
            .eval_cast(&utf8("true"))
            .unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(Utf8ToTinyintCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::Int8(1));
        assert_eq!(Utf8ToUTinyintCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::UInt8(1));
        assert_eq!(Utf8ToSmallintCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::Int16(1));
        assert_eq!(Utf8ToUSmallintCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::UInt16(1));
        assert_eq!(
            Utf8ToIntegerCastEvaluator.eval_cast(&utf8("1")).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(Utf8ToUIntegerCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::UInt32(1));
        assert_eq!(Utf8ToBigintCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::Int64(1));
        assert_eq!(Utf8ToUBigintCastEvaluator.eval_cast(&utf8("1")).unwrap(), DataValue::UInt64(1));
        assert_eq!(
            Utf8ToFloatCastEvaluator.eval_cast(&utf8("1.5")).unwrap(),
            DataValue::Float32(OrderedFloat(1.5))
        );
        assert_eq!(
            Utf8ToDoubleCastEvaluator.eval_cast(&utf8("1.5")).unwrap(),
            DataValue::Float64(OrderedFloat(1.5))
        );
        assert_eq!(
            Utf8ToCharCastEvaluator {
                len: 2,
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&utf8("ab"))
            .unwrap(),
            DataValue::Utf8 {
                value: "ab".to_string(),
                ty: Utf8Type::Fixed(2),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Utf8ToVarcharCastEvaluator {
                len: Some(2),
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&utf8("ab"))
            .unwrap(),
            DataValue::Utf8 {
                value: "ab".to_string(),
                ty: Utf8Type::Variable(Some(2)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Utf8ToDateCastEvaluator
                .eval_cast(&utf8("2024-01-02"))
                .unwrap(),
            DataValue::Date32(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap().num_days_from_ce())
        );
        assert_eq!(
            Utf8ToDatetimeCastEvaluator
                .eval_cast(&utf8("2024-01-02 03:04:05"))
                .unwrap(),
            DataValue::Date64(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .and_hms_opt(3, 4, 5)
                    .unwrap()
                    .and_utc()
                    .timestamp()
            )
        );
        assert_eq!(
            Utf8ToTimeCastEvaluator { precision: Some(0) }
                .eval_cast(&utf8("03:04:05"))
                .unwrap(),
            DataValue::Time32(DataValue::pack(3 * 3600 + 4 * 60 + 5, 0, 0), 0)
        );
        assert_eq!(
            Utf8ToTimeCastEvaluator { precision: Some(3) }
                .eval_cast(&utf8("03:04:05.123"))
                .unwrap(),
            {
                let time = chrono::NaiveTime::parse_from_str(
                    "03:04:05.123",
                    crate::types::value::TIME_FMT_WITHOUT_ZONE,
                )
                .unwrap();
                DataValue::Time32(
                    DataValue::pack(time.num_seconds_from_midnight(), time.nanosecond(), 3),
                    3,
                )
            }
        );
        assert_eq!(
            Utf8ToTimestampCastEvaluator {
                precision: Some(3),
                zone: false,
                to: LogicalType::TimeStamp(Some(3), false),
            }
            .eval_cast(&utf8("2024-01-02 03:04:05.123"))
            .unwrap(),
            DataValue::Time64(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .and_hms_milli_opt(3, 4, 5, 123)
                    .unwrap()
                    .and_utc()
                    .timestamp_millis(),
                3,
                false,
            )
        );
        assert_eq!(
            Utf8ToTimestampCastEvaluator {
                precision: Some(0),
                zone: true,
                to: LogicalType::TimeStamp(Some(0), true),
            }
            .eval_cast(&utf8("2024-01-02 03:04:05+00:00"))
            .unwrap(),
            DataValue::Time64(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .and_hms_opt(3, 4, 5)
                    .unwrap()
                    .and_utc()
                    .timestamp(),
                0,
                true,
            )
        );
        assert_eq!(
            Utf8ToDecimalCastEvaluator.eval_cast(&utf8("12.34")).unwrap(),
            DataValue::Decimal(Decimal::from_str("12.34").unwrap())
        );
    }
}
