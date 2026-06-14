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
use crate::types::evaluator::DataValue;
use crate::types::CharLengthUnits;
use crate::types::LogicalType;
use chrono::{Datelike, Timelike};
use std::hint;

macro_rules! time64_binary {
    ($name:ident, $op:tt) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Time64(v1, p1, _), DataValue::Time64(v2, p2, _)) => {
                    if let (Some(v1), Some(v2)) = (
                        DataValue::from_timestamp_precision(*v1, *p1),
                        DataValue::from_timestamp_precision(*v2, *p2),
                    ) {
                        let p = if p2 > p1 { *p2 } else { *p1 };
                        DataValue::Boolean(
                            DataValue::timestamp_precision(v1, p)
                                $op DataValue::timestamp_precision(v2, p),
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
    };
}

time64_binary!(time64_gt_binary_eval, >);
time64_binary!(time64_gt_eq_binary_eval, >=);

crate::define_cast_evaluator!(
    time64_to_char_cast_eval {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Time64(value, precision, zone) => |this| {
        to_char(
            DataValue::format_timestamp(*value, *precision).ok_or_else(|| {
                cast_fail(
                    LogicalType::TimeStamp(Some(*precision), *zone),
                    LogicalType::Char(this.len, this.unit),
                )
            })?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    time64_to_varchar_cast_eval {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Time64(value, precision, zone) => |this| {
        to_varchar(
            DataValue::format_timestamp(*value, *precision).ok_or_else(|| {
                cast_fail(
                    LogicalType::TimeStamp(Some(*precision), *zone),
                    LogicalType::Varchar(this.len, this.unit),
                )
            })?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    time64_to_date_cast_eval,
    DataValue::Time64(value, precision, zone) => {
        let value = DataValue::from_timestamp_precision(*value, *precision)
            .ok_or_else(|| {
                cast_fail(
                    LogicalType::TimeStamp(Some(*precision), *zone),
                    LogicalType::Date,
                )
            })?
            .naive_utc()
            .date()
            .num_days_from_ce();

        Ok(DataValue::Date32(value))
    }
);
crate::define_cast_evaluator!(
    time64_to_datetime_cast_eval,
    DataValue::Time64(value, precision, zone) => {
        let value = DataValue::from_timestamp_precision(*value, *precision)
            .ok_or_else(|| {
                cast_fail(
                    LogicalType::TimeStamp(Some(*precision), *zone),
                    LogicalType::DateTime,
                )
            })?
            .timestamp();

        Ok(DataValue::Date64(value))
    }
);
crate::define_cast_evaluator!(
    time64_to_time_cast_eval {
        precision: Option<u64>
    },
    DataValue::Time64(value, precision, zone) => |this| {
        let target_precision = this.precision.unwrap_or(0);
        let (value, nano) = DataValue::from_timestamp_precision(*value, *precision)
            .map(|date_time| {
                (
                    date_time.time().num_seconds_from_midnight(),
                    date_time.time().nanosecond(),
                )
            })
            .ok_or_else(|| {
                cast_fail(
                    LogicalType::TimeStamp(Some(*precision), *zone),
                    LogicalType::Time(this.precision),
                )
            })?;

        Ok(DataValue::Time32(
            DataValue::pack(value, nano, target_precision),
            target_precision,
        ))
    }
);
crate::define_cast_evaluator!(
    time64_to_timestamp_cast_eval {
        precision: Option<u64>,
        zone: bool
    },
    DataValue::Time64(value, _precision, _) => |this| {
        Ok(DataValue::Time64(*value, this.precision.unwrap_or(0), this.zone))
    }
);
time64_binary!(time64_lt_binary_eval, <);
time64_binary!(time64_lt_eq_binary_eval, <=);
time64_binary!(time64_eq_binary_eval, ==);
time64_binary!(time64_not_eq_binary_eval, !=);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::value::Utf8Type;
    use crate::types::CharLengthUnits;

    #[test]
    fn test_time64_binary_evaluators() {
        assert_eq!(
            time64_eq_binary_eval(
                &DataValue::Time64(1_738_734_177_256, 3, false),
                &DataValue::Time64(1_738_734_177_256_000, 6, false),
            )
            .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_time64_cast_evaluators() {
        let timestamp = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_milli_opt(3, 4, 5, 123)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        let value = DataValue::Time64(timestamp, 3, false);
        assert_eq!(
            time64_to_char_cast_eval(23, CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "2024-01-02 03:04:05.123".to_string(),
                ty: Utf8Type::Fixed(23),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            time64_to_varchar_cast_eval(Some(23), CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "2024-01-02 03:04:05.123".to_string(),
                ty: Utf8Type::Variable(Some(23)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            time64_to_date_cast_eval(&value).unwrap(),
            DataValue::Date32(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .num_days_from_ce()
            )
        );
        assert_eq!(
            time64_to_datetime_cast_eval(&value).unwrap(),
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
            time64_to_time_cast_eval(Some(3), &value).unwrap(),
            DataValue::Time32(DataValue::pack(3 * 3600 + 4 * 60 + 5, 123_000_000, 3), 3)
        );
        assert_eq!(
            time64_to_timestamp_cast_eval(Some(3), true, &value).unwrap(),
            DataValue::Time64(timestamp, 3, true)
        );
    }
}
