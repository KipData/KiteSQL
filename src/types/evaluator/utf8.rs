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
use crate::types::evaluator::cast::{to_char, to_varchar};
use crate::types::evaluator::DataValue;
use crate::types::value::Utf8Type;
use crate::types::CharLengthUnits;
use ordered_float::OrderedFloat;
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;
use std::hint;
use std::str::FromStr;

macro_rules! utf8_binary {
    ($name:ident, $body:expr) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) => {
                    $body(v1, v2)
                }
                (DataValue::Utf8 { .. }, DataValue::Null)
                | (DataValue::Null, DataValue::Utf8 { .. })
                | (DataValue::Null, DataValue::Null) => DataValue::Null,
                _ => unsafe { hint::unreachable_unchecked() },
            })
        }
    };
}

utf8_binary!(utf8_gt_binary_eval, |v1: &String, v2: &String| {
    DataValue::Boolean(v1 > v2)
});
utf8_binary!(utf8_gt_eq_binary_eval, |v1: &String, v2: &String| {
    DataValue::Boolean(v1 >= v2)
});
utf8_binary!(utf8_lt_binary_eval, |v1: &String, v2: &String| {
    DataValue::Boolean(v1 < v2)
});

crate::define_cast_evaluator!(utf8_to_boolean_cast_eval, DataValue::Utf8 { value, .. } => {
        Ok(DataValue::Boolean(bool::from_str(value)?))
    }
);
crate::define_cast_evaluator!(utf8_to_tinyint_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int8(i8::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_utinyint_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt8(u8::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_smallint_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int16(i16::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_usmallint_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt16(u16::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_integer_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int32(i32::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_uinteger_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt32(u32::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_bigint_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Int64(i64::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_ubigint_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::UInt64(u64::from_str(value)?))
});
crate::define_cast_evaluator!(utf8_to_float_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Float32(OrderedFloat(f32::from_str(value)?)))
});
crate::define_cast_evaluator!(utf8_to_double_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Float64(OrderedFloat(f64::from_str(value)?)))
});
crate::define_cast_evaluator!(
    utf8_to_char_cast_eval {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Utf8 { value, .. } => |this| to_char(value.clone(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    utf8_to_varchar_cast_eval {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Utf8 { value, .. } => |this| to_varchar(value.clone(), this.len, this.unit)
);
#[cfg(feature = "time")]
mod chrono_cast {
    use super::DataValue;
    use crate::types::evaluator::cast::cast_fail;
    use crate::types::LogicalType;
    use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

    crate::define_cast_evaluator!(utf8_to_date_cast_eval, DataValue::Utf8 { value, .. } => {
        Ok(DataValue::Date32(
            NaiveDate::parse_from_str(value, crate::types::value::DATE_FMT)?.num_days_from_ce(),
        ))
    });

    crate::define_cast_evaluator!(utf8_to_datetime_cast_eval, DataValue::Utf8 { value, .. } => {
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
        utf8_to_time_cast_eval {
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
        utf8_to_timestamp_cast_eval {
            precision: Option<u64>,
            zone: bool
        },
        DataValue::Utf8 { value, .. } => |this| {
            let precision = this.precision.unwrap_or(0);
            let target_type = || LogicalType::TimeStamp(this.precision, this.zone);
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
                        .ok_or_else(|| cast_fail(target_type(), target_type()))?,
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
                    .ok_or_else(|| cast_fail(target_type(), target_type()))?,
                0 => value.map(|date_time| date_time.timestamp())?,
                _ => unreachable!(),
            };

            Ok(DataValue::Time64(value, precision, this.zone))
        }
    );
}

#[cfg(feature = "time")]
pub use chrono_cast::*;
#[cfg(feature = "decimal")]
crate::define_cast_evaluator!(utf8_to_decimal_cast_eval, DataValue::Utf8 { value, .. } => {
    Ok(DataValue::Decimal(Decimal::from_str(value)?))
});
utf8_binary!(utf8_lt_eq_binary_eval, |v1: &String, v2: &String| {
    DataValue::Boolean(v1 <= v2)
});
utf8_binary!(utf8_eq_binary_eval, |v1: &String, v2: &String| {
    DataValue::Boolean(v1 == v2)
});
utf8_binary!(utf8_not_eq_binary_eval, |v1: &String, v2: &String| {
    DataValue::Boolean(v1 != v2)
});
utf8_binary!(
    utf8_string_concat_binary_eval,
    |v1: &String, v2: &String| {
        DataValue::Utf8 {
            value: v1.clone() + v2,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }
    }
);
pub fn utf8_like_binary_eval(
    escape_char: Option<char>,
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Utf8 { value, .. }, DataValue::Utf8 { value: pattern, .. }) => {
            DataValue::Boolean(string_like(value, pattern, escape_char))
        }
        (DataValue::Utf8 { .. }, DataValue::Null)
        | (DataValue::Null, DataValue::Utf8 { .. })
        | (DataValue::Null, DataValue::Null) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}
pub fn utf8_not_like_binary_eval(
    escape_char: Option<char>,
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Utf8 { value, .. }, DataValue::Utf8 { value: pattern, .. }) => {
            DataValue::Boolean(!string_like(value, pattern, escape_char))
        }
        (DataValue::Utf8 { .. }, DataValue::Null)
        | (DataValue::Null, DataValue::Utf8 { .. })
        | (DataValue::Null, DataValue::Null) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}

fn string_like(value: &str, pattern: &str, escape_char: Option<char>) -> bool {
    let mut value_idx = 0;
    let mut pattern_idx = 0;
    let mut last_many = None;

    while value_idx < value.len() {
        match next_like_pattern_token(pattern, pattern_idx, escape_char) {
            Some((LikePatternToken::Literal(pattern_ch), next_pattern_idx)) => {
                if let Some((value_ch, next_value_idx)) = next_char_at(value, value_idx) {
                    if value_ch == pattern_ch {
                        value_idx = next_value_idx;
                        pattern_idx = next_pattern_idx;
                        continue;
                    }
                }
            }
            Some((LikePatternToken::AnyOne, next_pattern_idx)) => {
                if let Some((_, next_value_idx)) = next_char_at(value, value_idx) {
                    value_idx = next_value_idx;
                    pattern_idx = next_pattern_idx;
                    continue;
                }
            }
            Some((LikePatternToken::AnyMany, next_pattern_idx)) => {
                pattern_idx = next_pattern_idx;
                last_many = Some((pattern_idx, value_idx));
                continue;
            }
            None => {}
        }

        let Some((after_many_pattern_idx, many_value_idx)) = last_many else {
            return false;
        };
        let Some((_, next_many_value_idx)) = next_char_at(value, many_value_idx) else {
            return false;
        };
        value_idx = next_many_value_idx;
        pattern_idx = after_many_pattern_idx;
        last_many = Some((after_many_pattern_idx, value_idx));
    }

    while let Some((token, next_pattern_idx)) =
        next_like_pattern_token(pattern, pattern_idx, escape_char)
    {
        if token != LikePatternToken::AnyMany {
            return false;
        }
        pattern_idx = next_pattern_idx;
    }
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LikePatternToken {
    Literal(char),
    AnyOne,
    AnyMany,
}

fn next_like_pattern_token(
    pattern: &str,
    index: usize,
    escape_char: Option<char>,
) -> Option<(LikePatternToken, usize)> {
    let (ch, next_index) = next_char_at(pattern, index)?;
    if escape_char.is_some_and(|escape_ch| escape_ch == ch) {
        if let Some((escaped_ch, escaped_next_index)) = next_char_at(pattern, next_index) {
            Some((LikePatternToken::Literal(escaped_ch), escaped_next_index))
        } else {
            Some((LikePatternToken::Literal(ch), next_index))
        }
    } else if ch == '%' {
        Some((LikePatternToken::AnyMany, next_index))
    } else if ch == '_' {
        Some((LikePatternToken::AnyOne, next_index))
    } else {
        Some((LikePatternToken::Literal(ch), next_index))
    }
}

fn next_char_at(input: &str, index: usize) -> Option<(char, usize)> {
    let ch = input.get(index..)?.chars().next()?;
    Some((ch, index + ch.len_utf8()))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;

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
            utf8_lt_binary_eval(&utf8("a"), &utf8("b")).unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            utf8_string_concat_binary_eval(&utf8("ab"), &utf8("cd")).unwrap(),
            utf8("abcd")
        );
        assert_eq!(
            utf8_like_binary_eval(None, &utf8("kite"), &utf8("ki%")).unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            utf8_not_like_binary_eval(None, &utf8("kite"), &utf8("ki%")).unwrap(),
            DataValue::Boolean(false)
        );
    }

    #[test]
    fn test_string_like_patterns() {
        let cases = [
            ("", "", None, true),
            ("", "%", None, true),
            ("", "_", None, false),
            ("a", "", None, false),
            ("a", "a", None, true),
            ("a", "_", None, true),
            ("ab", "_", None, false),
            ("ab", "__", None, true),
            ("abc", "a%", None, true),
            ("abc", "%c", None, true),
            ("abc", "%b%", None, true),
            ("abc", "a%c", None, true),
            ("abc", "a%d", None, false),
            ("abc", "%a", None, false),
            ("abc", "%%", None, true),
            ("abc", "a%%c", None, true),
            ("abc", "a%c%", None, true),
            ("abbbc", "a%bc", None, true),
            ("abXc", "a%bc", None, false),
            ("skite", "ki%", None, false),
            ("ki.e", "ki.e", None, true),
            ("kite", "ki.e", None, false),
            ("ki*e", "ki*e", None, true),
            ("F%ck", "F@%ck", Some('@'), true),
            ("Fack", "F@%ck", Some('@'), false),
            ("F_ck", "F@_ck", Some('@'), true),
            ("Fack", "F@_ck", Some('@'), false),
            ("@", "@", Some('@'), true),
            ("%", "\\%", Some('\\'), true),
            ("好", "_", None, true),
            ("好a", "好_", None, true),
            ("好a", "__", None, true),
            ("好a", "_", None, false),
            ("你好", "你%", None, true),
        ];

        for (value, pattern, escape_char, expected) in cases {
            assert_eq!(
                string_like(value, pattern, escape_char),
                expected,
                "value={value:?}, pattern={pattern:?}, escape_char={escape_char:?}"
            );
        }
    }

    #[test]
    fn test_utf8_cast_evaluators() {
        assert_eq!(
            utf8_to_boolean_cast_eval(&utf8("true")).unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            utf8_to_tinyint_cast_eval(&utf8("1")).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            utf8_to_utinyint_cast_eval(&utf8("1")).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            utf8_to_smallint_cast_eval(&utf8("1")).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            utf8_to_usmallint_cast_eval(&utf8("1")).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            utf8_to_integer_cast_eval(&utf8("1")).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            utf8_to_uinteger_cast_eval(&utf8("1")).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            utf8_to_bigint_cast_eval(&utf8("1")).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            utf8_to_ubigint_cast_eval(&utf8("1")).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            utf8_to_float_cast_eval(&utf8("1.5")).unwrap(),
            DataValue::Float32(OrderedFloat(1.5))
        );
        assert_eq!(
            utf8_to_double_cast_eval(&utf8("1.5")).unwrap(),
            DataValue::Float64(OrderedFloat(1.5))
        );
        assert_eq!(
            utf8_to_char_cast_eval(2, CharLengthUnits::Characters, &utf8("ab")).unwrap(),
            DataValue::Utf8 {
                value: "ab".to_string(),
                ty: Utf8Type::Fixed(2),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            utf8_to_varchar_cast_eval(Some(2), CharLengthUnits::Characters, &utf8("ab")).unwrap(),
            DataValue::Utf8 {
                value: "ab".to_string(),
                ty: Utf8Type::Variable(Some(2)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            utf8_to_date_cast_eval(&utf8("2024-01-02")).unwrap(),
            DataValue::Date32(
                chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .num_days_from_ce()
            )
        );
        assert_eq!(
            utf8_to_datetime_cast_eval(&utf8("2024-01-02 03:04:05")).unwrap(),
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
            utf8_to_time_cast_eval(Some(0), &utf8("03:04:05")).unwrap(),
            DataValue::Time32(DataValue::pack(3 * 3600 + 4 * 60 + 5, 0, 0), 0)
        );
        assert_eq!(
            utf8_to_time_cast_eval(Some(3), &utf8("03:04:05.123")).unwrap(),
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
            utf8_to_timestamp_cast_eval(Some(3), false, &utf8("2024-01-02 03:04:05.123")).unwrap(),
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
            utf8_to_timestamp_cast_eval(Some(0), true, &utf8("2024-01-02 03:04:05+00:00")).unwrap(),
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
            utf8_to_decimal_cast_eval(&utf8("12.34")).unwrap(),
            DataValue::Decimal(Decimal::from_str("12.34").unwrap())
        );
    }
}
