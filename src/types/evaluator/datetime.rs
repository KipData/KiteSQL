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

use crate::numeric_binary_evaluator_definition;
use crate::types::evaluator::cast::{cast_fail, to_char, to_varchar};
use crate::types::evaluator::DataValue;
use crate::types::LogicalType;
use chrono::{DateTime, Datelike, Timelike};
use sqlparser::ast::CharLengthUnits;

numeric_binary_evaluator_definition!(DateTime, DataValue::Date64);
crate::define_cast_evaluator!(
    Date64ToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits,
        to: LogicalType
    },
    DataValue::Date64(value) => |this| {
        to_char(
            DataValue::format_datetime(*value).ok_or_else(|| cast_fail(LogicalType::DateTime, this.to.clone()))?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    Date64ToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits,
        to: LogicalType
    },
    DataValue::Date64(value) => |this| {
        to_varchar(
            DataValue::format_datetime(*value).ok_or_else(|| cast_fail(LogicalType::DateTime, this.to.clone()))?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    Date64ToDateCastEvaluator {
        to: LogicalType
    },
    DataValue::Date64(value) => |this| {
        let value = DateTime::from_timestamp(*value, 0)
            .ok_or_else(|| cast_fail(LogicalType::DateTime, this.to.clone()))?
            .naive_utc()
            .date()
            .num_days_from_ce();

        Ok(DataValue::Date32(value))
    }
);
crate::define_cast_evaluator!(
    Date64ToTimeCastEvaluator {
        precision: Option<u64>,
        to: LogicalType
    },
    DataValue::Date64(value) => |this| {
        let precision = this.precision.unwrap_or(0);
        let value = DateTime::from_timestamp(*value, 0)
            .map(|date_time| date_time.time().num_seconds_from_midnight())
            .ok_or_else(|| cast_fail(LogicalType::DateTime, this.to.clone()))?;

        Ok(DataValue::Time32(DataValue::pack(value, 0, 0), precision))
    }
);
crate::define_cast_evaluator!(
    Date64ToTimestampCastEvaluator {
        precision: Option<u64>,
        zone: bool
    },
    DataValue::Date64(value) => |this| {
        Ok(DataValue::Time64(*value, this.precision.unwrap_or(0), this.zone))
    }
);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::CastEvaluator;
    use crate::types::value::Utf8Type;
    use sqlparser::ast::CharLengthUnits;

    #[test]
    fn test_datetime_cast_evaluators() {
        let value = DataValue::Date64(
            chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_opt(3, 4, 5)
                .unwrap()
                .and_utc()
                .timestamp(),
        );
        assert_eq!(
            Date64ToCharCastEvaluator {
                len: 19,
                unit: CharLengthUnits::Characters,
                to: LogicalType::Char(19, CharLengthUnits::Characters),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "2024-01-02 03:04:05".to_string(),
                ty: Utf8Type::Fixed(19),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Date64ToVarcharCastEvaluator {
                len: Some(19),
                unit: CharLengthUnits::Characters,
                to: LogicalType::Varchar(Some(19), CharLengthUnits::Characters),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "2024-01-02 03:04:05".to_string(),
                ty: Utf8Type::Variable(Some(19)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Date64ToDateCastEvaluator { to: LogicalType::Date }
                .eval_cast(&value)
                .unwrap(),
            DataValue::Date32(chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap().num_days_from_ce())
        );
        assert_eq!(
            Date64ToTimeCastEvaluator {
                precision: Some(0),
                to: LogicalType::Time(Some(0)),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Time32(DataValue::pack(3 * 3600 + 4 * 60 + 5, 0, 0), 0)
        );
        assert_eq!(
            Date64ToTimestampCastEvaluator {
                precision: Some(0),
                zone: true,
            }
            .eval_cast(&value)
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
    }
}
