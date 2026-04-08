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
use chrono::NaiveDate;
use sqlparser::ast::CharLengthUnits;

numeric_binary_evaluator_definition!(Date, DataValue::Date32);
crate::define_cast_evaluator!(
    Date32ToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits,
        to: LogicalType
    },
    DataValue::Date32(value) => |this| {
        to_char(
            DataValue::format_date(*value).ok_or_else(|| cast_fail(LogicalType::Date, this.to.clone()))?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    Date32ToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits,
        to: LogicalType
    },
    DataValue::Date32(value) => |this| {
        to_varchar(
            DataValue::format_date(*value).ok_or_else(|| cast_fail(LogicalType::Date, this.to.clone()))?,
            this.len,
            this.unit,
        )
    }
);
crate::define_cast_evaluator!(
    Date32ToDatetimeCastEvaluator {
        to: LogicalType
    },
    DataValue::Date32(value) => |this| {
        let value = NaiveDate::from_num_days_from_ce_opt(*value)
            .ok_or_else(|| cast_fail(LogicalType::Date, this.to.clone()))?
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| cast_fail(LogicalType::Date, this.to.clone()))?
            .and_utc()
            .timestamp();

        Ok(DataValue::Date64(value))
    }
);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::CastEvaluator;
    use crate::types::value::Utf8Type;
    use chrono::Datelike;

    #[test]
    fn test_date_cast_evaluators() {
        let value = DataValue::Date32(NaiveDate::from_ymd_opt(2024, 1, 2).unwrap().num_days_from_ce());
        assert_eq!(
            Date32ToCharCastEvaluator {
                len: 10,
                unit: CharLengthUnits::Characters,
                to: LogicalType::Char(10, CharLengthUnits::Characters),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "2024-01-02".to_string(),
                ty: Utf8Type::Fixed(10),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Date32ToVarcharCastEvaluator {
                len: Some(10),
                unit: CharLengthUnits::Characters,
                to: LogicalType::Varchar(Some(10), CharLengthUnits::Characters),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "2024-01-02".to_string(),
                ty: Utf8Type::Variable(Some(10)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Date32ToDatetimeCastEvaluator {
                to: LogicalType::DateTime,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Date64(
                NaiveDate::from_ymd_opt(2024, 1, 2)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .timestamp()
            )
        );
    }
}
