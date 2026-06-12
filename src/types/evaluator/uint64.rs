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
use crate::types::evaluator::DataValue;
use crate::types::LogicalType;

numeric_binary_evaluator_definition!(Uint64, DataValue::UInt64);
crate::define_integer_cast_evaluators!(Uint64, UInt64, u64, LogicalType::UBigint);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::value::Utf8Type;
    use crate::types::CharLengthUnits;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;

    #[test]
    fn test_uint64_cast_evaluators() {
        let value = DataValue::UInt64(1);

        assert_eq!(
            uint64_to_boolean_cast_eval(&value).unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            uint64_to_tinyint_cast_eval(&value).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            uint64_to_utinyint_cast_eval(&value).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            uint64_to_smallint_cast_eval(&value).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            uint64_to_usmallint_cast_eval(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            uint64_to_integer_cast_eval(&value).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            uint64_to_uinteger_cast_eval(&value).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            uint64_to_bigint_cast_eval(&value).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            uint64_to_ubigint_cast_eval(&value).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            uint64_to_float_cast_eval(&value).unwrap(),
            DataValue::Float32(OrderedFloat(1.0))
        );
        assert_eq!(
            uint64_to_double_cast_eval(&value).unwrap(),
            DataValue::Float64(OrderedFloat(1.0))
        );
        assert_eq!(
            uint64_to_char_cast_eval(1, CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "1".to_string(),
                ty: Utf8Type::Fixed(1),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            uint64_to_varchar_cast_eval(Some(1), CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "1".to_string(),
                ty: Utf8Type::Variable(Some(1)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            uint64_to_decimal_cast_eval(Some(2), &value).unwrap(),
            DataValue::Decimal(Decimal::new(100, 2))
        );
    }
}
