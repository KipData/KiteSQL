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
use crate::types::LogicalType;
use rust_decimal::prelude::FromPrimitive;
use std::hint;
pub fn float32_plus_unary_eval(value: &DataValue) -> DataValue {
    value.clone()
}
pub fn float32_minus_unary_eval(value: &DataValue) -> DataValue {
    match value {
        DataValue::Float32(value) => DataValue::Float32(-value),
        DataValue::Null => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    }
}

macro_rules! float32_binary {
    ($name:ident, $body:expr) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Float32(v1), DataValue::Float32(v2)) => $body(v1, v2),
                (DataValue::Float32(_), DataValue::Null)
                | (DataValue::Null, DataValue::Float32(_))
                | (DataValue::Null, DataValue::Null) => DataValue::Null,
                _ => unsafe { hint::unreachable_unchecked() },
            })
        }
    };
}

float32_binary!(
    float32_plus_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Float32(*v1 + *v2)
    }
);
float32_binary!(
    float32_minus_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Float32(*v1 - *v2)
    }
);
float32_binary!(
    float32_multiply_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Float32(*v1 * *v2)
    }
);
float32_binary!(
    float32_divide_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Float64(ordered_float::OrderedFloat(v1.0 as f64 / v2.0 as f64))
    }
);
float32_binary!(
    float32_gt_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Boolean(v1 > v2)
    }
);
float32_binary!(
    float32_gt_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Boolean(v1 >= v2)
    }
);
float32_binary!(
    float32_lt_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Boolean(v1 < v2)
    }
);
float32_binary!(
    float32_lt_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Boolean(v1 <= v2)
    }
);
float32_binary!(
    float32_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Boolean(v1 == v2)
    }
);
float32_binary!(
    float32_not_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Boolean(v1 != v2)
    }
);
float32_binary!(
    float32_mod_binary_eval,
    |v1: &ordered_float::OrderedFloat<f32>, v2: &ordered_float::OrderedFloat<f32>| {
        DataValue::Float32(*v1 % *v2)
    }
);

crate::define_float_cast_evaluators!(Float32, Float32, f32, LogicalType::Float, from_f32);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::value::Utf8Type;
    use crate::types::CharLengthUnits;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;

    #[test]
    fn test_float32_cast_evaluators() {
        let value = DataValue::Float32(OrderedFloat(1.5));

        assert_eq!(
            float32_to_float_cast_eval(&value).unwrap(),
            DataValue::Float32(OrderedFloat(1.5))
        );
        assert_eq!(
            float32_to_double_cast_eval(&value).unwrap(),
            DataValue::Float64(OrderedFloat(1.5))
        );
        assert_eq!(
            float32_to_tinyint_cast_eval(&value).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            float32_to_smallint_cast_eval(&value).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            float32_to_integer_cast_eval(&value).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            float32_to_bigint_cast_eval(&value).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            float32_to_utinyint_cast_eval(&value).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            float32_to_usmallint_cast_eval(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            float32_to_uinteger_cast_eval(&value).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            float32_to_ubigint_cast_eval(&value).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            float32_to_char_cast_eval(3, CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "1.5".to_string(),
                ty: Utf8Type::Fixed(3),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            float32_to_varchar_cast_eval(Some(3), CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "1.5".to_string(),
                ty: Utf8Type::Variable(Some(3)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            float32_to_decimal_cast_eval(None, Some(1), &value).unwrap(),
            DataValue::Decimal(Decimal::new(15, 1))
        );
    }
}
