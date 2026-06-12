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
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use std::hint;
pub fn float64_plus_unary_eval(value: &DataValue) -> DataValue {
    value.clone()
}
pub fn float64_minus_unary_eval(value: &DataValue) -> DataValue {
    match value {
        DataValue::Float64(value) => DataValue::Float64(-value),
        DataValue::Null => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    }
}

macro_rules! float64_binary {
    ($name:ident, $body:expr) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Float64(v1), DataValue::Float64(v2)) => $body(v1, v2),
                (DataValue::Float64(_), DataValue::Null)
                | (DataValue::Null, DataValue::Float64(_))
                | (DataValue::Null, DataValue::Null) => DataValue::Null,
                _ => unsafe { hint::unreachable_unchecked() },
            })
        }
    };
}

float64_binary!(
    float64_plus_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Float64(*v1 + *v2)
    }
);
float64_binary!(
    float64_minus_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Float64(*v1 - *v2)
    }
);

crate::define_cast_evaluator!(float64_to_float_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::Float32(ordered_float::OrderedFloat(value.0 as f32)))
});
crate::define_cast_evaluator!(float64_to_double_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::Float64(*value))
});
crate::define_cast_evaluator!(float64_to_tinyint_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::Int8(crate::float_to_int_cast!(value.into_inner(), i8, f64)?))
});
crate::define_cast_evaluator!(float64_to_smallint_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::Int16(crate::float_to_int_cast!(value.into_inner(), i16, f64)?))
});
crate::define_cast_evaluator!(float64_to_integer_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::Int32(crate::float_to_int_cast!(value.into_inner(), i32, f64)?))
});
crate::define_cast_evaluator!(float64_to_bigint_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::Int64(crate::float_to_int_cast!(value.into_inner(), i64, f64)?))
});
crate::define_cast_evaluator!(float64_to_utinyint_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::UInt8(crate::float_to_int_cast!(value.into_inner(), u8, f64)?))
});
crate::define_cast_evaluator!(float64_to_usmallint_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::UInt16(crate::float_to_int_cast!(value.into_inner(), u16, f64)?))
});
crate::define_cast_evaluator!(float64_to_uinteger_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::UInt32(crate::float_to_int_cast!(value.into_inner(), u32, f64)?))
});
crate::define_cast_evaluator!(float64_to_ubigint_cast_eval, DataValue::Float64(value) => {
    Ok(DataValue::UInt64(crate::float_to_int_cast!(value.into_inner(), u64, f64)?))
});
crate::define_cast_evaluator!(
    float64_to_char_cast_eval {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Float64(value) => |this| to_char(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    float64_to_varchar_cast_eval {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Float64(value) => |this| to_varchar(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    float64_to_decimal_cast_eval {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    DataValue::Float64(value) => |this| {
        let mut decimal = Decimal::from_f64(value.0).ok_or_else(|| {
            cast_fail(
                LogicalType::Double,
                LogicalType::Decimal(this.precision, this.scale),
            )
        })?;
        DataValue::decimal_round_f(&this.scale, &mut decimal);

        Ok(DataValue::Decimal(decimal))
    }
);
float64_binary!(
    float64_multiply_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Float64(*v1 * *v2)
    }
);
float64_binary!(
    float64_divide_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Float64(ordered_float::OrderedFloat(v1.0 / v2.0))
    }
);
float64_binary!(
    float64_gt_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Boolean(v1 > v2)
    }
);
float64_binary!(
    float64_gt_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Boolean(v1 >= v2)
    }
);
float64_binary!(
    float64_lt_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Boolean(v1 < v2)
    }
);
float64_binary!(
    float64_lt_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Boolean(v1 <= v2)
    }
);
float64_binary!(
    float64_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Boolean(v1 == v2)
    }
);
float64_binary!(
    float64_not_eq_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Boolean(v1 != v2)
    }
);
float64_binary!(
    float64_mod_binary_eval,
    |v1: &ordered_float::OrderedFloat<f64>, v2: &ordered_float::OrderedFloat<f64>| {
        DataValue::Float64(*v1 % *v2)
    }
);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::value::Utf8Type;
    use crate::types::CharLengthUnits;
    use rust_decimal::Decimal;

    #[test]
    fn test_float64_binary_and_cast_evaluators() {
        let value = DataValue::Float64(ordered_float::OrderedFloat(1.5));

        assert_eq!(
            float64_multiply_binary_eval(
                &DataValue::Float64(ordered_float::OrderedFloat(1.5)),
                &DataValue::Float64(ordered_float::OrderedFloat(2.0)),
            )
            .unwrap(),
            DataValue::Float64(ordered_float::OrderedFloat(3.0))
        );
        assert_eq!(
            float64_to_float_cast_eval(&value).unwrap(),
            DataValue::Float32(ordered_float::OrderedFloat(1.5))
        );
        assert_eq!(
            float64_to_double_cast_eval(&value).unwrap(),
            DataValue::Float64(ordered_float::OrderedFloat(1.5))
        );
        assert_eq!(
            float64_to_tinyint_cast_eval(&value).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            float64_to_smallint_cast_eval(&value).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            float64_to_integer_cast_eval(&value).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            float64_to_bigint_cast_eval(&value).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            float64_to_utinyint_cast_eval(&value).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            float64_to_usmallint_cast_eval(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            float64_to_uinteger_cast_eval(&value).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            float64_to_ubigint_cast_eval(&value).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            float64_to_char_cast_eval(3, CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "1.5".to_string(),
                ty: Utf8Type::Fixed(3),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            float64_to_varchar_cast_eval(Some(3), CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "1.5".to_string(),
                ty: Utf8Type::Variable(Some(3)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            float64_to_decimal_cast_eval(None, Some(1), &value).unwrap(),
            DataValue::Decimal(Decimal::new(15, 1))
        );
    }
}
