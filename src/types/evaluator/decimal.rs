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
use crate::types::CharLengthUnits;
use ordered_float::OrderedFloat;
use rust_decimal::prelude::ToPrimitive;
use std::hint;

macro_rules! decimal_binary {
    ($name:ident, $body:expr) => {
        pub fn $name(left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
            Ok(match (left, right) {
                (DataValue::Decimal(v1), DataValue::Decimal(v2)) => $body(v1, v2),
                (DataValue::Decimal(_), DataValue::Null)
                | (DataValue::Null, DataValue::Decimal(_))
                | (DataValue::Null, DataValue::Null) => DataValue::Null,
                _ => unsafe { hint::unreachable_unchecked() },
            })
        }
    };
}

decimal_binary!(
    decimal_plus_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Decimal(v1 + v2)
);
decimal_binary!(
    decimal_minus_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Decimal(v1 - v2)
);

crate::define_cast_evaluator!(decimal_to_float_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Float32(OrderedFloat(value.to_f32().ok_or_else(|| {
        crate::types::evaluator::cast::cast_fail(
            crate::types::LogicalType::Decimal(None, None),
            crate::types::LogicalType::Float,
        )
    })?)))
});
crate::define_cast_evaluator!(decimal_to_double_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Float64(OrderedFloat(value.to_f64().ok_or_else(|| {
        crate::types::evaluator::cast::cast_fail(
            crate::types::LogicalType::Decimal(None, None),
            crate::types::LogicalType::Double,
        )
    })?)))
});
crate::define_cast_evaluator!(decimal_to_decimal_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Decimal(*value))
});
crate::define_cast_evaluator!(
    decimal_to_char_cast_eval {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Decimal(value) => |this| to_char(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    decimal_to_varchar_cast_eval {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Decimal(value) => |this| to_varchar(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(decimal_to_tinyint_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Int8(crate::decimal_to_int_cast!(*value, i8)))
});
crate::define_cast_evaluator!(decimal_to_smallint_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Int16(crate::decimal_to_int_cast!(*value, i16)))
});
crate::define_cast_evaluator!(decimal_to_integer_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Int32(crate::decimal_to_int_cast!(*value, i32)))
});
crate::define_cast_evaluator!(decimal_to_bigint_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::Int64(crate::decimal_to_int_cast!(*value, i64)))
});
crate::define_cast_evaluator!(decimal_to_utinyint_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::UInt8(crate::decimal_to_int_cast!(*value, u8)))
});
crate::define_cast_evaluator!(decimal_to_usmallint_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::UInt16(crate::decimal_to_int_cast!(*value, u16)))
});
crate::define_cast_evaluator!(decimal_to_uinteger_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::UInt32(crate::decimal_to_int_cast!(*value, u32)))
});
crate::define_cast_evaluator!(decimal_to_ubigint_cast_eval, DataValue::Decimal(value) => {
    Ok(DataValue::UInt64(crate::decimal_to_int_cast!(*value, u64)))
});
decimal_binary!(
    decimal_multiply_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Decimal(v1 * v2)
);
decimal_binary!(
    decimal_divide_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Decimal(v1 / v2)
);
decimal_binary!(
    decimal_gt_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Boolean(v1 > v2)
);
decimal_binary!(
    decimal_gt_eq_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Boolean(v1 >= v2)
);
decimal_binary!(
    decimal_lt_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Boolean(v1 < v2)
);
decimal_binary!(
    decimal_lt_eq_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Boolean(v1 <= v2)
);
decimal_binary!(
    decimal_eq_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Boolean(v1 == v2)
);
decimal_binary!(
    decimal_not_eq_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Boolean(v1 != v2)
);
decimal_binary!(
    decimal_mod_binary_eval,
    |v1: &rust_decimal::Decimal, v2: &rust_decimal::Decimal| DataValue::Decimal(v1 % v2)
);

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::value::Utf8Type;
    use crate::types::CharLengthUnits;
    use rust_decimal::Decimal;

    #[test]
    fn test_decimal_cast_evaluators() {
        let value = DataValue::Decimal(Decimal::new(125, 1));

        assert_eq!(
            decimal_to_float_cast_eval(&value).unwrap(),
            DataValue::Float32(OrderedFloat(12.5))
        );
        assert_eq!(
            decimal_to_double_cast_eval(&value).unwrap(),
            DataValue::Float64(OrderedFloat(12.5))
        );
        assert_eq!(
            decimal_to_decimal_cast_eval(&value).unwrap(),
            DataValue::Decimal(Decimal::new(125, 1))
        );
        assert_eq!(
            decimal_to_char_cast_eval(4, CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "12.5".to_string(),
                ty: Utf8Type::Fixed(4),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            decimal_to_varchar_cast_eval(Some(4), CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "12.5".to_string(),
                ty: Utf8Type::Variable(Some(4)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            decimal_to_tinyint_cast_eval(&value).unwrap(),
            DataValue::Int8(12)
        );
        assert_eq!(
            decimal_to_smallint_cast_eval(&value).unwrap(),
            DataValue::Int16(12)
        );
        assert_eq!(
            decimal_to_integer_cast_eval(&value).unwrap(),
            DataValue::Int32(12)
        );
        assert_eq!(
            decimal_to_bigint_cast_eval(&value).unwrap(),
            DataValue::Int64(12)
        );
        assert_eq!(
            decimal_to_utinyint_cast_eval(&value).unwrap(),
            DataValue::UInt8(12)
        );
        assert_eq!(
            decimal_to_usmallint_cast_eval(&value).unwrap(),
            DataValue::UInt16(12)
        );
        assert_eq!(
            decimal_to_uinteger_cast_eval(&value).unwrap(),
            DataValue::UInt32(12)
        );
        assert_eq!(
            decimal_to_ubigint_cast_eval(&value).unwrap(),
            DataValue::UInt64(12)
        );
    }
}
