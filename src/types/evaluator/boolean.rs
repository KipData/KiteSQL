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
use std::hint;
pub fn boolean_not_unary_eval(value: &DataValue) -> DataValue {
    match value {
        DataValue::Boolean(value) => DataValue::Boolean(!value),
        DataValue::Null => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    }
}
pub fn boolean_and_binary_eval(
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 && *v2),
        (DataValue::Boolean(false), DataValue::Null)
        | (DataValue::Null, DataValue::Boolean(false)) => DataValue::Boolean(false),
        (DataValue::Null, DataValue::Null)
        | (DataValue::Boolean(true), DataValue::Null)
        | (DataValue::Null, DataValue::Boolean(true)) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}
pub fn boolean_or_binary_eval(
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 || *v2),
        (DataValue::Boolean(true), DataValue::Null)
        | (DataValue::Null, DataValue::Boolean(true)) => DataValue::Boolean(true),
        (DataValue::Null, DataValue::Null)
        | (DataValue::Boolean(false), DataValue::Null)
        | (DataValue::Null, DataValue::Boolean(false)) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}
pub fn boolean_eq_binary_eval(
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 == *v2),
        (DataValue::Null, DataValue::Boolean(_))
        | (DataValue::Boolean(_), DataValue::Null)
        | (DataValue::Null, DataValue::Null) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}

crate::define_cast_evaluator!(boolean_to_tinyint_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::Int8(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_utinyint_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::UInt8(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_smallint_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::Int16(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_usmallint_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::UInt16(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_integer_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::Int32(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_uinteger_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::UInt32(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_bigint_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::Int64(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_ubigint_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::UInt64(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(boolean_to_float_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::Float32(OrderedFloat(if *value { 1.0 } else { 0.0 })))
});
crate::define_cast_evaluator!(boolean_to_double_cast_eval, DataValue::Boolean(value) => {
    Ok(DataValue::Float64(OrderedFloat(if *value { 1.0 } else { 0.0 })))
});
crate::define_cast_evaluator!(
    boolean_to_char_cast_eval {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Boolean(value) => |this| to_char(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    boolean_to_varchar_cast_eval {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Boolean(value) => |this| to_varchar(value.to_string(), this.len, this.unit)
);
pub fn boolean_not_eq_binary_eval(
    left: &DataValue,
    right: &DataValue,
) -> Result<DataValue, DatabaseError> {
    Ok(match (left, right) {
        (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 != *v2),
        (DataValue::Null, DataValue::Boolean(_))
        | (DataValue::Boolean(_), DataValue::Null)
        | (DataValue::Null, DataValue::Null) => DataValue::Null,
        _ => unsafe { hint::unreachable_unchecked() },
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::value::Utf8Type;

    #[test]
    fn test_boolean_binary_evaluators() {
        assert_eq!(
            boolean_and_binary_eval(&DataValue::Boolean(true), &DataValue::Boolean(true)).unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            boolean_and_binary_eval(&DataValue::Boolean(false), &DataValue::Null).unwrap(),
            DataValue::Boolean(false)
        );
        assert_eq!(
            boolean_or_binary_eval(&DataValue::Boolean(false), &DataValue::Boolean(true)).unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_cast_evaluators() {
        let value = DataValue::Boolean(true);

        assert_eq!(
            boolean_to_tinyint_cast_eval(&value).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            boolean_to_utinyint_cast_eval(&value).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            boolean_to_smallint_cast_eval(&value).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            boolean_to_usmallint_cast_eval(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            boolean_to_integer_cast_eval(&value).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            boolean_to_uinteger_cast_eval(&value).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            boolean_to_bigint_cast_eval(&value).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            boolean_to_ubigint_cast_eval(&value).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            boolean_to_float_cast_eval(&value).unwrap(),
            DataValue::Float32(OrderedFloat(1.0))
        );
        assert_eq!(
            boolean_to_double_cast_eval(&value).unwrap(),
            DataValue::Float64(OrderedFloat(1.0))
        );
        assert_eq!(
            boolean_to_char_cast_eval(4, CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "true".to_string(),
                ty: Utf8Type::Fixed(4),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            boolean_to_varchar_cast_eval(Some(4), CharLengthUnits::Characters, &value).unwrap(),
            DataValue::Utf8 {
                value: "true".to_string(),
                ty: Utf8Type::Variable(Some(4)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            boolean_to_double_cast_eval(&DataValue::Boolean(false)).unwrap(),
            DataValue::Float64(OrderedFloat(0.0))
        );
        assert_eq!(
            boolean_to_varchar_cast_eval(
                None,
                CharLengthUnits::Characters,
                &DataValue::Boolean(false)
            )
            .unwrap(),
            DataValue::Utf8 {
                value: "false".to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }
        );
    }
}
