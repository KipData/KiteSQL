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
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanNotUnaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanAndBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanOrBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct BooleanNotEqBinaryEvaluator;

#[typetag::serde]
impl UnaryEvaluator for BooleanNotUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        match value {
            DataValue::Boolean(value) => DataValue::Boolean(!value),
            DataValue::Null => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}
#[typetag::serde]
impl BinaryEvaluator for BooleanAndBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
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
}

#[typetag::serde]
impl BinaryEvaluator for BooleanOrBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
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
}

#[typetag::serde]
impl BinaryEvaluator for BooleanEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 == *v2),
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

crate::define_cast_evaluator!(BooleanToTinyintCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::Int8(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToUTinyintCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::UInt8(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToSmallintCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::Int16(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToUSmallintCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::UInt16(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToIntegerCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::Int32(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToUIntegerCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::UInt32(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToBigintCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::Int64(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToUBigintCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::UInt64(if *value { 1 } else { 0 }))
});
crate::define_cast_evaluator!(BooleanToFloatCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::Float32(OrderedFloat(if *value { 1.0 } else { 0.0 })))
});
crate::define_cast_evaluator!(BooleanToDoubleCastEvaluator, DataValue::Boolean(value) => {
    Ok(DataValue::Float64(OrderedFloat(if *value { 1.0 } else { 0.0 })))
});
crate::define_cast_evaluator!(
    BooleanToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Boolean(value) => |this| to_char(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    BooleanToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Boolean(value) => |this| to_varchar(value.to_string(), this.len, this.unit)
);

#[typetag::serde]
impl BinaryEvaluator for BooleanNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Boolean(v1), DataValue::Boolean(v2)) => DataValue::Boolean(*v1 != *v2),
            (DataValue::Null, DataValue::Boolean(_))
            | (DataValue::Boolean(_), DataValue::Null)
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::{BinaryEvaluator, CastEvaluator};
    use crate::types::value::Utf8Type;

    #[test]
    fn test_boolean_binary_evaluators() {
        assert_eq!(
            BooleanAndBinaryEvaluator
                .binary_eval(&DataValue::Boolean(true), &DataValue::Boolean(true))
                .unwrap(),
            DataValue::Boolean(true)
        );
        assert_eq!(
            BooleanAndBinaryEvaluator
                .binary_eval(&DataValue::Boolean(false), &DataValue::Null)
                .unwrap(),
            DataValue::Boolean(false)
        );
        assert_eq!(
            BooleanOrBinaryEvaluator
                .binary_eval(&DataValue::Boolean(false), &DataValue::Boolean(true))
                .unwrap(),
            DataValue::Boolean(true)
        );
    }

    #[test]
    fn test_boolean_cast_evaluators() {
        let value = DataValue::Boolean(true);

        assert_eq!(
            BooleanToTinyintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            BooleanToUTinyintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            BooleanToSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            BooleanToUSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            BooleanToIntegerCastEvaluator
                .eval_cast(&value)
                .unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            BooleanToUIntegerCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            BooleanToBigintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            BooleanToUBigintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            BooleanToFloatCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float32(OrderedFloat(1.0))
        );
        assert_eq!(
            BooleanToDoubleCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float64(OrderedFloat(1.0))
        );
        assert_eq!(
            BooleanToCharCastEvaluator {
                len: 4,
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "true".to_string(),
                ty: Utf8Type::Fixed(4),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            BooleanToVarcharCastEvaluator {
                len: Some(4),
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "true".to_string(),
                ty: Utf8Type::Variable(Some(4)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            BooleanToDoubleCastEvaluator
                .eval_cast(&DataValue::Boolean(false))
                .unwrap(),
            DataValue::Float64(OrderedFloat(0.0))
        );
        assert_eq!(
            BooleanToVarcharCastEvaluator {
                len: None,
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&DataValue::Boolean(false))
            .unwrap(),
            DataValue::Utf8 {
                value: "false".to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }
        );
    }
}
