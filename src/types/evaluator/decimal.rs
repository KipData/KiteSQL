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
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use ordered_float::OrderedFloat;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalPlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalMinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalMultiplyBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalDivideBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalGtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalGtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalLtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalLtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalNotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct DecimalModBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for DecimalPlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 + v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalMinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 - v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

crate::define_cast_evaluator!(DecimalToFloatCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Float32(OrderedFloat(value.to_f32().ok_or_else(|| {
        crate::types::evaluator::cast::cast_fail(
            crate::types::LogicalType::Decimal(None, None),
            crate::types::LogicalType::Float,
        )
    })?)))
});
crate::define_cast_evaluator!(DecimalToDoubleCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Float64(OrderedFloat(value.to_f64().ok_or_else(|| {
        crate::types::evaluator::cast::cast_fail(
            crate::types::LogicalType::Decimal(None, None),
            crate::types::LogicalType::Double,
        )
    })?)))
});
crate::define_cast_evaluator!(DecimalToDecimalCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Decimal(*value))
});
crate::define_cast_evaluator!(
    DecimalToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Decimal(value) => |this| to_char(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    DecimalToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Decimal(value) => |this| to_varchar(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(DecimalToTinyintCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Int8(crate::decimal_to_int_cast!(*value, i8)))
});
crate::define_cast_evaluator!(DecimalToSmallintCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Int16(crate::decimal_to_int_cast!(*value, i16)))
});
crate::define_cast_evaluator!(DecimalToIntegerCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Int32(crate::decimal_to_int_cast!(*value, i32)))
});
crate::define_cast_evaluator!(DecimalToBigintCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::Int64(crate::decimal_to_int_cast!(*value, i64)))
});
crate::define_cast_evaluator!(DecimalToUTinyintCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::UInt8(crate::decimal_to_int_cast!(*value, u8)))
});
crate::define_cast_evaluator!(DecimalToUSmallintCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::UInt16(crate::decimal_to_int_cast!(*value, u16)))
});
crate::define_cast_evaluator!(DecimalToUIntegerCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::UInt32(crate::decimal_to_int_cast!(*value, u32)))
});
crate::define_cast_evaluator!(DecimalToUBigintCastEvaluator, DataValue::Decimal(value) => {
    Ok(DataValue::UInt64(crate::decimal_to_int_cast!(*value, u64)))
});

#[typetag::serde]
impl BinaryEvaluator for DecimalMultiplyBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 * v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalDivideBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 / v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalGtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 > v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalGtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 >= v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalLtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 < v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalLtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 <= v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 == v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalNotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Boolean(v1 != v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for DecimalModBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Decimal(v1), DataValue::Decimal(v2)) => DataValue::Decimal(v1 % v2),
            (DataValue::Decimal(_), DataValue::Null)
            | (DataValue::Null, DataValue::Decimal(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::types::evaluator::CastEvaluator;
    use crate::types::value::Utf8Type;
    use rust_decimal::Decimal;
    use sqlparser::ast::CharLengthUnits;

    #[test]
    fn test_decimal_cast_evaluators() {
        let value = DataValue::Decimal(Decimal::new(125, 1));

        assert_eq!(
            DecimalToFloatCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float32(OrderedFloat(12.5))
        );
        assert_eq!(
            DecimalToDoubleCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float64(OrderedFloat(12.5))
        );
        assert_eq!(
            DecimalToDecimalCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Decimal(Decimal::new(125, 1))
        );
        assert_eq!(
            DecimalToCharCastEvaluator {
                len: 4,
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "12.5".to_string(),
                ty: Utf8Type::Fixed(4),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            DecimalToVarcharCastEvaluator {
                len: Some(4),
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "12.5".to_string(),
                ty: Utf8Type::Variable(Some(4)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            DecimalToTinyintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int8(12)
        );
        assert_eq!(
            DecimalToSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int16(12)
        );
        assert_eq!(
            DecimalToIntegerCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int32(12)
        );
        assert_eq!(
            DecimalToBigintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int64(12)
        );
        assert_eq!(
            DecimalToUTinyintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt8(12)
        );
        assert_eq!(
            DecimalToUSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt16(12)
        );
        assert_eq!(
            DecimalToUIntegerCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt32(12)
        );
        assert_eq!(
            DecimalToUBigintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt64(12)
        );
    }
}
