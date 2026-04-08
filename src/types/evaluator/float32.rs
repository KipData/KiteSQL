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
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use crate::types::LogicalType;
use rust_decimal::prelude::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32PlusUnaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32MinusUnaryEvaluator;

#[typetag::serde]
impl UnaryEvaluator for Float32PlusUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        value.clone()
    }
}
#[typetag::serde]
impl UnaryEvaluator for Float32MinusUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        match value {
            DataValue::Float32(value) => DataValue::Float32(-value),
            DataValue::Null => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32PlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32MinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32MultiplyBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32DivideBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32NotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float32ModBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for Float32PlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 + *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32MinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 - *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

crate::define_float_cast_evaluators!(Float32, Float32, f32, LogicalType::Float, from_f32);
#[typetag::serde]
impl BinaryEvaluator for Float32MultiplyBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 * *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32DivideBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => {
                DataValue::Float64(ordered_float::OrderedFloat(**v1 as f64 / **v2 as f64))
            }
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 > v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 >= v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 < v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 <= v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 == v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Boolean(v1 != v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float32ModBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float32(v1), DataValue::Float32(v2)) => DataValue::Float32(*v1 % *v2),
            (DataValue::Float32(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float32(_))
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
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use sqlparser::ast::CharLengthUnits;

    #[test]
    fn test_float32_cast_evaluators() {
        let value = DataValue::Float32(OrderedFloat(1.5));

        assert_eq!(
            Float32ToFloatCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float32(OrderedFloat(1.5))
        );
        assert_eq!(
            Float32ToDoubleCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float64(OrderedFloat(1.5))
        );
        assert_eq!(Float32ToTinyintCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int8(1));
        assert_eq!(Float32ToSmallintCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int16(1));
        assert_eq!(Float32ToIntegerCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int32(1));
        assert_eq!(Float32ToBigintCastEvaluator.eval_cast(&value).unwrap(), DataValue::Int64(1));
        assert_eq!(Float32ToUTinyintCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt8(1));
        assert_eq!(
            Float32ToUSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(Float32ToUIntegerCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt32(1));
        assert_eq!(Float32ToUBigintCastEvaluator.eval_cast(&value).unwrap(), DataValue::UInt64(1));
        assert_eq!(
            Float32ToCharCastEvaluator {
                len: 3,
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "1.5".to_string(),
                ty: Utf8Type::Fixed(3),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Float32ToVarcharCastEvaluator {
                len: Some(3),
                unit: CharLengthUnits::Characters,
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Utf8 {
                value: "1.5".to_string(),
                ty: Utf8Type::Variable(Some(3)),
                unit: CharLengthUnits::Characters,
            }
        );
        assert_eq!(
            Float32ToDecimalCastEvaluator {
                scale: Some(1),
                to: LogicalType::Decimal(None, Some(1)),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Decimal(Decimal::new(15, 1))
        );
    }
}
