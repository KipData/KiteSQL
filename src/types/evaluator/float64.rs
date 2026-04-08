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
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use crate::types::LogicalType;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::hint;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64PlusUnaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64MinusUnaryEvaluator;

#[typetag::serde]
impl UnaryEvaluator for Float64PlusUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        value.clone()
    }
}
#[typetag::serde]
impl UnaryEvaluator for Float64MinusUnaryEvaluator {
    fn unary_eval(&self, value: &DataValue) -> DataValue {
        match value {
            DataValue::Float64(value) => DataValue::Float64(-value),
            DataValue::Null => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64PlusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64MinusBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64MultiplyBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64DivideBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64GtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64GtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64LtBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64LtEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64EqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64NotEqBinaryEvaluator;
#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct Float64ModBinaryEvaluator;

#[typetag::serde]
impl BinaryEvaluator for Float64PlusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Float64(*v1 + *v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64MinusBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Float64(*v1 - *v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}

crate::define_cast_evaluator!(Float64ToFloatCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::Float32(ordered_float::OrderedFloat(value.0 as f32)))
});
crate::define_cast_evaluator!(Float64ToDoubleCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::Float64(*value))
});
crate::define_cast_evaluator!(Float64ToTinyintCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::Int8(crate::float_to_int_cast!(value.into_inner(), i8, f64)?))
});
crate::define_cast_evaluator!(Float64ToSmallintCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::Int16(crate::float_to_int_cast!(value.into_inner(), i16, f64)?))
});
crate::define_cast_evaluator!(Float64ToIntegerCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::Int32(crate::float_to_int_cast!(value.into_inner(), i32, f64)?))
});
crate::define_cast_evaluator!(Float64ToBigintCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::Int64(crate::float_to_int_cast!(value.into_inner(), i64, f64)?))
});
crate::define_cast_evaluator!(Float64ToUTinyintCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::UInt8(crate::float_to_int_cast!(value.into_inner(), u8, f64)?))
});
crate::define_cast_evaluator!(Float64ToUSmallintCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::UInt16(crate::float_to_int_cast!(value.into_inner(), u16, f64)?))
});
crate::define_cast_evaluator!(Float64ToUIntegerCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::UInt32(crate::float_to_int_cast!(value.into_inner(), u32, f64)?))
});
crate::define_cast_evaluator!(Float64ToUBigintCastEvaluator, DataValue::Float64(value) => {
    Ok(DataValue::UInt64(crate::float_to_int_cast!(value.into_inner(), u64, f64)?))
});
crate::define_cast_evaluator!(
    Float64ToCharCastEvaluator {
        len: u32,
        unit: CharLengthUnits
    },
    DataValue::Float64(value) => |this| to_char(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    Float64ToVarcharCastEvaluator {
        len: Option<u32>,
        unit: CharLengthUnits
    },
    DataValue::Float64(value) => |this| to_varchar(value.to_string(), this.len, this.unit)
);
crate::define_cast_evaluator!(
    Float64ToDecimalCastEvaluator {
        scale: Option<u8>,
        to: LogicalType
    },
    DataValue::Float64(value) => |this| {
        let mut decimal = Decimal::from_f64(value.0).ok_or_else(|| {
            cast_fail(LogicalType::Double, this.to.clone())
        })?;
        DataValue::decimal_round_f(&this.scale, &mut decimal);

        Ok(DataValue::Decimal(decimal))
    }
);

#[typetag::serde]
impl BinaryEvaluator for Float64MultiplyBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Float64(*v1 * *v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64DivideBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => {
                DataValue::Float64(ordered_float::OrderedFloat(**v1 / **v2))
            }
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64GtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Boolean(v1 > v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64GtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Boolean(v1 >= v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64LtBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Boolean(v1 < v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64LtEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Boolean(v1 <= v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64EqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Boolean(v1 == v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64NotEqBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Boolean(v1 != v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
            | (DataValue::Null, DataValue::Null) => DataValue::Null,
            _ => unsafe { hint::unreachable_unchecked() },
        })
    }
}
#[typetag::serde]
impl BinaryEvaluator for Float64ModBinaryEvaluator {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(match (left, right) {
            (DataValue::Float64(v1), DataValue::Float64(v2)) => DataValue::Float64(*v1 % *v2),
            (DataValue::Float64(_), DataValue::Null)
            | (DataValue::Null, DataValue::Float64(_))
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
    use rust_decimal::Decimal;
    use sqlparser::ast::CharLengthUnits;

    #[test]
    fn test_float64_binary_and_cast_evaluators() {
        let value = DataValue::Float64(ordered_float::OrderedFloat(1.5));

        assert_eq!(
            Float64MultiplyBinaryEvaluator
                .binary_eval(
                    &DataValue::Float64(ordered_float::OrderedFloat(1.5)),
                    &DataValue::Float64(ordered_float::OrderedFloat(2.0)),
                )
                .unwrap(),
            DataValue::Float64(ordered_float::OrderedFloat(3.0))
        );
        assert_eq!(
            Float64ToFloatCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float32(ordered_float::OrderedFloat(1.5))
        );
        assert_eq!(
            Float64ToDoubleCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Float64(ordered_float::OrderedFloat(1.5))
        );
        assert_eq!(
            Float64ToTinyintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int8(1)
        );
        assert_eq!(
            Float64ToSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int16(1)
        );
        assert_eq!(
            Float64ToIntegerCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int32(1)
        );
        assert_eq!(
            Float64ToBigintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::Int64(1)
        );
        assert_eq!(
            Float64ToUTinyintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt8(1)
        );
        assert_eq!(
            Float64ToUSmallintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt16(1)
        );
        assert_eq!(
            Float64ToUIntegerCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt32(1)
        );
        assert_eq!(
            Float64ToUBigintCastEvaluator.eval_cast(&value).unwrap(),
            DataValue::UInt64(1)
        );
        assert_eq!(
            Float64ToCharCastEvaluator {
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
            Float64ToVarcharCastEvaluator {
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
            Float64ToDecimalCastEvaluator {
                scale: Some(1),
                to: LogicalType::Decimal(None, Some(1)),
            }
            .eval_cast(&value)
            .unwrap(),
            DataValue::Decimal(Decimal::new(15, 1))
        );
    }
}
