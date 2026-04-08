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
use crate::expression::BinaryOperator;
use crate::types::evaluator::boolean::*;
use crate::types::evaluator::date::*;
use crate::types::evaluator::datetime::*;
use crate::types::evaluator::decimal::*;
use crate::types::evaluator::float32::*;
use crate::types::evaluator::float64::*;
use crate::types::evaluator::int16::*;
use crate::types::evaluator::int32::*;
use crate::types::evaluator::int64::*;
use crate::types::evaluator::int8::*;
use crate::types::evaluator::null::NullBinaryEvaluator;
use crate::types::evaluator::time32::*;
use crate::types::evaluator::time64::*;
use crate::types::evaluator::tuple::{
    TupleEqBinaryEvaluator, TupleGtBinaryEvaluator, TupleGtEqBinaryEvaluator,
    TupleLtBinaryEvaluator, TupleLtEqBinaryEvaluator, TupleNotEqBinaryEvaluator,
};
use crate::types::evaluator::uint16::*;
use crate::types::evaluator::uint32::*;
use crate::types::evaluator::uint64::*;
use crate::types::evaluator::uint8::*;
use crate::types::evaluator::utf8::*;
use crate::types::evaluator::BinaryEvaluatorBox;
use crate::types::LogicalType;
use paste::paste;
use std::borrow::Cow;
use std::sync::Arc;

macro_rules! numeric_binary_evaluator {
    ($value_type:ident, $op:expr, $ty:expr) => {
        paste! {
            match $op {
                BinaryOperator::Plus => Ok(BinaryEvaluatorBox(Arc::new([<$value_type PlusBinaryEvaluator>]))),
                BinaryOperator::Minus => Ok(BinaryEvaluatorBox(Arc::new([<$value_type MinusBinaryEvaluator>]))),
                BinaryOperator::Multiply => Ok(BinaryEvaluatorBox(Arc::new([<$value_type MultiplyBinaryEvaluator>]))),
                BinaryOperator::Divide => Ok(BinaryEvaluatorBox(Arc::new([<$value_type DivideBinaryEvaluator>]))),
                BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new([<$value_type GtBinaryEvaluator>]))),
                BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new([<$value_type GtEqBinaryEvaluator>]))),
                BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new([<$value_type LtBinaryEvaluator>]))),
                BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new([<$value_type LtEqBinaryEvaluator>]))),
                BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new([<$value_type EqBinaryEvaluator>]))),
                BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new([<$value_type NotEqBinaryEvaluator>]))),
                BinaryOperator::Modulo => Ok(BinaryEvaluatorBox(Arc::new([<$value_type ModBinaryEvaluator>]))),
                _ => Err(DatabaseError::UnsupportedBinaryOperator($ty.clone(), $op)),
            }
        }
    };
}

pub(crate) fn create_binary_evaluator(
    ty: Cow<'_, LogicalType>,
    op: BinaryOperator,
) -> Result<BinaryEvaluatorBox, DatabaseError> {
    let ty = ty.as_ref();
    match ty {
        LogicalType::Tinyint => numeric_binary_evaluator!(Int8, op, ty),
        LogicalType::Smallint => numeric_binary_evaluator!(Int16, op, ty),
        LogicalType::Integer => numeric_binary_evaluator!(Int32, op, ty),
        LogicalType::Bigint => numeric_binary_evaluator!(Int64, op, ty),
        LogicalType::UTinyint => numeric_binary_evaluator!(UInt8, op, ty),
        LogicalType::USmallint => numeric_binary_evaluator!(UInt16, op, ty),
        LogicalType::UInteger => numeric_binary_evaluator!(UInt32, op, ty),
        LogicalType::UBigint => numeric_binary_evaluator!(UInt64, op, ty),
        LogicalType::Float => numeric_binary_evaluator!(Float32, op, ty),
        LogicalType::Double => numeric_binary_evaluator!(Float64, op, ty),
        LogicalType::Date => numeric_binary_evaluator!(Date, op, ty),
        LogicalType::DateTime => numeric_binary_evaluator!(DateTime, op, ty),
        LogicalType::Time(_) => match op {
            BinaryOperator::Plus => Ok(BinaryEvaluatorBox(Arc::new(TimePlusBinaryEvaluator))),
            BinaryOperator::Minus => Ok(BinaryEvaluatorBox(Arc::new(TimeMinusBinaryEvaluator))),
            BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(TimeGtBinaryEvaluator))),
            BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(TimeGtEqBinaryEvaluator))),
            BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(TimeLtBinaryEvaluator))),
            BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(TimeLtEqBinaryEvaluator))),
            BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(TimeEqBinaryEvaluator))),
            BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new(TimeNotEqBinaryEvaluator))),
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
        LogicalType::TimeStamp(_, _) => match op {
            BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(Time64GtBinaryEvaluator))),
            BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(Time64GtEqBinaryEvaluator))),
            BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(Time64LtBinaryEvaluator))),
            BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(Time64LtEqBinaryEvaluator))),
            BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(Time64EqBinaryEvaluator))),
            BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new(Time64NotEqBinaryEvaluator))),
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
        LogicalType::Decimal(_, _) => numeric_binary_evaluator!(Decimal, op, ty),
        LogicalType::Boolean => match op {
            BinaryOperator::And => Ok(BinaryEvaluatorBox(Arc::new(BooleanAndBinaryEvaluator))),
            BinaryOperator::Or => Ok(BinaryEvaluatorBox(Arc::new(BooleanOrBinaryEvaluator))),
            BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(BooleanEqBinaryEvaluator))),
            BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new(BooleanNotEqBinaryEvaluator))),
            _ => Err(DatabaseError::UnsupportedBinaryOperator(LogicalType::Boolean, op)),
        },
        LogicalType::Varchar(_, _) | LogicalType::Char(_, _) => match op {
            BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(Utf8GtBinaryEvaluator))),
            BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(Utf8LtBinaryEvaluator))),
            BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(Utf8GtEqBinaryEvaluator))),
            BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(Utf8LtEqBinaryEvaluator))),
            BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(Utf8EqBinaryEvaluator))),
            BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new(Utf8NotEqBinaryEvaluator))),
            BinaryOperator::StringConcat => Ok(BinaryEvaluatorBox(Arc::new(
                Utf8StringConcatBinaryEvaluator,
            ))),
            BinaryOperator::Like(escape_char) => {
                Ok(BinaryEvaluatorBox(Arc::new(Utf8LikeBinaryEvaluator {
                    escape_char,
                })))
            }
            BinaryOperator::NotLike(escape_char) => {
                Ok(BinaryEvaluatorBox(Arc::new(Utf8NotLikeBinaryEvaluator {
                    escape_char,
                })))
            }
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
        LogicalType::SqlNull => Ok(BinaryEvaluatorBox(Arc::new(NullBinaryEvaluator))),
        LogicalType::Tuple(_) => match op {
            BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(TupleEqBinaryEvaluator))),
            BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new(TupleNotEqBinaryEvaluator))),
            BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(TupleGtBinaryEvaluator))),
            BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(TupleGtEqBinaryEvaluator))),
            BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(TupleLtBinaryEvaluator))),
            BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(TupleLtEqBinaryEvaluator))),
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
    }
}

#[macro_export]
macro_rules! numeric_binary_evaluator_definition {
    ($value_type:ident, $compute_type:path) => {
        paste::paste! {
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type PlusBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type MinusBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type MultiplyBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type DivideBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type GtBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type GtEqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type LtBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type LtEqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type EqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type NotEqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type ModBinaryEvaluator>];

            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type PlusBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(v1.checked_add(*v2).ok_or($crate::errors::DatabaseError::OverFlow)?),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type MinusBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(v1.checked_sub(*v2).ok_or($crate::errors::DatabaseError::OverFlow)?),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type MultiplyBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(v1.checked_mul(*v2).ok_or($crate::errors::DatabaseError::OverFlow)?),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type DivideBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Float64(ordered_float::OrderedFloat(*v1 as f64 / *v2 as f64)),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type GtBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Boolean(v1 > v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type GtEqBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Boolean(v1 >= v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type LtBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Boolean(v1 < v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type LtEqBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Boolean(v1 <= v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type EqBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Boolean(v1 == v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type NotEqBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $crate::types::value::DataValue::Boolean(v1 != v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::BinaryEvaluator for [<$value_type ModBinaryEvaluator>] {
                fn binary_eval(
                    &self,
                    left: &$crate::types::value::DataValue,
                    right: &$crate::types::value::DataValue,
                ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                    Ok(match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(*v1 % *v2),
                        ($compute_type(_), $crate::types::value::DataValue::Null)
                        | ($crate::types::value::DataValue::Null, $compute_type(_))
                        | ($crate::types::value::DataValue::Null, $crate::types::value::DataValue::Null) => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    })
                }
            }
        }
    };
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::create_binary_evaluator;
    use crate::errors::DatabaseError;
    use crate::expression::BinaryOperator;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::BinaryEvaluatorBox;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};

    fn create(ty: LogicalType, op: BinaryOperator) -> Result<BinaryEvaluatorBox, DatabaseError> {
        create_binary_evaluator(Cow::Owned(ty), op)
    }

    #[test]
    fn test_binary_evaluator_serialization() -> Result<(), DatabaseError> {
        let evaluator = create(LogicalType::Boolean, BinaryOperator::NotEq)?;
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        evaluator.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            BinaryEvaluatorBox::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            evaluator
        );

        Ok(())
    }
}
