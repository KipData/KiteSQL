pub mod boolean;
pub mod date;
pub mod datetime;
pub mod decimal;
pub mod float32;
pub mod float64;
pub mod int16;
pub mod int32;
pub mod int64;
pub mod int8;
pub mod null;
pub mod time;
pub mod time64;
pub mod tuple;
pub mod uint16;
pub mod uint32;
pub mod uint64;
pub mod uint8;
pub mod utf8;

use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, UnaryOperator};
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
use crate::types::evaluator::time::*;
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
use crate::types::evaluator::utf8::{
    Utf8EqBinaryEvaluator, Utf8GtBinaryEvaluator, Utf8GtEqBinaryEvaluator, Utf8LtBinaryEvaluator,
    Utf8LtEqBinaryEvaluator, Utf8NotEqBinaryEvaluator, Utf8StringConcatBinaryEvaluator,
};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use paste::paste;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[typetag::serde(tag = "binary")]
pub trait BinaryEvaluator: Send + Sync + Debug {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue;
}

#[typetag::serde(tag = "unary")]
pub trait UnaryEvaluator: Send + Sync + Debug {
    fn unary_eval(&self, value: &DataValue) -> DataValue;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinaryEvaluatorBox(pub Arc<dyn BinaryEvaluator>);

impl BinaryEvaluatorBox {
    pub fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
        self.0.binary_eval(left, right)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnaryEvaluatorBox(pub Arc<dyn UnaryEvaluator>);

impl UnaryEvaluatorBox {
    pub fn unary_eval(&self, value: &DataValue) -> DataValue {
        self.0.unary_eval(value)
    }
}

impl PartialEq for BinaryEvaluatorBox {
    fn eq(&self, _: &Self) -> bool {
        // FIXME
        true
    }
}

impl Eq for BinaryEvaluatorBox {}

impl Hash for BinaryEvaluatorBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i8(42)
    }
}

impl PartialEq for UnaryEvaluatorBox {
    fn eq(&self, _: &Self) -> bool {
        // FIXME
        true
    }
}

impl Eq for UnaryEvaluatorBox {}

impl Hash for UnaryEvaluatorBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i8(42)
    }
}

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
                _ => {
                    return Err(DatabaseError::UnsupportedBinaryOperator(
                        $ty,
                        $op,
                    ))
                }
            }
        }
    };
}

macro_rules! numeric_unary_evaluator {
    ($value_type:ident, $op:expr, $ty:expr) => {
        paste! {
            match $op {
                UnaryOperator::Plus => Ok(UnaryEvaluatorBox(Arc::new([<$value_type PlusUnaryEvaluator>]))),
                UnaryOperator::Minus => Ok(UnaryEvaluatorBox(Arc::new([<$value_type MinusUnaryEvaluator>]))),
                _ => {
                    return Err(DatabaseError::UnsupportedUnaryOperator(
                        $ty,
                        $op,
                    ))
                }
            }
        }
    };
}

pub struct EvaluatorFactory;

impl EvaluatorFactory {
    pub fn unary_create(
        ty: LogicalType,
        op: UnaryOperator,
    ) -> Result<UnaryEvaluatorBox, DatabaseError> {
        match ty {
            LogicalType::Tinyint => numeric_unary_evaluator!(Int8, op, LogicalType::Tinyint),
            LogicalType::Smallint => numeric_unary_evaluator!(Int16, op, LogicalType::Smallint),
            LogicalType::Integer => numeric_unary_evaluator!(Int32, op, LogicalType::Integer),
            LogicalType::Bigint => numeric_unary_evaluator!(Int64, op, LogicalType::Bigint),
            LogicalType::Boolean => match op {
                UnaryOperator::Not => Ok(UnaryEvaluatorBox(Arc::new(BooleanNotUnaryEvaluator))),
                _ => Err(DatabaseError::UnsupportedUnaryOperator(ty, op)),
            },
            LogicalType::Float => numeric_unary_evaluator!(Float32, op, LogicalType::Float),
            LogicalType::Double => numeric_unary_evaluator!(Float64, op, LogicalType::Double),
            _ => Err(DatabaseError::UnsupportedUnaryOperator(ty, op)),
        }
    }
    pub fn binary_create(
        ty: LogicalType,
        op: BinaryOperator,
    ) -> Result<BinaryEvaluatorBox, DatabaseError> {
        match ty {
            LogicalType::Tinyint => numeric_binary_evaluator!(Int8, op, LogicalType::Tinyint),
            LogicalType::Smallint => numeric_binary_evaluator!(Int16, op, LogicalType::Smallint),
            LogicalType::Integer => numeric_binary_evaluator!(Int32, op, LogicalType::Integer),
            LogicalType::Bigint => numeric_binary_evaluator!(Int64, op, LogicalType::Bigint),
            LogicalType::UTinyint => numeric_binary_evaluator!(UInt8, op, LogicalType::UTinyint),
            LogicalType::USmallint => numeric_binary_evaluator!(UInt16, op, LogicalType::USmallint),
            LogicalType::UInteger => numeric_binary_evaluator!(UInt32, op, LogicalType::UInteger),
            LogicalType::UBigint => numeric_binary_evaluator!(UInt64, op, LogicalType::UBigint),
            LogicalType::Float => numeric_binary_evaluator!(Float32, op, LogicalType::Float),
            LogicalType::Double => numeric_binary_evaluator!(Float64, op, LogicalType::Double),
            LogicalType::Date => numeric_binary_evaluator!(Date, op, LogicalType::Date),
            LogicalType::DateTime => numeric_binary_evaluator!(DateTime, op, LogicalType::DateTime),
            LogicalType::Time(_, _) => match op {
                BinaryOperator::Plus => Ok(BinaryEvaluatorBox(Arc::new(TimePlusBinaryEvaluator))),
                BinaryOperator::Minus => Ok(BinaryEvaluatorBox(Arc::new(TimeMinusBinaryEvaluator))),
                BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(TimeGtBinaryEvaluator))),
                BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(TimeGtEqBinaryEvaluator))),
                BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(TimeLtBinaryEvaluator))),
                BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(TimeLtEqBinaryEvaluator))),
                BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(TimeEqBinaryEvaluator))),
                BinaryOperator::NotEq => Ok(BinaryEvaluatorBox(Arc::new(TimeNotEqBinaryEvaluator))),
                _ => Err(DatabaseError::UnsupportedBinaryOperator(ty, op)),
            },
            LogicalType::TimeStamp(_, _) => match op {
                BinaryOperator::Plus => Ok(BinaryEvaluatorBox(Arc::new(Time64PlusBinaryEvaluator))),
                BinaryOperator::Minus => {
                    Ok(BinaryEvaluatorBox(Arc::new(Time64MinusBinaryEvaluator)))
                }
                BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(Time64GtBinaryEvaluator))),
                BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(Time64GtEqBinaryEvaluator))),
                BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(Time64LtBinaryEvaluator))),
                BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(Time64LtEqBinaryEvaluator))),
                BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(Time64EqBinaryEvaluator))),
                BinaryOperator::NotEq => {
                    Ok(BinaryEvaluatorBox(Arc::new(Time64NotEqBinaryEvaluator)))
                }
                _ => Err(DatabaseError::UnsupportedBinaryOperator(ty, op)),
            },
            LogicalType::Decimal(_, _) => numeric_binary_evaluator!(Decimal, op, ty),
            LogicalType::Boolean => match op {
                BinaryOperator::And => Ok(BinaryEvaluatorBox(Arc::new(BooleanAndBinaryEvaluator))),
                BinaryOperator::Or => Ok(BinaryEvaluatorBox(Arc::new(BooleanOrBinaryEvaluator))),
                BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(BooleanEqBinaryEvaluator))),
                BinaryOperator::NotEq => {
                    Ok(BinaryEvaluatorBox(Arc::new(BooleanNotEqBinaryEvaluator)))
                }
                _ => Err(DatabaseError::UnsupportedBinaryOperator(
                    LogicalType::Boolean,
                    op,
                )),
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
                _ => Err(DatabaseError::UnsupportedBinaryOperator(ty, op)),
            },
            LogicalType::SqlNull => Ok(BinaryEvaluatorBox(Arc::new(NullBinaryEvaluator))),
            LogicalType::Tuple(_) => match op {
                BinaryOperator::Eq => Ok(BinaryEvaluatorBox(Arc::new(TupleEqBinaryEvaluator))),
                BinaryOperator::NotEq => {
                    Ok(BinaryEvaluatorBox(Arc::new(TupleNotEqBinaryEvaluator)))
                }
                BinaryOperator::Gt => Ok(BinaryEvaluatorBox(Arc::new(TupleGtBinaryEvaluator))),
                BinaryOperator::GtEq => Ok(BinaryEvaluatorBox(Arc::new(TupleGtEqBinaryEvaluator))),
                BinaryOperator::Lt => Ok(BinaryEvaluatorBox(Arc::new(TupleLtBinaryEvaluator))),
                BinaryOperator::LtEq => Ok(BinaryEvaluatorBox(Arc::new(TupleLtEqBinaryEvaluator))),
                _ => Err(DatabaseError::UnsupportedBinaryOperator(ty, op)),
            },
        }
    }
}

#[macro_export]
macro_rules! numeric_unary_evaluator_definition {
    ($value_type:ident, $compute_type:path) => {
        paste! {
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type PlusUnaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type MinusUnaryEvaluator>];

            #[typetag::serde]
            impl UnaryEvaluator for [<$value_type PlusUnaryEvaluator>] {
                fn unary_eval(&self, value: &DataValue) -> DataValue {
                    value.clone()
                }
            }
            #[typetag::serde]
            impl UnaryEvaluator for [<$value_type MinusUnaryEvaluator>] {
                fn unary_eval(&self, value: &DataValue) -> DataValue {
                    match value {
                        $compute_type(value) => $compute_type(-value),
                        DataValue::Null => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! numeric_binary_evaluator_definition {
    ($value_type:ident, $compute_type:path) => {
        paste! {
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type PlusBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type MinusBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type MultiplyBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type DivideBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type GtBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type GtEqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type LtBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type LtEqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type EqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type NotEqBinaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
            pub struct [<$value_type ModBinaryEvaluator>];

            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type PlusBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(*v1 + *v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type MinusBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(*v1 - *v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type MultiplyBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(*v1 * *v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type DivideBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Float64(ordered_float::OrderedFloat(*v1 as f64 / *v2 as f64)),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type GtBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Boolean(v1 > v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type GtEqBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Boolean(v1 >= v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type LtBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Boolean(v1 < v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type LtEqBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Boolean(v1 <= v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type EqBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Boolean(v1 == v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type NotEqBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => DataValue::Boolean(v1 != v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
            #[typetag::serde]
            impl BinaryEvaluator for [<$value_type ModBinaryEvaluator>] {
                fn binary_eval(&self, left: &DataValue, right: &DataValue) -> DataValue {
                    match (left, right) {
                        ($compute_type(v1), $compute_type(v2)) => $compute_type(*v1 % *v2),
                        ($compute_type(_), DataValue::Null) | (DataValue::Null, $compute_type(_)) | (DataValue::Null, DataValue::Null) => DataValue::Null,
                        _ => unsafe { hint::unreachable_unchecked() },
                    }
                }
            }
        }
    };
}

#[cfg(test)]
mod test {
    use crate::errors::DatabaseError;
    use crate::expression::BinaryOperator;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::boolean::{BooleanNotEqBinaryEvaluator, BooleanNotUnaryEvaluator};
    use crate::types::evaluator::{BinaryEvaluatorBox, EvaluatorFactory, UnaryEvaluatorBox};
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::LogicalType;
    use ordered_float::OrderedFloat;
    use sqlparser::ast::CharLengthUnits;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;

    #[test]
    fn test_binary_op_arithmetic_plus() -> Result<(), DatabaseError> {
        let plus_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Plus)?;
        let plus_i32_1 = plus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let plus_i32_2 = plus_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Null);
        let plus_i32_3 = plus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int32(1));
        let plus_i32_4 = plus_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1));

        assert_eq!(plus_i32_1, plus_i32_2);
        assert_eq!(plus_i32_2, plus_i32_3);
        assert_eq!(plus_i32_4, DataValue::Int32(2));

        let plus_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Bigint, BinaryOperator::Plus)?;
        let plus_i64_1 = plus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let plus_i64_2 = plus_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Null);
        let plus_i64_3 = plus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int64(1));
        let plus_i64_4 = plus_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Int64(1));

        assert_eq!(plus_i64_1, plus_i64_2);
        assert_eq!(plus_i64_2, plus_i64_3);
        assert_eq!(plus_i64_4, DataValue::Int64(2));

        let plus_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Double, BinaryOperator::Plus)?;
        let plus_f64_1 = plus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let plus_f64_2 = plus_evaluator
            .0
            .binary_eval(&DataValue::Float64(OrderedFloat(1.0)), &DataValue::Null);
        let plus_f64_3 = plus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Float64(OrderedFloat(1.0)));
        let plus_f64_4 = plus_evaluator.0.binary_eval(
            &DataValue::Float64(OrderedFloat(1.0)),
            &DataValue::Float64(OrderedFloat(1.0)),
        );

        assert_eq!(plus_f64_1, plus_f64_2);
        assert_eq!(plus_f64_2, plus_f64_3);
        assert_eq!(plus_f64_4, DataValue::Float64(OrderedFloat(2.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_arithmetic_minus() -> Result<(), DatabaseError> {
        let minus_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Minus)?;
        let minus_i32_1 = minus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let minus_i32_2 = minus_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Null);
        let minus_i32_3 = minus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int32(1));
        let minus_i32_4 = minus_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1));

        assert_eq!(minus_i32_1, minus_i32_2);
        assert_eq!(minus_i32_2, minus_i32_3);
        assert_eq!(minus_i32_4, DataValue::Int32(0));

        let minus_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Bigint, BinaryOperator::Minus)?;
        let minus_i64_1 = minus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let minus_i64_2 = minus_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Null);
        let minus_i64_3 = minus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int64(1));
        let minus_i64_4 = minus_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Int64(1));

        assert_eq!(minus_i64_1, minus_i64_2);
        assert_eq!(minus_i64_2, minus_i64_3);
        assert_eq!(minus_i64_4, DataValue::Int64(0));

        let minus_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Double, BinaryOperator::Minus)?;
        let minus_f64_1 = minus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let minus_f64_2 = minus_evaluator
            .0
            .binary_eval(&DataValue::Float64(OrderedFloat(1.0)), &DataValue::Null);
        let minus_f64_3 = minus_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Float64(OrderedFloat(1.0)));
        let minus_f64_4 = minus_evaluator.0.binary_eval(
            &DataValue::Float64(OrderedFloat(1.0)),
            &DataValue::Float64(OrderedFloat(1.0)),
        );

        assert_eq!(minus_f64_1, minus_f64_2);
        assert_eq!(minus_f64_2, minus_f64_3);
        assert_eq!(minus_f64_4, DataValue::Float64(OrderedFloat(0.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_arithmetic_multiply() -> Result<(), DatabaseError> {
        let multiply_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Multiply)?;
        let multiply_i32_1 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let multiply_i32_2 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Null);
        let multiply_i32_3 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int32(1));
        let multiply_i32_4 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1));

        assert_eq!(multiply_i32_1, multiply_i32_2);
        assert_eq!(multiply_i32_2, multiply_i32_3);
        assert_eq!(multiply_i32_4, DataValue::Int32(1));

        let multiply_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Bigint, BinaryOperator::Multiply)?;
        let multiply_i64_1 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let multiply_i64_2 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Null);
        let multiply_i64_3 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int64(1));
        let multiply_i64_4 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Int64(1));

        assert_eq!(multiply_i64_1, multiply_i64_2);
        assert_eq!(multiply_i64_2, multiply_i64_3);
        assert_eq!(multiply_i64_4, DataValue::Int64(1));

        let multiply_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Double, BinaryOperator::Multiply)?;
        let multiply_f64_1 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let multiply_f64_2 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Float64(OrderedFloat(1.0)), &DataValue::Null);
        let multiply_f64_3 = multiply_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Float64(OrderedFloat(1.0)));
        let multiply_f64_4 = multiply_evaluator.0.binary_eval(
            &DataValue::Float64(OrderedFloat(1.0)),
            &DataValue::Float64(OrderedFloat(1.0)),
        );

        assert_eq!(multiply_f64_1, multiply_f64_2);
        assert_eq!(multiply_f64_2, multiply_f64_3);
        assert_eq!(multiply_f64_4, DataValue::Float64(OrderedFloat(1.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_arithmetic_divide() -> Result<(), DatabaseError> {
        let divide_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Divide)?;
        let divide_i32_1 = divide_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let divide_i32_2 = divide_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Null);
        let divide_i32_3 = divide_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int32(1));
        let divide_i32_4 = divide_evaluator
            .0
            .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1));

        assert_eq!(divide_i32_1, divide_i32_2);
        assert_eq!(divide_i32_2, divide_i32_3);
        assert_eq!(divide_i32_4, DataValue::Float64(OrderedFloat(1.0)));

        let divide_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Bigint, BinaryOperator::Divide)?;
        let divide_i64_1 = divide_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let divide_i64_2 = divide_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Null);
        let divide_i64_3 = divide_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Int64(1));
        let divide_i64_4 = divide_evaluator
            .0
            .binary_eval(&DataValue::Int64(1), &DataValue::Int64(1));

        assert_eq!(divide_i64_1, divide_i64_2);
        assert_eq!(divide_i64_2, divide_i64_3);
        assert_eq!(divide_i64_4, DataValue::Float64(OrderedFloat(1.0)));

        let divide_evaluator =
            EvaluatorFactory::binary_create(LogicalType::Double, BinaryOperator::Divide)?;
        let divide_f64_1 = divide_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Null);
        let divide_f64_2 = divide_evaluator
            .0
            .binary_eval(&DataValue::Float64(OrderedFloat(1.0)), &DataValue::Null);
        let divide_f64_3 = divide_evaluator
            .0
            .binary_eval(&DataValue::Null, &DataValue::Float64(OrderedFloat(1.0)));
        let divide_f64_4 = divide_evaluator.0.binary_eval(
            &DataValue::Float64(OrderedFloat(1.0)),
            &DataValue::Float64(OrderedFloat(1.0)),
        );

        assert_eq!(divide_f64_1, divide_f64_2);
        assert_eq!(divide_f64_2, divide_f64_3);
        assert_eq!(divide_f64_4, DataValue::Float64(OrderedFloat(1.0)));

        Ok(())
    }

    #[test]
    fn test_binary_op_i32_compare() -> Result<(), DatabaseError> {
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Gt)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Int32(1), &DataValue::Int32(0),),
            DataValue::Boolean(true)
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Lt)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Int32(1), &DataValue::Int32(0),),
            DataValue::Boolean(false)
        );
        let evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::GtEq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1),),
            DataValue::Boolean(true)
        );
        let evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::LtEq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1),),
            DataValue::Boolean(true)
        );
        let evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::NotEq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1),),
            DataValue::Boolean(false)
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Eq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Int32(1), &DataValue::Int32(1),),
            DataValue::Boolean(true)
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Gt)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Int32(0),),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Lt)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Int32(0),),
            DataValue::Null
        );
        let evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::GtEq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Int32(1),),
            DataValue::Null
        );
        let evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::LtEq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Int32(1),),
            DataValue::Null
        );
        let evaluator =
            EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::NotEq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Int32(1),),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Eq)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Int32(1),),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Eq)?;
        assert_eq!(
            evaluator.0.binary_eval(&DataValue::Null, &DataValue::Null,),
            DataValue::Null
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_bool_compare() -> Result<(), DatabaseError> {
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Boolean, BinaryOperator::And)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Boolean(true), &DataValue::Boolean(true),),
            DataValue::Boolean(true)
        );
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Boolean(false), &DataValue::Boolean(true),),
            DataValue::Boolean(false)
        );
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Boolean(false), &DataValue::Boolean(false),),
            DataValue::Boolean(false)
        );
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Boolean(true),),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(LogicalType::Boolean, BinaryOperator::Or)?;
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Boolean(true), &DataValue::Boolean(true),),
            DataValue::Boolean(true)
        );
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Boolean(false), &DataValue::Boolean(true),),
            DataValue::Boolean(true)
        );
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Boolean(false), &DataValue::Boolean(false),),
            DataValue::Boolean(false)
        );
        assert_eq!(
            evaluator
                .0
                .binary_eval(&DataValue::Null, &DataValue::Boolean(true),),
            DataValue::Boolean(true)
        );

        Ok(())
    }

    #[test]
    fn test_binary_op_utf8_compare() -> Result<(), DatabaseError> {
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::Gt,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: "b".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Boolean(false)
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::Lt,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: "b".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Boolean(true)
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::GtEq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Boolean(true)
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::LtEq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Boolean(true)
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::NotEq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Boolean(false)
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::Eq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Boolean(true)
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::Gt,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Null,
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::Lt,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Null,
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::GtEq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Null,
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::LtEq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Null,
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Null
        );
        let evaluator = EvaluatorFactory::binary_create(
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            BinaryOperator::NotEq,
        )?;
        assert_eq!(
            evaluator.0.binary_eval(
                &DataValue::Null,
                &DataValue::Utf8 {
                    value: "a".to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                },
            ),
            DataValue::Null
        );

        Ok(())
    }

    #[test]
    fn test_reference_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        let binary_evaluator = BinaryEvaluatorBox(Arc::new(BooleanNotEqBinaryEvaluator));
        binary_evaluator.encode(&mut cursor, false, &mut reference_tables)?;

        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            BinaryEvaluatorBox::decode::<RocksTransaction, _>(
                &mut cursor,
                None,
                &reference_tables
            )?,
            binary_evaluator
        );
        cursor.seek(SeekFrom::Start(0))?;
        let unary_evaluator = UnaryEvaluatorBox(Arc::new(BooleanNotUnaryEvaluator));
        unary_evaluator.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            UnaryEvaluatorBox::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            unary_evaluator
        );

        Ok(())
    }
}
