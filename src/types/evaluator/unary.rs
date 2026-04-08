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
use crate::expression::UnaryOperator;
use crate::types::evaluator::boolean::BooleanNotUnaryEvaluator;
use crate::types::evaluator::float32::*;
use crate::types::evaluator::float64::*;
use crate::types::evaluator::int16::*;
use crate::types::evaluator::int32::*;
use crate::types::evaluator::int64::*;
use crate::types::evaluator::int8::*;
use crate::types::evaluator::UnaryEvaluatorBox;
use crate::types::LogicalType;
use paste::paste;
use std::borrow::Cow;
use std::sync::Arc;

macro_rules! numeric_unary_evaluator {
    ($value_type:ident, $op:expr, $ty:expr) => {
        paste! {
            match $op {
                UnaryOperator::Plus => Ok(UnaryEvaluatorBox(Arc::new([<$value_type PlusUnaryEvaluator>]))),
                UnaryOperator::Minus => Ok(UnaryEvaluatorBox(Arc::new([<$value_type MinusUnaryEvaluator>]))),
                _ => Err(DatabaseError::UnsupportedUnaryOperator($ty.clone(), $op)),
            }
        }
    };
}

pub(crate) fn create_unary_evaluator(
    ty: Cow<'_, LogicalType>,
    op: UnaryOperator,
) -> Result<UnaryEvaluatorBox, DatabaseError> {
    let ty = ty.as_ref();
    match ty {
        LogicalType::Tinyint => numeric_unary_evaluator!(Int8, op, ty),
        LogicalType::Smallint => numeric_unary_evaluator!(Int16, op, ty),
        LogicalType::Integer => numeric_unary_evaluator!(Int32, op, ty),
        LogicalType::Bigint => numeric_unary_evaluator!(Int64, op, ty),
        LogicalType::Boolean => match op {
            UnaryOperator::Not => Ok(UnaryEvaluatorBox(Arc::new(BooleanNotUnaryEvaluator))),
            _ => Err(DatabaseError::UnsupportedUnaryOperator(ty.clone(), op)),
        },
        LogicalType::Float => numeric_unary_evaluator!(Float32, op, ty),
        LogicalType::Double => numeric_unary_evaluator!(Float64, op, ty),
        _ => Err(DatabaseError::UnsupportedUnaryOperator(ty.clone(), op)),
    }
}

#[macro_export]
macro_rules! numeric_unary_evaluator_definition {
    ($value_type:ident, $compute_type:path) => {
        paste::paste! {
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type PlusUnaryEvaluator>];
            #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
            pub struct [<$value_type MinusUnaryEvaluator>];

            #[typetag::serde]
            impl $crate::types::evaluator::UnaryEvaluator for [<$value_type PlusUnaryEvaluator>] {
                fn unary_eval(&self, value: &$crate::types::value::DataValue) -> $crate::types::value::DataValue {
                    value.clone()
                }
            }
            #[typetag::serde]
            impl $crate::types::evaluator::UnaryEvaluator for [<$value_type MinusUnaryEvaluator>] {
                fn unary_eval(&self, value: &$crate::types::value::DataValue) -> $crate::types::value::DataValue {
                    match value {
                        $compute_type(value) => $compute_type(-value),
                        $crate::types::value::DataValue::Null => $crate::types::value::DataValue::Null,
                        _ => unsafe { std::hint::unreachable_unchecked() },
                    }
                }
            }
        }
    };
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::create_unary_evaluator;
    use crate::errors::DatabaseError;
    use crate::expression::UnaryOperator;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::UnaryEvaluatorBox;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use ordered_float::OrderedFloat;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};

    fn create(ty: LogicalType, op: UnaryOperator) -> Result<UnaryEvaluatorBox, DatabaseError> {
        create_unary_evaluator(Cow::Owned(ty), op)
    }

    #[test]
    fn test_numeric_unary_evaluators() -> Result<(), DatabaseError> {
        let cases = vec![
            (
                LogicalType::Integer,
                UnaryOperator::Plus,
                DataValue::Int32(7),
                DataValue::Int32(7),
            ),
            (
                LogicalType::Integer,
                UnaryOperator::Minus,
                DataValue::Int32(7),
                DataValue::Int32(-7),
            ),
            (
                LogicalType::Bigint,
                UnaryOperator::Minus,
                DataValue::Int64(7),
                DataValue::Int64(-7),
            ),
            (
                LogicalType::Double,
                UnaryOperator::Minus,
                DataValue::Float64(OrderedFloat(1.5)),
                DataValue::Float64(OrderedFloat(-1.5)),
            ),
        ];

        for (ty, op, value, expected) in cases {
            let evaluator = create(ty, op)?;
            assert_eq!(evaluator.unary_eval(&value), expected);
            assert_eq!(evaluator.unary_eval(&DataValue::Null), DataValue::Null);
        }

        Ok(())
    }

    #[test]
    fn test_boolean_unary_evaluator() -> Result<(), DatabaseError> {
        let evaluator = create(LogicalType::Boolean, UnaryOperator::Not)?;
        assert_eq!(
            evaluator.unary_eval(&DataValue::Boolean(true)),
            DataValue::Boolean(false)
        );
        assert_eq!(evaluator.unary_eval(&DataValue::Null), DataValue::Null);
        Ok(())
    }

    #[test]
    fn test_unary_evaluator_rejects_unsupported_operator() {
        let err = create(LogicalType::Boolean, UnaryOperator::Plus).unwrap_err();
        assert!(matches!(
            err,
            DatabaseError::UnsupportedUnaryOperator(LogicalType::Boolean, UnaryOperator::Plus)
        ));
    }

    #[test]
    fn test_unary_evaluator_serialization() -> Result<(), DatabaseError> {
        let evaluator = create(LogicalType::Boolean, UnaryOperator::Not)?;
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        evaluator.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            UnaryEvaluatorBox::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            evaluator
        );

        Ok(())
    }
}
