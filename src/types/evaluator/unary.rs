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
use crate::types::evaluator::boolean::boolean_not_unary_eval;
use crate::types::evaluator::float32::*;
use crate::types::evaluator::float64::*;
use crate::types::evaluator::int16::*;
use crate::types::evaluator::int32::*;
use crate::types::evaluator::int64::*;
use crate::types::evaluator::int8::*;
use crate::types::evaluator::UnaryEvaluatorRef;
use crate::types::LogicalType;
use std::borrow::Cow;

const UNARY_INT8_PLUS: u16 = 0;
const UNARY_INT8_MINUS: u16 = 1;
const UNARY_INT16_PLUS: u16 = 2;
const UNARY_INT16_MINUS: u16 = 3;
const UNARY_INT32_PLUS: u16 = 4;
const UNARY_INT32_MINUS: u16 = 5;
const UNARY_INT64_PLUS: u16 = 6;
const UNARY_INT64_MINUS: u16 = 7;
const UNARY_BOOLEAN_NOT: u16 = 8;
const UNARY_FLOAT32_PLUS: u16 = 9;
const UNARY_FLOAT32_MINUS: u16 = 10;
const UNARY_FLOAT64_PLUS: u16 = 11;
const UNARY_FLOAT64_MINUS: u16 = 12;

// Evaluator positions are serialized ABI. Do not reorder or reuse existing
// positions; only append new positions at the end of the current layout.

fn numeric_unary_ref(
    plus: u16,
    minus: u16,
    ty: &LogicalType,
    op: UnaryOperator,
) -> Result<UnaryEvaluatorRef, DatabaseError> {
    let pos = match op {
        UnaryOperator::Plus => plus,
        UnaryOperator::Minus => minus,
        _ => return Err(DatabaseError::UnsupportedUnaryOperator(ty.clone(), op)),
    };
    Ok(UnaryEvaluatorRef::new(pos))
}

pub fn unary_create(
    ty: Cow<'_, LogicalType>,
    op: UnaryOperator,
) -> Result<UnaryEvaluatorRef, DatabaseError> {
    let ty = ty.as_ref();
    match ty {
        LogicalType::Tinyint => numeric_unary_ref(UNARY_INT8_PLUS, UNARY_INT8_MINUS, ty, op),
        LogicalType::Smallint => numeric_unary_ref(UNARY_INT16_PLUS, UNARY_INT16_MINUS, ty, op),
        LogicalType::Integer => numeric_unary_ref(UNARY_INT32_PLUS, UNARY_INT32_MINUS, ty, op),
        LogicalType::Bigint => numeric_unary_ref(UNARY_INT64_PLUS, UNARY_INT64_MINUS, ty, op),
        LogicalType::Boolean => match op {
            UnaryOperator::Not => Ok(UnaryEvaluatorRef::new(UNARY_BOOLEAN_NOT)),
            _ => Err(DatabaseError::UnsupportedUnaryOperator(ty.clone(), op)),
        },
        LogicalType::Float => numeric_unary_ref(UNARY_FLOAT32_PLUS, UNARY_FLOAT32_MINUS, ty, op),
        LogicalType::Double => numeric_unary_ref(UNARY_FLOAT64_PLUS, UNARY_FLOAT64_MINUS, ty, op),
        _ => Err(DatabaseError::UnsupportedUnaryOperator(ty.clone(), op)),
    }
}

pub(crate) fn eval_unary(
    pos: u16,
    value: &crate::types::value::DataValue,
) -> crate::types::value::DataValue {
    match pos {
        UNARY_INT8_PLUS => int8_plus_unary_eval(value),
        UNARY_INT8_MINUS => int8_minus_unary_eval(value),
        UNARY_INT16_PLUS => int16_plus_unary_eval(value),
        UNARY_INT16_MINUS => int16_minus_unary_eval(value),
        UNARY_INT32_PLUS => int32_plus_unary_eval(value),
        UNARY_INT32_MINUS => int32_minus_unary_eval(value),
        UNARY_INT64_PLUS => int64_plus_unary_eval(value),
        UNARY_INT64_MINUS => int64_minus_unary_eval(value),
        UNARY_BOOLEAN_NOT => boolean_not_unary_eval(value),
        UNARY_FLOAT32_PLUS => float32_plus_unary_eval(value),
        UNARY_FLOAT32_MINUS => float32_minus_unary_eval(value),
        UNARY_FLOAT64_PLUS => float64_plus_unary_eval(value),
        UNARY_FLOAT64_MINUS => float64_minus_unary_eval(value),
        _ => unreachable!("unknown unary evaluator position {pos}"),
    }
}

#[macro_export]
macro_rules! numeric_unary_evaluator_definition {
    ($value_type:ident, $compute_type:path) => {
        paste::paste! {
            pub fn [<$value_type:snake _plus_unary_eval>](
                value: &$crate::types::value::DataValue,
            ) -> $crate::types::value::DataValue {
                value.clone()
            }
            pub fn [<$value_type:snake _minus_unary_eval>](
                value: &$crate::types::value::DataValue,
            ) -> $crate::types::value::DataValue {
                match value {
                    $compute_type(value) => $compute_type(-value),
                    $crate::types::value::DataValue::Null => $crate::types::value::DataValue::Null,
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
        }
    };
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::errors::DatabaseError;
    use crate::expression::UnaryOperator;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::UnaryEvaluatorRef;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use ordered_float::OrderedFloat;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};

    fn create(ty: LogicalType, op: UnaryOperator) -> Result<UnaryEvaluatorRef, DatabaseError> {
        unary_create(Cow::Owned(ty), op)
    }

    #[test]
    fn test_unary_evaluator_positions_are_stable() -> Result<(), DatabaseError> {
        assert_eq!(
            create(LogicalType::Integer, UnaryOperator::Minus)?.pos,
            UNARY_INT32_MINUS
        );
        assert_eq!(
            create(LogicalType::Boolean, UnaryOperator::Not)?.pos,
            UNARY_BOOLEAN_NOT
        );
        assert_eq!(
            create(LogicalType::Double, UnaryOperator::Plus)?.pos,
            UNARY_FLOAT64_PLUS
        );

        Ok(())
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
    fn test_boolean_unary_eval() -> Result<(), DatabaseError> {
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
        let mut arena = crate::planner::TableArena::default();

        evaluator.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            UnaryEvaluatorRef::decode::<RocksTransaction, _, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut arena
            )?,
            evaluator
        );

        Ok(())
    }
}
