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
#[cfg(feature = "time")]
use crate::types::evaluator::date::*;
#[cfg(feature = "time")]
use crate::types::evaluator::datetime::*;
#[cfg(feature = "decimal")]
use crate::types::evaluator::decimal::*;
use crate::types::evaluator::float32::*;
use crate::types::evaluator::float64::*;
use crate::types::evaluator::int16::*;
use crate::types::evaluator::int32::*;
use crate::types::evaluator::int64::*;
use crate::types::evaluator::int8::*;
use crate::types::evaluator::null::*;
#[cfg(feature = "time")]
use crate::types::evaluator::time32::*;
#[cfg(feature = "time")]
use crate::types::evaluator::time64::*;
use crate::types::evaluator::tuple::*;
use crate::types::evaluator::uint16::*;
use crate::types::evaluator::uint32::*;
use crate::types::evaluator::uint64::*;
use crate::types::evaluator::uint8::*;
use crate::types::evaluator::utf8::*;
use crate::types::evaluator::{BinaryEvaluatorParams, BinaryEvaluatorRef};
use crate::types::LogicalType;
use paste::paste;
use std::borrow::Cow;

const NUMERIC_PLUS_OFFSET: u16 = 0;
const NUMERIC_MINUS_OFFSET: u16 = 1;
const NUMERIC_MULTIPLY_OFFSET: u16 = 2;
const NUMERIC_DIVIDE_OFFSET: u16 = 3;
const NUMERIC_GT_OFFSET: u16 = 4;
const NUMERIC_GT_EQ_OFFSET: u16 = 5;
const NUMERIC_LT_OFFSET: u16 = 6;
const NUMERIC_LT_EQ_OFFSET: u16 = 7;
const NUMERIC_EQ_OFFSET: u16 = 8;
const NUMERIC_NOT_EQ_OFFSET: u16 = 9;
const NUMERIC_MODULO_OFFSET: u16 = 10;
const NUMERIC_OPS_LEN: u16 = NUMERIC_MODULO_OFFSET + 1;

#[cfg(feature = "time")]
const TIME_PLUS_OFFSET: u16 = 0;
#[cfg(feature = "time")]
const TIME_MINUS_OFFSET: u16 = 1;
#[cfg(feature = "time")]
const TIME_GT_OFFSET: u16 = 2;
#[cfg(feature = "time")]
const TIME_GT_EQ_OFFSET: u16 = 3;
#[cfg(feature = "time")]
const TIME_LT_OFFSET: u16 = 4;
#[cfg(feature = "time")]
const TIME_LT_EQ_OFFSET: u16 = 5;
#[cfg(feature = "time")]
const TIME_EQ_OFFSET: u16 = 6;
#[cfg(feature = "time")]
const TIME_NOT_EQ_OFFSET: u16 = 7;
const TIME_OPS_LEN: u16 = 8;

#[cfg(feature = "time")]
const TIMESTAMP_GT_OFFSET: u16 = 0;
#[cfg(feature = "time")]
const TIMESTAMP_GT_EQ_OFFSET: u16 = 1;
#[cfg(feature = "time")]
const TIMESTAMP_LT_OFFSET: u16 = 2;
#[cfg(feature = "time")]
const TIMESTAMP_LT_EQ_OFFSET: u16 = 3;
#[cfg(feature = "time")]
const TIMESTAMP_EQ_OFFSET: u16 = 4;
#[cfg(feature = "time")]
const TIMESTAMP_NOT_EQ_OFFSET: u16 = 5;
const TIMESTAMP_OPS_LEN: u16 = 6;

const BOOLEAN_AND_OFFSET: u16 = 0;
const BOOLEAN_OR_OFFSET: u16 = 1;
const BOOLEAN_EQ_OFFSET: u16 = 2;
const BOOLEAN_NOT_EQ_OFFSET: u16 = 3;
const BOOLEAN_OPS_LEN: u16 = BOOLEAN_NOT_EQ_OFFSET + 1;

const UTF8_GT_OFFSET: u16 = 0;
const UTF8_LT_OFFSET: u16 = 1;
const UTF8_GT_EQ_OFFSET: u16 = 2;
const UTF8_LT_EQ_OFFSET: u16 = 3;
const UTF8_EQ_OFFSET: u16 = 4;
const UTF8_NOT_EQ_OFFSET: u16 = 5;
const UTF8_STRING_CONCAT_OFFSET: u16 = 6;
const UTF8_LIKE_OFFSET: u16 = 7;
const UTF8_NOT_LIKE_OFFSET: u16 = 8;
const UTF8_OPS_LEN: u16 = UTF8_NOT_LIKE_OFFSET + 1;

const SQL_NULL_OPS_LEN: u16 = 1;

const TUPLE_EQ_OFFSET: u16 = 0;
const TUPLE_NOT_EQ_OFFSET: u16 = 1;
const TUPLE_GT_OFFSET: u16 = 2;
const TUPLE_GT_EQ_OFFSET: u16 = 3;
const TUPLE_LT_OFFSET: u16 = 4;
const TUPLE_LT_EQ_OFFSET: u16 = 5;

const BINARY_INT8_BASE: u16 = 0;
const BINARY_INT16_BASE: u16 = BINARY_INT8_BASE + NUMERIC_OPS_LEN;
const BINARY_INT32_BASE: u16 = BINARY_INT16_BASE + NUMERIC_OPS_LEN;
const BINARY_INT64_BASE: u16 = BINARY_INT32_BASE + NUMERIC_OPS_LEN;
const BINARY_UINT8_BASE: u16 = BINARY_INT64_BASE + NUMERIC_OPS_LEN;
const BINARY_UINT16_BASE: u16 = BINARY_UINT8_BASE + NUMERIC_OPS_LEN;
const BINARY_UINT32_BASE: u16 = BINARY_UINT16_BASE + NUMERIC_OPS_LEN;
const BINARY_UINT64_BASE: u16 = BINARY_UINT32_BASE + NUMERIC_OPS_LEN;
const BINARY_FLOAT32_BASE: u16 = BINARY_UINT64_BASE + NUMERIC_OPS_LEN;
const BINARY_FLOAT64_BASE: u16 = BINARY_FLOAT32_BASE + NUMERIC_OPS_LEN;
const BINARY_DATE_BASE: u16 = BINARY_FLOAT64_BASE + NUMERIC_OPS_LEN;
const BINARY_DATETIME_BASE: u16 = BINARY_DATE_BASE + NUMERIC_OPS_LEN;
const BINARY_DECIMAL_BASE: u16 = BINARY_DATETIME_BASE + NUMERIC_OPS_LEN;
const BINARY_TIME_BASE: u16 = BINARY_DECIMAL_BASE + NUMERIC_OPS_LEN;
const BINARY_TIME64_BASE: u16 = BINARY_TIME_BASE + TIME_OPS_LEN;
const BINARY_BOOLEAN_BASE: u16 = BINARY_TIME64_BASE + TIMESTAMP_OPS_LEN;
const BINARY_UTF8_BASE: u16 = BINARY_BOOLEAN_BASE + BOOLEAN_OPS_LEN;
const BINARY_SQL_NULL: u16 = BINARY_UTF8_BASE + UTF8_OPS_LEN;
const BINARY_TUPLE_BASE: u16 = BINARY_SQL_NULL + SQL_NULL_OPS_LEN;

// Evaluator positions are serialized ABI. Do not reorder or reuse existing
// positions; only append new positions at the end of the current layout.

const fn binary_pos(base: u16, offset: u16) -> u16 {
    base + offset
}

fn unit_binary_ref(pos: u16) -> Result<BinaryEvaluatorRef, DatabaseError> {
    Ok(BinaryEvaluatorRef::new(pos, BinaryEvaluatorParams::Unit))
}

fn numeric_binary_pos(
    base: u16,
    ty: &LogicalType,
    op: BinaryOperator,
) -> Result<u16, DatabaseError> {
    Ok(base
        + match op {
            BinaryOperator::Plus => NUMERIC_PLUS_OFFSET,
            BinaryOperator::Minus => NUMERIC_MINUS_OFFSET,
            BinaryOperator::Multiply => NUMERIC_MULTIPLY_OFFSET,
            BinaryOperator::Divide => NUMERIC_DIVIDE_OFFSET,
            BinaryOperator::Gt => NUMERIC_GT_OFFSET,
            BinaryOperator::GtEq => NUMERIC_GT_EQ_OFFSET,
            BinaryOperator::Lt => NUMERIC_LT_OFFSET,
            BinaryOperator::LtEq => NUMERIC_LT_EQ_OFFSET,
            BinaryOperator::Eq => NUMERIC_EQ_OFFSET,
            BinaryOperator::NotEq => NUMERIC_NOT_EQ_OFFSET,
            BinaryOperator::Modulo => NUMERIC_MODULO_OFFSET,
            _ => return Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        })
}

fn numeric_binary_ref(
    base: u16,
    ty: &LogicalType,
    op: BinaryOperator,
) -> Result<BinaryEvaluatorRef, DatabaseError> {
    unit_binary_ref(numeric_binary_pos(base, ty, op)?)
}

pub fn binary_create(
    ty: Cow<'_, LogicalType>,
    op: BinaryOperator,
) -> Result<BinaryEvaluatorRef, DatabaseError> {
    let ty = ty.as_ref();
    match ty {
        LogicalType::Tinyint => numeric_binary_ref(BINARY_INT8_BASE, ty, op),
        LogicalType::Smallint => numeric_binary_ref(BINARY_INT16_BASE, ty, op),
        LogicalType::Integer => numeric_binary_ref(BINARY_INT32_BASE, ty, op),
        LogicalType::Bigint => numeric_binary_ref(BINARY_INT64_BASE, ty, op),
        LogicalType::UTinyint => numeric_binary_ref(BINARY_UINT8_BASE, ty, op),
        LogicalType::USmallint => numeric_binary_ref(BINARY_UINT16_BASE, ty, op),
        LogicalType::UInteger => numeric_binary_ref(BINARY_UINT32_BASE, ty, op),
        LogicalType::UBigint => numeric_binary_ref(BINARY_UINT64_BASE, ty, op),
        LogicalType::Float => numeric_binary_ref(BINARY_FLOAT32_BASE, ty, op),
        LogicalType::Double => numeric_binary_ref(BINARY_FLOAT64_BASE, ty, op),
        #[cfg(feature = "time")]
        LogicalType::Date => numeric_binary_ref(BINARY_DATE_BASE, ty, op),
        #[cfg(not(feature = "time"))]
        LogicalType::Date => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        #[cfg(feature = "time")]
        LogicalType::DateTime => numeric_binary_ref(BINARY_DATETIME_BASE, ty, op),
        #[cfg(not(feature = "time"))]
        LogicalType::DateTime => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        #[cfg(feature = "time")]
        LogicalType::Time(_) => match op {
            BinaryOperator::Plus => unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_PLUS_OFFSET)),
            BinaryOperator::Minus => {
                unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_MINUS_OFFSET))
            }
            BinaryOperator::Gt => unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_GT_OFFSET)),
            BinaryOperator::GtEq => {
                unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_GT_EQ_OFFSET))
            }
            BinaryOperator::Lt => unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_LT_OFFSET)),
            BinaryOperator::LtEq => {
                unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_LT_EQ_OFFSET))
            }
            BinaryOperator::Eq => unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_EQ_OFFSET)),
            BinaryOperator::NotEq => {
                unit_binary_ref(binary_pos(BINARY_TIME_BASE, TIME_NOT_EQ_OFFSET))
            }
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
        #[cfg(not(feature = "time"))]
        LogicalType::Time(_) => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        #[cfg(feature = "time")]
        LogicalType::TimeStamp(_, _) => match op {
            BinaryOperator::Gt => {
                unit_binary_ref(binary_pos(BINARY_TIME64_BASE, TIMESTAMP_GT_OFFSET))
            }
            BinaryOperator::GtEq => {
                unit_binary_ref(binary_pos(BINARY_TIME64_BASE, TIMESTAMP_GT_EQ_OFFSET))
            }
            BinaryOperator::Lt => {
                unit_binary_ref(binary_pos(BINARY_TIME64_BASE, TIMESTAMP_LT_OFFSET))
            }
            BinaryOperator::LtEq => {
                unit_binary_ref(binary_pos(BINARY_TIME64_BASE, TIMESTAMP_LT_EQ_OFFSET))
            }
            BinaryOperator::Eq => {
                unit_binary_ref(binary_pos(BINARY_TIME64_BASE, TIMESTAMP_EQ_OFFSET))
            }
            BinaryOperator::NotEq => {
                unit_binary_ref(binary_pos(BINARY_TIME64_BASE, TIMESTAMP_NOT_EQ_OFFSET))
            }
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
        #[cfg(not(feature = "time"))]
        LogicalType::TimeStamp(_, _) => {
            Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op))
        }
        #[cfg(feature = "decimal")]
        LogicalType::Decimal(_, _) => numeric_binary_ref(BINARY_DECIMAL_BASE, ty, op),
        #[cfg(not(feature = "decimal"))]
        LogicalType::Decimal(_, _) => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        LogicalType::Boolean => match op {
            BinaryOperator::And => {
                unit_binary_ref(binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_AND_OFFSET))
            }
            BinaryOperator::Or => {
                unit_binary_ref(binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_OR_OFFSET))
            }
            BinaryOperator::Eq => {
                unit_binary_ref(binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_EQ_OFFSET))
            }
            BinaryOperator::NotEq => {
                unit_binary_ref(binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_NOT_EQ_OFFSET))
            }
            _ => Err(DatabaseError::UnsupportedBinaryOperator(
                LogicalType::Boolean,
                op,
            )),
        },
        LogicalType::Varchar(_, _) | LogicalType::Char(_, _) => match op {
            BinaryOperator::Gt => unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_GT_OFFSET)),
            BinaryOperator::Lt => unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_LT_OFFSET)),
            BinaryOperator::GtEq => {
                unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_GT_EQ_OFFSET))
            }
            BinaryOperator::LtEq => {
                unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_LT_EQ_OFFSET))
            }
            BinaryOperator::Eq => unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_EQ_OFFSET)),
            BinaryOperator::NotEq => {
                unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_NOT_EQ_OFFSET))
            }
            BinaryOperator::StringConcat => {
                unit_binary_ref(binary_pos(BINARY_UTF8_BASE, UTF8_STRING_CONCAT_OFFSET))
            }
            BinaryOperator::Like(escape_char) => Ok(BinaryEvaluatorRef::new(
                binary_pos(BINARY_UTF8_BASE, UTF8_LIKE_OFFSET),
                BinaryEvaluatorParams::Like { escape_char },
            )),
            BinaryOperator::NotLike(escape_char) => Ok(BinaryEvaluatorRef::new(
                binary_pos(BINARY_UTF8_BASE, UTF8_NOT_LIKE_OFFSET),
                BinaryEvaluatorParams::Like { escape_char },
            )),
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
        LogicalType::SqlNull => unit_binary_ref(BINARY_SQL_NULL),
        LogicalType::Tuple(_) => match op {
            BinaryOperator::Eq => unit_binary_ref(binary_pos(BINARY_TUPLE_BASE, TUPLE_EQ_OFFSET)),
            BinaryOperator::NotEq => {
                unit_binary_ref(binary_pos(BINARY_TUPLE_BASE, TUPLE_NOT_EQ_OFFSET))
            }
            BinaryOperator::Gt => unit_binary_ref(binary_pos(BINARY_TUPLE_BASE, TUPLE_GT_OFFSET)),
            BinaryOperator::GtEq => {
                unit_binary_ref(binary_pos(BINARY_TUPLE_BASE, TUPLE_GT_EQ_OFFSET))
            }
            BinaryOperator::Lt => unit_binary_ref(binary_pos(BINARY_TUPLE_BASE, TUPLE_LT_OFFSET)),
            BinaryOperator::LtEq => {
                unit_binary_ref(binary_pos(BINARY_TUPLE_BASE, TUPLE_LT_EQ_OFFSET))
            }
            _ => Err(DatabaseError::UnsupportedBinaryOperator(ty.clone(), op)),
        },
    }
}

macro_rules! eval_numeric_binary {
    ($pos:expr, $base:expr, $value_type:ident, $left:expr, $right:expr) => {
        paste! {
            match $pos - $base {
                NUMERIC_PLUS_OFFSET => [<$value_type:snake _plus_binary_eval>]($left, $right),
                NUMERIC_MINUS_OFFSET => [<$value_type:snake _minus_binary_eval>]($left, $right),
                NUMERIC_MULTIPLY_OFFSET => [<$value_type:snake _multiply_binary_eval>]($left, $right),
                NUMERIC_DIVIDE_OFFSET => [<$value_type:snake _divide_binary_eval>]($left, $right),
                NUMERIC_GT_OFFSET => [<$value_type:snake _gt_binary_eval>]($left, $right),
                NUMERIC_GT_EQ_OFFSET => [<$value_type:snake _gt_eq_binary_eval>]($left, $right),
                NUMERIC_LT_OFFSET => [<$value_type:snake _lt_binary_eval>]($left, $right),
                NUMERIC_LT_EQ_OFFSET => [<$value_type:snake _lt_eq_binary_eval>]($left, $right),
                NUMERIC_EQ_OFFSET => [<$value_type:snake _eq_binary_eval>]($left, $right),
                NUMERIC_NOT_EQ_OFFSET => [<$value_type:snake _not_eq_binary_eval>]($left, $right),
                NUMERIC_MODULO_OFFSET => [<$value_type:snake _mod_binary_eval>]($left, $right),
                _ => unreachable!(),
            }
        }
    };
}

pub(crate) fn eval_binary(
    pos: u16,
    params: &BinaryEvaluatorParams,
    left: &crate::types::value::DataValue,
    right: &crate::types::value::DataValue,
) -> Result<crate::types::value::DataValue, DatabaseError> {
    match pos {
        BINARY_INT8_BASE..BINARY_INT16_BASE => {
            eval_numeric_binary!(pos, BINARY_INT8_BASE, Int8, left, right)
        }
        BINARY_INT16_BASE..BINARY_INT32_BASE => {
            eval_numeric_binary!(pos, BINARY_INT16_BASE, Int16, left, right)
        }
        BINARY_INT32_BASE..BINARY_INT64_BASE => {
            eval_numeric_binary!(pos, BINARY_INT32_BASE, Int32, left, right)
        }
        BINARY_INT64_BASE..BINARY_UINT8_BASE => {
            eval_numeric_binary!(pos, BINARY_INT64_BASE, Int64, left, right)
        }
        BINARY_UINT8_BASE..BINARY_UINT16_BASE => {
            eval_numeric_binary!(pos, BINARY_UINT8_BASE, Uint8, left, right)
        }
        BINARY_UINT16_BASE..BINARY_UINT32_BASE => {
            eval_numeric_binary!(pos, BINARY_UINT16_BASE, Uint16, left, right)
        }
        BINARY_UINT32_BASE..BINARY_UINT64_BASE => {
            eval_numeric_binary!(pos, BINARY_UINT32_BASE, Uint32, left, right)
        }
        BINARY_UINT64_BASE..BINARY_FLOAT32_BASE => {
            eval_numeric_binary!(pos, BINARY_UINT64_BASE, Uint64, left, right)
        }
        BINARY_FLOAT32_BASE..BINARY_FLOAT64_BASE => {
            eval_numeric_binary!(pos, BINARY_FLOAT32_BASE, Float32, left, right)
        }
        BINARY_FLOAT64_BASE..BINARY_DATE_BASE => {
            eval_numeric_binary!(pos, BINARY_FLOAT64_BASE, Float64, left, right)
        }
        #[cfg(feature = "time")]
        BINARY_DATE_BASE..BINARY_DATETIME_BASE => {
            eval_numeric_binary!(pos, BINARY_DATE_BASE, Date, left, right)
        }
        #[cfg(not(feature = "time"))]
        BINARY_DATE_BASE..BINARY_DATETIME_BASE => Err(DatabaseError::UnsupportedStmt(
            "time types require the `time` feature".to_string(),
        )),
        #[cfg(feature = "time")]
        BINARY_DATETIME_BASE..BINARY_DECIMAL_BASE => {
            eval_numeric_binary!(pos, BINARY_DATETIME_BASE, DateTime, left, right)
        }
        #[cfg(not(feature = "time"))]
        BINARY_DATETIME_BASE..BINARY_DECIMAL_BASE => Err(DatabaseError::UnsupportedStmt(
            "time types require the `time` feature".to_string(),
        )),
        #[cfg(feature = "decimal")]
        BINARY_DECIMAL_BASE..BINARY_TIME_BASE => {
            eval_numeric_binary!(pos, BINARY_DECIMAL_BASE, Decimal, left, right)
        }
        #[cfg(not(feature = "decimal"))]
        BINARY_DECIMAL_BASE..BINARY_TIME_BASE => Err(DatabaseError::UnsupportedStmt(
            "DECIMAL requires the `decimal` feature".to_string(),
        )),
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_PLUS_OFFSET) => {
            time_plus_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_MINUS_OFFSET) => {
            time_minus_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_GT_OFFSET) => time_gt_binary_eval(left, right),
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_GT_EQ_OFFSET) => {
            time_gt_eq_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_LT_OFFSET) => time_lt_binary_eval(left, right),
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_LT_EQ_OFFSET) => {
            time_lt_eq_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_EQ_OFFSET) => time_eq_binary_eval(left, right),
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME_BASE, TIME_NOT_EQ_OFFSET) => {
            time_not_eq_binary_eval(left, right)
        }
        #[cfg(not(feature = "time"))]
        BINARY_TIME_BASE..BINARY_TIME64_BASE => Err(DatabaseError::UnsupportedStmt(
            "time types require the `time` feature".to_string(),
        )),
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME64_BASE, TIMESTAMP_GT_OFFSET) => {
            time64_gt_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME64_BASE, TIMESTAMP_GT_EQ_OFFSET) => {
            time64_gt_eq_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME64_BASE, TIMESTAMP_LT_OFFSET) => {
            time64_lt_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME64_BASE, TIMESTAMP_LT_EQ_OFFSET) => {
            time64_lt_eq_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME64_BASE, TIMESTAMP_EQ_OFFSET) => {
            time64_eq_binary_eval(left, right)
        }
        #[cfg(feature = "time")]
        x if x == binary_pos(BINARY_TIME64_BASE, TIMESTAMP_NOT_EQ_OFFSET) => {
            time64_not_eq_binary_eval(left, right)
        }
        #[cfg(not(feature = "time"))]
        BINARY_TIME64_BASE..BINARY_BOOLEAN_BASE => Err(DatabaseError::UnsupportedStmt(
            "time types require the `time` feature".to_string(),
        )),
        x if x == binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_AND_OFFSET) => {
            boolean_and_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_OR_OFFSET) => {
            boolean_or_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_EQ_OFFSET) => {
            boolean_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_NOT_EQ_OFFSET) => {
            boolean_not_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_GT_OFFSET) => utf8_gt_binary_eval(left, right),
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_LT_OFFSET) => utf8_lt_binary_eval(left, right),
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_GT_EQ_OFFSET) => {
            utf8_gt_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_LT_EQ_OFFSET) => {
            utf8_lt_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_EQ_OFFSET) => utf8_eq_binary_eval(left, right),
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_NOT_EQ_OFFSET) => {
            utf8_not_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_STRING_CONCAT_OFFSET) => {
            utf8_string_concat_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_LIKE_OFFSET) => {
            let BinaryEvaluatorParams::Like { escape_char } = params else {
                unreachable!()
            };
            utf8_like_binary_eval(*escape_char, left, right)
        }
        x if x == binary_pos(BINARY_UTF8_BASE, UTF8_NOT_LIKE_OFFSET) => {
            let BinaryEvaluatorParams::Like { escape_char } = params else {
                unreachable!()
            };
            utf8_not_like_binary_eval(*escape_char, left, right)
        }
        BINARY_SQL_NULL => null_binary_eval(left, right),
        x if x == binary_pos(BINARY_TUPLE_BASE, TUPLE_EQ_OFFSET) => {
            tuple_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_TUPLE_BASE, TUPLE_NOT_EQ_OFFSET) => {
            tuple_not_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_TUPLE_BASE, TUPLE_GT_OFFSET) => {
            tuple_gt_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_TUPLE_BASE, TUPLE_GT_EQ_OFFSET) => {
            tuple_gt_eq_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_TUPLE_BASE, TUPLE_LT_OFFSET) => {
            tuple_lt_binary_eval(left, right)
        }
        x if x == binary_pos(BINARY_TUPLE_BASE, TUPLE_LT_EQ_OFFSET) => {
            tuple_lt_eq_binary_eval(left, right)
        }
        _ => unreachable!("unknown binary evaluator position {pos}"),
    }
}

#[macro_export]
macro_rules! numeric_binary_evaluator_definition {
    ($value_type:ident, $compute_type:path) => {
        paste::paste! {
            pub fn [<$value_type:snake _plus_binary_eval>](
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
            pub fn [<$value_type:snake _minus_binary_eval>](
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
            pub fn [<$value_type:snake _multiply_binary_eval>](
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
            pub fn [<$value_type:snake _divide_binary_eval>](
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
            pub fn [<$value_type:snake _gt_binary_eval>](
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
            pub fn [<$value_type:snake _gt_eq_binary_eval>](
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
            pub fn [<$value_type:snake _lt_binary_eval>](
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
            pub fn [<$value_type:snake _lt_eq_binary_eval>](
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
            pub fn [<$value_type:snake _eq_binary_eval>](
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
            pub fn [<$value_type:snake _not_eq_binary_eval>](
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
            pub fn [<$value_type:snake _mod_binary_eval>](
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
    };
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::errors::DatabaseError;
    use crate::expression::BinaryOperator;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::BinaryEvaluatorRef;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};

    fn create(ty: LogicalType, op: BinaryOperator) -> Result<BinaryEvaluatorRef, DatabaseError> {
        binary_create(Cow::Owned(ty), op)
    }

    #[test]
    fn test_binary_evaluator_positions_are_stable() -> Result<(), DatabaseError> {
        assert_eq!(
            create(LogicalType::Integer, BinaryOperator::Plus)?.pos,
            BINARY_INT32_BASE
        );
        assert_eq!(
            create(LogicalType::Boolean, BinaryOperator::NotEq)?.pos,
            binary_pos(BINARY_BOOLEAN_BASE, BOOLEAN_NOT_EQ_OFFSET)
        );
        assert_eq!(
            create(
                LogicalType::Varchar(None, crate::types::CharLengthUnits::Characters),
                BinaryOperator::StringConcat
            )?
            .pos,
            binary_pos(BINARY_UTF8_BASE, UTF8_STRING_CONCAT_OFFSET)
        );

        Ok(())
    }

    #[test]
    fn test_binary_evaluator_serialization() -> Result<(), DatabaseError> {
        let evaluator = create(LogicalType::Boolean, BinaryOperator::NotEq)?;
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();

        evaluator.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            BinaryEvaluatorRef::decode::<RocksTransaction, _, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut arena,
            )?,
            evaluator
        );

        Ok(())
    }
}
