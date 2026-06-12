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
use crate::types::evaluator::null::{null_cast_eval, to_sql_null_cast_eval};
#[cfg(feature = "time")]
use crate::types::evaluator::time32::*;
#[cfg(feature = "time")]
use crate::types::evaluator::time64::*;
use crate::types::evaluator::tuple::eval_tuple_cast;
use crate::types::evaluator::uint16::*;
use crate::types::evaluator::uint32::*;
use crate::types::evaluator::uint64::*;
use crate::types::evaluator::uint8::*;
use crate::types::evaluator::utf8::*;
use crate::types::evaluator::{CastEvaluatorParams, CastEvaluatorRef};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;
use crate::types::LogicalType;
use paste::paste;
use std::borrow::Cow;

pub(crate) fn cast_fail(from: LogicalType, to: LogicalType) -> DatabaseError {
    DatabaseError::CastFail {
        from,
        to,
        span: None,
    }
}

const CAST_TYPE_STRIDE: u16 = 32;
const CAST_SQL_NULL: u16 = 0;
const CAST_BOOLEAN: u16 = 1;
const CAST_TINYINT: u16 = 2;
const CAST_UTINYINT: u16 = 3;
const CAST_SMALLINT: u16 = 4;
const CAST_USMALLINT: u16 = 5;
const CAST_INTEGER: u16 = 6;
const CAST_UINTEGER: u16 = 7;
const CAST_BIGINT: u16 = 8;
const CAST_UBIGINT: u16 = 9;
const CAST_FLOAT: u16 = 10;
const CAST_DOUBLE: u16 = 11;
const CAST_CHAR: u16 = 12;
const CAST_VARCHAR: u16 = 13;
const CAST_DATE: u16 = 14;
const CAST_DATETIME: u16 = 15;
const CAST_TIME: u16 = 16;
const CAST_TIMESTAMP: u16 = 17;
#[cfg(feature = "decimal")]
const CAST_DECIMAL: u16 = 18;
const CAST_TUPLE: u16 = 19;

// Cast positions are serialized ABI. Type codes above must never be reordered
// or reused; new cast families should append a new code and keep old positions.
fn cast_type_code(ty: &LogicalType) -> u16 {
    match ty {
        LogicalType::SqlNull => CAST_SQL_NULL,
        LogicalType::Boolean => CAST_BOOLEAN,
        LogicalType::Tinyint => CAST_TINYINT,
        LogicalType::UTinyint => CAST_UTINYINT,
        LogicalType::Smallint => CAST_SMALLINT,
        LogicalType::USmallint => CAST_USMALLINT,
        LogicalType::Integer => CAST_INTEGER,
        LogicalType::UInteger => CAST_UINTEGER,
        LogicalType::Bigint => CAST_BIGINT,
        LogicalType::UBigint => CAST_UBIGINT,
        LogicalType::Float => CAST_FLOAT,
        LogicalType::Double => CAST_DOUBLE,
        LogicalType::Char(_, _) => CAST_CHAR,
        LogicalType::Varchar(_, _) => CAST_VARCHAR,
        LogicalType::Date => CAST_DATE,
        LogicalType::DateTime => CAST_DATETIME,
        LogicalType::Time(_) => CAST_TIME,
        LogicalType::TimeStamp(_, _) => CAST_TIMESTAMP,
        #[cfg(feature = "decimal")]
        LogicalType::Decimal(_, _) => CAST_DECIMAL,
        #[cfg(not(feature = "decimal"))]
        LogicalType::Decimal(_, _) => unreachable!("DECIMAL requires the `decimal` feature"),
        LogicalType::Tuple(_) => CAST_TUPLE,
    }
}

fn cast_pos(from: &LogicalType, to: &LogicalType) -> u16 {
    cast_type_code(from) * CAST_TYPE_STRIDE + cast_type_code(to)
}

#[cfg(not(feature = "time"))]
fn is_chrono_type(ty: &LogicalType) -> bool {
    matches!(
        ty,
        LogicalType::Date
            | LogicalType::DateTime
            | LogicalType::Time(_)
            | LogicalType::TimeStamp(_, _)
    )
}

#[cfg(not(feature = "time"))]
fn is_chrono_type_code(code: u16) -> bool {
    matches!(code, CAST_DATE | CAST_DATETIME | CAST_TIME | CAST_TIMESTAMP)
}

pub(crate) fn to_char(
    value: String,
    len: u32,
    unit: CharLengthUnits,
) -> Result<DataValue, DatabaseError> {
    if DataValue::check_string_len(&value, len as usize, unit) {
        return Err(DatabaseError::TooLong);
    }

    Ok(DataValue::Utf8 {
        value,
        ty: Utf8Type::Fixed(len),
        unit,
    })
}

pub(crate) fn to_varchar(
    value: String,
    len: Option<u32>,
    unit: CharLengthUnits,
) -> Result<DataValue, DatabaseError> {
    if let Some(len) = len {
        if DataValue::check_string_len(&value, len as usize, unit) {
            return Err(DatabaseError::TooLong);
        }
    }

    Ok(DataValue::Utf8 {
        value,
        ty: Utf8Type::Variable(len),
        unit,
    })
}

#[macro_export]
macro_rules! numeric_to_boolean_cast {
    ($value:expr, $from:expr) => {
        match $value {
            0 => Ok($crate::types::value::DataValue::Boolean(false)),
            1 => Ok($crate::types::value::DataValue::Boolean(true)),
            _ => Err($crate::types::evaluator::cast::cast_fail(
                $from,
                $crate::types::LogicalType::Boolean,
            )),
        }
    };
}

#[macro_export]
macro_rules! float_to_int_cast {
    ($float_value:expr, $int_type:ty, $float_type:ty) => {{
        let float_value: $float_type = $float_value;
        if float_value.is_nan() {
            Ok(0)
        } else if float_value <= 0.0 || float_value > <$int_type>::MAX as $float_type {
            Err($crate::errors::DatabaseError::OverFlow)
        } else {
            Ok(float_value as $int_type)
        }
    }};
}

#[cfg(feature = "decimal")]
#[macro_export]
macro_rules! decimal_to_int_cast {
    ($decimal:expr, $int_type:ty) => {{
        let d = $decimal;
        if d.is_sign_negative() {
            if <$int_type>::MIN == 0 {
                0
            } else {
                let min = rust_decimal::Decimal::from(<$int_type>::MIN);
                if d <= min {
                    <$int_type>::MIN
                } else {
                    d.to_i128().unwrap() as $int_type
                }
            }
        } else {
            let max = rust_decimal::Decimal::from(<$int_type>::MAX);
            if d >= max {
                <$int_type>::MAX
            } else {
                d.to_i128().unwrap() as $int_type
            }
        }
    }};
}

#[macro_export]
macro_rules! define_cast_evaluator {
    ($name:ident, $pattern:pat => $body:block) => {
        pub fn $name(
            value: &$crate::types::value::DataValue,
        ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
            match value {
                $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                $pattern => $body,
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
    };
    ($name:ident, $pattern:pat => |$this:ident| $body:expr) => {
        pub fn $name(
            value: &$crate::types::value::DataValue,
        ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
            match value {
                $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                $pattern => {
                    let $this = ();
                    $body
                }
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
    };
    ($name:ident, $pattern:pat => |$this:ident| $body:block) => {
        pub fn $name(
            value: &$crate::types::value::DataValue,
        ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
            match value {
                $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                $pattern => {
                    let $this = ();
                    $body
                }
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
    };
    ($name:ident { $($field:ident : $field_ty:ty),+ $(,)? }, $pattern:pat => |$this:ident| $body:expr) => {
        pub fn $name(
            $($field: $field_ty,)+
            value: &$crate::types::value::DataValue,
        ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
            struct This {
                $($field: $field_ty),+
            }
            let $this = This { $($field),+ };
            match value {
                $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                $pattern => $body,
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
    };
    ($name:ident { $($field:ident : $field_ty:ty),+ $(,)? }, $pattern:pat => $body:block) => {
        pub fn $name(
            $($field: $field_ty,)+
            value: &$crate::types::value::DataValue,
        ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
            match value {
                $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                $pattern => $body,
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
    };
}

#[macro_export]
macro_rules! define_integer_cast_evaluators {
    ($prefix:ident, $variant:ident, $src_ty:ty, $from_ty:expr) => {
        paste::paste! {
            $crate::define_cast_evaluator!([<$prefix:snake _to_boolean_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                $crate::numeric_to_boolean_cast!(*value, $from_ty)
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_tinyint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int8(i8::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_utinyint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt8(u8::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_smallint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int16(i16::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_usmallint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt16(u16::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_integer_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int32(i32::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_uinteger_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt32(u32::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_bigint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int64(i64::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_ubigint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt64(u64::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_float_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Float32(ordered_float::OrderedFloat(*value as f32)))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_double_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Float64(ordered_float::OrderedFloat(*value as f64)))
            });
            $crate::define_cast_evaluator!(
                [<$prefix:snake _to_char_cast_eval>] {
                    len: u32,
                    unit: crate::types::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_char(value.to_string(), this.len, this.unit)
                }
            );
            $crate::define_cast_evaluator!(
                [<$prefix:snake _to_varchar_cast_eval>] {
                    len: Option<u32>,
                    unit: crate::types::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_varchar(value.to_string(), this.len, this.unit)
                }
            );
            #[cfg(feature = "decimal")]
            $crate::define_cast_evaluator!(
                [<$prefix:snake _to_decimal_cast_eval>] {
                    scale: Option<u8>
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    let mut decimal = rust_decimal::Decimal::from(*value);
                    $crate::types::value::DataValue::decimal_round_i(&this.scale, &mut decimal);
                    Ok($crate::types::value::DataValue::Decimal(decimal))
                }
            );
        }
    };
}

#[macro_export]
macro_rules! define_float_cast_evaluators {
    ($prefix:ident, $variant:ident, $src_ty:ty, $from_ty:expr, $into_decimal:ident) => {
        paste::paste! {
            $crate::define_cast_evaluator!([<$prefix:snake _to_float_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::$variant(*value))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_double_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Float64(ordered_float::OrderedFloat(value.0 as f64)))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_tinyint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int8($crate::float_to_int_cast!(value.into_inner(), i8, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_smallint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int16($crate::float_to_int_cast!(value.into_inner(), i16, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_integer_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int32($crate::float_to_int_cast!(value.into_inner(), i32, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_bigint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int64($crate::float_to_int_cast!(value.into_inner(), i64, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_utinyint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt8($crate::float_to_int_cast!(value.into_inner(), u8, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_usmallint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt16($crate::float_to_int_cast!(value.into_inner(), u16, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_uinteger_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt32($crate::float_to_int_cast!(value.into_inner(), u32, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix:snake _to_ubigint_cast_eval>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt64($crate::float_to_int_cast!(value.into_inner(), u64, $src_ty)?))
            });
            $crate::define_cast_evaluator!(
                [<$prefix:snake _to_char_cast_eval>] {
                    len: u32,
                    unit: crate::types::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_char(value.to_string(), this.len, this.unit)
                }
            );
            $crate::define_cast_evaluator!(
                [<$prefix:snake _to_varchar_cast_eval>] {
                    len: Option<u32>,
                    unit: crate::types::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_varchar(value.to_string(), this.len, this.unit)
                }
            );
            #[cfg(feature = "decimal")]
            $crate::define_cast_evaluator!(
                [<$prefix:snake _to_decimal_cast_eval>] {
                    precision: Option<u8>,
                    scale: Option<u8>,
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    let mut decimal = rust_decimal::Decimal::$into_decimal(value.0).ok_or_else(|| {
                        $crate::types::evaluator::cast::cast_fail(
                            $from_ty,
                            $crate::types::LogicalType::Decimal(this.precision, this.scale),
                        )
                    })?;
                    $crate::types::value::DataValue::decimal_round_f(&this.scale, &mut decimal);
                    Ok($crate::types::value::DataValue::Decimal(decimal))
                }
            );
        }
    };
}
pub fn identity_cast_eval(value: &DataValue) -> Result<DataValue, DatabaseError> {
    Ok(value.clone())
}

macro_rules! cast_ref {
    ($from:expr, $to:expr) => {{
        Ok(CastEvaluatorRef::new(
            cast_pos($from, $to),
            CastEvaluatorParams::Unit,
        ))
    }};
}

macro_rules! cast_string_ref {
    ($from:expr, $to:expr, $len:expr, $unit:expr) => {{
        Ok(CastEvaluatorRef::new(
            cast_pos($from, $to),
            CastEvaluatorParams::String {
                len: $len,
                unit: $unit,
            },
        ))
    }};
}

#[cfg(feature = "decimal")]
macro_rules! cast_decimal_ref {
    ($from:expr, $to:expr, $precision:expr, $scale:expr) => {{
        Ok(CastEvaluatorRef::new(
            cast_pos($from, $to),
            CastEvaluatorParams::Decimal {
                precision: $precision,
                scale: $scale,
            },
        ))
    }};
}

macro_rules! cast_precision_ref {
    ($from:expr, $to:expr, $precision:expr) => {{
        Ok(CastEvaluatorRef::new(
            cast_pos($from, $to),
            CastEvaluatorParams::Precision {
                precision: $precision,
            },
        ))
    }};
}

macro_rules! cast_timestamp_ref {
    ($from:expr, $to:expr, $precision:expr, $zone:expr) => {{
        Ok(CastEvaluatorRef::new(
            cast_pos($from, $to),
            CastEvaluatorParams::Timestamp {
                precision: $precision,
                zone: $zone,
            },
        ))
    }};
}

macro_rules! build_integer_cast {
    ($cast:ident, $prefix:ident, $to:expr, $from:expr) => {{
        paste! {
            match $to {
                LogicalType::SqlNull => $cast!($from, $to),
                LogicalType::Boolean => $cast!($from, $to),
                LogicalType::Tinyint => $cast!($from, $to),
                LogicalType::UTinyint => $cast!($from, $to),
                LogicalType::Smallint => $cast!($from, $to),
                LogicalType::USmallint => $cast!($from, $to),
                LogicalType::Integer => $cast!($from, $to),
                LogicalType::UInteger => $cast!($from, $to),
                LogicalType::Bigint => $cast!($from, $to),
                LogicalType::UBigint => $cast!($from, $to),
                LogicalType::Float => $cast!($from, $to),
                LogicalType::Double => $cast!($from, $to),
                LogicalType::Char(len, unit) => cast_string_ref!(
                    $from,
                    $to,
                    Some(*len),
                    *unit
                ),
                LogicalType::Varchar(len, unit) => cast_string_ref!(
                    $from,
                    $to,
                    *len,
                    *unit
                ),
                #[cfg(feature = "decimal")]
                LogicalType::Decimal(precision, scale) => cast_decimal_ref!(
                    $from,
                    $to,
                    *precision,
                    *scale
                ),
                _ => Err(cast_fail($from.clone(), $to.clone())),
            }
        }
    }};
}

pub fn cast_create(
    from: Cow<'_, LogicalType>,
    to: Cow<'_, LogicalType>,
) -> Result<CastEvaluatorRef, DatabaseError> {
    let from = from.as_ref();
    let to = to.as_ref();
    if from == to {
        return Ok(CastEvaluatorRef::new(
            cast_pos(from, to),
            CastEvaluatorParams::Identity,
        ));
    }
    #[cfg(not(feature = "time"))]
    if is_chrono_type(from) || is_chrono_type(to) {
        return Err(DatabaseError::UnsupportedStmt(
            "time types require the `time` feature".to_string(),
        ));
    }

    match (from, to) {
        (LogicalType::SqlNull, _) => cast_ref!(from, to),
        (_, LogicalType::SqlNull) => cast_ref!(from, to),
        (LogicalType::Boolean, LogicalType::Tinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::UTinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::Smallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::USmallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::Integer) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::UInteger) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::Bigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::UBigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::Float) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::Double) => {
            cast_ref!(from, to)
        }
        (LogicalType::Boolean, LogicalType::Char(len, unit)) => {
            cast_string_ref!(from, to, Some(*len), *unit)
        }
        (LogicalType::Boolean, LogicalType::Varchar(len, unit)) => {
            cast_string_ref!(from, to, *len, *unit)
        }
        (LogicalType::Tinyint, _) => build_integer_cast!(cast_ref, Int8, to, from),
        (LogicalType::Smallint, _) => build_integer_cast!(cast_ref, Int16, to, from),
        (LogicalType::Integer, _) => build_integer_cast!(cast_ref, Int32, to, from),
        (LogicalType::Bigint, _) => build_integer_cast!(cast_ref, Int64, to, from),
        (LogicalType::UTinyint, _) => build_integer_cast!(cast_ref, UInt8, to, from),
        (LogicalType::USmallint, _) => build_integer_cast!(cast_ref, UInt16, to, from),
        (LogicalType::UInteger, _) => build_integer_cast!(cast_ref, UInt32, to, from),
        (LogicalType::UBigint, _) => build_integer_cast!(cast_ref, UInt64, to, from),
        (LogicalType::Float, LogicalType::Tinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::UTinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::Smallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::USmallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::Integer) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::UInteger) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::Bigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::UBigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::Double) => {
            cast_ref!(from, to)
        }
        (LogicalType::Float, LogicalType::Char(len, unit)) => {
            cast_string_ref!(from, to, Some(*len), *unit)
        }
        (LogicalType::Float, LogicalType::Varchar(len, unit)) => {
            cast_string_ref!(from, to, *len, *unit)
        }
        #[cfg(feature = "decimal")]
        (LogicalType::Float, LogicalType::Decimal(precision, scale)) => {
            cast_decimal_ref!(from, to, *precision, *scale)
        }
        (LogicalType::Double, LogicalType::Float) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::Tinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::UTinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::Smallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::USmallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::Integer) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::UInteger) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::Bigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::UBigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Double, LogicalType::Char(len, unit)) => {
            cast_string_ref!(from, to, Some(*len), *unit)
        }
        (LogicalType::Double, LogicalType::Varchar(len, unit)) => {
            cast_string_ref!(from, to, *len, *unit)
        }
        #[cfg(feature = "decimal")]
        (LogicalType::Double, LogicalType::Decimal(precision, scale)) => {
            cast_decimal_ref!(from, to, *precision, *scale)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Boolean) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Tinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::UTinyint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Smallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::USmallint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Integer) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::UInteger) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Bigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::UBigint) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Float) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Double) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Char(len, unit)) => {
            cast_string_ref!(from, to, Some(*len), *unit)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Varchar(len, unit)) => {
            cast_string_ref!(from, to, *len, *unit)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Date) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::DateTime) => {
            cast_ref!(from, to)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Time(precision)) => {
            cast_precision_ref!(from, to, *precision)
        }
        (
            LogicalType::Char(_, _) | LogicalType::Varchar(_, _),
            LogicalType::TimeStamp(precision, zone),
        ) => {
            cast_timestamp_ref!(from, to, *precision, *zone)
        }
        #[cfg(feature = "decimal")]
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Decimal(_, _)) => {
            cast_ref!(from, to)
        }
        (LogicalType::Date, LogicalType::Char(_, _)) => {
            cast_string_ref!(from, to, Some(char_params(to).0), char_params(to).1)
        }
        (LogicalType::Date, LogicalType::Varchar(_, _)) => {
            let (len, unit) = varchar_params(to);
            cast_string_ref!(from, to, len, unit)
        }
        (LogicalType::Date, LogicalType::DateTime) => {
            cast_ref!(from, to)
        }
        (LogicalType::DateTime, LogicalType::Char(_, _)) => {
            cast_string_ref!(from, to, Some(char_params(to).0), char_params(to).1)
        }
        (LogicalType::DateTime, LogicalType::Varchar(_, _)) => {
            let (len, unit) = varchar_params(to);
            cast_string_ref!(from, to, len, unit)
        }
        (LogicalType::DateTime, LogicalType::Date) => {
            cast_ref!(from, to)
        }
        (LogicalType::DateTime, LogicalType::Time(precision)) => {
            cast_precision_ref!(from, to, *precision)
        }
        (LogicalType::DateTime, LogicalType::TimeStamp(precision, zone)) => {
            cast_timestamp_ref!(from, to, *precision, *zone)
        }
        (LogicalType::Time(_), LogicalType::Char(_, _)) => {
            cast_string_ref!(from, to, Some(char_params(to).0), char_params(to).1)
        }
        (LogicalType::Time(_), LogicalType::Varchar(_, _)) => {
            let (len, unit) = varchar_params(to);
            cast_string_ref!(from, to, len, unit)
        }
        (LogicalType::Time(_), LogicalType::Time(precision)) => {
            cast_precision_ref!(from, to, *precision)
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Char(_, _)) => {
            cast_string_ref!(from, to, Some(char_params(to).0), char_params(to).1)
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Varchar(_, _)) => {
            let (len, unit) = varchar_params(to);
            cast_string_ref!(from, to, len, unit)
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Date) => {
            cast_ref!(from, to)
        }
        (LogicalType::TimeStamp(_, _), LogicalType::DateTime) => {
            cast_ref!(from, to)
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Time(precision)) => {
            cast_precision_ref!(from, to, *precision)
        }
        (LogicalType::TimeStamp(_, _), LogicalType::TimeStamp(precision, zone)) => {
            cast_timestamp_ref!(from, to, *precision, *zone)
        }
        #[cfg(feature = "decimal")]
        (LogicalType::Decimal(_, _), to) => match to {
            LogicalType::Float
            | LogicalType::Double
            | LogicalType::Decimal(_, _)
            | LogicalType::Tinyint
            | LogicalType::Smallint
            | LogicalType::Integer
            | LogicalType::Bigint
            | LogicalType::UTinyint
            | LogicalType::USmallint
            | LogicalType::UInteger
            | LogicalType::UBigint => cast_ref!(from, to),
            LogicalType::Char(len, unit) => cast_string_ref!(from, to, Some(*len), *unit),
            LogicalType::Varchar(len, unit) => cast_string_ref!(from, to, *len, *unit),
            _ => Err(cast_fail(from.clone(), to.clone())),
        },
        (LogicalType::Tuple(from_types), LogicalType::Tuple(to_types)) => {
            let evaluators = from_types
                .iter()
                .zip(to_types.iter())
                .map(|(from, to)| cast_create(Cow::Borrowed(from), Cow::Borrowed(to)))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CastEvaluatorRef::new(
                cast_pos(from, to),
                CastEvaluatorParams::Tuple { evaluators },
            ))
        }
        _ => Err(cast_fail(from.clone(), to.clone())),
    }
}

fn char_params(ty: &LogicalType) -> (u32, CharLengthUnits) {
    let LogicalType::Char(len, unit) = ty else {
        unreachable!("cast target must be char")
    };
    (*len, *unit)
}

fn varchar_params(ty: &LogicalType) -> (Option<u32>, CharLengthUnits) {
    let LogicalType::Varchar(len, unit) = ty else {
        unreachable!("cast target must be varchar")
    };
    (*len, *unit)
}

fn string_param(params: &CastEvaluatorParams) -> (Option<u32>, CharLengthUnits) {
    let CastEvaluatorParams::String { len, unit } = params else {
        unreachable!("cast evaluator must have string parameters")
    };
    (*len, *unit)
}

#[cfg(feature = "decimal")]
fn decimal_param(params: &CastEvaluatorParams) -> (Option<u8>, Option<u8>) {
    let CastEvaluatorParams::Decimal { precision, scale } = params else {
        unreachable!("cast evaluator must have decimal parameters")
    };
    (*precision, *scale)
}

#[cfg(feature = "time")]
fn precision_param(params: &CastEvaluatorParams) -> Option<u64> {
    let CastEvaluatorParams::Precision { precision } = params else {
        unreachable!("cast evaluator must have precision parameter")
    };
    *precision
}

#[cfg(feature = "time")]
fn timestamp_param(params: &CastEvaluatorParams) -> (Option<u64>, bool) {
    let CastEvaluatorParams::Timestamp { precision, zone } = params else {
        unreachable!("cast evaluator must have timestamp parameters")
    };
    (*precision, *zone)
}

macro_rules! eval_integer_cast_by_pos {
    ($prefix:ident, $to_code:expr, $params:expr, $value:expr) => {{
        let params = $params;
        paste! {
            match $to_code {
                CAST_SQL_NULL => to_sql_null_cast_eval($value),
                CAST_BOOLEAN => [<$prefix:snake _to_boolean_cast_eval>]($value),
                CAST_TINYINT => [<$prefix:snake _to_tinyint_cast_eval>]($value),
                CAST_UTINYINT => [<$prefix:snake _to_utinyint_cast_eval>]($value),
                CAST_SMALLINT => [<$prefix:snake _to_smallint_cast_eval>]($value),
                CAST_USMALLINT => [<$prefix:snake _to_usmallint_cast_eval>]($value),
                CAST_INTEGER => [<$prefix:snake _to_integer_cast_eval>]($value),
                CAST_UINTEGER => [<$prefix:snake _to_uinteger_cast_eval>]($value),
                CAST_BIGINT => [<$prefix:snake _to_bigint_cast_eval>]($value),
                CAST_UBIGINT => [<$prefix:snake _to_ubigint_cast_eval>]($value),
                CAST_FLOAT => [<$prefix:snake _to_float_cast_eval>]($value),
                CAST_DOUBLE => [<$prefix:snake _to_double_cast_eval>]($value),
                CAST_CHAR => {
                    let (len, unit) = string_param(params);
                    let len = len.expect("char cast must have fixed length");
                    [<$prefix:snake _to_char_cast_eval>](len, unit, $value)
                }
                CAST_VARCHAR => {
                    let (len, unit) = string_param(params);
                    [<$prefix:snake _to_varchar_cast_eval>](len, unit, $value)
                }
                #[cfg(feature = "decimal")]
                CAST_DECIMAL => {
                    let (_, scale) = decimal_param(params);
                    [<$prefix:snake _to_decimal_cast_eval>](scale, $value)
                }
                _ => unreachable!("invalid integer cast evaluator position"),
            }
        }
    }};
}

impl CastEvaluatorRef {
    pub fn eval(&self, value: &DataValue) -> Result<DataValue, DatabaseError> {
        let params = &self.params;
        let from_code = self.pos / CAST_TYPE_STRIDE;
        let to_code = self.pos % CAST_TYPE_STRIDE;

        macro_rules! run {
            ($evaluator:ident) => {
                $evaluator(value)
            };
            ($evaluator:ident { $($field:ident),+ $(,)? }) => {
                $evaluator($($field,)+ value)
            };
            ($evaluator:ident { $($field:ident : $field_value:expr),+ $(,)? }) => {
                $evaluator($($field_value,)+ value)
            };
        }

        if matches!(params, CastEvaluatorParams::Identity) {
            return run!(identity_cast_eval);
        }
        #[cfg(not(feature = "time"))]
        if is_chrono_type_code(from_code) || is_chrono_type_code(to_code) {
            return Err(DatabaseError::UnsupportedStmt(
                "time types require the `time` feature".to_string(),
            ));
        }

        match (from_code, to_code) {
            (CAST_SQL_NULL, _) => run!(null_cast_eval),
            (_, CAST_SQL_NULL) => run!(to_sql_null_cast_eval),
            (CAST_BOOLEAN, CAST_TINYINT) => run!(boolean_to_tinyint_cast_eval),
            (CAST_BOOLEAN, CAST_UTINYINT) => run!(boolean_to_utinyint_cast_eval),
            (CAST_BOOLEAN, CAST_SMALLINT) => run!(boolean_to_smallint_cast_eval),
            (CAST_BOOLEAN, CAST_USMALLINT) => run!(boolean_to_usmallint_cast_eval),
            (CAST_BOOLEAN, CAST_INTEGER) => run!(boolean_to_integer_cast_eval),
            (CAST_BOOLEAN, CAST_UINTEGER) => run!(boolean_to_uinteger_cast_eval),
            (CAST_BOOLEAN, CAST_BIGINT) => run!(boolean_to_bigint_cast_eval),
            (CAST_BOOLEAN, CAST_UBIGINT) => run!(boolean_to_ubigint_cast_eval),
            (CAST_BOOLEAN, CAST_FLOAT) => run!(boolean_to_float_cast_eval),
            (CAST_BOOLEAN, CAST_DOUBLE) => run!(boolean_to_double_cast_eval),
            (CAST_BOOLEAN, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(boolean_to_char_cast_eval { len, unit })
            }
            (CAST_BOOLEAN, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(boolean_to_varchar_cast_eval { len, unit })
            }
            (CAST_TINYINT, _) => eval_integer_cast_by_pos!(Int8, to_code, params, value),
            (CAST_SMALLINT, _) => eval_integer_cast_by_pos!(Int16, to_code, params, value),
            (CAST_INTEGER, _) => eval_integer_cast_by_pos!(Int32, to_code, params, value),
            (CAST_BIGINT, _) => eval_integer_cast_by_pos!(Int64, to_code, params, value),
            (CAST_UTINYINT, _) => eval_integer_cast_by_pos!(Uint8, to_code, params, value),
            (CAST_USMALLINT, _) => eval_integer_cast_by_pos!(Uint16, to_code, params, value),
            (CAST_UINTEGER, _) => eval_integer_cast_by_pos!(Uint32, to_code, params, value),
            (CAST_UBIGINT, _) => eval_integer_cast_by_pos!(Uint64, to_code, params, value),
            (CAST_FLOAT, CAST_TINYINT) => run!(float32_to_tinyint_cast_eval),
            (CAST_FLOAT, CAST_UTINYINT) => run!(float32_to_utinyint_cast_eval),
            (CAST_FLOAT, CAST_SMALLINT) => run!(float32_to_smallint_cast_eval),
            (CAST_FLOAT, CAST_USMALLINT) => run!(float32_to_usmallint_cast_eval),
            (CAST_FLOAT, CAST_INTEGER) => run!(float32_to_integer_cast_eval),
            (CAST_FLOAT, CAST_UINTEGER) => run!(float32_to_uinteger_cast_eval),
            (CAST_FLOAT, CAST_BIGINT) => run!(float32_to_bigint_cast_eval),
            (CAST_FLOAT, CAST_UBIGINT) => run!(float32_to_ubigint_cast_eval),
            (CAST_FLOAT, CAST_DOUBLE) => run!(float32_to_double_cast_eval),
            (CAST_FLOAT, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(float32_to_char_cast_eval { len, unit })
            }
            (CAST_FLOAT, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(float32_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "decimal")]
            (CAST_FLOAT, CAST_DECIMAL) => {
                let (precision, scale) = decimal_param(params);
                run!(float32_to_decimal_cast_eval { precision, scale })
            }
            (CAST_DOUBLE, CAST_FLOAT) => run!(float64_to_float_cast_eval),
            (CAST_DOUBLE, CAST_TINYINT) => run!(float64_to_tinyint_cast_eval),
            (CAST_DOUBLE, CAST_UTINYINT) => run!(float64_to_utinyint_cast_eval),
            (CAST_DOUBLE, CAST_SMALLINT) => run!(float64_to_smallint_cast_eval),
            (CAST_DOUBLE, CAST_USMALLINT) => run!(float64_to_usmallint_cast_eval),
            (CAST_DOUBLE, CAST_INTEGER) => run!(float64_to_integer_cast_eval),
            (CAST_DOUBLE, CAST_UINTEGER) => run!(float64_to_uinteger_cast_eval),
            (CAST_DOUBLE, CAST_BIGINT) => run!(float64_to_bigint_cast_eval),
            (CAST_DOUBLE, CAST_UBIGINT) => run!(float64_to_ubigint_cast_eval),
            (CAST_DOUBLE, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(float64_to_char_cast_eval { len, unit })
            }
            (CAST_DOUBLE, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(float64_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "decimal")]
            (CAST_DOUBLE, CAST_DECIMAL) => {
                let (precision, scale) = decimal_param(params);
                run!(float64_to_decimal_cast_eval { precision, scale })
            }
            (CAST_CHAR | CAST_VARCHAR, CAST_BOOLEAN) => {
                run!(utf8_to_boolean_cast_eval)
            }
            (CAST_CHAR | CAST_VARCHAR, CAST_TINYINT) => run!(utf8_to_tinyint_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_UTINYINT) => run!(utf8_to_utinyint_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_SMALLINT) => run!(utf8_to_smallint_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_USMALLINT) => run!(utf8_to_usmallint_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_INTEGER) => run!(utf8_to_integer_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_UINTEGER) => run!(utf8_to_uinteger_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_BIGINT) => run!(utf8_to_bigint_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_UBIGINT) => run!(utf8_to_ubigint_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_FLOAT) => run!(utf8_to_float_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_DOUBLE) => run!(utf8_to_double_cast_eval),
            (CAST_CHAR | CAST_VARCHAR, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(utf8_to_char_cast_eval { len, unit })
            }
            (CAST_CHAR | CAST_VARCHAR, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(utf8_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_CHAR | CAST_VARCHAR, CAST_DATE) => run!(utf8_to_date_cast_eval),
            #[cfg(feature = "time")]
            (CAST_CHAR | CAST_VARCHAR, CAST_DATETIME) => run!(utf8_to_datetime_cast_eval),
            #[cfg(feature = "time")]
            (CAST_CHAR | CAST_VARCHAR, CAST_TIME) => run!(utf8_to_time_cast_eval {
                precision: precision_param(params)
            }),
            #[cfg(feature = "time")]
            (CAST_CHAR | CAST_VARCHAR, CAST_TIMESTAMP) => {
                let (precision, zone) = timestamp_param(params);
                run!(utf8_to_timestamp_cast_eval { precision, zone })
            }
            #[cfg(feature = "decimal")]
            (CAST_CHAR | CAST_VARCHAR, CAST_DECIMAL) => run!(utf8_to_decimal_cast_eval),
            #[cfg(feature = "time")]
            (CAST_DATE, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(date32_to_char_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_DATE, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(date32_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_DATE, CAST_DATETIME) => run!(date32_to_datetime_cast_eval),
            #[cfg(feature = "time")]
            (CAST_DATETIME, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(date64_to_char_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_DATETIME, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(date64_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_DATETIME, CAST_DATE) => run!(date64_to_date_cast_eval),
            #[cfg(feature = "time")]
            (CAST_DATETIME, CAST_TIME) => run!(date64_to_time_cast_eval {
                precision: precision_param(params)
            }),
            #[cfg(feature = "time")]
            (CAST_DATETIME, CAST_TIMESTAMP) => {
                let (precision, zone) = timestamp_param(params);
                run!(date64_to_timestamp_cast_eval { precision, zone })
            }
            #[cfg(feature = "time")]
            (CAST_TIME, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(time32_to_char_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_TIME, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(time32_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_TIME, CAST_TIME) => run!(time32_to_time_cast_eval {
                precision: precision_param(params)
            }),
            #[cfg(feature = "time")]
            (CAST_TIMESTAMP, CAST_CHAR) => {
                let (len, unit) = string_param(params);
                let len = len.expect("char cast must have fixed length");
                run!(time64_to_char_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_TIMESTAMP, CAST_VARCHAR) => {
                let (len, unit) = string_param(params);
                run!(time64_to_varchar_cast_eval { len, unit })
            }
            #[cfg(feature = "time")]
            (CAST_TIMESTAMP, CAST_DATE) => run!(time64_to_date_cast_eval),
            #[cfg(feature = "time")]
            (CAST_TIMESTAMP, CAST_DATETIME) => run!(time64_to_datetime_cast_eval),
            #[cfg(feature = "time")]
            (CAST_TIMESTAMP, CAST_TIME) => run!(time64_to_time_cast_eval {
                precision: precision_param(params)
            }),
            #[cfg(feature = "time")]
            (CAST_TIMESTAMP, CAST_TIMESTAMP) => {
                let (precision, zone) = timestamp_param(params);
                run!(time64_to_timestamp_cast_eval { precision, zone })
            }
            #[cfg(feature = "decimal")]
            (CAST_DECIMAL, to_code) => match to_code {
                CAST_FLOAT => run!(decimal_to_float_cast_eval),
                CAST_DOUBLE => run!(decimal_to_double_cast_eval),
                CAST_DECIMAL => run!(decimal_to_decimal_cast_eval),
                CAST_CHAR => {
                    let (len, unit) = string_param(params);
                    let len = len.expect("char cast must have fixed length");
                    run!(decimal_to_char_cast_eval { len, unit })
                }
                CAST_VARCHAR => {
                    let (len, unit) = string_param(params);
                    run!(decimal_to_varchar_cast_eval { len, unit })
                }
                CAST_TINYINT => run!(decimal_to_tinyint_cast_eval),
                CAST_SMALLINT => run!(decimal_to_smallint_cast_eval),
                CAST_INTEGER => run!(decimal_to_integer_cast_eval),
                CAST_BIGINT => run!(decimal_to_bigint_cast_eval),
                CAST_UTINYINT => run!(decimal_to_utinyint_cast_eval),
                CAST_USMALLINT => run!(decimal_to_usmallint_cast_eval),
                CAST_UINTEGER => run!(decimal_to_uinteger_cast_eval),
                CAST_UBIGINT => run!(decimal_to_ubigint_cast_eval),
                _ => unreachable!("invalid decimal cast evaluator position"),
            },
            (CAST_TUPLE, CAST_TUPLE) => {
                let CastEvaluatorParams::Tuple { evaluators } = params else {
                    unreachable!("tuple cast must have tuple parameters")
                };
                eval_tuple_cast(evaluators, value)
            }
            _ => unreachable!("invalid cast evaluator position"),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::CastEvaluatorRef;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};

    fn create(from: LogicalType, to: LogicalType) -> Result<CastEvaluatorRef, DatabaseError> {
        cast_create(Cow::Owned(from), Cow::Owned(to))
    }

    #[test]
    fn test_cast_evaluator_positions_are_stable() -> Result<(), DatabaseError> {
        assert_eq!(
            create(LogicalType::Integer, LogicalType::Bigint)?.pos(),
            CAST_INTEGER * CAST_TYPE_STRIDE + CAST_BIGINT
        );
        assert_eq!(
            create(
                LogicalType::Varchar(None, crate::types::CharLengthUnits::Characters),
                LogicalType::Date
            )?
            .pos(),
            CAST_VARCHAR * CAST_TYPE_STRIDE + CAST_DATE
        );
        assert_eq!(
            create(
                LogicalType::TimeStamp(Some(3), true),
                LogicalType::Time(Some(0))
            )?
            .pos(),
            CAST_TIMESTAMP * CAST_TYPE_STRIDE + CAST_TIME
        );

        Ok(())
    }

    #[test]
    fn test_cast_evaluator_serialization() -> Result<(), DatabaseError> {
        let evaluator = create(LogicalType::Integer, LogicalType::Bigint)?;
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();

        evaluator.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            CastEvaluatorRef::decode::<RocksTransaction, _, _>(
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
