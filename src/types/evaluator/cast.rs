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
use crate::types::evaluator::date::*;
use crate::types::evaluator::datetime::*;
use crate::types::evaluator::decimal::*;
use crate::types::evaluator::float32::*;
use crate::types::evaluator::float64::*;
use crate::types::evaluator::int16::*;
use crate::types::evaluator::int32::*;
use crate::types::evaluator::int64::*;
use crate::types::evaluator::int8::*;
use crate::types::evaluator::null::{NullCastEvaluator, ToSqlNullCastEvaluator};
use crate::types::evaluator::time32::*;
use crate::types::evaluator::time64::*;
use crate::types::evaluator::tuple::TupleCastEvaluator;
use crate::types::evaluator::uint16::*;
use crate::types::evaluator::uint32::*;
use crate::types::evaluator::uint64::*;
use crate::types::evaluator::uint8::*;
use crate::types::evaluator::utf8::*;
use crate::types::evaluator::{CastEvaluator, CastEvaluatorBox};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use paste::paste;
use serde::{Deserialize, Serialize};
use sqlparser::ast::CharLengthUnits;
use std::borrow::Cow;
use std::sync::Arc;

pub(crate) fn cast_fail(from: LogicalType, to: LogicalType) -> DatabaseError {
    DatabaseError::CastFail {
        from,
        to,
        span: None,
    }
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
        #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name;

        #[typetag::serde]
        impl $crate::types::evaluator::CastEvaluator for $name {
            fn eval_cast(
                &self,
                value: &$crate::types::value::DataValue,
            ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                match value {
                    $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                    $pattern => $body,
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
        }
    };
    ($name:ident, $pattern:pat => |$this:ident| $body:expr) => {
        #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name;

        #[typetag::serde]
        impl $crate::types::evaluator::CastEvaluator for $name {
            fn eval_cast(
                &self,
                value: &$crate::types::value::DataValue,
            ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                match value {
                    $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                    $pattern => {
                        let $this = self;
                        $body
                    }
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
        }
    };
    ($name:ident, $pattern:pat => |$this:ident| $body:block) => {
        #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name;

        #[typetag::serde]
        impl $crate::types::evaluator::CastEvaluator for $name {
            fn eval_cast(
                &self,
                value: &$crate::types::value::DataValue,
            ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                match value {
                    $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                    $pattern => {
                        let $this = self;
                        $body
                    }
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
        }
    };
    ($name:ident { $($field:ident : $field_ty:ty),+ $(,)? }, $pattern:pat => |$this:ident| $body:expr) => {
        #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name {
            $(pub $field: $field_ty),+
        }

        #[typetag::serde]
        impl $crate::types::evaluator::CastEvaluator for $name {
            fn eval_cast(
                &self,
                value: &$crate::types::value::DataValue,
            ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                match value {
                    $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                    $pattern => {
                        let $this = self;
                        $body
                    }
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
        }
    };
    ($name:ident { $($field:ident : $field_ty:ty),+ $(,)? }, $pattern:pat => $body:block) => {
        #[derive(Debug, PartialEq, Eq, Clone, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name {
            $(pub $field: $field_ty),+
        }

        #[typetag::serde]
        impl $crate::types::evaluator::CastEvaluator for $name {
            fn eval_cast(
                &self,
                value: &$crate::types::value::DataValue,
            ) -> Result<$crate::types::value::DataValue, $crate::errors::DatabaseError> {
                match value {
                    $crate::types::value::DataValue::Null => Ok($crate::types::value::DataValue::Null),
                    $pattern => $body,
                    _ => unsafe { std::hint::unreachable_unchecked() },
                }
            }
        }
    };
}

#[macro_export]
macro_rules! define_integer_cast_evaluators {
    ($prefix:ident, $variant:ident, $src_ty:ty, $from_ty:expr) => {
        paste::paste! {
            $crate::define_cast_evaluator!([<$prefix ToBooleanCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                $crate::numeric_to_boolean_cast!(*value, $from_ty)
            });
            $crate::define_cast_evaluator!([<$prefix ToTinyintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int8(i8::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUTinyintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt8(u8::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToSmallintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int16(i16::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUSmallintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt16(u16::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToIntegerCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int32(i32::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUIntegerCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt32(u32::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToBigintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int64(i64::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUBigintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt64(u64::try_from(*value)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToFloatCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Float32(ordered_float::OrderedFloat(*value as f32)))
            });
            $crate::define_cast_evaluator!([<$prefix ToDoubleCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Float64(ordered_float::OrderedFloat(*value as f64)))
            });
            $crate::define_cast_evaluator!(
                [<$prefix ToCharCastEvaluator>] {
                    len: u32,
                    unit: sqlparser::ast::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_char(value.to_string(), this.len, this.unit)
                }
            );
            $crate::define_cast_evaluator!(
                [<$prefix ToVarcharCastEvaluator>] {
                    len: Option<u32>,
                    unit: sqlparser::ast::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_varchar(value.to_string(), this.len, this.unit)
                }
            );
            $crate::define_cast_evaluator!(
                [<$prefix ToDecimalCastEvaluator>] {
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
            $crate::define_cast_evaluator!([<$prefix ToFloatCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::$variant(*value))
            });
            $crate::define_cast_evaluator!([<$prefix ToDoubleCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Float64(ordered_float::OrderedFloat(value.0 as f64)))
            });
            $crate::define_cast_evaluator!([<$prefix ToTinyintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int8($crate::float_to_int_cast!(value.into_inner(), i8, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToSmallintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int16($crate::float_to_int_cast!(value.into_inner(), i16, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToIntegerCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int32($crate::float_to_int_cast!(value.into_inner(), i32, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToBigintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::Int64($crate::float_to_int_cast!(value.into_inner(), i64, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUTinyintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt8($crate::float_to_int_cast!(value.into_inner(), u8, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUSmallintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt16($crate::float_to_int_cast!(value.into_inner(), u16, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUIntegerCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt32($crate::float_to_int_cast!(value.into_inner(), u32, $src_ty)?))
            });
            $crate::define_cast_evaluator!([<$prefix ToUBigintCastEvaluator>], $crate::types::value::DataValue::$variant(value) => {
                Ok($crate::types::value::DataValue::UInt64($crate::float_to_int_cast!(value.into_inner(), u64, $src_ty)?))
            });
            $crate::define_cast_evaluator!(
                [<$prefix ToCharCastEvaluator>] {
                    len: u32,
                    unit: sqlparser::ast::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_char(value.to_string(), this.len, this.unit)
                }
            );
            $crate::define_cast_evaluator!(
                [<$prefix ToVarcharCastEvaluator>] {
                    len: Option<u32>,
                    unit: sqlparser::ast::CharLengthUnits
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    $crate::types::evaluator::cast::to_varchar(value.to_string(), this.len, this.unit)
                }
            );
            $crate::define_cast_evaluator!(
                [<$prefix ToDecimalCastEvaluator>] {
                    scale: Option<u8>,
                    to: $crate::types::LogicalType
                },
                $crate::types::value::DataValue::$variant(value) => |this| {
                    let mut decimal = rust_decimal::Decimal::$into_decimal(value.0).ok_or_else(|| {
                        $crate::types::evaluator::cast::cast_fail($from_ty, this.to.clone())
                    })?;
                    $crate::types::value::DataValue::decimal_round_f(&this.scale, &mut decimal);
                    Ok($crate::types::value::DataValue::Decimal(decimal))
                }
            );
        }
    };
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, Serialize, Deserialize)]
pub struct IdentityCastEvaluator;

#[typetag::serde]
impl CastEvaluator for IdentityCastEvaluator {
    fn eval_cast(&self, value: &DataValue) -> Result<DataValue, DatabaseError> {
        Ok(value.clone())
    }
}

macro_rules! box_cast {
    ($evaluator:expr) => {
        Ok(CastEvaluatorBox(Arc::new($evaluator)))
    };
}

macro_rules! build_integer_cast {
    ($prefix:ident, $to:expr, $from:expr) => {{
        paste! {
            match $to {
                LogicalType::SqlNull => box_cast!(ToSqlNullCastEvaluator),
                LogicalType::Boolean => box_cast!([<$prefix ToBooleanCastEvaluator>]),
                LogicalType::Tinyint => box_cast!([<$prefix ToTinyintCastEvaluator>]),
                LogicalType::UTinyint => box_cast!([<$prefix ToUTinyintCastEvaluator>]),
                LogicalType::Smallint => box_cast!([<$prefix ToSmallintCastEvaluator>]),
                LogicalType::USmallint => box_cast!([<$prefix ToUSmallintCastEvaluator>]),
                LogicalType::Integer => box_cast!([<$prefix ToIntegerCastEvaluator>]),
                LogicalType::UInteger => box_cast!([<$prefix ToUIntegerCastEvaluator>]),
                LogicalType::Bigint => box_cast!([<$prefix ToBigintCastEvaluator>]),
                LogicalType::UBigint => box_cast!([<$prefix ToUBigintCastEvaluator>]),
                LogicalType::Float => box_cast!([<$prefix ToFloatCastEvaluator>]),
                LogicalType::Double => box_cast!([<$prefix ToDoubleCastEvaluator>]),
                LogicalType::Char(len, unit) => box_cast!([<$prefix ToCharCastEvaluator>] { len: *len, unit: *unit }),
                LogicalType::Varchar(len, unit) => box_cast!([<$prefix ToVarcharCastEvaluator>] { len: *len, unit: *unit }),
                LogicalType::Decimal(_, scale) => box_cast!([<$prefix ToDecimalCastEvaluator>] { scale: *scale }),
                _ => Err(cast_fail($from.clone(), $to.clone())),
            }
        }
    }};
}

pub fn create_cast_evaluator(
    from: Cow<'_, LogicalType>,
    to: Cow<'_, LogicalType>,
) -> Result<CastEvaluatorBox, DatabaseError> {
    let from = from.as_ref();
    let to = to.as_ref();
    if from == to {
        return box_cast!(IdentityCastEvaluator);
    }

    match (from, to) {
        (LogicalType::SqlNull, _) => box_cast!(NullCastEvaluator),
        (_, LogicalType::SqlNull) => box_cast!(ToSqlNullCastEvaluator),
        (LogicalType::Boolean, LogicalType::Tinyint) => box_cast!(BooleanToTinyintCastEvaluator),
        (LogicalType::Boolean, LogicalType::UTinyint) => box_cast!(BooleanToUTinyintCastEvaluator),
        (LogicalType::Boolean, LogicalType::Smallint) => box_cast!(BooleanToSmallintCastEvaluator),
        (LogicalType::Boolean, LogicalType::USmallint) => {
            box_cast!(BooleanToUSmallintCastEvaluator)
        }
        (LogicalType::Boolean, LogicalType::Integer) => box_cast!(BooleanToIntegerCastEvaluator),
        (LogicalType::Boolean, LogicalType::UInteger) => box_cast!(BooleanToUIntegerCastEvaluator),
        (LogicalType::Boolean, LogicalType::Bigint) => box_cast!(BooleanToBigintCastEvaluator),
        (LogicalType::Boolean, LogicalType::UBigint) => box_cast!(BooleanToUBigintCastEvaluator),
        (LogicalType::Boolean, LogicalType::Float) => box_cast!(BooleanToFloatCastEvaluator),
        (LogicalType::Boolean, LogicalType::Double) => box_cast!(BooleanToDoubleCastEvaluator),
        (LogicalType::Boolean, LogicalType::Char(len, unit)) => {
            box_cast!(BooleanToCharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Boolean, LogicalType::Varchar(len, unit)) => {
            box_cast!(BooleanToVarcharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Tinyint, _) => build_integer_cast!(Int8, to, from),
        (LogicalType::Smallint, _) => build_integer_cast!(Int16, to, from),
        (LogicalType::Integer, _) => build_integer_cast!(Int32, to, from),
        (LogicalType::Bigint, _) => build_integer_cast!(Int64, to, from),
        (LogicalType::UTinyint, _) => build_integer_cast!(UInt8, to, from),
        (LogicalType::USmallint, _) => build_integer_cast!(UInt16, to, from),
        (LogicalType::UInteger, _) => build_integer_cast!(UInt32, to, from),
        (LogicalType::UBigint, _) => build_integer_cast!(UInt64, to, from),
        (LogicalType::Float, LogicalType::Tinyint) => box_cast!(Float32ToTinyintCastEvaluator),
        (LogicalType::Float, LogicalType::UTinyint) => box_cast!(Float32ToUTinyintCastEvaluator),
        (LogicalType::Float, LogicalType::Smallint) => box_cast!(Float32ToSmallintCastEvaluator),
        (LogicalType::Float, LogicalType::USmallint) => box_cast!(Float32ToUSmallintCastEvaluator),
        (LogicalType::Float, LogicalType::Integer) => box_cast!(Float32ToIntegerCastEvaluator),
        (LogicalType::Float, LogicalType::UInteger) => box_cast!(Float32ToUIntegerCastEvaluator),
        (LogicalType::Float, LogicalType::Bigint) => box_cast!(Float32ToBigintCastEvaluator),
        (LogicalType::Float, LogicalType::UBigint) => box_cast!(Float32ToUBigintCastEvaluator),
        (LogicalType::Float, LogicalType::Double) => box_cast!(Float32ToDoubleCastEvaluator),
        (LogicalType::Float, LogicalType::Char(len, unit)) => {
            box_cast!(Float32ToCharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Float, LogicalType::Varchar(len, unit)) => {
            box_cast!(Float32ToVarcharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Float, LogicalType::Decimal(_, scale)) => {
            box_cast!(Float32ToDecimalCastEvaluator {
                scale: *scale,
                to: to.clone()
            })
        }
        (LogicalType::Double, LogicalType::Float) => box_cast!(Float64ToFloatCastEvaluator),
        (LogicalType::Double, LogicalType::Tinyint) => box_cast!(Float64ToTinyintCastEvaluator),
        (LogicalType::Double, LogicalType::UTinyint) => box_cast!(Float64ToUTinyintCastEvaluator),
        (LogicalType::Double, LogicalType::Smallint) => box_cast!(Float64ToSmallintCastEvaluator),
        (LogicalType::Double, LogicalType::USmallint) => box_cast!(Float64ToUSmallintCastEvaluator),
        (LogicalType::Double, LogicalType::Integer) => box_cast!(Float64ToIntegerCastEvaluator),
        (LogicalType::Double, LogicalType::UInteger) => box_cast!(Float64ToUIntegerCastEvaluator),
        (LogicalType::Double, LogicalType::Bigint) => box_cast!(Float64ToBigintCastEvaluator),
        (LogicalType::Double, LogicalType::UBigint) => box_cast!(Float64ToUBigintCastEvaluator),
        (LogicalType::Double, LogicalType::Char(len, unit)) => {
            box_cast!(Float64ToCharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Double, LogicalType::Varchar(len, unit)) => {
            box_cast!(Float64ToVarcharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Double, LogicalType::Decimal(_, scale)) => {
            box_cast!(Float64ToDecimalCastEvaluator {
                scale: *scale,
                to: to.clone()
            })
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Boolean) => {
            box_cast!(Utf8ToBooleanCastEvaluator { from: from.clone() })
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Tinyint) => {
            box_cast!(Utf8ToTinyintCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::UTinyint) => {
            box_cast!(Utf8ToUTinyintCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Smallint) => {
            box_cast!(Utf8ToSmallintCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::USmallint) => {
            box_cast!(Utf8ToUSmallintCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Integer) => {
            box_cast!(Utf8ToIntegerCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::UInteger) => {
            box_cast!(Utf8ToUIntegerCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Bigint) => {
            box_cast!(Utf8ToBigintCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::UBigint) => {
            box_cast!(Utf8ToUBigintCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Float) => {
            box_cast!(Utf8ToFloatCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Double) => {
            box_cast!(Utf8ToDoubleCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Char(len, unit)) => {
            box_cast!(Utf8ToCharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Varchar(len, unit)) => {
            box_cast!(Utf8ToVarcharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Date) => {
            box_cast!(Utf8ToDateCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::DateTime) => {
            box_cast!(Utf8ToDatetimeCastEvaluator)
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Time(precision)) => {
            box_cast!(Utf8ToTimeCastEvaluator {
                precision: *precision
            })
        }
        (
            LogicalType::Char(_, _) | LogicalType::Varchar(_, _),
            LogicalType::TimeStamp(precision, zone),
        ) => {
            box_cast!(Utf8ToTimestampCastEvaluator {
                precision: *precision,
                zone: *zone,
                to: to.clone()
            })
        }
        (LogicalType::Char(_, _) | LogicalType::Varchar(_, _), LogicalType::Decimal(_, _)) => {
            box_cast!(Utf8ToDecimalCastEvaluator)
        }
        (LogicalType::Date, LogicalType::Char(len, unit)) => {
            box_cast!(Date32ToCharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::Date, LogicalType::Varchar(len, unit)) => {
            box_cast!(Date32ToVarcharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::Date, LogicalType::DateTime) => {
            box_cast!(Date32ToDatetimeCastEvaluator { to: to.clone() })
        }
        (LogicalType::DateTime, LogicalType::Char(len, unit)) => {
            box_cast!(Date64ToCharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::DateTime, LogicalType::Varchar(len, unit)) => {
            box_cast!(Date64ToVarcharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::DateTime, LogicalType::Date) => {
            box_cast!(Date64ToDateCastEvaluator { to: to.clone() })
        }
        (LogicalType::DateTime, LogicalType::Time(precision)) => {
            box_cast!(Date64ToTimeCastEvaluator {
                precision: *precision,
                to: to.clone()
            })
        }
        (LogicalType::DateTime, LogicalType::TimeStamp(precision, zone)) => {
            box_cast!(Date64ToTimestampCastEvaluator {
                precision: *precision,
                zone: *zone
            })
        }
        (LogicalType::Time(_), LogicalType::Char(len, unit)) => {
            box_cast!(Time32ToCharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::Time(_), LogicalType::Varchar(len, unit)) => {
            box_cast!(Time32ToVarcharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::Time(_), LogicalType::Time(precision)) => {
            box_cast!(Time32ToTimeCastEvaluator {
                precision: *precision
            })
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Char(len, unit)) => {
            box_cast!(Time64ToCharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Varchar(len, unit)) => {
            box_cast!(Time64ToVarcharCastEvaluator {
                len: *len,
                unit: *unit,
                to: to.clone()
            })
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Date) => {
            box_cast!(Time64ToDateCastEvaluator {
                from: from.clone(),
                to: to.clone()
            })
        }
        (LogicalType::TimeStamp(_, _), LogicalType::DateTime) => {
            box_cast!(Time64ToDatetimeCastEvaluator {
                from: from.clone(),
                to: to.clone()
            })
        }
        (LogicalType::TimeStamp(_, _), LogicalType::Time(precision)) => {
            box_cast!(Time64ToTimeCastEvaluator {
                precision: *precision,
                from: from.clone(),
                to: to.clone()
            })
        }
        (LogicalType::TimeStamp(_, _), LogicalType::TimeStamp(precision, zone)) => {
            box_cast!(Time64ToTimestampCastEvaluator {
                precision: *precision,
                zone: *zone
            })
        }
        (LogicalType::Decimal(_, _), LogicalType::Float) => box_cast!(DecimalToFloatCastEvaluator),
        (LogicalType::Decimal(_, _), LogicalType::Double) => {
            box_cast!(DecimalToDoubleCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::Decimal(_, _)) => {
            box_cast!(DecimalToDecimalCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::Char(len, unit)) => {
            box_cast!(DecimalToCharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Decimal(_, _), LogicalType::Varchar(len, unit)) => {
            box_cast!(DecimalToVarcharCastEvaluator {
                len: *len,
                unit: *unit
            })
        }
        (LogicalType::Decimal(_, _), LogicalType::Tinyint) => {
            box_cast!(DecimalToTinyintCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::Smallint) => {
            box_cast!(DecimalToSmallintCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::Integer) => {
            box_cast!(DecimalToIntegerCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::Bigint) => {
            box_cast!(DecimalToBigintCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::UTinyint) => {
            box_cast!(DecimalToUTinyintCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::USmallint) => {
            box_cast!(DecimalToUSmallintCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::UInteger) => {
            box_cast!(DecimalToUIntegerCastEvaluator)
        }
        (LogicalType::Decimal(_, _), LogicalType::UBigint) => {
            box_cast!(DecimalToUBigintCastEvaluator)
        }
        (LogicalType::Tuple(from_types), LogicalType::Tuple(to_types)) => {
            let evaluators = from_types
                .iter()
                .zip(to_types.iter())
                .map(|(from, to)| create_cast_evaluator(Cow::Borrowed(from), Cow::Borrowed(to)))
                .collect::<Result<Vec<_>, _>>()?;
            box_cast!(TupleCastEvaluator {
                element_evaluators: evaluators
            })
        }
        _ => Err(cast_fail(from.clone(), to.clone())),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::create_cast_evaluator;
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::evaluator::CastEvaluatorBox;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use std::io::{Cursor, Seek, SeekFrom};

    fn create(from: LogicalType, to: LogicalType) -> Result<CastEvaluatorBox, DatabaseError> {
        create_cast_evaluator(Cow::Owned(from), Cow::Owned(to))
    }

    #[test]
    fn test_cast_evaluator_serialization() -> Result<(), DatabaseError> {
        let evaluator = create(LogicalType::Integer, LogicalType::Bigint)?;
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        evaluator.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            CastEvaluatorBox::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            evaluator
        );

        Ok(())
    }
}
