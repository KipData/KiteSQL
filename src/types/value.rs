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

use super::LogicalType;
use crate::errors::DatabaseError;
use crate::storage::table_codec::{BumpBytes, BOUND_MAX_TAG, NOTNULL_TAG, NULL_TAG};
use byteorder::ReadBytesExt;
use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use sqlparser::ast::CharLengthUnits;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::Hash;
use std::io::{Read, Write};
use std::str::FromStr;
use std::sync::LazyLock;
use std::{cmp, fmt, mem};

static UNIX_DATETIME: LazyLock<NaiveDateTime> =
    LazyLock::new(|| DateTime::from_timestamp(0, 0).unwrap().naive_utc());

static UNIX_TIME: LazyLock<NaiveTime> = LazyLock::new(|| NaiveTime::from_hms_opt(0, 0, 0).unwrap());

pub const DATE_FMT: &str = "%Y-%m-%d";
pub const DATE_TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";
pub const TIME_STAMP_FMT_WITHOUT_ZONE: &str = "%Y-%m-%d %H:%M:%S%.f";
pub const TIME_STAMP_FMT_WITH_ZONE: &str = "%Y-%m-%d %H:%M:%S%.f%z";
pub const TIME_STAMP_FMT_WITHOUT_PRECISION: &str = "%Y-%m-%d %H:%M:%S%z";
pub const TIME_FMT: &str = "%H:%M:%S";
pub const TIME_FMT_WITHOUT_ZONE: &str = "%H:%M:%S%.f";
pub const TIME_FMT_WITH_ZONE: &str = "%H:%M:%S%.f%z";
pub const TIME_FMT_WITHOUT_PRECISION: &str = "%H:%M:%S%z";

pub const ONE_SEC_TO_NANO: u32 = 1_000_000_000;
pub const ONE_DAY_TO_SEC: u32 = 86_400;

const ENCODE_GROUP_SIZE: usize = 8;
const ENCODE_MARKER: u8 = 0xFF;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum Utf8Type {
    Variable(Option<u32>),
    Fixed(u32),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum DataValue {
    Null,
    Boolean(bool),
    Float32(OrderedFloat<f32>),
    Float64(OrderedFloat<f64>),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Utf8 {
        value: String,
        ty: Utf8Type,
        unit: CharLengthUnits,
    },
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(i32),
    /// Date stored as a signed 64bit int timestamp since UNIX epoch 1970-01-01
    Date64(i64),
    Time32(u32, u64),
    Time64(i64, u64, bool),
    Decimal(Decimal),
    /// (values, is_upper)
    Tuple(Vec<DataValue>, bool),
}

#[derive(Clone, Copy)]
pub struct TupleMappingRef<'a> {
    index_to_scan: &'a [usize],
    target_len: usize,
}

impl<'a> TupleMappingRef<'a> {
    pub fn new(index_to_scan: &'a [usize], target_len: usize) -> Self {
        TupleMappingRef {
            index_to_scan,
            target_len,
        }
    }

    #[inline]
    pub fn target_len(&self) -> usize {
        self.target_len
    }

    #[inline]
    pub fn scan_index(&self, index_pos: usize) -> Option<usize> {
        self.index_to_scan.get(index_pos).copied().and_then(|slot| {
            if slot == usize::MAX {
                None
            } else {
                Some(slot)
            }
        })
    }
}

enum TupleCollector<'a> {
    Mapped {
        mapping: TupleMappingRef<'a>,
        values: Vec<DataValue>,
    },
    Ordered(Vec<DataValue>),
}

impl<'a> TupleCollector<'a> {
    fn new(mapping: Option<TupleMappingRef<'a>>, tuple_len: usize) -> Self {
        if let Some(mapping) = mapping {
            TupleCollector::Mapped {
                values: vec![DataValue::Null; mapping.target_len()],
                mapping,
            }
        } else {
            TupleCollector::Ordered(Vec::with_capacity(tuple_len))
        }
    }

    fn push(&mut self, index_pos: usize, value: DataValue) {
        match self {
            TupleCollector::Mapped { mapping, values } => {
                if let Some(target_pos) = mapping.scan_index(index_pos) {
                    values[target_pos] = value;
                }
            }
            TupleCollector::Ordered(values) => values.push(value),
        }
    }

    fn finish(self) -> Vec<DataValue> {
        match self {
            TupleCollector::Mapped { values, .. } => values,
            TupleCollector::Ordered(values) => values,
        }
    }
}

macro_rules! generate_get_option {
    ($data_value:ident, $($prefix:ident : $variant:ident($field:ty)),*) => {
        impl $data_value {
            $(
                pub fn $prefix(&self) -> $field {
                    if let $data_value::$variant(val) = self {
                        Some(val.clone())
                    } else {
                        None
                    }
                }
            )*
        }
    };
}

generate_get_option!(DataValue,
    bool : Boolean(Option<bool>),
    i8 : Int8(Option<i8>),
    i16 : Int16(Option<i16>),
    i32 : Int32(Option<i32>),
    i64 : Int64(Option<i64>),
    u8 : UInt8(Option<u8>),
    u16 : UInt16(Option<u16>),
    u32 : UInt32(Option<u32>),
    u64 : UInt64(Option<u64>),
    decimal : Decimal(Option<Decimal>)
);

impl PartialEq for DataValue {
    fn eq(&self, other: &Self) -> bool {
        use DataValue::*;

        if self.is_null() && other.is_null() {
            return true;
        }

        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => v1.eq(v2),
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => v1.eq(v2),
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Utf8 { value: v1, .. }, Utf8 { value: v2, .. }) => v1.eq(v2),
            (Utf8 { .. }, _) => false,
            (Null, Null) => true,
            (Null, _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1.eq(v2),
            (Date64(_), _) => false,
            (Time32(v1, ..), Time32(v2, ..)) => v1.eq(v2),
            (Time32(..), _) => false,
            (Time64(v1, ..), Time64(v2, ..)) => v1.eq(v2),
            (Time64(..), _) => false,
            (Decimal(v1), Decimal(v2)) => v1.eq(v2),
            (Decimal(_), _) => false,
            (Tuple(values_1, is_upper_1), Tuple(values_2, is_upper_2)) => {
                values_1.eq(values_2) && is_upper_1.eq(is_upper_2)
            }
            (Tuple(..), _) => false,
        }
    }
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use DataValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => v1.partial_cmp(v2),
            (Float32(_), _) => None,
            (Float64(v1), Float64(v2)) => v1.partial_cmp(v2),
            (Float64(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Utf8 { value: v1, .. }, Utf8 { value: v2, .. }) => v1.partial_cmp(v2),
            (Utf8 { .. }, _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (Date64(v1), Date64(v2)) => v1.partial_cmp(v2),
            (Date64(_), _) => None,
            (Time32(v1, ..), Time32(v2, ..)) => v1.partial_cmp(v2),
            (Time32(..), _) => None,
            (Time64(v1, ..), Time64(v2, ..)) => v1.partial_cmp(v2),
            (Time64(..), _) => None,
            (Decimal(v1), Decimal(v2)) => v1.partial_cmp(v2),
            (Decimal(_), _) => None,
            (Tuple(..), _) => None,
        }
    }
}

macro_rules! encode_u {
    ($writer:ident, $u:expr) => {
        $writer.write_all(&$u.to_be_bytes())?
    };
}

macro_rules! decode_u {
    ($reader:ident, $ty:ty) => {{
        let mut buf = [0u8; std::mem::size_of::<$ty>()];
        $reader.read_exact(&mut buf)?;
        <$ty>::from_be_bytes(buf)
    }};
}

impl Eq for DataValue {}

impl Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use DataValue::*;
        match self {
            Boolean(v) => v.hash(state),
            Float32(v) => v.hash(state),
            Float64(v) => v.hash(state),
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8 { value: v, .. } => v.hash(state),
            Null => 1.hash(state),
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Time32(v, ..) => v.hash(state),
            Time64(v, ..) => v.hash(state),
            Decimal(v) => v.hash(state),
            Tuple(values, is_upper) => {
                values.hash(state);
                is_upper.hash(state);
            }
        }
    }
}
macro_rules! varchar_cast {
    ($value:expr, $len:expr, $ty:expr, $unit:expr) => {{
        let s_value = $value.to_string();
        if let Some(len) = $len {
            if Self::check_string_len(&s_value, *len as usize, $unit) {
                return Err(DatabaseError::TooLong);
            }
        }
        Ok(DataValue::Utf8 {
            value: s_value,
            ty: $ty,
            unit: $unit,
        })
    }};
}

macro_rules! numeric_to_boolean {
    ($value:expr, $from_ty:expr) => {
        match $value {
            0 => Ok(DataValue::Boolean(false)),
            1 => Ok(DataValue::Boolean(true)),
            _ => Err(DatabaseError::CastFail {
                from: $from_ty,
                to: LogicalType::Boolean,
            }),
        }
    };
}

macro_rules! float_to_int {
    ($float_value:expr, $int_type:ty, $float_type:ty) => {{
        let float_value: $float_type = $float_value;
        if float_value.is_nan() {
            Ok(0)
        } else if float_value <= 0.0 || float_value > <$int_type>::MAX as $float_type {
            Err(DatabaseError::OverFlow)
        } else {
            Ok(float_value as $int_type)
        }
    }};
}

macro_rules! decimal_to_int {
    ($decimal:expr, $int_type:ty) => {{
        let d = $decimal;
        if d.is_sign_negative() {
            if <$int_type>::MIN == 0 {
                0
            } else {
                let min = Decimal::from(<$int_type>::MIN);
                if d <= min {
                    <$int_type>::MIN
                } else {
                    d.to_i128().unwrap() as $int_type
                }
            }
        } else {
            let max = Decimal::from(<$int_type>::MAX);
            if d >= max {
                <$int_type>::MAX
            } else {
                d.to_i128().unwrap() as $int_type
            }
        }
    }};
}

impl DataValue {
    pub fn float(&self) -> Option<f32> {
        if let DataValue::Float32(val) = self {
            Some(val.0)
        } else {
            None
        }
    }

    pub fn double(&self) -> Option<f64> {
        if let DataValue::Float64(val) = self {
            Some(val.0)
        } else {
            None
        }
    }

    pub fn utf8(&self) -> Option<&str> {
        if let DataValue::Utf8 { value, .. } = self {
            Some(value)
        } else {
            None
        }
    }

    pub fn new_utf8(string: String) -> Self {
        DataValue::Utf8 {
            ty: Utf8Type::Fixed(string.len() as u32),
            value: string,
            unit: CharLengthUnits::Characters,
        }
    }

    pub fn date(&self) -> Option<NaiveDate> {
        if let DataValue::Date32(val) = self {
            NaiveDate::from_num_days_from_ce_opt(*val)
        } else {
            None
        }
    }

    pub fn datetime(&self) -> Option<NaiveDateTime> {
        if let DataValue::Date64(val) = self {
            DateTime::from_timestamp(*val, 0).map(|dt| dt.naive_utc())
        } else {
            None
        }
    }

    pub fn time(&self) -> Option<NaiveTime> {
        if let DataValue::Time32(val, ..) = self {
            NaiveTime::from_num_seconds_from_midnight_opt(*val, 0)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn check_string_len(string: &str, len: usize, unit: CharLengthUnits) -> bool {
        match unit {
            CharLengthUnits::Characters => string.chars().count() > len,
            CharLengthUnits::Octets => string.len() > len,
        }
    }

    #[inline]
    pub(crate) fn check_len(&self, logic_type: &LogicalType) -> Result<(), DatabaseError> {
        let is_over_len = match (logic_type, self) {
            (LogicalType::Varchar(None, _), _) => false,
            (
                LogicalType::Varchar(Some(len), CharLengthUnits::Characters),
                DataValue::Utf8 {
                    value: val,
                    ty: Utf8Type::Variable(_),
                    unit: CharLengthUnits::Characters,
                },
            )
            | (
                LogicalType::Char(len, CharLengthUnits::Characters),
                DataValue::Utf8 {
                    value: val,
                    ty: Utf8Type::Fixed(_),
                    unit: CharLengthUnits::Characters,
                },
            ) => Self::check_string_len(val, *len as usize, CharLengthUnits::Characters),
            (
                LogicalType::Varchar(Some(len), CharLengthUnits::Octets),
                DataValue::Utf8 {
                    value: val,
                    ty: Utf8Type::Variable(_),
                    unit: CharLengthUnits::Octets,
                },
            )
            | (
                LogicalType::Char(len, CharLengthUnits::Octets),
                DataValue::Utf8 {
                    value: val,
                    ty: Utf8Type::Fixed(_),
                    unit: CharLengthUnits::Octets,
                },
            ) => Self::check_string_len(val, *len as usize, CharLengthUnits::Octets),
            (LogicalType::Decimal(full_len, scale_len), DataValue::Decimal(val)) => {
                if let Some(len) = full_len {
                    let mantissa = val.mantissa().abs();
                    if mantissa != 0 && mantissa.ilog10() + 1 > *len as u32 {
                        return Err(DatabaseError::TooLong);
                    }
                }
                if let Some(len) = scale_len {
                    if val.scale() > *len as u32 {
                        return Err(DatabaseError::TooLong);
                    }
                }
                false
            }
            _ => false,
        };

        if is_over_len {
            return Err(DatabaseError::TooLong);
        }

        Ok(())
    }

    pub fn pack(a: u32, b: u32, precision: u64) -> u32 {
        assert!(b <= ONE_SEC_TO_NANO);
        assert!(a <= ONE_DAY_TO_SEC);
        // Scale down `a` to fit
        let scaled_b = b / (1000000000 / 10_u32.pow(precision as u32)); // Now 0-1_000_000
        let p = match precision {
            1 => 28,
            2 => 25,
            3 => 22,
            4 => 18,
            _ => 31,
        };
        (scaled_b << p) | a
    }

    pub fn unpack(combined: u32, precision: u64) -> (u32, u32) {
        let p = match precision {
            1 => 28,
            2 => 25,
            3 => 22,
            4 => 18,
            _ => 31,
        };
        let scaled_a = combined >> p;
        let b = combined & (2_u32.pow(p) - 1);
        (b, scaled_a * (1000000000 / 10_u32.pow(precision as u32)))
    }

    fn format_date(value: i32) -> Option<String> {
        Self::date_format(value).map(|fmt| format!("{fmt}"))
    }

    fn format_datetime(value: i64) -> Option<String> {
        Self::date_time_format(value).map(|fmt| format!("{fmt}"))
    }

    fn format_time(value: u32, precision: u64) -> Option<String> {
        Self::time_format(value, precision).map(|fmt| format!("{fmt}"))
    }

    fn format_timestamp(value: i64, precision: u64) -> Option<String> {
        Self::time_stamp_format(value, precision, false).map(|fmt| format!("{fmt}"))
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }

    #[inline]
    pub fn init(logic_type: &LogicalType) -> DataValue {
        match logic_type {
            LogicalType::SqlNull => DataValue::Null,
            LogicalType::Boolean => DataValue::Boolean(false),
            LogicalType::Tinyint => DataValue::Int8(0),
            LogicalType::UTinyint => DataValue::UInt8(0),
            LogicalType::Smallint => DataValue::Int16(0),
            LogicalType::USmallint => DataValue::UInt16(0),
            LogicalType::Integer => DataValue::Int32(0),
            LogicalType::UInteger => DataValue::UInt32(0),
            LogicalType::Bigint => DataValue::Int64(0),
            LogicalType::UBigint => DataValue::UInt64(0),
            LogicalType::Float => DataValue::Float32(OrderedFloat(0.0)),
            LogicalType::Double => DataValue::Float64(OrderedFloat(0.0)),
            LogicalType::Char(len, unit) => DataValue::Utf8 {
                value: String::new(),
                ty: Utf8Type::Fixed(*len),
                unit: *unit,
            },
            LogicalType::Varchar(len, unit) => DataValue::Utf8 {
                value: String::new(),
                ty: Utf8Type::Variable(*len),
                unit: *unit,
            },
            LogicalType::Date => DataValue::Date32(UNIX_DATETIME.num_days_from_ce()),
            LogicalType::DateTime => DataValue::Date64(UNIX_DATETIME.and_utc().timestamp()),
            LogicalType::Time(precision) => match precision {
                Some(i) => DataValue::Time32(UNIX_TIME.num_seconds_from_midnight(), *i),
                None => DataValue::Time32(UNIX_TIME.num_seconds_from_midnight(), 0),
            },
            LogicalType::TimeStamp(precision, zone) => match precision {
                Some(3) => DataValue::Time64(UNIX_DATETIME.and_utc().timestamp_millis(), 3, *zone),
                Some(6) => DataValue::Time64(UNIX_DATETIME.and_utc().timestamp_micros(), 6, *zone),
                Some(9) => {
                    if let Some(value) = UNIX_DATETIME.and_utc().timestamp_nanos_opt() {
                        DataValue::Time64(value, 9, *zone)
                    } else {
                        unreachable!()
                    }
                }
                None => DataValue::Time64(UNIX_DATETIME.and_utc().timestamp(), 0, *zone),
                _ => unreachable!(),
            },
            LogicalType::Decimal(_, _) => DataValue::Decimal(Decimal::new(0, 0)),
            LogicalType::Tuple(types) => {
                let values = types.iter().map(DataValue::init).collect_vec();

                DataValue::Tuple(values, false)
            }
        }
    }

    #[inline]
    pub fn logical_type(&self) -> LogicalType {
        match self {
            DataValue::Null => LogicalType::SqlNull,
            DataValue::Boolean(_) => LogicalType::Boolean,
            DataValue::Float32(_) => LogicalType::Float,
            DataValue::Float64(_) => LogicalType::Double,
            DataValue::Int8(_) => LogicalType::Tinyint,
            DataValue::Int16(_) => LogicalType::Smallint,
            DataValue::Int32(_) => LogicalType::Integer,
            DataValue::Int64(_) => LogicalType::Bigint,
            DataValue::UInt8(_) => LogicalType::UTinyint,
            DataValue::UInt16(_) => LogicalType::USmallint,
            DataValue::UInt32(_) => LogicalType::UInteger,
            DataValue::UInt64(_) => LogicalType::UBigint,
            DataValue::Utf8 {
                ty: Utf8Type::Variable(len),
                unit,
                ..
            } => LogicalType::Varchar(*len, *unit),
            DataValue::Utf8 {
                ty: Utf8Type::Fixed(len),
                unit,
                ..
            } => LogicalType::Char(*len, *unit),
            DataValue::Date32(_) => LogicalType::Date,
            DataValue::Date64(_) => LogicalType::DateTime,
            DataValue::Time32(..) => LogicalType::Time(None),
            DataValue::Time64(..) => LogicalType::TimeStamp(None, false),
            DataValue::Decimal(_) => LogicalType::Decimal(None, None),
            DataValue::Tuple(values, ..) => {
                let types = values.iter().map(|v| v.logical_type()).collect_vec();
                LogicalType::Tuple(types)
            }
        }
    }

    // EncodeBytes guarantees the encoded value is in ascending order for comparison,
    // encoding with the following rule:
    //
    //	[group1][marker1]...[groupN][markerN]
    //	group is 8 bytes slice which is padding with 0.
    //	marker is `0xFF - padding 0 count`
    //
    // For example:
    //
    //	[] -> [0, 0, 0, 0, 0, 0, 0, 0, 247]
    //	[1, 2, 3] -> [1, 2, 3, 0, 0, 0, 0, 0, 250]
    //	[1, 2, 3, 0] -> [1, 2, 3, 0, 0, 0, 0, 0, 251]
    //	[1, 2, 3, 4, 5, 6, 7, 8] -> [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]
    //
    // Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    #[inline]
    // FIXME
    fn encode_string(b: &mut BumpBytes, data: &[u8]) {
        let d_len = data.len();
        let needed_groups = d_len / ENCODE_GROUP_SIZE + 1;
        Self::realloc_bytes(b, needed_groups * (ENCODE_GROUP_SIZE + 1));

        let mut idx = 0;

        loop {
            let remain = d_len.saturating_sub(idx);

            if remain >= ENCODE_GROUP_SIZE {
                b.extend_from_slice(&data[idx..idx + ENCODE_GROUP_SIZE]);
                b.push(ENCODE_MARKER);
                idx += ENCODE_GROUP_SIZE;
                continue;
            }

            let pad_count = ENCODE_GROUP_SIZE - remain;

            if remain > 0 {
                b.extend_from_slice(&data[idx..]);
            }

            for _ in 0..pad_count {
                b.push(0);
            }

            b.push(ENCODE_MARKER - pad_count as u8);
            break;
        }
    }

    #[inline]
    fn decode_string<R: Read>(reader: &mut R) -> std::io::Result<Vec<u8>> {
        let mut result = Vec::new();

        loop {
            let mut group = [0u8; ENCODE_GROUP_SIZE];
            reader.read_exact(&mut group)?;

            let mut marker = [0u8; 1];
            reader.read_exact(&mut marker)?;
            let marker = marker[0];

            let pad_count = (ENCODE_MARKER - marker) as usize;

            if pad_count > ENCODE_GROUP_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "memcomparable string marker out of range",
                ));
            }

            let data_len = ENCODE_GROUP_SIZE - pad_count;
            result.extend_from_slice(&group[..data_len]);

            if pad_count != 0 {
                break;
            }
        }

        Ok(result)
    }

    #[inline]
    fn realloc_bytes(b: &mut BumpBytes, size: usize) {
        if size > 0 {
            b.reserve(size);
        }
    }

    #[inline(always)]
    pub fn memcomparable_encode_with_null_order(
        &self,
        b: &mut BumpBytes,
        nulls_first: bool,
    ) -> Result<(), DatabaseError> {
        let (null_tag, not_null_tag) = if nulls_first {
            (NOTNULL_TAG, NULL_TAG)
        } else {
            (NULL_TAG, NOTNULL_TAG)
        };
        if let DataValue::Null = self {
            b.push(null_tag);
            return Ok(());
        }
        b.push(not_null_tag);

        match self {
            DataValue::Null => (),
            DataValue::Int8(v) => encode_u!(b, *v as u8 ^ 0x80_u8),
            DataValue::Int16(v) => encode_u!(b, *v as u16 ^ 0x8000_u16),
            DataValue::Int32(v) | DataValue::Date32(v) => {
                encode_u!(b, *v as u32 ^ 0x80000000_u32)
            }
            DataValue::Int64(v) | DataValue::Date64(v) | DataValue::Time64(v, ..) => {
                encode_u!(b, *v as u64 ^ 0x8000000000000000_u64)
            }
            DataValue::UInt8(v) => encode_u!(b, v),
            DataValue::UInt16(v) => encode_u!(b, v),
            DataValue::UInt32(v) | DataValue::Time32(v, ..) => encode_u!(b, v),
            DataValue::UInt64(v) => encode_u!(b, v),
            DataValue::Utf8 { value: v, .. } => Self::encode_string(b, v.as_bytes()),
            DataValue::Boolean(v) => b.push(if *v { b'1' } else { b'0' }),
            DataValue::Float32(f) => {
                let mut u = f.to_bits();

                if f.0 >= 0_f32 {
                    u |= 0x80000000_u32;
                } else {
                    u = !u;
                }

                encode_u!(b, u);
            }
            DataValue::Float64(f) => {
                let mut u = f.to_bits();

                if f.0 >= 0_f64 {
                    u |= 0x8000000000000000_u64;
                } else {
                    u = !u;
                }

                encode_u!(b, u);
            }
            DataValue::Decimal(v) => Self::serialize_decimal(*v, b)?,
            DataValue::Tuple(values, is_upper) => {
                let last = values.len() - 1;

                for (i, v) in values.iter().enumerate() {
                    v.memcomparable_encode(b)?;
                    if i == last && *is_upper {
                        b.push(BOUND_MAX_TAG);
                    }
                }
            }
        }

        Ok(())
    }

    #[inline]
    pub fn memcomparable_encode(&self, b: &mut BumpBytes) -> Result<(), DatabaseError> {
        self.memcomparable_encode_with_null_order(b, false)
    }

    pub fn memcomparable_decode<R: Read>(
        reader: &mut R,
        ty: &LogicalType,
    ) -> Result<DataValue, DatabaseError> {
        Self::memcomparable_decode_mapping(reader, ty, None)
    }

    #[inline]
    pub fn memcomparable_decode_mapping<R: Read>(
        reader: &mut R,
        ty: &LogicalType,
        // for index cover mapping reduce one layer of conversion
        tuple_mapping: Option<TupleMappingRef<'_>>,
    ) -> Result<DataValue, DatabaseError> {
        if reader.read_u8()? == NULL_TAG {
            return Ok(DataValue::Null);
        }
        match ty {
            LogicalType::SqlNull => Ok(DataValue::Null),
            LogicalType::Tinyint => {
                let u = decode_u!(reader, u8);
                Ok(DataValue::Int8((u ^ 0x80) as i8))
            }
            LogicalType::Smallint => {
                let u = decode_u!(reader, u16);
                Ok(DataValue::Int16((u ^ 0x8000) as i16))
            }
            LogicalType::Integer | LogicalType::Date | LogicalType::Time(_) => {
                let u = decode_u!(reader, u32);
                Ok(DataValue::Int32((u ^ 0x8000_0000) as i32))
            }
            LogicalType::Bigint | LogicalType::DateTime | LogicalType::TimeStamp(..) => {
                let u = decode_u!(reader, u64);
                Ok(DataValue::Int64((u ^ 0x8000_0000_0000_0000) as i64))
            }
            LogicalType::UTinyint => Ok(DataValue::UInt8(decode_u!(reader, u8))),
            LogicalType::USmallint => Ok(DataValue::UInt16(decode_u!(reader, u16))),
            LogicalType::UInteger => Ok(DataValue::UInt32(decode_u!(reader, u32))),
            LogicalType::UBigint => Ok(DataValue::UInt64(decode_u!(reader, u64))),
            LogicalType::Float => {
                let mut u = decode_u!(reader, u32);

                // 反向还原
                if (u & 0x8000_0000) != 0 {
                    u &= !0x8000_0000;
                } else {
                    u = !u;
                }

                Ok(DataValue::Float32(f32::from_bits(u).into()))
            }
            LogicalType::Double => {
                let mut u = decode_u!(reader, u64);

                if (u & 0x8000_0000_0000_0000) != 0 {
                    u &= !0x8000_0000_0000_0000;
                } else {
                    u = !u;
                }

                Ok(DataValue::Float64(f64::from_bits(u).into()))
            }
            LogicalType::Boolean => {
                let mut b = [0u8; 1];
                reader.read_exact(&mut b)?;
                Ok(DataValue::Boolean(b[0] == b'1'))
            }
            LogicalType::Varchar(len, unit) => Ok(DataValue::Utf8 {
                value: String::from_utf8(Self::decode_string(reader)?)?,
                ty: Utf8Type::Variable(*len),
                unit: *unit,
            }),
            LogicalType::Char(len, unit) => Ok(DataValue::Utf8 {
                value: String::from_utf8(Self::decode_string(reader)?)?,
                ty: Utf8Type::Fixed(*len),
                unit: *unit,
            }),
            LogicalType::Decimal(..) => Ok(DataValue::Decimal(Self::deserialize_decimal(reader)?)),
            LogicalType::Tuple(tys) => {
                let mut collector = TupleCollector::new(tuple_mapping, tys.len());

                for (index_pos, ty) in tys.iter().enumerate() {
                    let value = Self::memcomparable_decode_mapping(reader, ty, None)?;
                    collector.push(index_pos, value);
                }
                Ok(DataValue::Tuple(collector.finish(), false))
            }
        }
    }

    // https://github.com/risingwavelabs/memcomparable/blob/main/src/ser.rs#L468
    pub fn serialize_decimal(decimal: Decimal, bytes: &mut BumpBytes) -> Result<(), DatabaseError> {
        if decimal.is_zero() {
            bytes.push(0x15);
            return Ok(());
        }
        let (exponent, significand) = Self::decimal_e_m(decimal);
        if decimal.is_sign_positive() {
            match exponent {
                11.. => {
                    bytes.push(0x22);
                    bytes.push(exponent as u8);
                }
                0..=10 => {
                    bytes.push(0x17 + exponent as u8);
                }
                _ => {
                    bytes.push(0x16);
                    bytes.push(!(-exponent) as u8);
                }
            }
            bytes.extend_from_slice(&significand)
        } else {
            match exponent {
                11.. => {
                    bytes.push(0x8);
                    bytes.push(!exponent as u8);
                }
                0..=10 => {
                    bytes.push(0x13 - exponent as u8);
                }
                _ => {
                    bytes.push(0x14);
                    bytes.push(-exponent as u8);
                }
            }
            for b in significand {
                bytes.push(!b);
            }
        }
        Ok(())
    }

    fn decimal_e_m(decimal: Decimal) -> (i8, Vec<u8>) {
        if decimal.is_zero() {
            return (0, vec![]);
        }
        const POW10: [u128; 30] = [
            1,
            10,
            100,
            1000,
            10000,
            100000,
            1000000,
            10000000,
            100000000,
            1000000000,
            10000000000,
            100000000000,
            1000000000000,
            10000000000000,
            100000000000000,
            1000000000000000,
            10000000000000000,
            100000000000000000,
            1000000000000000000,
            10000000000000000000,
            100000000000000000000,
            1000000000000000000000,
            10000000000000000000000,
            100000000000000000000000,
            1000000000000000000000000,
            10000000000000000000000000,
            100000000000000000000000000,
            1000000000000000000000000000,
            10000000000000000000000000000,
            100000000000000000000000000000,
        ];
        let mut mantissa = decimal.mantissa().unsigned_abs();
        let prec = POW10.as_slice().partition_point(|&p| p <= mantissa);

        let e10 = prec as i32 - decimal.scale() as i32;
        let e100 = if e10 >= 0 { (e10 + 1) / 2 } else { e10 / 2 };
        // Maybe need to add a zero at the beginning.
        // e.g. 111.11 -> 2(exponent which is 100 based) + 0.011111(mantissa).
        // So, the `digit_num` of 111.11 will be 6.
        let mut digit_num = if e10 == 2 * e100 { prec } else { prec + 1 };

        let mut byte_array = Vec::with_capacity(16);
        // Remove trailing zero.
        while mantissa.is_multiple_of(10) && mantissa != 0 {
            mantissa /= 10;
            digit_num -= 1;
        }

        // Cases like: 0.12345, not 0.01111.
        if digit_num % 2 == 1 {
            mantissa *= 10;
            // digit_num += 1;
        }
        while mantissa >> 64 != 0 {
            let byte = (mantissa % 100) as u8 * 2 + 1;
            byte_array.push(byte);
            mantissa /= 100;
        }
        // optimize for division
        let mut mantissa = mantissa as u64;
        while mantissa != 0 {
            let byte = (mantissa % 100) as u8 * 2 + 1;
            byte_array.push(byte);
            mantissa /= 100;
        }
        byte_array[0] -= 1;
        byte_array.reverse();

        (e100 as i8, byte_array)
    }

    pub fn deserialize_decimal<R: Read>(mut reader: R) -> Result<Decimal, DatabaseError> {
        // decode exponent
        let flag = reader.read_u8()?;
        let exponent = match flag {
            0x08 => !reader.read_u8()? as i8,
            0x09..=0x13 => (0x13 - flag) as i8,
            0x14 => -(reader.read_u8()? as i8),
            0x15 => return Ok(Decimal::ZERO),
            0x16 => -!(reader.read_u8()? as i8),
            0x17..=0x21 => (flag - 0x17) as i8,
            0x22 => reader.read_u8()? as i8,
            b => {
                return Err(DatabaseError::InvalidValue(format!(
                    "invalid decimal exponent: {b}"
                )))
            }
        };
        // decode mantissa
        let neg = (0x07..0x15).contains(&flag);
        let mut mantissa: i128 = 0;
        let mut mlen = 0i8;
        loop {
            let mut b = reader.read_u8()?;
            if neg {
                b = !b;
            }
            let x = b / 2;
            mantissa = mantissa * 100 + x as i128;
            mlen += 1;
            if b & 1 == 0 {
                break;
            }
        }

        // get scale
        let mut scale = (mlen - exponent) * 2;
        if scale <= 0 {
            // e.g. 1(mantissa) + 2(exponent) (which is 100).
            for _i in 0..-scale {
                mantissa *= 10;
            }
            scale = 0;
        } else if mantissa % 10 == 0 {
            // Remove unnecessary zeros.
            // e.g. 0.01_11_10 should be 0.01_11_1
            mantissa /= 10;
            scale -= 1;
        }

        if neg {
            mantissa = -mantissa;
        }
        Ok(rust_decimal::Decimal::from_i128_with_scale(
            mantissa,
            scale as u32,
        ))
    }

    #[inline]
    pub fn is_true(&self) -> Result<bool, DatabaseError> {
        if self.is_null() {
            return Ok(false);
        }
        if let DataValue::Boolean(is_true) = self {
            Ok(*is_true)
        } else {
            Err(DatabaseError::InvalidType)
        }
    }

    pub fn cast(self, to: &LogicalType) -> Result<DataValue, DatabaseError> {
        let value = match self {
            DataValue::Null => Ok(DataValue::Null),
            DataValue::Boolean(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Boolean => Ok(DataValue::Boolean(value)),
                LogicalType::Tinyint => Ok(DataValue::Int8(value.into())),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value.into())),
                LogicalType::Smallint => Ok(DataValue::Int16(value.into())),
                LogicalType::USmallint => Ok(DataValue::UInt16(value.into())),
                LogicalType::Integer => Ok(DataValue::Int32(value.into())),
                LogicalType::UInteger => Ok(DataValue::UInt32(value.into())),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(value.into())),
                LogicalType::Double => Ok(DataValue::Float64(value.into())),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Float32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(value)),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(value.0.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal =
                        Decimal::from_f32(value.0).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?;
                    Self::decimal_round_f(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Tinyint => {
                    Ok(DataValue::Int8(float_to_int!(value.into_inner(), i8, f32)?))
                }
                LogicalType::Smallint => Ok(DataValue::Int16(float_to_int!(
                    value.into_inner(),
                    i16,
                    f32
                )?)),
                LogicalType::Integer => Ok(DataValue::Int32(float_to_int!(
                    value.into_inner(),
                    i32,
                    f32
                )?)),
                LogicalType::Bigint => Ok(DataValue::Int64(float_to_int!(
                    value.into_inner(),
                    i64,
                    f32
                )?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(float_to_int!(
                    value.into_inner(),
                    u8,
                    f32
                )?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(float_to_int!(
                    value.into_inner(),
                    u16,
                    f32
                )?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(float_to_int!(
                    value.into_inner(),
                    u32,
                    f32
                )?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(float_to_int!(
                    value.into_inner(),
                    u64,
                    f32
                )?)),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Float64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(value.0 as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value)),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal =
                        Decimal::from_f64(value.0).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?;
                    Self::decimal_round_f(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Tinyint => {
                    Ok(DataValue::Int8(float_to_int!(value.into_inner(), i8, f64)?))
                }
                LogicalType::Smallint => Ok(DataValue::Int16(float_to_int!(
                    value.into_inner(),
                    i16,
                    f64
                )?)),
                LogicalType::Integer => Ok(DataValue::Int32(float_to_int!(
                    value.into_inner(),
                    i32,
                    f64
                )?)),
                LogicalType::Bigint => Ok(DataValue::Int64(float_to_int!(
                    value.into_inner(),
                    i64,
                    f64
                )?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(float_to_int!(
                    value.into_inner(),
                    u8,
                    f64
                )?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(float_to_int!(
                    value.into_inner(),
                    u16,
                    f64
                )?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(float_to_int!(
                    value.into_inner(),
                    u32,
                    f64
                )?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(float_to_int!(
                    value.into_inner(),
                    u64,
                    f64
                )?)),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Int8(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(value)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::try_from(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::try_from(value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(u64::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(value.into())),
                LogicalType::Integer => Ok(DataValue::Int32(value.into())),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(value.into())),
                LogicalType::Double => Ok(DataValue::Float64(value.into())),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Int16(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::try_from(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::try_from(value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(u64::try_from(value)?)),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(value)),
                LogicalType::Integer => Ok(DataValue::Int32(value.into())),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(value.into())),
                LogicalType::Double => Ok(DataValue::Float64(value.into())),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Int32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::try_from(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::try_from(value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(u64::try_from(value)?)),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::try_from(value)?)),
                LogicalType::Integer => Ok(DataValue::Int32(value)),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(value as f32))),
                LogicalType::Double => Ok(DataValue::Float64(value.into())),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Int64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::try_from(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::try_from(value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(u64::try_from(value)?)),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::try_from(value)?)),
                LogicalType::Integer => Ok(DataValue::Int32(i32::try_from(value)?)),
                LogicalType::Bigint => Ok(DataValue::Int64(value)),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(value as f32))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(value as f64))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::UInt8(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(value)),
                LogicalType::Smallint => Ok(DataValue::Int16(value.into())),
                LogicalType::USmallint => Ok(DataValue::UInt16(value.into())),
                LogicalType::Integer => Ok(DataValue::Int32(value.into())),
                LogicalType::UInteger => Ok(DataValue::UInt32(value.into())),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(value.into())),
                LogicalType::Double => Ok(DataValue::Float64(value.into())),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::UInt16(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(value)),
                LogicalType::Integer => Ok(DataValue::Int32(value.into())),
                LogicalType::UInteger => Ok(DataValue::UInt32(value.into())),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(value.into())),
                LogicalType::Double => Ok(DataValue::Float64(value.into())),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::UInt32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::try_from(value)?)),
                LogicalType::Integer => Ok(DataValue::Int32(i32::try_from(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(value)),
                LogicalType::Bigint => Ok(DataValue::Int64(value.into())),
                LogicalType::UBigint => Ok(DataValue::UInt64(value.into())),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(value as f32))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(value.into()))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::UInt64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::try_from(value)?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::try_from(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::try_from(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::try_from(value)?)),
                LogicalType::Integer => Ok(DataValue::Int32(i32::try_from(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::try_from(value)?)),
                LogicalType::Bigint => Ok(DataValue::Int64(i64::try_from(value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(value)),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(value as f32))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(value as f64))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Decimal(_, option) => {
                    let mut decimal = Decimal::from(value);
                    Self::decimal_round_i(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                LogicalType::Boolean => numeric_to_boolean!(value, self.logical_type()),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Utf8 { ref value, .. } => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Boolean => Ok(DataValue::Boolean(bool::from_str(value)?)),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::from_str(value)?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::from_str(value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::from_str(value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::from_str(value)?)),
                LogicalType::Integer => Ok(DataValue::Int32(i32::from_str(value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::from_str(value)?)),
                LogicalType::Bigint => Ok(DataValue::Int64(i64::from_str(value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(u64::from_str(value)?)),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(f32::from_str(value)?))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(f64::from_str(value)?))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Date => {
                    let value = NaiveDate::parse_from_str(value, DATE_FMT)
                        .map(|date| date.num_days_from_ce())
                        .unwrap();
                    Ok(DataValue::Date32(value))
                }
                LogicalType::DateTime => {
                    let value = NaiveDateTime::parse_from_str(value, DATE_TIME_FMT)
                        .or_else(|_| {
                            NaiveDate::parse_from_str(value, DATE_FMT)
                                .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
                        })
                        .map(|date_time| date_time.and_utc().timestamp())?;

                    Ok(DataValue::Date64(value))
                }
                LogicalType::Time(precision) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    let fmt = if precision == 0 {
                        TIME_FMT
                    } else {
                        TIME_FMT_WITHOUT_ZONE
                    };
                    let (value, nano) = match precision {
                        0 => (
                            NaiveTime::parse_from_str(value, fmt)
                                .map(|time| time.num_seconds_from_midnight())?,
                            0,
                        ),
                        _ => NaiveTime::parse_from_str(value, fmt)
                            .map(|time| (time.num_seconds_from_midnight(), time.nanosecond()))?,
                    };
                    Ok(DataValue::Time32(
                        Self::pack(value, nano, precision),
                        precision,
                    ))
                }
                LogicalType::TimeStamp(precision, zone) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    let fmt = match (precision, *zone) {
                        (0, false) => DATE_TIME_FMT,
                        (0, true) => TIME_STAMP_FMT_WITHOUT_PRECISION,
                        (3 | 6 | 9, false) => TIME_STAMP_FMT_WITHOUT_ZONE,
                        _ => TIME_STAMP_FMT_WITH_ZONE,
                    };
                    let complete_value = if *zone {
                        match value.contains("+") {
                            false => format!("{}+00:00", value.clone()),
                            true => value.clone(),
                        }
                    } else {
                        value.clone()
                    };
                    if precision == 0 && !*zone {
                        return Ok(DataValue::Time64(
                            NaiveDateTime::parse_from_str(&complete_value, fmt)
                                .map(|date_time| date_time.and_utc().timestamp())?,
                            precision,
                            *zone,
                        ));
                    }
                    let value = DateTime::parse_from_str(&complete_value, fmt);
                    let value = match precision {
                        3 => value.map(|date_time| date_time.timestamp_millis())?,
                        6 => value.map(|date_time| date_time.timestamp_micros())?,
                        9 => {
                            if let Some(value) =
                                value.map(|date_time| date_time.timestamp_nanos_opt())?
                            {
                                value
                            } else {
                                return Err(DatabaseError::CastFail {
                                    from: self.logical_type(),
                                    to: to.clone(),
                                });
                            }
                        }
                        0 => value.map(|date_time| date_time.timestamp())?,
                        _ => unreachable!(),
                    };
                    Ok(DataValue::Time64(value, precision, *zone))
                }
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(Decimal::from_str(value)?)),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Date32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_date(value).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone()
                        })?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_date(value).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone()
                        })?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => Ok(DataValue::Date32(value)),
                LogicalType::DateTime => {
                    let value = NaiveDate::from_num_days_from_ce_opt(value)
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?
                        .and_hms_opt(0, 0, 0)
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?
                        .and_utc()
                        .timestamp();

                    Ok(DataValue::Date64(value))
                }
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Date64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_datetime(value).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone()
                        })?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_datetime(value).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone()
                        })?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => {
                    let value = DateTime::from_timestamp(value, 0)
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?
                        .naive_utc()
                        .date()
                        .num_days_from_ce();

                    Ok(DataValue::Date32(value))
                }
                LogicalType::DateTime => Ok(DataValue::Date64(value)),
                LogicalType::Time(precision) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    let value = DateTime::from_timestamp(value, 0)
                        .map(|date_time| date_time.time().num_seconds_from_midnight())
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?;

                    Ok(DataValue::Time32(Self::pack(value, 0, 0), precision))
                }
                LogicalType::TimeStamp(precision, zone) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    Ok(DataValue::Time64(value, precision, *zone))
                }
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Time32(value, precision) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_time(value, precision).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone()
                        })?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_time(value, precision).ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone()
                        })?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Time(to_precision) => {
                    Ok(DataValue::Time32(value, to_precision.unwrap_or(0)))
                }
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Time64(value, precision, _) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_timestamp(value, precision).ok_or(
                            DatabaseError::CastFail {
                                from: self.logical_type(),
                                to: to.clone()
                            }
                        )?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_timestamp(value, precision).ok_or(
                            DatabaseError::CastFail {
                                from: self.logical_type(),
                                to: to.clone()
                            }
                        )?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => {
                    let value = Self::from_timestamp_precision(value, precision)
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?
                        .naive_utc()
                        .date()
                        .num_days_from_ce();

                    Ok(DataValue::Date32(value))
                }
                LogicalType::DateTime => {
                    let value = Self::from_timestamp_precision(value, precision)
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?
                        .timestamp();
                    Ok(DataValue::Date64(value))
                }
                LogicalType::Time(p) => {
                    let p = p.unwrap_or(0);
                    let (value, nano) = Self::from_timestamp_precision(value, precision)
                        .map(|date_time| {
                            (
                                date_time.time().num_seconds_from_midnight(),
                                date_time.time().nanosecond(),
                            )
                        })
                        .ok_or(DatabaseError::CastFail {
                            from: self.logical_type(),
                            to: to.clone(),
                        })?;
                    Ok(DataValue::Time32(Self::pack(value, nano, p), p))
                }
                LogicalType::TimeStamp(to_precision, zone) => {
                    Ok(DataValue::Time64(value, to_precision.unwrap_or(0), *zone))
                }
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Decimal(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(value.to_f32().ok_or(
                    DatabaseError::CastFail {
                        from: self.logical_type(),
                        to: to.clone(),
                    },
                )?))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(value.to_f64().ok_or(
                    DatabaseError::CastFail {
                        from: self.logical_type(),
                        to: to.clone(),
                    },
                )?))),
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(value)),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                LogicalType::Tinyint => Ok(DataValue::Int8(decimal_to_int!(value, i8))),
                LogicalType::Smallint => Ok(DataValue::Int16(decimal_to_int!(value, i16))),
                LogicalType::Integer => Ok(DataValue::Int32(decimal_to_int!(value, i32))),
                LogicalType::Bigint => Ok(DataValue::Int64(decimal_to_int!(value, i64))),
                LogicalType::UTinyint => Ok(DataValue::UInt8(decimal_to_int!(value, u8))),
                LogicalType::USmallint => Ok(DataValue::UInt16(decimal_to_int!(value, u16))),
                LogicalType::UInteger => Ok(DataValue::UInt32(decimal_to_int!(value, u32))),
                LogicalType::UBigint => Ok(DataValue::UInt64(decimal_to_int!(value, u64))),
                _ => Err(DatabaseError::CastFail {
                    from: self.logical_type(),
                    to: to.clone(),
                }),
            },
            DataValue::Tuple(mut values, is_upper) => match to {
                LogicalType::Tuple(types) => {
                    for (i, value) in values.iter_mut().enumerate() {
                        if types[i] != value.logical_type() {
                            *value = mem::replace(value, DataValue::Null).cast(&types[i])?;
                        }
                    }
                    Ok(DataValue::Tuple(values, is_upper))
                }
                _ => Err(DatabaseError::CastFail {
                    from: LogicalType::Tuple(values.iter().map(DataValue::logical_type).collect()),
                    to: to.clone(),
                }),
            },
        }?;
        value.check_len(to)?;
        Ok(value)
    }

    #[inline]
    pub fn common_prefix_length(&self, target: &DataValue) -> Option<usize> {
        if self.is_null() && target.is_null() {
            return Some(0);
        }
        if self.is_null() || target.is_null() {
            return None;
        }

        if let (DataValue::Utf8 { value: v1, .. }, DataValue::Utf8 { value: v2, .. }) =
            (self, target)
        {
            let min_len = cmp::min(v1.len(), v2.len());

            let mut v1_iter = v1.get(0..min_len).unwrap().chars();
            let mut v2_iter = v2.get(0..min_len).unwrap().chars();

            for i in 0..min_len {
                if v1_iter.next() != v2_iter.next() {
                    return Some(i);
                }
            }

            return Some(min_len);
        }
        Some(0)
    }

    #[inline]
    pub(crate) fn values_to_tuple(mut values: Vec<DataValue>) -> Option<DataValue> {
        if values.len() > 1 {
            Some(DataValue::Tuple(values, false))
        } else {
            values.pop()
        }
    }

    fn decimal_round_i(option: &Option<u8>, decimal: &mut Decimal) {
        if let Some(scale) = option {
            let new_decimal = decimal.trunc_with_scale(*scale as u32);
            let _ = mem::replace(decimal, new_decimal);
        }
    }

    fn decimal_round_f(option: &Option<u8>, decimal: &mut Decimal) {
        if let Some(scale) = option {
            let new_decimal = decimal.round_dp_with_strategy(
                *scale as u32,
                rust_decimal::RoundingStrategy::MidpointAwayFromZero,
            );
            let _ = mem::replace(decimal, new_decimal);
        }
    }

    fn date_format<'a>(v: i32) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        NaiveDate::from_num_days_from_ce_opt(v).map(|date| date.format(DATE_FMT))
    }

    fn date_time_format<'a>(v: i64) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        DateTime::from_timestamp(v, 0).map(|date_time| date_time.format(DATE_TIME_FMT))
    }

    fn time_format<'a>(v: u32, precision: u64) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        let (v, n) = Self::unpack(v, precision);
        NaiveTime::from_num_seconds_from_midnight_opt(v, n)
            .map(|time| time.format(TIME_FMT_WITHOUT_ZONE))
    }

    fn time_stamp_format<'a>(
        v: i64,
        precision: u64,
        _zone: bool,
    ) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        Self::from_timestamp_precision(v, precision)
            .map(|date_time| date_time.format(TIME_STAMP_FMT_WITHOUT_ZONE))
    }

    fn decimal_format(v: &Decimal) -> String {
        v.to_string()
    }

    pub fn timestamp_precision(v: DateTime<Utc>, precision: u64) -> i64 {
        match precision {
            3 => v.timestamp_millis(),
            6 => v.timestamp_micros(),
            9 => v.timestamp_nanos_opt().unwrap_or(0),
            0 => v.timestamp(),
            _ => unreachable!(),
        }
    }

    pub fn from_timestamp_precision(v: i64, precision: u64) -> Option<DateTime<chrono::Utc>> {
        match precision {
            0 => DateTime::from_timestamp(v, 0),
            3 => DateTime::from_timestamp_millis(v),
            6 => DateTime::from_timestamp_micros(v),
            9 => {
                let secs = v.div_euclid(ONE_SEC_TO_NANO as i64);
                let nsecs = v.rem_euclid(ONE_SEC_TO_NANO as i64) as u32;
                DateTime::from_timestamp(secs, nsecs)
            }
            _ => unreachable!(),
        }
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for DataValue {
            fn from(value: $ty) -> Self {
                DataValue::$scalar(value)
            }
        }

        impl From<Option<$ty>> for DataValue {
            fn from(value: Option<$ty>) -> Self {
                if let Some(value) = value {
                    DataValue::$scalar(value)
                } else {
                    DataValue::Null
                }
            }
        }
    };
}

impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);
impl_scalar!(Decimal, Decimal);

impl From<f32> for DataValue {
    fn from(value: f32) -> Self {
        DataValue::Float32(OrderedFloat(value))
    }
}

impl From<Option<f32>> for DataValue {
    fn from(value: Option<f32>) -> Self {
        if let Some(value) = value {
            DataValue::Float32(OrderedFloat(value))
        } else {
            DataValue::Null
        }
    }
}

impl From<f64> for DataValue {
    fn from(value: f64) -> Self {
        DataValue::Float64(OrderedFloat(value))
    }
}

impl From<Option<f64>> for DataValue {
    fn from(value: Option<f64>) -> Self {
        if let Some(value) = value {
            DataValue::Float64(OrderedFloat(value))
        } else {
            DataValue::Null
        }
    }
}

impl From<String> for DataValue {
    fn from(value: String) -> Self {
        DataValue::Utf8 {
            value,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        }
    }
}

impl From<Option<String>> for DataValue {
    fn from(value: Option<String>) -> Self {
        if let Some(value) = value {
            DataValue::Utf8 {
                value,
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            }
        } else {
            DataValue::Null
        }
    }
}

impl From<&NaiveDate> for DataValue {
    fn from(value: &NaiveDate) -> Self {
        DataValue::Date32(value.num_days_from_ce())
    }
}

impl From<Option<&NaiveDate>> for DataValue {
    fn from(value: Option<&NaiveDate>) -> Self {
        if let Some(value) = value {
            DataValue::Date32(value.num_days_from_ce())
        } else {
            DataValue::Null
        }
    }
}

impl From<&NaiveDateTime> for DataValue {
    fn from(value: &NaiveDateTime) -> Self {
        DataValue::Date64(value.and_utc().timestamp())
    }
}

impl From<Option<&NaiveDateTime>> for DataValue {
    fn from(value: Option<&NaiveDateTime>) -> Self {
        if let Some(value) = value {
            DataValue::Date64(value.and_utc().timestamp())
        } else {
            DataValue::Null
        }
    }
}

impl From<&NaiveTime> for DataValue {
    fn from(value: &NaiveTime) -> Self {
        DataValue::Time32(
            Self::pack(value.num_seconds_from_midnight(), value.nanosecond(), 4),
            6,
        )
    }
}

impl From<Option<&NaiveTime>> for DataValue {
    fn from(value: Option<&NaiveTime>) -> Self {
        if let Some(value) = value {
            DataValue::Time32(
                Self::pack(value.num_seconds_from_midnight(), value.nanosecond(), 4),
                0,
            )
        } else {
            DataValue::Null
        }
    }
}

impl TryFrom<&sqlparser::ast::Value> for DataValue {
    type Error = DatabaseError;

    fn try_from(value: &sqlparser::ast::Value) -> Result<Self, Self::Error> {
        Ok(match value {
            sqlparser::ast::Value::Number(n, _) => {
                // use i32 to handle most cases
                if let Ok(v) = n.parse::<i32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<i64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f32>() {
                    v.into()
                } else {
                    return Err(DatabaseError::InvalidValue(n.to_string()));
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::Boolean(b) => (*b).into(),
            sqlparser::ast::Value::Null => Self::Null,
            v => return Err(DatabaseError::UnsupportedStmt(format!("{v:?}"))),
        })
    }
}

macro_rules! format_float_option {
    ($F:expr, $EXPR:expr) => {{
        let formatted_string = format!("{:?}", $EXPR);
        let formatted_result = if let Some(i) = formatted_string.find('.') {
            format!("{:.1$}", $EXPR, formatted_string.len() - i - 1)
        } else {
            format!("{:.1}", $EXPR)
        };

        write!($F, "{}", formatted_result)
    }};
}

impl fmt::Display for DataValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DataValue::Boolean(e) => write!(f, "{e}")?,
            DataValue::Float32(e) => format_float_option!(f, e)?,
            DataValue::Float64(e) => format_float_option!(f, e)?,
            DataValue::Int8(e) => write!(f, "{e}")?,
            DataValue::Int16(e) => write!(f, "{e}")?,
            DataValue::Int32(e) => write!(f, "{e}")?,
            DataValue::Int64(e) => write!(f, "{e}")?,
            DataValue::UInt8(e) => write!(f, "{e}")?,
            DataValue::UInt16(e) => write!(f, "{e}")?,
            DataValue::UInt32(e) => write!(f, "{e}")?,
            DataValue::UInt64(e) => write!(f, "{e}")?,
            DataValue::Utf8 { value: e, .. } => write!(f, "{e}")?,
            DataValue::Null => write!(f, "null")?,
            DataValue::Date32(e) => write!(f, "{}", DataValue::date_format(*e).unwrap())?,
            DataValue::Date64(e) => write!(f, "{}", DataValue::date_time_format(*e).unwrap())?,
            DataValue::Time32(e, precision) => {
                write!(f, "{}", DataValue::time_format(*e, *precision).unwrap())?
            }
            DataValue::Time64(e, precision, zone) => write!(
                f,
                "{}",
                DataValue::time_stamp_format(*e, *precision, *zone).unwrap()
            )?,
            DataValue::Decimal(e) => write!(f, "{}", DataValue::decimal_format(e))?,
            DataValue::Tuple(values, ..) => {
                write!(f, "(")?;
                let len = values.len();

                for (i, value) in values.iter().enumerate() {
                    value.fmt(f)?;
                    if len != i + 1 {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ")")?;
            }
        };
        Ok(())
    }
}

impl fmt::Debug for DataValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DataValue::Boolean(_) => write!(f, "Boolean({self})"),
            DataValue::Float32(_) => write!(f, "Float32({self})"),
            DataValue::Float64(_) => write!(f, "Float64({self})"),
            DataValue::Int8(_) => write!(f, "Int8({self})"),
            DataValue::Int16(_) => write!(f, "Int16({self})"),
            DataValue::Int32(_) => write!(f, "Int32({self})"),
            DataValue::Int64(_) => write!(f, "Int64({self})"),
            DataValue::UInt8(_) => write!(f, "UInt8({self})"),
            DataValue::UInt16(_) => write!(f, "UInt16({self})"),
            DataValue::UInt32(_) => write!(f, "UInt32({self})"),
            DataValue::UInt64(_) => write!(f, "UInt64({self})"),
            DataValue::Utf8 { .. } => write!(f, "Utf8(\"{self}\")"),
            DataValue::Null => write!(f, "null"),
            DataValue::Date32(_) => write!(f, "Date32({self})"),
            DataValue::Date64(_) => write!(f, "Date64({self})"),
            DataValue::Time32(..) => write!(f, "Time32({self})"),
            DataValue::Time64(..) => write!(f, "Time64({self})"),
            DataValue::Decimal(_) => write!(f, "Decimal({self})"),
            DataValue::Tuple(..) => {
                write!(f, "Tuple({self}")?;
                if matches!(self, DataValue::Tuple(_, true)) {
                    write!(f, " [is upper]")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::errors::DatabaseError;
    use crate::storage::table_codec::BumpBytes;
    use crate::types::value::{DataValue, TupleMappingRef, Utf8Type};
    use crate::types::LogicalType;
    use bumpalo::Bump;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;
    use sqlparser::ast::CharLengthUnits;
    use std::io::Cursor;

    #[test]
    fn test_mem_comparable_null() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_i8_0 = BumpBytes::new_in(&arena);
        let mut key_i8_1 = BumpBytes::new_in(&arena);
        let mut key_i8_2 = BumpBytes::new_in(&arena);
        let mut key_i8_3 = BumpBytes::new_in(&arena);

        let value_0 = DataValue::Null;
        let value_1 = DataValue::Int8(i8::MIN);
        let value_2 = DataValue::Int8(-1_i8);
        let value_3 = DataValue::Int8(i8::MAX);

        value_0.memcomparable_encode(&mut key_i8_0)?;
        value_1.memcomparable_encode(&mut key_i8_1)?;
        value_2.memcomparable_encode(&mut key_i8_2)?;
        value_3.memcomparable_encode(&mut key_i8_3)?;

        println!("{key_i8_0:?} < {key_i8_1:?}");
        println!("{key_i8_1:?} < {key_i8_2:?}");
        println!("{key_i8_2:?} < {key_i8_3:?}");
        assert!(key_i8_1 < key_i8_2);
        assert!(key_i8_2 < key_i8_3);
        assert!(key_i8_3 < key_i8_0);

        assert_eq!(
            value_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_0.as_slice()),
                &LogicalType::Tinyint
            )?
        );
        assert_eq!(
            value_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_1.as_slice()),
                &LogicalType::Tinyint
            )?
        );
        assert_eq!(
            value_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_2.as_slice()),
                &LogicalType::Tinyint
            )?
        );
        assert_eq!(
            value_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_3.as_slice()),
                &LogicalType::Tinyint
            )?
        );

        Ok(())
    }

    #[test]
    fn test_mem_comparable_int() -> Result<(), DatabaseError> {
        let arena = Bump::new();

        // ---------- Int8 ----------
        let mut key_i8_0 = BumpBytes::new_in(&arena);
        let mut key_i8_1 = BumpBytes::new_in(&arena);
        let mut key_i8_2 = BumpBytes::new_in(&arena);
        let mut key_i8_3 = BumpBytes::new_in(&arena);

        let v_i8_0 = DataValue::Null;
        let v_i8_1 = DataValue::Int8(i8::MIN);
        let v_i8_2 = DataValue::Int8(-1);
        let v_i8_3 = DataValue::Int8(i8::MAX);

        v_i8_0.memcomparable_encode(&mut key_i8_0)?;
        v_i8_1.memcomparable_encode(&mut key_i8_1)?;
        v_i8_2.memcomparable_encode(&mut key_i8_2)?;
        v_i8_3.memcomparable_encode(&mut key_i8_3)?;

        assert!(key_i8_1 < key_i8_2);
        assert!(key_i8_2 < key_i8_3);
        assert!(key_i8_3 < key_i8_0);

        assert_eq!(
            v_i8_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_0.as_slice()),
                &LogicalType::Tinyint
            )?
        );
        assert_eq!(
            v_i8_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_1.as_slice()),
                &LogicalType::Tinyint
            )?
        );
        assert_eq!(
            v_i8_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_2.as_slice()),
                &LogicalType::Tinyint
            )?
        );
        assert_eq!(
            v_i8_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i8_3.as_slice()),
                &LogicalType::Tinyint
            )?
        );

        // ---------- Int16 ----------
        let mut key_i16_0 = BumpBytes::new_in(&arena);
        let mut key_i16_1 = BumpBytes::new_in(&arena);
        let mut key_i16_2 = BumpBytes::new_in(&arena);
        let mut key_i16_3 = BumpBytes::new_in(&arena);

        let v_i16_0 = DataValue::Null;
        let v_i16_1 = DataValue::Int16(i16::MIN);
        let v_i16_2 = DataValue::Int16(-1);
        let v_i16_3 = DataValue::Int16(i16::MAX);

        v_i16_0.memcomparable_encode(&mut key_i16_0)?;
        v_i16_1.memcomparable_encode(&mut key_i16_1)?;
        v_i16_2.memcomparable_encode(&mut key_i16_2)?;
        v_i16_3.memcomparable_encode(&mut key_i16_3)?;

        assert!(key_i16_1 < key_i16_2);
        assert!(key_i16_2 < key_i16_3);
        assert!(key_i16_3 < key_i16_0);

        assert_eq!(
            v_i16_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i16_0.as_slice()),
                &LogicalType::Smallint
            )?
        );
        assert_eq!(
            v_i16_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i16_1.as_slice()),
                &LogicalType::Smallint
            )?
        );
        assert_eq!(
            v_i16_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i16_2.as_slice()),
                &LogicalType::Smallint
            )?
        );
        assert_eq!(
            v_i16_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i16_3.as_slice()),
                &LogicalType::Smallint
            )?
        );

        // ---------- Int32 ----------
        let mut key_i32_0 = BumpBytes::new_in(&arena);
        let mut key_i32_1 = BumpBytes::new_in(&arena);
        let mut key_i32_2 = BumpBytes::new_in(&arena);
        let mut key_i32_3 = BumpBytes::new_in(&arena);

        let v_i32_0 = DataValue::Null;
        let v_i32_1 = DataValue::Int32(i32::MIN);
        let v_i32_2 = DataValue::Int32(-1);
        let v_i32_3 = DataValue::Int32(i32::MAX);

        v_i32_0.memcomparable_encode(&mut key_i32_0)?;
        v_i32_1.memcomparable_encode(&mut key_i32_1)?;
        v_i32_2.memcomparable_encode(&mut key_i32_2)?;
        v_i32_3.memcomparable_encode(&mut key_i32_3)?;

        assert!(key_i32_1 < key_i32_2);
        assert!(key_i32_2 < key_i32_3);
        assert!(key_i32_3 < key_i32_0);

        assert_eq!(
            v_i32_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i32_0.as_slice()),
                &LogicalType::Integer
            )?
        );
        assert_eq!(
            v_i32_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i32_1.as_slice()),
                &LogicalType::Integer
            )?
        );
        assert_eq!(
            v_i32_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i32_2.as_slice()),
                &LogicalType::Integer
            )?
        );
        assert_eq!(
            v_i32_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i32_3.as_slice()),
                &LogicalType::Integer
            )?
        );

        // ---------- Int64 ----------
        let mut key_i64_0 = BumpBytes::new_in(&arena);
        let mut key_i64_1 = BumpBytes::new_in(&arena);
        let mut key_i64_2 = BumpBytes::new_in(&arena);
        let mut key_i64_3 = BumpBytes::new_in(&arena);

        let v_i64_0 = DataValue::Null;
        let v_i64_1 = DataValue::Int64(i64::MIN);
        let v_i64_2 = DataValue::Int64(-1);
        let v_i64_3 = DataValue::Int64(i64::MAX);

        v_i64_0.memcomparable_encode(&mut key_i64_0)?;
        v_i64_1.memcomparable_encode(&mut key_i64_1)?;
        v_i64_2.memcomparable_encode(&mut key_i64_2)?;
        v_i64_3.memcomparable_encode(&mut key_i64_3)?;

        assert!(key_i64_1 < key_i64_2);
        assert!(key_i64_2 < key_i64_3);
        assert!(key_i64_3 < key_i64_0);

        assert_eq!(
            v_i64_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i64_0.as_slice()),
                &LogicalType::Bigint
            )?
        );
        assert_eq!(
            v_i64_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i64_1.as_slice()),
                &LogicalType::Bigint
            )?
        );
        assert_eq!(
            v_i64_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i64_2.as_slice()),
                &LogicalType::Bigint
            )?
        );
        assert_eq!(
            v_i64_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(key_i64_3.as_slice()),
                &LogicalType::Bigint
            )?
        );

        Ok(())
    }

    #[test]
    fn test_mem_comparable_float() -> Result<(), DatabaseError> {
        let arena = Bump::new();

        // ---------- Float32 ----------
        let mut key_f32_0 = BumpBytes::new_in(&arena);
        let mut key_f32_1 = BumpBytes::new_in(&arena);
        let mut key_f32_2 = BumpBytes::new_in(&arena);
        let mut key_f32_3 = BumpBytes::new_in(&arena);

        let v_f32_0 = DataValue::Null;
        let v_f32_1 = DataValue::Float32(OrderedFloat(f32::MIN));
        let v_f32_2 = DataValue::Float32(OrderedFloat(-1.0));
        let v_f32_3 = DataValue::Float32(OrderedFloat(f32::MAX));

        v_f32_0.memcomparable_encode(&mut key_f32_0)?;
        v_f32_1.memcomparable_encode(&mut key_f32_1)?;
        v_f32_2.memcomparable_encode(&mut key_f32_2)?;
        v_f32_3.memcomparable_encode(&mut key_f32_3)?;

        assert!(key_f32_1 < key_f32_2);
        assert!(key_f32_2 < key_f32_3);
        assert!(key_f32_3 < key_f32_0);

        assert_eq!(
            v_f32_0,
            DataValue::memcomparable_decode(&mut Cursor::new(&key_f32_0[..]), &LogicalType::Float)?
        );
        assert_eq!(
            v_f32_1,
            DataValue::memcomparable_decode(&mut Cursor::new(&key_f32_1[..]), &LogicalType::Float)?
        );
        assert_eq!(
            v_f32_2,
            DataValue::memcomparable_decode(&mut Cursor::new(&key_f32_2[..]), &LogicalType::Float)?
        );
        assert_eq!(
            v_f32_3,
            DataValue::memcomparable_decode(&mut Cursor::new(&key_f32_3[..]), &LogicalType::Float)?
        );

        // ---------- Float64 ----------
        let mut key_f64_0 = BumpBytes::new_in(&arena);
        let mut key_f64_1 = BumpBytes::new_in(&arena);
        let mut key_f64_2 = BumpBytes::new_in(&arena);
        let mut key_f64_3 = BumpBytes::new_in(&arena);

        let v_f64_0 = DataValue::Null;
        let v_f64_1 = DataValue::Float64(OrderedFloat(f64::MIN));
        let v_f64_2 = DataValue::Float64(OrderedFloat(-1.0));
        let v_f64_3 = DataValue::Float64(OrderedFloat(f64::MAX));

        v_f64_0.memcomparable_encode(&mut key_f64_0)?;
        v_f64_1.memcomparable_encode(&mut key_f64_1)?;
        v_f64_2.memcomparable_encode(&mut key_f64_2)?;
        v_f64_3.memcomparable_encode(&mut key_f64_3)?;

        assert!(key_f64_1 < key_f64_2);
        assert!(key_f64_2 < key_f64_3);
        assert!(key_f64_3 < key_f64_0);

        assert_eq!(
            v_f64_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_f64_0[..]),
                &LogicalType::Double
            )?
        );
        assert_eq!(
            v_f64_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_f64_1[..]),
                &LogicalType::Double
            )?
        );
        assert_eq!(
            v_f64_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_f64_2[..]),
                &LogicalType::Double
            )?
        );
        assert_eq!(
            v_f64_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_f64_3[..]),
                &LogicalType::Double
            )?
        );

        Ok(())
    }

    #[test]
    fn test_mem_comparable_decimal() -> Result<(), DatabaseError> {
        let arena = Bump::new();

        let mut key_decimal_0 = BumpBytes::new_in(&arena);
        let mut key_decimal_1 = BumpBytes::new_in(&arena);
        let mut key_decimal_2 = BumpBytes::new_in(&arena);
        let mut key_decimal_3 = BumpBytes::new_in(&arena);

        let v_decimal_0 = DataValue::Null;
        let v_decimal_1 = DataValue::Decimal(Decimal::MIN);
        let v_decimal_2 = DataValue::Decimal(Decimal::new(-1, 0));
        let v_decimal_3 = DataValue::Decimal(Decimal::MAX);

        v_decimal_0.memcomparable_encode(&mut key_decimal_0)?;
        v_decimal_1.memcomparable_encode(&mut key_decimal_1)?;
        v_decimal_2.memcomparable_encode(&mut key_decimal_2)?;
        v_decimal_3.memcomparable_encode(&mut key_decimal_3)?;

        println!("{key_decimal_0:?} < {key_decimal_1:?}");
        println!("{key_decimal_1:?} < {key_decimal_2:?}");
        println!("{key_decimal_2:?} < {key_decimal_3:?}");

        assert!(key_decimal_1 < key_decimal_2);
        assert!(key_decimal_2 < key_decimal_3);
        assert!(key_decimal_3 < key_decimal_0);

        assert_eq!(
            v_decimal_0,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_decimal_0[..]),
                &LogicalType::Decimal(None, None)
            )?
        );
        assert_eq!(
            v_decimal_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_decimal_1[..]),
                &LogicalType::Decimal(None, None)
            )?
        );
        assert_eq!(
            v_decimal_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_decimal_2[..]),
                &LogicalType::Decimal(None, None)
            )?
        );
        assert_eq!(
            v_decimal_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_decimal_3[..]),
                &LogicalType::Decimal(None, None)
            )?
        );

        Ok(())
    }

    #[test]
    fn test_mem_comparable_tuple_lower() -> Result<(), DatabaseError> {
        let arena = Bump::new();

        let mut key_tuple_1 = BumpBytes::new_in(&arena);
        let mut key_tuple_2 = BumpBytes::new_in(&arena);
        let mut key_tuple_3 = BumpBytes::new_in(&arena);

        let v_tuple_1 = DataValue::Tuple(
            vec![DataValue::Null, DataValue::Int8(0), DataValue::Int8(1)],
            false,
        );

        let v_tuple_2 = DataValue::Tuple(
            vec![DataValue::Int8(0), DataValue::Int8(0), DataValue::Int8(1)],
            false,
        );

        let v_tuple_3 = DataValue::Tuple(
            vec![DataValue::Int8(0), DataValue::Int8(0), DataValue::Int8(2)],
            false,
        );

        v_tuple_1.memcomparable_encode(&mut key_tuple_1)?;
        v_tuple_2.memcomparable_encode(&mut key_tuple_2)?;
        v_tuple_3.memcomparable_encode(&mut key_tuple_3)?;

        println!("{key_tuple_1:?} < {key_tuple_2:?}");
        println!("{key_tuple_2:?} < {key_tuple_3:?}");

        assert!(key_tuple_2 < key_tuple_3);
        assert!(key_tuple_3 < key_tuple_1);

        assert_eq!(
            v_tuple_1,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_tuple_1[..]),
                &LogicalType::Tuple(vec![
                    LogicalType::Tinyint,
                    LogicalType::Tinyint,
                    LogicalType::Tinyint,
                ])
            )?
        );
        assert_eq!(
            v_tuple_2,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_tuple_2[..]),
                &LogicalType::Tuple(vec![
                    LogicalType::Tinyint,
                    LogicalType::Tinyint,
                    LogicalType::Tinyint,
                ])
            )?
        );
        assert_eq!(
            v_tuple_3,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_tuple_3[..]),
                &LogicalType::Tuple(vec![
                    LogicalType::Tinyint,
                    LogicalType::Tinyint,
                    LogicalType::Tinyint,
                ])
            )?
        );

        Ok(())
    }

    #[test]
    fn test_mem_comparable_tuple_upper() -> Result<(), DatabaseError> {
        use DataValue::*;

        fn logical_eq(lhs: &DataValue, rhs: &DataValue) -> bool {
            match (lhs, rhs) {
                (Tuple(lv, _), Tuple(rv, _)) => {
                    lv.len() == rv.len() && lv.iter().zip(rv.iter()).all(|(l, r)| logical_eq(l, r))
                }
                _ => lhs == rhs,
            }
        }

        let arena = Bump::new();

        let mut key_tuple_1 = BumpBytes::new_in(&arena);
        let mut key_tuple_2 = BumpBytes::new_in(&arena);
        let mut key_tuple_3 = BumpBytes::new_in(&arena);

        let v_tuple_1 = Tuple(
            vec![Null, Int8(0), Int8(1)],
            true, // upper bound
        );

        let v_tuple_2 = Tuple(vec![Int8(0), Int8(0), Int8(1)], true);

        let v_tuple_3 = Tuple(vec![Int8(0), Int8(0), Int8(2)], true);

        v_tuple_1.memcomparable_encode(&mut key_tuple_1)?;
        v_tuple_2.memcomparable_encode(&mut key_tuple_2)?;
        v_tuple_3.memcomparable_encode(&mut key_tuple_3)?;

        assert!(key_tuple_2 < key_tuple_3);
        assert!(key_tuple_3 < key_tuple_1);

        let ty = LogicalType::Tuple(vec![
            LogicalType::Tinyint,
            LogicalType::Tinyint,
            LogicalType::Tinyint,
        ]);

        let d1 = DataValue::memcomparable_decode(&mut Cursor::new(&key_tuple_1[..]), &ty)?;
        let d2 = DataValue::memcomparable_decode(&mut Cursor::new(&key_tuple_2[..]), &ty)?;
        let d3 = DataValue::memcomparable_decode(&mut Cursor::new(&key_tuple_3[..]), &ty)?;

        assert!(logical_eq(&v_tuple_1, &d1));
        assert!(logical_eq(&v_tuple_2, &d2));
        assert!(logical_eq(&v_tuple_3, &d3));

        Ok(())
    }

    #[test]
    fn test_memcomparable_decode_mapping_orders_values() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_tuple = BumpBytes::new_in(&arena);

        let value = DataValue::Tuple(
            vec![
                DataValue::Int32(1),
                DataValue::Int32(2),
                DataValue::Int32(3),
            ],
            false,
        );
        value.memcomparable_encode(&mut key_tuple)?;

        let ty = LogicalType::Tuple(vec![
            LogicalType::Integer,
            LogicalType::Integer,
            LogicalType::Integer,
        ]);
        let index_to_scan = vec![1, usize::MAX, 0];
        let mapping = TupleMappingRef::new(&index_to_scan, 2);
        let decoded = DataValue::memcomparable_decode_mapping(
            &mut Cursor::new(&key_tuple[..]),
            &ty,
            Some(mapping),
        )?;

        assert_eq!(
            decoded,
            DataValue::Tuple(vec![DataValue::Int32(3), DataValue::Int32(1)], false)
        );

        Ok(())
    }

    #[test]
    fn test_mem_comparable_utf8() -> Result<(), DatabaseError> {
        let arena = Bump::new();

        let mut key_null = BumpBytes::new_in(&arena);
        let mut key_a = BumpBytes::new_in(&arena);
        let mut key_ab = BumpBytes::new_in(&arena);
        let mut key_b = BumpBytes::new_in(&arena);
        let mut key_zh = BumpBytes::new_in(&arena);

        let v_null = DataValue::Null;

        let v_a = DataValue::Utf8 {
            value: "a".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        };

        let v_ab = DataValue::Utf8 {
            value: "ab".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        };

        let v_b = DataValue::Utf8 {
            value: "b".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        };

        let v_zh = DataValue::Utf8 {
            value: "中".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        };

        v_null.memcomparable_encode(&mut key_null)?;
        v_a.memcomparable_encode(&mut key_a)?;
        v_ab.memcomparable_encode(&mut key_ab)?;
        v_b.memcomparable_encode(&mut key_b)?;
        v_zh.memcomparable_encode(&mut key_zh)?;

        // ordering
        assert!(key_a < key_ab);
        assert!(key_ab < key_b);
        assert!(key_b < key_zh);
        assert!(key_zh < key_null);

        // decode check
        assert_eq!(
            v_a,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_a[..]),
                &LogicalType::Varchar(None, CharLengthUnits::Characters)
            )?
        );

        assert_eq!(
            v_ab,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_ab[..]),
                &LogicalType::Varchar(None, CharLengthUnits::Characters)
            )?
        );

        assert_eq!(
            v_b,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_b[..]),
                &LogicalType::Varchar(None, CharLengthUnits::Characters)
            )?
        );

        assert_eq!(
            v_zh,
            DataValue::memcomparable_decode(
                &mut Cursor::new(&key_zh[..]),
                &LogicalType::Varchar(None, CharLengthUnits::Characters)
            )?
        );

        Ok(())
    }
}
