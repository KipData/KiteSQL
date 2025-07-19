use super::LogicalType;
use crate::errors::DatabaseError;
use crate::storage::table_codec::{BumpBytes, BOUND_MAX_TAG, BOUND_MIN_TAG};
use crate::types::LogicalType::{Date, TimeStamp};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use sqlparser::ast::CharLengthUnits;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::str::FromStr;
use std::sync::LazyLock;
use std::{cmp, fmt, mem};

pub static NULL_VALUE: LazyLock<DataValue> = LazyLock::new(|| DataValue::Null);

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
    Time(u32, u64, bool),
    Time64(i64, u64, bool),
    Decimal(Decimal),
    /// (values, is_upper)
    Tuple(Vec<DataValue>, bool),
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
            (Time(v1, ..), Time(v2, ..)) => v1.eq(v2),
            (Time(..), _) => false,
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
            (Time(v1, ..), Time(v2, ..)) => v1.partial_cmp(v2),
            (Time(..), _) => None,
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
            Time(v, ..) => v.hash(state),
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
    ($value:expr) => {
        match $value {
            0 => Ok(DataValue::Boolean(false)),
            1 => Ok(DataValue::Boolean(true)),
            _ => Err(DatabaseError::CastFail),
        }
    };
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
        if let DataValue::Time(val, ..) = self {
            NaiveTime::from_num_seconds_from_midnight_opt(*val, 0)
        } else {
            None
        }
    }

    #[inline]
    pub(crate) fn check_string_len(string: &str, len: usize, unit: CharLengthUnits) -> bool {
        match unit {
            CharLengthUnits::Characters => string.chars().count() > len,
            CharLengthUnits::Octets => string.as_bytes().len() > len,
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
        assert!(b <= 1_000_000_000);
        assert!(a <= 86_400);
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
        let b = combined & 2_u32.pow(p) - 1;
        (b, scaled_a * (1000000000 / 10_u32.pow(precision as u32)))
    }

    fn format_date(value: i32) -> Option<String> {
        Self::date_format(value).map(|fmt| format!("{}", fmt))
    }

    fn format_datetime(value: i64) -> Option<String> {
        Self::date_time_format(value).map(|fmt| format!("{}", fmt))
    }

    fn format_time(value: u32, presicion: u64) -> Option<String> {
        Self::time_format(value, presicion, false).map(|fmt| format!("{}", fmt))
    }

    fn format_timestamp(value: i64, precision: u64) -> Option<String> {
        Self::time_stamp_format(value, precision, false).map(|fmt| format!("{}", fmt))
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
            LogicalType::Time(precision, zone) => match precision {
                Some(i) => DataValue::Time(UNIX_TIME.num_seconds_from_midnight(), *i, *zone),
                None => DataValue::Time(UNIX_TIME.num_seconds_from_midnight(), 0, *zone),
            },
            TimeStamp(precision, zone) => match precision {
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
    pub fn to_raw<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        match self {
            DataValue::Null => (),
            DataValue::Boolean(v) => {
                writer.write_u8(*v as u8)?;
                return Ok(());
            }
            DataValue::Float32(v) => {
                writer.write_f32::<LittleEndian>(v.0)?;
                return Ok(());
            }
            DataValue::Float64(v) => {
                writer.write_f64::<LittleEndian>(v.0)?;
                return Ok(());
            }
            DataValue::Int8(v) => {
                writer.write_i8(*v)?;
                return Ok(());
            }
            DataValue::Int16(v) => {
                writer.write_i16::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Int32(v) => {
                writer.write_i32::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Int64(v) => {
                writer.write_i64::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::UInt8(v) => {
                writer.write_u8(*v)?;
                return Ok(());
            }
            DataValue::UInt16(v) => {
                writer.write_u16::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::UInt32(v) => {
                writer.write_u32::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::UInt64(v) => {
                writer.write_u64::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Utf8 { value: v, ty, unit } => match ty {
                Utf8Type::Variable(_) => {
                    let bytes = v.as_bytes();

                    writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                    writer.write_all(bytes)?;
                    return Ok(());
                }
                Utf8Type::Fixed(len) => match unit {
                    CharLengthUnits::Characters => {
                        let chars_len = *len as usize;
                        let v = format!("{:len$}", v, len = chars_len);
                        let bytes = v.as_bytes();

                        writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                        writer.write_all(bytes)?;
                        return Ok(());
                    }
                    CharLengthUnits::Octets => {
                        let octets_len = *len as usize;
                        let bytes = v.as_bytes();
                        debug_assert!(octets_len >= bytes.len());

                        writer.write_all(bytes)?;
                        for _ in 0..octets_len - bytes.len() {
                            writer.write_u8(b' ')?;
                        }
                        return Ok(());
                    }
                },
            },
            DataValue::Date32(v) => {
                writer.write_i32::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Date64(v) => {
                writer.write_i64::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Time(v, ..) => {
                writer.write_u32::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Time64(v, ..) => {
                writer.write_i64::<LittleEndian>(*v)?;
                return Ok(());
            }
            DataValue::Decimal(v) => {
                writer.write_all(&v.serialize())?;
                return Ok(());
            }
            DataValue::Tuple(..) => unreachable!(),
        }
        Ok(())
    }

    #[inline]
    pub fn from_raw<R: Read + Seek>(
        reader: &mut R,
        ty: &LogicalType,
        is_projection: bool,
    ) -> Result<Option<Self>, DatabaseError> {
        let value = match ty {
            LogicalType::SqlNull => {
                if !is_projection {
                    return Ok(None);
                }
                DataValue::Null
            }
            LogicalType::Boolean => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(1))?;
                    return Ok(None);
                }
                DataValue::Boolean(reader.read_u8()? != 0)
            }
            LogicalType::Tinyint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(1))?;
                    return Ok(None);
                }
                DataValue::Int8(reader.read_i8()?)
            }
            LogicalType::UTinyint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(1))?;
                    return Ok(None);
                }
                DataValue::UInt8(reader.read_u8()?)
            }
            LogicalType::Smallint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(2))?;
                    return Ok(None);
                }
                DataValue::Int16(reader.read_i16::<LittleEndian>()?)
            }
            LogicalType::USmallint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(2))?;
                    return Ok(None);
                }
                DataValue::UInt16(reader.read_u16::<LittleEndian>()?)
            }
            LogicalType::Integer => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Int32(reader.read_i32::<LittleEndian>()?)
            }
            LogicalType::UInteger => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::UInt32(reader.read_u32::<LittleEndian>()?)
            }
            LogicalType::Bigint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Int64(reader.read_i64::<LittleEndian>()?)
            }
            LogicalType::UBigint => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::UInt64(reader.read_u64::<LittleEndian>()?)
            }
            LogicalType::Float => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Float32(OrderedFloat(reader.read_f32::<LittleEndian>()?))
            }
            LogicalType::Double => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Float64(OrderedFloat(reader.read_f64::<LittleEndian>()?))
            }
            LogicalType::Char(ty_len, unit) => {
                // https://dev.mysql.com/doc/refman/8.0/en/char.html#:~:text=If%20a%20given%20value%20is%20stored%20into%20the%20CHAR(4)%20and%20VARCHAR(4)%20columns%2C%20the%20values%20retrieved%20from%20the%20columns%20are%20not%20always%20the%20same%20because%20trailing%20spaces%20are%20removed%20from%20CHAR%20columns%20upon%20retrieval.%20The%20following%20example%20illustrates%20this%20difference%3A
                let len = match unit {
                    CharLengthUnits::Characters => reader.read_u32::<LittleEndian>()?,
                    CharLengthUnits::Octets => *ty_len,
                } as usize;
                if !is_projection {
                    reader.seek(SeekFrom::Current(len as i64))?;
                    return Ok(None);
                }
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;
                let last_non_zero_index = match bytes.iter().rposition(|&x| x != b' ') {
                    Some(index) => index + 1,
                    None => 0,
                };
                bytes.truncate(last_non_zero_index);

                DataValue::Utf8 {
                    value: String::from_utf8(bytes)?,
                    ty: Utf8Type::Fixed(*ty_len),
                    unit: *unit,
                }
            }
            LogicalType::Varchar(ty_len, unit) => {
                let len = reader.read_u32::<LittleEndian>()? as usize;
                if !is_projection {
                    reader.seek(SeekFrom::Current(len as i64))?;
                    return Ok(None);
                }
                let mut bytes = vec![0; len];
                reader.read_exact(&mut bytes)?;

                DataValue::Utf8 {
                    value: String::from_utf8(bytes)?,
                    ty: Utf8Type::Variable(*ty_len),
                    unit: *unit,
                }
            }
            LogicalType::Date => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Date32(reader.read_i32::<LittleEndian>()?)
            }
            LogicalType::DateTime => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Date64(reader.read_i64::<LittleEndian>()?)
            }
            LogicalType::Time(precision, zone) => {
                let precision = match precision {
                    Some(precision) => *precision,
                    None => 0,
                };
                if !is_projection {
                    reader.seek(SeekFrom::Current(4))?;
                    return Ok(None);
                }
                DataValue::Time(reader.read_u32::<LittleEndian>()?, precision, *zone)
            }
            TimeStamp(precision, zone) => {
                let precision = match precision {
                    Some(precision) => *precision,
                    None => 0,
                };
                if !is_projection {
                    reader.seek(SeekFrom::Current(8))?;
                    return Ok(None);
                }
                DataValue::Time64(reader.read_i64::<LittleEndian>()?, precision, *zone)
            }
            LogicalType::Decimal(_, _) => {
                if !is_projection {
                    reader.seek(SeekFrom::Current(16))?;
                    return Ok(None);
                }
                let mut bytes = [0u8; 16];
                reader.read_exact(&mut bytes)?;

                DataValue::Decimal(Decimal::deserialize(bytes))
            }
            LogicalType::Tuple(_) => unreachable!(),
        };
        Ok(Some(value))
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
            DataValue::Time(..) => LogicalType::Time(None, false),
            DataValue::Time64(..) => TimeStamp(None, false),
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
    fn encode_bytes(b: &mut BumpBytes, data: &[u8]) {
        let d_len = data.len();
        let realloc_size = (d_len / ENCODE_GROUP_SIZE + 1) * (ENCODE_GROUP_SIZE + 1);
        Self::realloc_bytes(b, realloc_size);

        let mut idx = 0;
        while idx <= d_len {
            let remain = d_len - idx;
            let pad_count: usize;

            if remain >= ENCODE_GROUP_SIZE {
                b.extend_from_slice(&data[idx..idx + ENCODE_GROUP_SIZE]);
                pad_count = 0;
            } else {
                pad_count = ENCODE_GROUP_SIZE - remain;
                b.extend_from_slice(&data[idx..]);
                b.extend_from_slice(&vec![0; pad_count]);
            }

            b.push(ENCODE_MARKER - pad_count as u8);
            idx += ENCODE_GROUP_SIZE;
        }
    }

    #[inline]
    fn realloc_bytes(b: &mut BumpBytes, size: usize) {
        let len = b.len();

        if size > len {
            b.reserve(size - len);
            b.resize(size, 0);
        }
    }

    #[inline]
    pub fn memcomparable_encode(&self, b: &mut BumpBytes) -> Result<(), DatabaseError> {
        match self {
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
            DataValue::UInt32(v) | DataValue::Time(v, ..) => encode_u!(b, v),
            DataValue::UInt64(v) => encode_u!(b, v),
            DataValue::Utf8 { value: v, .. } => Self::encode_bytes(b, v.as_bytes()),
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
            DataValue::Null => (),
            DataValue::Decimal(v) => Self::serialize_decimal(*v, b)?,
            DataValue::Tuple(values, is_upper) => {
                let last = values.len() - 1;

                for (i, v) in values.iter().enumerate() {
                    v.memcomparable_encode(b)?;
                    if (v.is_null() || i == last) && *is_upper {
                        b.push(BOUND_MAX_TAG);
                    } else {
                        b.push(BOUND_MIN_TAG);
                    }
                }
            }
        }

        Ok(())
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
        while mantissa % 10 == 0 && mantissa != 0 {
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
                _ => Err(DatabaseError::CastFail),
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
                    let mut decimal = Decimal::from_f32(value.0).ok_or(DatabaseError::CastFail)?;
                    Self::decimal_round_f(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                _ => Err(DatabaseError::CastFail),
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
                    let mut decimal = Decimal::from_f64(value.0).ok_or(DatabaseError::CastFail)?;
                    Self::decimal_round_f(option, &mut decimal);

                    Ok(DataValue::Decimal(decimal))
                }
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
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
                LogicalType::Boolean => numeric_to_boolean!(value),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Utf8 { value, .. } => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Boolean => Ok(DataValue::Boolean(bool::from_str(&value)?)),
                LogicalType::Tinyint => Ok(DataValue::Int8(i8::from_str(&value)?)),
                LogicalType::UTinyint => Ok(DataValue::UInt8(u8::from_str(&value)?)),
                LogicalType::Smallint => Ok(DataValue::Int16(i16::from_str(&value)?)),
                LogicalType::USmallint => Ok(DataValue::UInt16(u16::from_str(&value)?)),
                LogicalType::Integer => Ok(DataValue::Int32(i32::from_str(&value)?)),
                LogicalType::UInteger => Ok(DataValue::UInt32(u32::from_str(&value)?)),
                LogicalType::Bigint => Ok(DataValue::Int64(i64::from_str(&value)?)),
                LogicalType::UBigint => Ok(DataValue::UInt64(u64::from_str(&value)?)),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(f32::from_str(&value)?))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(f64::from_str(&value)?))),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                Date => {
                    let value = NaiveDate::parse_from_str(&value, DATE_FMT)
                        .map(|date| date.num_days_from_ce())?;
                    Ok(DataValue::Date32(value))
                }
                LogicalType::DateTime => {
                    let value = NaiveDateTime::parse_from_str(&value, DATE_TIME_FMT)
                        .or_else(|_| {
                            NaiveDate::parse_from_str(&value, DATE_FMT)
                                .map(|date| date.and_hms_opt(0, 0, 0).unwrap())
                        })
                        .map(|date_time| date_time.and_utc().timestamp())?;

                    Ok(DataValue::Date64(value))
                }
                LogicalType::Time(precision, zone) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    let fmt = match (precision, *zone) {
                        (0, false) => TIME_FMT,
                        (0, true) => TIME_FMT_WITHOUT_PRECISION,
                        (1..5, false) => TIME_FMT_WITHOUT_ZONE,
                        _ => TIME_FMT_WITH_ZONE,
                    };
                    let complete_value = if *zone {
                        match value.contains("+") {
                            false => format!("{}+00:00", value.clone()),
                            true => value.clone(),
                        }
                    } else {
                        value.clone()
                    };
                    let (value, nano) = match precision {
                        0 => (
                            NaiveTime::parse_from_str(&complete_value, fmt)
                                .map(|time| time.num_seconds_from_midnight())?,
                            0,
                        ),
                        _ => NaiveTime::parse_from_str(&complete_value, fmt)
                            .map(|time| (time.num_seconds_from_midnight(), time.nanosecond()))?,
                    };
                    Ok(DataValue::Time(
                        Self::pack(value, nano, precision),
                        precision,
                        *zone,
                    ))
                }
                TimeStamp(precision, zone) => {
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
                                return Err(DatabaseError::CastFail);
                            }
                        }
                        0 => value.map(|date_time| date_time.timestamp())?,
                        _ => unreachable!(),
                    };
                    Ok(DataValue::Time64(value, precision, *zone))
                }
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(Decimal::from_str(&value)?)),
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Date32(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_date(value).ok_or(DatabaseError::CastFail)?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_date(value).ok_or(DatabaseError::CastFail)?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => Ok(DataValue::Date32(value)),
                LogicalType::DateTime => {
                    let value = NaiveDate::from_num_days_from_ce_opt(value)
                        .ok_or(DatabaseError::CastFail)?
                        .and_hms_opt(0, 0, 0)
                        .ok_or(DatabaseError::CastFail)?
                        .and_utc()
                        .timestamp();

                    Ok(DataValue::Date64(value))
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Date64(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_datetime(value).ok_or(DatabaseError::CastFail)?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_datetime(value).ok_or(DatabaseError::CastFail)?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                LogicalType::Date => {
                    let value = DateTime::from_timestamp(value, 0)
                        .ok_or(DatabaseError::CastFail)?
                        .naive_utc()
                        .date()
                        .num_days_from_ce();

                    Ok(DataValue::Date32(value))
                }
                LogicalType::DateTime => Ok(DataValue::Date64(value)),
                LogicalType::Time(precision, zone) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    let value = DateTime::from_timestamp(value, 0)
                        .map(|date_time| date_time.time().num_seconds_from_midnight())
                        .ok_or(DatabaseError::CastFail)?;

                    Ok(DataValue::Time(Self::pack(value, 0, 0), precision, *zone))
                }
                TimeStamp(precision, zone) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    Ok(DataValue::Time64(value, precision, *zone))
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Time(value, precision, _) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_time(value, precision).ok_or(DatabaseError::CastFail)?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_time(value, precision).ok_or(DatabaseError::CastFail)?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Time64(value, precision, _) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(
                        Self::format_timestamp(value, precision).ok_or(DatabaseError::CastFail)?,
                        Some(len),
                        Utf8Type::Fixed(*len),
                        *unit
                    )
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(
                        Self::format_timestamp(value, precision).ok_or(DatabaseError::CastFail)?,
                        len,
                        Utf8Type::Variable(*len),
                        *unit
                    )
                }
                Date => {
                    let value = match precision {
                        0 => DateTime::from_timestamp(value, 0),
                        3 => DateTime::from_timestamp_millis(value),
                        6 => DateTime::from_timestamp_micros(value),
                        9 => {
                            let secs = value.div_euclid(1_000_000_000);
                            let nsecs = value.rem_euclid(1_000_000_000) as u32;
                            DateTime::from_timestamp(secs, nsecs)
                        }
                        _ => unreachable!(),
                    }
                    .ok_or(DatabaseError::CastFail)?
                    .naive_utc()
                    .date()
                    .num_days_from_ce();

                    Ok(DataValue::Date32(value))
                }
                LogicalType::DateTime => {
                    let value = match precision {
                        0 => DateTime::from_timestamp(value, 0),
                        3 => DateTime::from_timestamp_millis(value),
                        6 => DateTime::from_timestamp_micros(value),
                        9 => {
                            let secs = value.div_euclid(1_000_000_000);
                            let nsecs = value.rem_euclid(1_000_000_000) as u32;
                            DateTime::from_timestamp(secs, nsecs)
                        }
                        _ => unreachable!(),
                    }
                    .ok_or(DatabaseError::CastFail)?
                    .timestamp();
                    Ok(DataValue::Date64(value))
                }
                LogicalType::Time(p, zone) => {
                    let p = match p {
                        Some(p) => *p,
                        None => 0,
                    };
                    let (value, nano) = match precision {
                        0 => DateTime::from_timestamp(value, 0),
                        3 => DateTime::from_timestamp_millis(value),
                        6 => DateTime::from_timestamp_micros(value),
                        9 => {
                            let secs = value.div_euclid(1_000_000_000);
                            let nsecs = value.rem_euclid(1_000_000_000) as u32;
                            DateTime::from_timestamp(secs, nsecs)
                        }
                        _ => unreachable!(),
                    }
                    .map(|date_time| {
                        (
                            date_time.time().num_seconds_from_midnight(),
                            date_time.time().nanosecond(),
                        )
                    })
                    .ok_or(DatabaseError::CastFail)?;
                    Ok(DataValue::Time(Self::pack(value, nano, p), p, *zone))
                }
                TimeStamp(precision, zone) => {
                    let precision = match precision {
                        Some(precision) => *precision,
                        None => 0,
                    };
                    Ok(DataValue::Time64(value, precision, *zone))
                }
                _ => Err(DatabaseError::CastFail),
            },
            DataValue::Decimal(value) => match to {
                LogicalType::SqlNull => Ok(DataValue::Null),
                LogicalType::Float => Ok(DataValue::Float32(OrderedFloat(
                    value.to_f32().ok_or(DatabaseError::CastFail)?,
                ))),
                LogicalType::Double => Ok(DataValue::Float64(OrderedFloat(
                    value.to_f64().ok_or(DatabaseError::CastFail)?,
                ))),
                LogicalType::Decimal(_, _) => Ok(DataValue::Decimal(value)),
                LogicalType::Char(len, unit) => {
                    varchar_cast!(value, Some(len), Utf8Type::Fixed(*len), *unit)
                }
                LogicalType::Varchar(len, unit) => {
                    varchar_cast!(value, len, Utf8Type::Variable(*len), *unit)
                }
                _ => Err(DatabaseError::CastFail),
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
                _ => Err(DatabaseError::CastFail),
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

    fn time_format<'a>(
        v: u32,
        precision: u64,
        _zone: bool,
    ) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        let (v, n) = Self::unpack(v, precision);
        NaiveTime::from_num_seconds_from_midnight_opt(v, n)
            .map(|time| time.format(TIME_FMT_WITHOUT_ZONE))
    }

    fn time_stamp_format<'a>(
        v: i64,
        precision: u64,
        _zone: bool,
    ) -> Option<DelayedFormat<StrftimeItems<'a>>> {
        match precision {
            0 => DateTime::from_timestamp(v, 0)
                .map(|date_time| date_time.format(TIME_STAMP_FMT_WITHOUT_ZONE)),
            3 => DateTime::from_timestamp_millis(v)
                .map(|date_time| date_time.format(TIME_STAMP_FMT_WITHOUT_ZONE)),
            6 => DateTime::from_timestamp_micros(v)
                .map(|date_time| date_time.format(TIME_STAMP_FMT_WITHOUT_ZONE)),
            9 => {
                let secs = v.div_euclid(1_000_000_000);
                let nsecs = v.rem_euclid(1_000_000_000) as u32;
                DateTime::from_timestamp(secs, nsecs)
                    .map(|date_time| date_time.format(TIME_STAMP_FMT_WITHOUT_ZONE))
            }
            _ => unreachable!(),
        }
    }

    fn decimal_format(v: &Decimal) -> String {
        v.to_string()
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
        DataValue::Time(
            Self::pack(value.num_seconds_from_midnight(), value.nanosecond(), 4),
            6,
            false,
        )
    }
}

impl From<Option<&NaiveTime>> for DataValue {
    fn from(value: Option<&NaiveTime>) -> Self {
        if let Some(value) = value {
            DataValue::Time(
                Self::pack(value.num_seconds_from_midnight(), value.nanosecond(), 4),
                0,
                false,
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
            v => return Err(DatabaseError::UnsupportedStmt(format!("{:?}", v))),
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
            DataValue::Boolean(e) => write!(f, "{}", e)?,
            DataValue::Float32(e) => format_float_option!(f, e)?,
            DataValue::Float64(e) => format_float_option!(f, e)?,
            DataValue::Int8(e) => write!(f, "{}", e)?,
            DataValue::Int16(e) => write!(f, "{}", e)?,
            DataValue::Int32(e) => write!(f, "{}", e)?,
            DataValue::Int64(e) => write!(f, "{}", e)?,
            DataValue::UInt8(e) => write!(f, "{}", e)?,
            DataValue::UInt16(e) => write!(f, "{}", e)?,
            DataValue::UInt32(e) => write!(f, "{}", e)?,
            DataValue::UInt64(e) => write!(f, "{}", e)?,
            DataValue::Utf8 { value: e, .. } => write!(f, "{}", e)?,
            DataValue::Null => write!(f, "null")?,
            DataValue::Date32(e) => write!(f, "{}", DataValue::date_format(*e).unwrap())?,
            DataValue::Date64(e) => write!(f, "{}", DataValue::date_time_format(*e).unwrap())?,
            DataValue::Time(e, precision, zone) => write!(
                f,
                "{}",
                DataValue::time_format(*e, *precision, *zone).unwrap()
            )?,
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
            DataValue::Boolean(_) => write!(f, "Boolean({})", self),
            DataValue::Float32(_) => write!(f, "Float32({})", self),
            DataValue::Float64(_) => write!(f, "Float64({})", self),
            DataValue::Int8(_) => write!(f, "Int8({})", self),
            DataValue::Int16(_) => write!(f, "Int16({})", self),
            DataValue::Int32(_) => write!(f, "Int32({})", self),
            DataValue::Int64(_) => write!(f, "Int64({})", self),
            DataValue::UInt8(_) => write!(f, "UInt8({})", self),
            DataValue::UInt16(_) => write!(f, "UInt16({})", self),
            DataValue::UInt32(_) => write!(f, "UInt32({})", self),
            DataValue::UInt64(_) => write!(f, "UInt64({})", self),
            DataValue::Utf8 { .. } => write!(f, "Utf8(\"{}\")", self),
            DataValue::Null => write!(f, "null"),
            DataValue::Date32(_) => write!(f, "Date32({})", self),
            DataValue::Date64(_) => write!(f, "Date64({})", self),
            DataValue::Time(..) => write!(f, "Time({})", self),
            DataValue::Time64(..) => write!(f, "Time64({})", self),
            DataValue::Decimal(_) => write!(f, "Decimal({})", self),
            DataValue::Tuple(..) => {
                write!(f, "Tuple({}", self)?;
                if matches!(self, DataValue::Tuple(_, true)) {
                    write!(f, " [is upper]")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::errors::DatabaseError;
    use crate::storage::table_codec::BumpBytes;
    use crate::types::value::DataValue;
    use bumpalo::Bump;
    use ordered_float::OrderedFloat;
    use rust_decimal::Decimal;

    #[test]
    fn test_mem_comparable_null() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_i8_0 = BumpBytes::new_in(&arena);
        let mut key_i8_1 = BumpBytes::new_in(&arena);
        let mut key_i8_2 = BumpBytes::new_in(&arena);
        let mut key_i8_3 = BumpBytes::new_in(&arena);

        DataValue::Null.memcomparable_encode(&mut key_i8_0)?;
        DataValue::Int8(i8::MIN).memcomparable_encode(&mut key_i8_1)?;
        DataValue::Int8(-1_i8).memcomparable_encode(&mut key_i8_2)?;
        DataValue::Int8(i8::MAX).memcomparable_encode(&mut key_i8_3)?;

        println!("{:?} < {:?}", key_i8_0, key_i8_1);
        println!("{:?} < {:?}", key_i8_1, key_i8_2);
        println!("{:?} < {:?}", key_i8_2, key_i8_3);
        assert!(key_i8_0 < key_i8_1);
        assert!(key_i8_1 < key_i8_2);
        assert!(key_i8_2 < key_i8_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_int() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_i8_1 = BumpBytes::new_in(&arena);
        let mut key_i8_2 = BumpBytes::new_in(&arena);
        let mut key_i8_3 = BumpBytes::new_in(&arena);

        DataValue::Int8(i8::MIN).memcomparable_encode(&mut key_i8_1)?;
        DataValue::Int8(-1_i8).memcomparable_encode(&mut key_i8_2)?;
        DataValue::Int8(i8::MAX).memcomparable_encode(&mut key_i8_3)?;

        println!("{:?} < {:?}", key_i8_1, key_i8_2);
        println!("{:?} < {:?}", key_i8_2, key_i8_3);
        assert!(key_i8_1 < key_i8_2);
        assert!(key_i8_2 < key_i8_3);

        let mut key_i16_1 = BumpBytes::new_in(&arena);
        let mut key_i16_2 = BumpBytes::new_in(&arena);
        let mut key_i16_3 = BumpBytes::new_in(&arena);

        DataValue::Int16(i16::MIN).memcomparable_encode(&mut key_i16_1)?;
        DataValue::Int16(-1_i16).memcomparable_encode(&mut key_i16_2)?;
        DataValue::Int16(i16::MAX).memcomparable_encode(&mut key_i16_3)?;

        println!("{:?} < {:?}", key_i16_1, key_i16_2);
        println!("{:?} < {:?}", key_i16_2, key_i16_3);
        assert!(key_i16_1 < key_i16_2);
        assert!(key_i16_2 < key_i16_3);

        let mut key_i32_1 = BumpBytes::new_in(&arena);
        let mut key_i32_2 = BumpBytes::new_in(&arena);
        let mut key_i32_3 = BumpBytes::new_in(&arena);

        DataValue::Int32(i32::MIN).memcomparable_encode(&mut key_i32_1)?;
        DataValue::Int32(-1_i32).memcomparable_encode(&mut key_i32_2)?;
        DataValue::Int32(i32::MAX).memcomparable_encode(&mut key_i32_3)?;

        println!("{:?} < {:?}", key_i32_1, key_i32_2);
        println!("{:?} < {:?}", key_i32_2, key_i32_3);
        assert!(key_i32_1 < key_i32_2);
        assert!(key_i32_2 < key_i32_3);

        let mut key_i64_1 = BumpBytes::new_in(&arena);
        let mut key_i64_2 = BumpBytes::new_in(&arena);
        let mut key_i64_3 = BumpBytes::new_in(&arena);

        DataValue::Int64(i64::MIN).memcomparable_encode(&mut key_i64_1)?;
        DataValue::Int64(-1_i64).memcomparable_encode(&mut key_i64_2)?;
        DataValue::Int64(i64::MAX).memcomparable_encode(&mut key_i64_3)?;

        println!("{:?} < {:?}", key_i64_1, key_i64_2);
        println!("{:?} < {:?}", key_i64_2, key_i64_3);
        assert!(key_i64_1 < key_i64_2);
        assert!(key_i64_2 < key_i64_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_float() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_f32_1 = BumpBytes::new_in(&arena);
        let mut key_f32_2 = BumpBytes::new_in(&arena);
        let mut key_f32_3 = BumpBytes::new_in(&arena);

        DataValue::Float32(OrderedFloat(f32::MIN)).memcomparable_encode(&mut key_f32_1)?;
        DataValue::Float32(OrderedFloat(-1_f32)).memcomparable_encode(&mut key_f32_2)?;
        DataValue::Float32(OrderedFloat(f32::MAX)).memcomparable_encode(&mut key_f32_3)?;

        println!("{:?} < {:?}", key_f32_1, key_f32_2);
        println!("{:?} < {:?}", key_f32_2, key_f32_3);
        assert!(key_f32_1 < key_f32_2);
        assert!(key_f32_2 < key_f32_3);

        let mut key_f64_1 = BumpBytes::new_in(&arena);
        let mut key_f64_2 = BumpBytes::new_in(&arena);
        let mut key_f64_3 = BumpBytes::new_in(&arena);

        DataValue::Float64(OrderedFloat(f64::MIN)).memcomparable_encode(&mut key_f64_1)?;
        DataValue::Float64(OrderedFloat(-1_f64)).memcomparable_encode(&mut key_f64_2)?;
        DataValue::Float64(OrderedFloat(f64::MAX)).memcomparable_encode(&mut key_f64_3)?;

        println!("{:?} < {:?}", key_f64_1, key_f64_2);
        println!("{:?} < {:?}", key_f64_2, key_f64_3);
        assert!(key_f64_1 < key_f64_2);
        assert!(key_f64_2 < key_f64_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_decimal() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_deciaml_1 = BumpBytes::new_in(&arena);
        let mut key_deciaml_2 = BumpBytes::new_in(&arena);
        let mut key_deciaml_3 = BumpBytes::new_in(&arena);

        DataValue::Decimal(Decimal::MIN).memcomparable_encode(&mut key_deciaml_1)?;
        DataValue::Decimal(Decimal::new(-1, 0)).memcomparable_encode(&mut key_deciaml_2)?;
        DataValue::Decimal(Decimal::MAX).memcomparable_encode(&mut key_deciaml_3)?;

        println!("{:?} < {:?}", key_deciaml_1, key_deciaml_2);
        println!("{:?} < {:?}", key_deciaml_2, key_deciaml_3);
        assert!(key_deciaml_1 < key_deciaml_2);
        assert!(key_deciaml_2 < key_deciaml_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_tuple_lower() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_tuple_1 = BumpBytes::new_in(&arena);
        let mut key_tuple_2 = BumpBytes::new_in(&arena);
        let mut key_tuple_3 = BumpBytes::new_in(&arena);

        DataValue::Tuple(
            vec![DataValue::Null, DataValue::Int8(0), DataValue::Int8(1)],
            false,
        )
        .memcomparable_encode(&mut key_tuple_1)?;
        DataValue::Tuple(
            vec![DataValue::Int8(0), DataValue::Int8(0), DataValue::Int8(1)],
            false,
        )
        .memcomparable_encode(&mut key_tuple_2)?;
        DataValue::Tuple(
            vec![DataValue::Int8(0), DataValue::Int8(0), DataValue::Int8(2)],
            false,
        )
        .memcomparable_encode(&mut key_tuple_3)?;

        println!("{:?} < {:?}", key_tuple_1, key_tuple_2);
        println!("{:?} < {:?}", key_tuple_2, key_tuple_3);
        assert!(key_tuple_1 < key_tuple_2);
        assert!(key_tuple_2 < key_tuple_3);

        Ok(())
    }

    #[test]
    fn test_mem_comparable_tuple_upper() -> Result<(), DatabaseError> {
        let arena = Bump::new();
        let mut key_tuple_1 = BumpBytes::new_in(&arena);
        let mut key_tuple_2 = BumpBytes::new_in(&arena);
        let mut key_tuple_3 = BumpBytes::new_in(&arena);

        DataValue::Tuple(
            vec![DataValue::Null, DataValue::Int8(0), DataValue::Int8(1)],
            true,
        )
        .memcomparable_encode(&mut key_tuple_1)?;
        DataValue::Tuple(
            vec![DataValue::Int8(0), DataValue::Int8(0), DataValue::Int8(1)],
            true,
        )
        .memcomparable_encode(&mut key_tuple_2)?;
        DataValue::Tuple(
            vec![DataValue::Int8(0), DataValue::Int8(0), DataValue::Int8(2)],
            true,
        )
        .memcomparable_encode(&mut key_tuple_3)?;

        println!("{:?} < {:?}", key_tuple_2, key_tuple_3);
        println!("{:?} < {:?}", key_tuple_3, key_tuple_1);
        assert!(key_tuple_2 < key_tuple_3);
        assert!(key_tuple_3 < key_tuple_1);

        Ok(())
    }
}
