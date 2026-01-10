use crate::errors::DatabaseError;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use bumpalo::collections::Vec;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use kite_sql_serde_macros::ReferenceSerialization;
use ordered_float::OrderedFloat;
use rust_decimal::Decimal;
use sqlparser::ast::CharLengthUnits;
use std::fmt::Debug;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

macro_rules! impl_tuple_value_serializable {
    ($name:ident, $variant:ident, $write_fn:expr, $read_fn:expr) => {
        impl TupleValueSerializable for $name {
            fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError> {
                let DataValue::$variant(v) = value else {
                    unsafe { std::hint::unreachable_unchecked() }
                };
                ($write_fn)(writer, v)?;
                Ok(())
            }

            fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
                Ok(DataValue::$variant(($read_fn)(reader)?))
            }
        }
    };
}

pub trait TupleValueSerializable: Debug {
    fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError>;
    fn filling_value(
        &self,
        reader: &mut Cursor<&[u8]>,
        values: &mut std::vec::Vec<DataValue>,
    ) -> Result<(), DatabaseError> {
        values.push(self.from_raw(reader)?);
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ReferenceSerialization)]
pub enum TupleValueSerializableImpl {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Char {
        len: u32,
        unit: CharLengthUnits,
    },
    Varchar {
        len: Option<u32>,
        unit: CharLengthUnits,
    },
    Date,
    DateTime,
    Time {
        precision: Option<u64>,
    },
    Timestamp {
        precision: Option<u64>,
        zone: bool,
    },
    Decimal,
    SkipFixed(usize),
    SkipVariable,
}

impl TupleValueSerializable for TupleValueSerializableImpl {
    fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        match self {
            TupleValueSerializableImpl::Boolean => BooleanSerializable.to_raw(value, writer),
            TupleValueSerializableImpl::Int8 => Int8Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::Int16 => Int16Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::Int32 => Int32Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::Int64 => Int64Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::UInt8 => UInt8Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::UInt16 => UInt16Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::UInt32 => UInt32Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::UInt64 => UInt64Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::Float32 => Float32Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::Float64 => Float64Serializable.to_raw(value, writer),
            TupleValueSerializableImpl::Char { len, unit } => CharSerializable {
                len: *len,
                unit: *unit,
            }
            .to_raw(value, writer),
            TupleValueSerializableImpl::Varchar { len, unit } => VarcharSerializable {
                len: *len,
                unit: *unit,
            }
            .to_raw(value, writer),
            TupleValueSerializableImpl::Date => DateSerializable.to_raw(value, writer),
            TupleValueSerializableImpl::DateTime => DateTimeSerializable.to_raw(value, writer),
            TupleValueSerializableImpl::Time { precision } => TimeSerializable {
                precision: *precision,
            }
            .to_raw(value, writer),
            TupleValueSerializableImpl::Timestamp { precision, zone } => TimeStampSerializable {
                precision: *precision,
                zone: *zone,
            }
            .to_raw(value, writer),
            TupleValueSerializableImpl::Decimal => DecimalSerializable.to_raw(value, writer),
            TupleValueSerializableImpl::SkipFixed(len) => SkipFixed(*len).to_raw(value, writer),
            TupleValueSerializableImpl::SkipVariable => SkipVariable.to_raw(value, writer),
        }
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        match self {
            TupleValueSerializableImpl::Boolean => BooleanSerializable.from_raw(reader),
            TupleValueSerializableImpl::Int8 => Int8Serializable.from_raw(reader),
            TupleValueSerializableImpl::Int16 => Int16Serializable.from_raw(reader),
            TupleValueSerializableImpl::Int32 => Int32Serializable.from_raw(reader),
            TupleValueSerializableImpl::Int64 => Int64Serializable.from_raw(reader),
            TupleValueSerializableImpl::UInt8 => UInt8Serializable.from_raw(reader),
            TupleValueSerializableImpl::UInt16 => UInt16Serializable.from_raw(reader),
            TupleValueSerializableImpl::UInt32 => UInt32Serializable.from_raw(reader),
            TupleValueSerializableImpl::UInt64 => UInt64Serializable.from_raw(reader),
            TupleValueSerializableImpl::Float32 => Float32Serializable.from_raw(reader),
            TupleValueSerializableImpl::Float64 => Float64Serializable.from_raw(reader),
            TupleValueSerializableImpl::Char { len, unit } => CharSerializable {
                len: *len,
                unit: *unit,
            }
            .from_raw(reader),
            TupleValueSerializableImpl::Varchar { len, unit } => VarcharSerializable {
                len: *len,
                unit: *unit,
            }
            .from_raw(reader),
            TupleValueSerializableImpl::Date => DateSerializable.from_raw(reader),
            TupleValueSerializableImpl::DateTime => DateTimeSerializable.from_raw(reader),
            TupleValueSerializableImpl::Time { precision } => TimeSerializable {
                precision: *precision,
            }
            .from_raw(reader),
            TupleValueSerializableImpl::Timestamp { precision, zone } => TimeStampSerializable {
                precision: *precision,
                zone: *zone,
            }
            .from_raw(reader),
            TupleValueSerializableImpl::Decimal => DecimalSerializable.from_raw(reader),
            TupleValueSerializableImpl::SkipFixed(len) => SkipFixed(*len).from_raw(reader),
            TupleValueSerializableImpl::SkipVariable => SkipVariable.from_raw(reader),
        }
    }

    fn filling_value(
        &self,
        reader: &mut Cursor<&[u8]>,
        values: &mut std::vec::Vec<DataValue>,
    ) -> Result<(), DatabaseError> {
        match self {
            TupleValueSerializableImpl::Boolean => {
                BooleanSerializable.filling_value(reader, values)
            }
            TupleValueSerializableImpl::Int8 => Int8Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::Int16 => Int16Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::Int32 => Int32Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::Int64 => Int64Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::UInt8 => UInt8Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::UInt16 => UInt16Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::UInt32 => UInt32Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::UInt64 => UInt64Serializable.filling_value(reader, values),
            TupleValueSerializableImpl::Float32 => {
                Float32Serializable.filling_value(reader, values)
            }
            TupleValueSerializableImpl::Float64 => {
                Float64Serializable.filling_value(reader, values)
            }
            TupleValueSerializableImpl::Char { len, unit } => CharSerializable {
                len: *len,
                unit: *unit,
            }
            .filling_value(reader, values),
            TupleValueSerializableImpl::Varchar { len, unit } => VarcharSerializable {
                len: *len,
                unit: *unit,
            }
            .filling_value(reader, values),
            TupleValueSerializableImpl::Date => DateSerializable.filling_value(reader, values),
            TupleValueSerializableImpl::DateTime => {
                DateTimeSerializable.filling_value(reader, values)
            }
            TupleValueSerializableImpl::Time { precision } => TimeSerializable {
                precision: *precision,
            }
            .filling_value(reader, values),
            TupleValueSerializableImpl::Timestamp { precision, zone } => TimeStampSerializable {
                precision: *precision,
                zone: *zone,
            }
            .filling_value(reader, values),
            TupleValueSerializableImpl::Decimal => {
                DecimalSerializable.filling_value(reader, values)
            }
            TupleValueSerializableImpl::SkipFixed(len) => {
                SkipFixed(*len).filling_value(reader, values)
            }
            TupleValueSerializableImpl::SkipVariable => SkipVariable.filling_value(reader, values),
        }
    }
}

#[derive(Debug)]
struct BooleanSerializable;

#[derive(Debug)]
struct Int8Serializable;
#[derive(Debug)]
struct Int16Serializable;
#[derive(Debug)]
struct Int32Serializable;
#[derive(Debug)]
struct Int64Serializable;

#[derive(Debug)]
struct UInt8Serializable;
#[derive(Debug)]
struct UInt16Serializable;
#[derive(Debug)]
struct UInt32Serializable;
#[derive(Debug)]
struct UInt64Serializable;

#[derive(Debug)]
struct Float32Serializable;
#[derive(Debug)]
struct Float64Serializable;

#[derive(Debug)]
struct CharSerializable {
    len: u32,
    unit: CharLengthUnits,
}
#[derive(Debug)]
struct VarcharSerializable {
    len: Option<u32>,
    unit: CharLengthUnits,
}

#[derive(Debug)]
struct DateSerializable;
#[derive(Debug)]
struct DateTimeSerializable;

#[derive(Debug)]
struct TimeSerializable {
    precision: Option<u64>,
}
#[derive(Debug)]
struct TimeStampSerializable {
    precision: Option<u64>,
    zone: bool,
}

#[derive(Debug)]
struct DecimalSerializable;

#[derive(Debug)]
struct SkipFixed(usize);
#[derive(Debug)]
struct SkipVariable;

// Int
impl_tuple_value_serializable!(
    Int8Serializable,
    Int8,
    |writer: &mut Vec<u8>, &value| writer.write_i8(value),
    |reader: &mut Cursor<&[u8]>| reader.read_i8()
);
impl_tuple_value_serializable!(
    Int16Serializable,
    Int16,
    |writer: &mut Vec<u8>, &value| writer.write_i16::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_i16::<LittleEndian>()
);
impl_tuple_value_serializable!(
    Int32Serializable,
    Int32,
    |writer: &mut Vec<u8>, &value| writer.write_i32::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_i32::<LittleEndian>()
);
impl_tuple_value_serializable!(
    Int64Serializable,
    Int64,
    |writer: &mut Vec<u8>, &value| writer.write_i64::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_i64::<LittleEndian>()
);

// Uint
impl_tuple_value_serializable!(
    UInt8Serializable,
    UInt8,
    |writer: &mut Vec<u8>, &value| writer.write_u8(value),
    |reader: &mut Cursor<&[u8]>| reader.read_u8()
);
impl_tuple_value_serializable!(
    UInt16Serializable,
    UInt16,
    |writer: &mut Vec<u8>, &value| writer.write_u16::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_u16::<LittleEndian>()
);
impl_tuple_value_serializable!(
    UInt32Serializable,
    UInt32,
    |writer: &mut Vec<u8>, &value| writer.write_u32::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_u32::<LittleEndian>()
);
impl_tuple_value_serializable!(
    UInt64Serializable,
    UInt64,
    |writer: &mut Vec<u8>, &value| writer.write_u64::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_u64::<LittleEndian>()
);

// Float
impl_tuple_value_serializable!(
    Float32Serializable,
    Float32,
    |writer: &mut Vec<u8>, value: &OrderedFloat::<f32>| writer
        .write_f32::<LittleEndian>(value.into_inner()),
    |reader: &mut Cursor<&[u8]>| reader.read_f32::<LittleEndian>().map(OrderedFloat::<f32>)
);
impl_tuple_value_serializable!(
    Float64Serializable,
    Float64,
    |writer: &mut Vec<u8>, value: &OrderedFloat::<f64>| writer
        .write_f64::<LittleEndian>(value.into_inner()),
    |reader: &mut Cursor<&[u8]>| reader.read_f64::<LittleEndian>().map(OrderedFloat::<f64>)
);

impl_tuple_value_serializable!(
    BooleanSerializable,
    Boolean,
    |writer: &mut Vec<u8>, &value| writer.write_u8(value as u8),
    |reader: &mut Cursor<&[u8]>| reader.read_u8().map(|v| v != 0)
);

impl TupleValueSerializable for CharSerializable {
    fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        let DataValue::Utf8 {
            value,
            unit,
            ty: Utf8Type::Fixed(len),
        } = value
        else {
            unsafe { std::hint::unreachable_unchecked() }
        };
        match unit {
            CharLengthUnits::Characters => {
                let chars_len = *len as usize;
                let v = format!("{value:chars_len$}");
                let bytes = v.as_bytes();

                writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                writer.write_all(bytes)?;
            }
            CharLengthUnits::Octets => {
                let octets_len = *len as usize;
                let bytes = value.as_bytes();
                debug_assert!(octets_len >= bytes.len());

                writer.write_all(bytes)?;
                for _ in 0..octets_len - bytes.len() {
                    writer.write_u8(b' ')?;
                }
            }
        }
        Ok(())
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        // https://dev.mysql.com/doc/refman/8.0/en/char.html#:~:text=If%20a%20given%20value%20is%20stored%20into%20the%20CHAR(4)%20and%20VARCHAR(4)%20columns%2C%20the%20values%20retrieved%20from%20the%20columns%20are%20not%20always%20the%20same%20because%20trailing%20spaces%20are%20removed%20from%20CHAR%20columns%20upon%20retrieval.%20The%20following%20example%20illustrates%20this%20difference%3A
        let len = match self.unit {
            CharLengthUnits::Characters => reader.read_u32::<LittleEndian>()?,
            CharLengthUnits::Octets => self.len,
        } as usize;
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes)?;
        let last_non_zero_index = match bytes.iter().rposition(|&x| x != b' ') {
            Some(index) => index + 1,
            None => 0,
        };
        bytes.truncate(last_non_zero_index);

        Ok(DataValue::Utf8 {
            value: String::from_utf8(bytes)?,
            ty: Utf8Type::Fixed(self.len),
            unit: self.unit,
        })
    }
}

impl TupleValueSerializable for VarcharSerializable {
    fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        let DataValue::Utf8 {
            value,
            ty: Utf8Type::Variable(_),
            ..
        } = value
        else {
            unsafe { std::hint::unreachable_unchecked() }
        };
        let bytes = value.as_bytes();

        writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
        writer.write_all(bytes)?;
        Ok(())
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        let len = reader.read_u32::<LittleEndian>()? as usize;
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes)?;

        Ok(DataValue::Utf8 {
            value: String::from_utf8(bytes)?,
            ty: Utf8Type::Variable(self.len),
            unit: self.unit,
        })
    }
}

impl_tuple_value_serializable!(
    DateSerializable,
    Date32,
    |writer: &mut Vec<u8>, &value| writer.write_i32::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_i32::<LittleEndian>()
);
impl_tuple_value_serializable!(
    DateTimeSerializable,
    Date64,
    |writer: &mut Vec<u8>, &value| writer.write_i64::<LittleEndian>(value),
    |reader: &mut Cursor<&[u8]>| reader.read_i64::<LittleEndian>()
);

impl TupleValueSerializable for TimeSerializable {
    fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        let DataValue::Time32(v, ..) = value else {
            unsafe { std::hint::unreachable_unchecked() }
        };
        writer.write_u32::<LittleEndian>(*v)?;
        Ok(())
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        let precision = self.precision.unwrap_or_default();
        Ok(DataValue::Time32(
            reader.read_u32::<LittleEndian>()?,
            precision,
        ))
    }
}

impl TupleValueSerializable for TimeStampSerializable {
    fn to_raw(&self, value: &DataValue, writer: &mut Vec<u8>) -> Result<(), DatabaseError> {
        let DataValue::Time64(v, ..) = value else {
            unsafe { std::hint::unreachable_unchecked() }
        };
        writer.write_i64::<LittleEndian>(*v)?;
        Ok(())
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        let precision = self.precision.unwrap_or_default();
        Ok(DataValue::Time64(
            reader.read_i64::<LittleEndian>()?,
            precision,
            self.zone,
        ))
    }
}

impl_tuple_value_serializable!(
    DecimalSerializable,
    Decimal,
    |writer: &mut Vec<u8>, &value: &Decimal| writer.write_all(&value.serialize()),
    |reader: &mut Cursor<&[u8]>| {
        let mut bytes = [0u8; 16];
        reader.read_exact(&mut bytes)?;
        Result::<_, DatabaseError>::Ok(Decimal::deserialize(bytes))
    }
);

impl TupleValueSerializable for SkipFixed {
    fn to_raw(&self, _: &DataValue, _: &mut Vec<u8>) -> Result<(), DatabaseError> {
        unreachable!();
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        reader.seek(SeekFrom::Current(self.0 as i64))?;
        Ok(DataValue::Null)
    }

    fn filling_value(
        &self,
        reader: &mut Cursor<&[u8]>,
        _: &mut std::vec::Vec<DataValue>,
    ) -> Result<(), DatabaseError> {
        let _ = self.from_raw(reader)?;
        Ok(())
    }
}

impl TupleValueSerializable for SkipVariable {
    fn to_raw(&self, _: &DataValue, _: &mut Vec<u8>) -> Result<(), DatabaseError> {
        unreachable!();
    }

    fn from_raw(&self, reader: &mut Cursor<&[u8]>) -> Result<DataValue, DatabaseError> {
        let len = reader.read_u32::<LittleEndian>()? as usize;
        reader.seek(SeekFrom::Current(len as i64))?;
        Ok(DataValue::Null)
    }

    fn filling_value(
        &self,
        reader: &mut Cursor<&[u8]>,
        _: &mut std::vec::Vec<DataValue>,
    ) -> Result<(), DatabaseError> {
        let _ = self.from_raw(reader)?;
        Ok(())
    }
}

impl LogicalType {
    pub fn skip_serializable(&self) -> TupleValueSerializableImpl {
        self.raw_len()
            .map(TupleValueSerializableImpl::SkipFixed)
            .unwrap_or(TupleValueSerializableImpl::SkipVariable)
    }

    pub fn serializable(&self) -> TupleValueSerializableImpl {
        match self {
            LogicalType::Boolean => TupleValueSerializableImpl::Boolean,
            LogicalType::Tinyint => TupleValueSerializableImpl::Int8,
            LogicalType::UTinyint => TupleValueSerializableImpl::UInt8,
            LogicalType::Smallint => TupleValueSerializableImpl::Int16,
            LogicalType::USmallint => TupleValueSerializableImpl::UInt16,
            LogicalType::Integer => TupleValueSerializableImpl::Int32,
            LogicalType::UInteger => TupleValueSerializableImpl::UInt32,
            LogicalType::Bigint => TupleValueSerializableImpl::Int64,
            LogicalType::UBigint => TupleValueSerializableImpl::UInt64,
            LogicalType::Float => TupleValueSerializableImpl::Float32,
            LogicalType::Double => TupleValueSerializableImpl::Float64,
            LogicalType::Char(len, unit) => TupleValueSerializableImpl::Char {
                len: *len,
                unit: *unit,
            },
            LogicalType::Varchar(len, unit) => TupleValueSerializableImpl::Varchar {
                len: *len,
                unit: *unit,
            },
            LogicalType::Date => TupleValueSerializableImpl::Date,
            LogicalType::DateTime => TupleValueSerializableImpl::DateTime,
            LogicalType::Time(precision) => TupleValueSerializableImpl::Time {
                precision: *precision,
            },
            LogicalType::TimeStamp(precision, zone) => TupleValueSerializableImpl::Timestamp {
                precision: *precision,
                zone: *zone,
            },
            LogicalType::Decimal(_, _) => TupleValueSerializableImpl::Decimal,
            LogicalType::SqlNull | LogicalType::Tuple(_) => unreachable!(),
        }
    }
}
