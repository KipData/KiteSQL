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
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::value::Utf8Type;
use crate::types::CharLengthUnits;
use ordered_float::OrderedFloat;
#[cfg(feature = "decimal")]
use rust_decimal::Decimal;
use std::io::{Read, Write};

const TAG_NULL: u8 = 0;
const TAG_BOOLEAN: u8 = 1;
const TAG_FLOAT32: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_INT8: u8 = 4;
const TAG_INT16: u8 = 5;
const TAG_INT32: u8 = 6;
const TAG_INT64: u8 = 7;
const TAG_UINT8: u8 = 8;
const TAG_UINT16: u8 = 9;
const TAG_UINT32: u8 = 10;
const TAG_UINT64: u8 = 11;
const TAG_UTF8: u8 = 12;
const TAG_DATE32: u8 = 13;
const TAG_DATE64: u8 = 14;
const TAG_TIME32: u8 = 15;
const TAG_TIME64: u8 = 16;
const TAG_DECIMAL: u8 = 17;
const TAG_TUPLE: u8 = 18;

impl ReferenceSerialization for Utf8Type {
    fn encode<W: Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        match self {
            Utf8Type::Variable(len) => {
                0u8.encode(writer, is_direct, reference_tables, arena)?;
                len.encode(writer, is_direct, reference_tables, arena)
            }
            Utf8Type::Fixed(len) => {
                1u8.encode(writer, is_direct, reference_tables, arena)?;
                len.encode(writer, is_direct, reference_tables, arena)
            }
        }
    }

    fn decode<T: Transaction, R: Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        match u8::decode(reader, drive, reference_tables, arena)? {
            0 => Ok(Utf8Type::Variable(Option::<u32>::decode(
                reader,
                drive,
                reference_tables,
                arena,
            )?)),
            1 => Ok(Utf8Type::Fixed(u32::decode(
                reader,
                drive,
                reference_tables,
                arena,
            )?)),
            tag => Err(DatabaseError::InvalidValue(format!(
                "invalid utf8 type tag: {tag}"
            ))),
        }
    }
}

impl DataValue {
    pub(crate) fn encode_reference_value<W: Write>(
        &self,
        writer: &mut W,
    ) -> Result<(), DatabaseError> {
        match self {
            DataValue::Null => write_u8(writer, TAG_NULL),
            DataValue::Boolean(value) => {
                write_u8(writer, TAG_BOOLEAN)?;
                write_bool(writer, *value)
            }
            DataValue::Float32(value) => {
                write_u8(writer, TAG_FLOAT32)?;
                write_f32(writer, value.0)
            }
            DataValue::Float64(value) => {
                write_u8(writer, TAG_FLOAT64)?;
                write_f64(writer, value.0)
            }
            DataValue::Int8(value) => {
                write_u8(writer, TAG_INT8)?;
                write_i8(writer, *value)
            }
            DataValue::Int16(value) => {
                write_u8(writer, TAG_INT16)?;
                write_i16(writer, *value)
            }
            DataValue::Int32(value) => {
                write_u8(writer, TAG_INT32)?;
                write_i32(writer, *value)
            }
            DataValue::Int64(value) => {
                write_u8(writer, TAG_INT64)?;
                write_i64(writer, *value)
            }
            DataValue::UInt8(value) => {
                write_u8(writer, TAG_UINT8)?;
                write_u8(writer, *value)
            }
            DataValue::UInt16(value) => {
                write_u8(writer, TAG_UINT16)?;
                write_u16(writer, *value)
            }
            DataValue::UInt32(value) => {
                write_u8(writer, TAG_UINT32)?;
                write_u32(writer, *value)
            }
            DataValue::UInt64(value) => {
                write_u8(writer, TAG_UINT64)?;
                write_u64(writer, *value)
            }
            DataValue::Utf8 { value, ty, unit } => {
                write_u8(writer, TAG_UTF8)?;
                write_string(writer, value)?;
                write_utf8_type(writer, ty)?;
                write_char_length_units(writer, *unit)
            }
            DataValue::Date32(value) => {
                write_u8(writer, TAG_DATE32)?;
                write_i32(writer, *value)
            }
            DataValue::Date64(value) => {
                write_u8(writer, TAG_DATE64)?;
                write_i64(writer, *value)
            }
            DataValue::Time32(value, precision) => {
                write_u8(writer, TAG_TIME32)?;
                write_u32(writer, *value)?;
                write_u64(writer, *precision)
            }
            DataValue::Time64(value, precision, with_tz) => {
                write_u8(writer, TAG_TIME64)?;
                write_i64(writer, *value)?;
                write_u64(writer, *precision)?;
                write_bool(writer, *with_tz)
            }
            #[cfg(feature = "decimal")]
            DataValue::Decimal(value) => {
                write_u8(writer, TAG_DECIMAL)?;
                writer.write_all(&value.serialize())?;
                Ok(())
            }
            DataValue::Tuple(values, is_upper) => {
                write_u8(writer, TAG_TUPLE)?;
                write_len(writer, values.len())?;
                for value in values {
                    value.encode_reference_value(writer)?;
                }
                write_bool(writer, *is_upper)
            }
        }
    }

    pub(crate) fn decode_reference_value<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        match read_u8(reader)? {
            TAG_NULL => Ok(DataValue::Null),
            TAG_BOOLEAN => Ok(DataValue::Boolean(read_bool(reader)?)),
            TAG_FLOAT32 => Ok(DataValue::Float32(OrderedFloat(read_f32(reader)?))),
            TAG_FLOAT64 => Ok(DataValue::Float64(OrderedFloat(read_f64(reader)?))),
            TAG_INT8 => Ok(DataValue::Int8(read_i8(reader)?)),
            TAG_INT16 => Ok(DataValue::Int16(read_i16(reader)?)),
            TAG_INT32 => Ok(DataValue::Int32(read_i32(reader)?)),
            TAG_INT64 => Ok(DataValue::Int64(read_i64(reader)?)),
            TAG_UINT8 => Ok(DataValue::UInt8(read_u8(reader)?)),
            TAG_UINT16 => Ok(DataValue::UInt16(read_u16(reader)?)),
            TAG_UINT32 => Ok(DataValue::UInt32(read_u32(reader)?)),
            TAG_UINT64 => Ok(DataValue::UInt64(read_u64(reader)?)),
            TAG_UTF8 => Ok(DataValue::Utf8 {
                value: read_string(reader)?,
                ty: read_utf8_type(reader)?,
                unit: read_char_length_units(reader)?,
            }),
            TAG_DATE32 => Ok(DataValue::Date32(read_i32(reader)?)),
            TAG_DATE64 => Ok(DataValue::Date64(read_i64(reader)?)),
            TAG_TIME32 => Ok(DataValue::Time32(read_u32(reader)?, read_u64(reader)?)),
            TAG_TIME64 => Ok(DataValue::Time64(
                read_i64(reader)?,
                read_u64(reader)?,
                read_bool(reader)?,
            )),
            #[cfg(feature = "decimal")]
            TAG_DECIMAL => {
                let mut bytes = [0; 16];
                reader.read_exact(&mut bytes)?;
                Ok(DataValue::Decimal(Decimal::deserialize(bytes)))
            }
            #[cfg(not(feature = "decimal"))]
            TAG_DECIMAL => {
                let mut bytes = [0; 16];
                reader.read_exact(&mut bytes)?;
                Err(DatabaseError::UnsupportedStmt(
                    "DECIMAL requires the `decimal` feature".to_string(),
                ))
            }
            TAG_TUPLE => {
                let len = read_len(reader)?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(DataValue::decode_reference_value(reader)?);
                }
                Ok(DataValue::Tuple(values, read_bool(reader)?))
            }
            tag => Err(DatabaseError::InvalidValue(format!(
                "invalid data value tag: {tag}"
            ))),
        }
    }
}

impl ReferenceSerialization for DataValue {
    fn encode<W: Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        _: bool,
        _: &mut ReferenceTables,
        _: &A,
    ) -> Result<(), DatabaseError> {
        self.encode_reference_value(writer)
    }

    fn decode<T: Transaction, R: Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        _: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        _: &ReferenceTables,
        _: &mut A,
    ) -> Result<Self, DatabaseError> {
        DataValue::decode_reference_value(reader)
    }
}

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<(), DatabaseError> {
    writer.write_all(&[value])?;
    Ok(())
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, DatabaseError> {
    let mut bytes = [0];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn write_bool<W: Write>(writer: &mut W, value: bool) -> Result<(), DatabaseError> {
    write_u8(writer, u8::from(value))
}

fn read_bool<R: Read>(reader: &mut R) -> Result<bool, DatabaseError> {
    match read_u8(reader)? {
        0 => Ok(false),
        1 => Ok(true),
        value => Err(DatabaseError::InvalidValue(format!(
            "invalid bool value: {value}"
        ))),
    }
}

fn write_i8<W: Write>(writer: &mut W, value: i8) -> Result<(), DatabaseError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_i8<R: Read>(reader: &mut R) -> Result<i8, DatabaseError> {
    Ok(i8::from_le_bytes([read_u8(reader)?]))
}

macro_rules! implement_raw_num {
    ($write_name:ident, $read_name:ident, $ty:ty) => {
        fn $write_name<W: Write>(writer: &mut W, value: $ty) -> Result<(), DatabaseError> {
            writer.write_all(&value.to_le_bytes())?;
            Ok(())
        }

        fn $read_name<R: Read>(reader: &mut R) -> Result<$ty, DatabaseError> {
            let mut bytes = [0; std::mem::size_of::<$ty>()];
            reader.read_exact(&mut bytes)?;
            Ok(<$ty>::from_le_bytes(bytes))
        }
    };
}

implement_raw_num!(write_i16, read_i16, i16);
implement_raw_num!(write_i32, read_i32, i32);
implement_raw_num!(write_i64, read_i64, i64);
implement_raw_num!(write_u16, read_u16, u16);
implement_raw_num!(write_u32, read_u32, u32);
implement_raw_num!(write_u64, read_u64, u64);
implement_raw_num!(write_f32, read_f32, f32);
implement_raw_num!(write_f64, read_f64, f64);

fn write_len<W: Write>(writer: &mut W, len: usize) -> Result<(), DatabaseError> {
    write_u32(writer, len.try_into()?)
}

fn read_len<R: Read>(reader: &mut R) -> Result<usize, DatabaseError> {
    Ok(read_u32(reader)? as usize)
}

fn write_string<W: Write>(writer: &mut W, value: &str) -> Result<(), DatabaseError> {
    write_len(writer, value.len())?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, DatabaseError> {
    let len = read_len(reader)?;
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes)?;
    Ok(String::from_utf8(bytes)?)
}

fn write_utf8_type<W: Write>(writer: &mut W, value: &Utf8Type) -> Result<(), DatabaseError> {
    match value {
        Utf8Type::Variable(len) => {
            write_u8(writer, 0)?;
            match len {
                None => write_u8(writer, 0),
                Some(len) => {
                    write_u8(writer, 1)?;
                    write_u32(writer, *len)
                }
            }
        }
        Utf8Type::Fixed(len) => {
            write_u8(writer, 1)?;
            write_u32(writer, *len)
        }
    }
}

fn read_utf8_type<R: Read>(reader: &mut R) -> Result<Utf8Type, DatabaseError> {
    match read_u8(reader)? {
        0 => match read_u8(reader)? {
            0 => Ok(Utf8Type::Variable(None)),
            1 => Ok(Utf8Type::Variable(Some(read_u32(reader)?))),
            tag => Err(DatabaseError::InvalidValue(format!(
                "invalid option tag: {tag}"
            ))),
        },
        1 => Ok(Utf8Type::Fixed(read_u32(reader)?)),
        tag => Err(DatabaseError::InvalidValue(format!(
            "invalid utf8 type tag: {tag}"
        ))),
    }
}

fn write_char_length_units<W: Write>(
    writer: &mut W,
    value: CharLengthUnits,
) -> Result<(), DatabaseError> {
    write_u8(
        writer,
        match value {
            CharLengthUnits::Characters => 0,
            CharLengthUnits::Octets => 1,
        },
    )
}

fn read_char_length_units<R: Read>(reader: &mut R) -> Result<CharLengthUnits, DatabaseError> {
    match read_u8(reader)? {
        0 => Ok(CharLengthUnits::Characters),
        1 => Ok(CharLengthUnits::Octets),
        tag => Err(DatabaseError::InvalidValue(format!(
            "invalid char length units tag: {tag}"
        ))),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::CharLengthUnits;
    use ordered_float::OrderedFloat;
    #[cfg(feature = "decimal")]
    use rust_decimal::Decimal;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let sources = vec![
            DataValue::Null,
            DataValue::Boolean(true),
            DataValue::Float32(OrderedFloat(32.5)),
            DataValue::Float64(OrderedFloat(64.5)),
            DataValue::Int8(-8),
            DataValue::Int16(-16),
            DataValue::Int32(32),
            DataValue::Int64(-64),
            DataValue::UInt8(8),
            DataValue::UInt16(16),
            DataValue::UInt32(32),
            DataValue::UInt64(64),
            DataValue::Utf8 {
                value: "hello".to_string(),
                ty: Utf8Type::Variable(Some(16)),
                unit: CharLengthUnits::Characters,
            },
            DataValue::Utf8 {
                value: "octets".to_string(),
                ty: Utf8Type::Fixed(8),
                unit: CharLengthUnits::Octets,
            },
            DataValue::Date32(12),
            DataValue::Date64(34),
            DataValue::Time32(56, 3),
            DataValue::Time64(78, 6, true),
            #[cfg(feature = "decimal")]
            DataValue::Decimal(Decimal::new(12345, 2)),
            DataValue::Tuple(vec![DataValue::Null, DataValue::Int32(42)], false),
        ];

        let mut reference_tables = ReferenceTables::new();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let mut arena = crate::planner::TableArena::default();

        for source in &sources {
            source.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        }

        cursor.seek(SeekFrom::Start(0))?;

        for source in sources {
            let decoded = DataValue::decode::<RocksTransaction, _, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut arena,
            )?;
            assert_eq!(source, decoded);
        }

        Ok(())
    }
}
