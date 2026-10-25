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

use super::SpillCodec;
use crate::errors::DatabaseError;
use crate::planner::operator::sort::SortField;
use crate::planner::{ExecMetaArena, MetaArena, ScalarQueryRef};
use crate::types::tuple::{Tuple, TupleImpl};
use crate::types::value::DataValue;
use std::io::{Read, Write};
use std::mem::size_of;

pub(crate) struct SortRow {
    pub(crate) sort_values: Vec<DataValue>,
    pub(crate) tuple: TupleImpl,
    pub(crate) outer_values: Vec<(ScalarQueryRef, DataValue)>,
}

impl SortRow {
    pub(crate) fn new(
        sort_fields: &[SortField],
        tuple: TupleImpl,
        arena: &mut ExecMetaArena<impl MetaArena>,
    ) -> Result<Self, DatabaseError> {
        let sort_values = sort_fields
            .iter()
            .map(|field| {
                arena
                    .expression(field.expr)
                    .eval(arena, Some(&tuple))
                    .map(|v| v.into_owned())
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            sort_values,
            tuple,
            outer_values: arena.take_outer_values(),
        })
    }
}

impl SpillCodec for SortRow {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        self.sort_values.encode(writer)?;
        self.tuple.encode(writer)?;
        self.outer_values.encode(writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        Ok(Self {
            sort_values: Vec::<DataValue>::decode(reader)?,
            tuple: TupleImpl::decode(reader)?,
            outer_values: Vec::decode(reader)?,
        })
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        Ok(self.sort_values.encoded_len()?
            + self.tuple.encoded_len()?
            + self.outer_values.encoded_len()?)
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>()
            .saturating_add(
                self.outer_values
                    .estimated_size()
                    .saturating_sub(size_of::<Vec<(ScalarQueryRef, DataValue)>>()),
            )
            .saturating_add(
                self.sort_values
                    .estimated_size()
                    .saturating_sub(size_of::<Vec<DataValue>>()),
            )
            .saturating_add(
                self.tuple
                    .estimated_size()
                    .saturating_sub(size_of::<TupleImpl>()),
            )
    }
}

impl<A: SpillCodec, B: SpillCodec> SpillCodec for (A, B) {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        self.0.encode(writer)?;
        self.1.encode(writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        Ok((A::decode(reader)?, B::decode(reader)?))
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        Ok(self.0.encoded_len()? + self.1.encoded_len()?)
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.0.estimated_size().saturating_sub(size_of::<A>()))
            .saturating_add(self.1.estimated_size().saturating_sub(size_of::<B>()))
    }
}

impl SpillCodec for ScalarQueryRef {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        writer.write_all(&(self.arena_id as u64).to_le_bytes())?;
        writer.write_all(&(self.pos as u64).to_le_bytes())?;
        writer.write_all(&[u8::from(self.is_outer_value)])?;
        Ok(())
    }
    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        let arena_id = usize::try_from(u64::from_le_bytes(bytes))?;
        reader.read_exact(&mut bytes)?;
        let pos = usize::try_from(u64::from_le_bytes(bytes))?;
        let mut marker = [0];
        reader.read_exact(&mut marker)?;
        let is_outer_value = match marker[0] {
            0 => false,
            1 => true,
            tag => {
                return Err(DatabaseError::InvalidValue(format!(
                    "invalid scalar reference marker: {tag}"
                )))
            }
        };
        Ok(Self {
            arena_id,
            pos,
            is_outer_value,
        })
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        Ok(17)
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>()
    }
}

impl SpillCodec for DataValue {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        self.encode_reference_value(writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        Self::decode_reference_value(reader)
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        use crate::types::value::Utf8Type;
        let payload = match self {
            DataValue::Parameter { .. } | DataValue::InitValue { .. } => {
                return Err(DatabaseError::InvalidValue(
                    "unbound parameter cannot be serialized".into(),
                ))
            }
            DataValue::Null => 0,
            DataValue::Boolean(_) | DataValue::Int8(_) | DataValue::UInt8(_) => 1,
            DataValue::Int16(_) | DataValue::UInt16(_) => 2,
            DataValue::Float32(_)
            | DataValue::Int32(_)
            | DataValue::UInt32(_)
            | DataValue::Date32(_) => 4,
            DataValue::Float64(_)
            | DataValue::Int64(_)
            | DataValue::UInt64(_)
            | DataValue::Date64(_) => 8,
            DataValue::Time32(..) => 12,
            DataValue::Time64(..) => 17,
            #[cfg(feature = "decimal")]
            DataValue::Decimal(_) => 16,
            DataValue::Binary { value, len } => {
                let _: u32 = value.len().try_into()?;
                4 + value.len() + 1 + if len.is_some() { 4 } else { 0 }
            }
            DataValue::Utf8 { value, ty, .. } => {
                let _: u32 = value.len().try_into()?;
                4 + value.len()
                    + 1
                    + match ty {
                        Utf8Type::Variable(None) => 2,
                        Utf8Type::Variable(Some(_)) => 6,
                        Utf8Type::Fixed(_) => 5,
                    }
            }
            DataValue::Tuple(values) => values.encoded_len()?,
        };
        Ok(1 + payload) // Every DataValue starts with a one-byte variant tag.
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>().saturating_add(estimated_dynamic_value_size(self))
    }
}

impl<T: SpillCodec> SpillCodec for Vec<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        encode_seq(self.len(), self.iter(), writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        let mut len = [0; size_of::<u32>()];
        reader.read_exact(&mut len)?;
        let len = u32::from_le_bytes(len) as usize;
        let mut values = Vec::with_capacity(len);
        for _ in 0..len {
            values.push(T::decode(reader)?);
        }
        Ok(values)
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        seq_encoded_len(self.len(), self.iter())
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>()
            .saturating_add(self.capacity().saturating_mul(size_of::<T>()))
            .saturating_add(
                self.iter()
                    .map(|value| value.estimated_size().saturating_sub(size_of::<T>()))
                    .fold(0usize, usize::saturating_add),
            )
    }
}

impl<T: SpillCodec> SpillCodec for Option<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        match self {
            Some(value) => {
                writer.write_all(&[1])?;
                value.encode(writer)
            }
            None => Ok(writer.write_all(&[0])?),
        }
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        let mut tag = [0];
        reader.read_exact(&mut tag)?;
        match tag[0] {
            0 => Ok(None),
            1 => Ok(Some(T::decode(reader)?)),
            tag => Err(DatabaseError::InvalidValue(format!(
                "invalid spill option tag: {tag}"
            ))),
        }
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        Ok(1 + match self {
            Some(value) => value.encoded_len()?,
            None => 0,
        })
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>().saturating_add(
            self.as_ref()
                .map(|value| value.estimated_size().saturating_sub(size_of::<T>()))
                .unwrap_or_default(),
        )
    }
}

impl SpillCodec for Tuple {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        self.pk.encode(writer)?;
        self.values.encode(writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        Ok(Self::new(
            Option::<DataValue>::decode(reader)?,
            Vec::<DataValue>::decode(reader)?,
        ))
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        Ok(self.pk.encoded_len()? + self.values.encoded_len()?)
    }

    fn estimated_size(&self) -> usize {
        size_of::<Self>()
            .saturating_add(
                self.values
                    .capacity()
                    .saturating_mul(size_of::<DataValue>()),
            )
            .saturating_add(
                self.values
                    .iter()
                    .map(estimated_dynamic_value_size)
                    .fold(0usize, usize::saturating_add),
            )
            .saturating_add(
                self.pk
                    .as_ref()
                    .map(estimated_dynamic_value_size)
                    .unwrap_or_default(),
            )
    }
}

impl SpillCodec for TupleImpl {
    // Same layout as `Tuple`, so a spilled Concat decodes back as a flat tuple.
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        self.pk_slot().encode(writer)?;
        encode_seq(self.len(), self.iter(), writer)
    }

    fn decode<R: Read>(reader: &mut R) -> Result<Self, DatabaseError> {
        Tuple::decode(reader).map(TupleImpl::Tuple)
    }

    fn encoded_len(&self) -> Result<usize, DatabaseError> {
        Ok(self.pk_slot().encoded_len()? + seq_encoded_len(self.len(), self.iter())?)
    }

    fn estimated_size(&self) -> usize {
        match self {
            TupleImpl::Tuple(tuple) => tuple.estimated_size(),
            TupleImpl::Concat(_) => size_of::<Tuple>()
                .saturating_add(self.len().saturating_mul(size_of::<DataValue>()))
                .saturating_add(
                    self.iter()
                        .chain(self.pk())
                        .map(estimated_dynamic_value_size)
                        .fold(0usize, usize::saturating_add),
                ),
        }
    }
}

#[inline]
fn encode_seq<'v, T: SpillCodec + 'v, W: Write>(
    len: usize,
    values: impl IntoIterator<Item = &'v T>,
    writer: &mut W,
) -> Result<(), DatabaseError> {
    let len: u32 = len.try_into()?;
    writer.write_all(&len.to_le_bytes())?;
    values
        .into_iter()
        .try_for_each(|value| value.encode(writer))
}

#[inline]
fn seq_encoded_len<'v, T: SpillCodec + 'v>(
    len: usize,
    values: impl IntoIterator<Item = &'v T>,
) -> Result<usize, DatabaseError> {
    let _: u32 = len.try_into()?;
    values.into_iter().try_fold(
        size_of::<u32>(),
        |len, value| Ok(len + value.encoded_len()?),
    )
}

fn estimated_dynamic_value_size(value: &DataValue) -> usize {
    match value {
        DataValue::Utf8 { value, .. } => value.capacity(),
        DataValue::Binary { value, .. } => value.capacity(),
        DataValue::Tuple(values) => values
            .capacity()
            .saturating_mul(size_of::<DataValue>())
            .saturating_add(
                values
                    .iter()
                    .map(estimated_dynamic_value_size)
                    .fold(0usize, usize::saturating_add),
            ),
        _ => 0,
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::value::Utf8Type;
    use crate::types::{CharLengthUnits, LogicalType};
    use std::fmt::Debug;

    fn round_trip<T: SpillCodec + PartialEq + Debug>(value: &T) -> Result<(), DatabaseError> {
        let mut bytes = Vec::new();
        value.encode(&mut bytes)?;
        assert_eq!(value.encoded_len()?, bytes.len());
        let mut reader = bytes.as_slice();
        assert_eq!(&T::decode(&mut reader)?, value);
        assert!(reader.is_empty());
        assert!(value.estimated_size() >= size_of::<T>());
        Ok(())
    }

    fn values() -> Vec<DataValue> {
        let values = vec![
            DataValue::Binary {
                value: vec![0, 255, 128],
                len: Some(8),
            },
            DataValue::from(Vec::<u8>::new()),
            DataValue::Null,
            DataValue::Boolean(false),
            DataValue::Boolean(true),
            DataValue::Int8(i8::MIN),
            DataValue::Int16(i16::MAX),
            DataValue::Int32(i32::MIN),
            DataValue::Int64(i64::MAX),
            DataValue::UInt8(u8::MAX),
            DataValue::UInt16(u16::MAX),
            DataValue::UInt32(u32::MAX),
            DataValue::UInt64(u64::MAX),
            DataValue::Float32((-1.25).into()),
            DataValue::Float64(2.5.into()),
            DataValue::Float64(f64::INFINITY.into()),
            DataValue::Utf8 {
                value: "text中\0".into(),
                ty: Utf8Type::Variable(Some(20)),
                unit: CharLengthUnits::Characters,
            },
            DataValue::Utf8 {
                value: "fixed".into(),
                ty: Utf8Type::Fixed(8),
                unit: CharLengthUnits::Octets,
            },
            DataValue::new_utf8(String::new()),
            DataValue::Date32(-1),
            DataValue::Date64(1_700_000_000),
            DataValue::Time32(12345, 3),
            DataValue::Time64(-12345, 6, false),
            DataValue::Time64(12345, 6, true),
            DataValue::Tuple(Vec::new()),
            DataValue::Tuple(vec![
                DataValue::new_utf8("nested".into()),
                DataValue::Tuple(vec![DataValue::Null, DataValue::Int32(7)]),
            ]),
        ];
        #[cfg(feature = "decimal")]
        let values = {
            let mut values = values;
            values.push(DataValue::Decimal(rust_decimal::Decimal::new(-12345, 3)));
            values
        };
        values
    }

    #[test]
    fn data_value_codec_covers_all_materialized_variants() -> Result<(), DatabaseError> {
        for value in values() {
            round_trip(&value)?;
        }
        assert!(matches!(
            DataValue::Parameter {
                id: 1,
                ty: LogicalType::Integer
            }
            .encode(&mut Vec::new()),
            Err(DatabaseError::InvalidValue(_))
        ));
        assert!(DataValue::Tuple(vec![DataValue::Parameter {
            id: 1,
            ty: LogicalType::Integer
        }])
        .encoded_len()
        .is_err());
        assert!(matches!(
            DataValue::decode(&mut &[255][..]),
            Err(DatabaseError::InvalidValue(_))
        ));
        assert!(DataValue::decode(&mut &[7][..]).is_err());
        Ok(())
    }

    #[test]
    fn composed_codecs_cover_containers_tuples_and_sort_rows() -> Result<(), DatabaseError> {
        let values = values();
        round_trip(&Vec::<DataValue>::new())?;
        round_trip(&values)?;
        round_trip(&Option::<DataValue>::None)?;
        round_trip(&Some(DataValue::Null))?;
        round_trip(&Some(DataValue::new_utf8("owned".into())))?;
        round_trip(&(Some(DataValue::Int32(3)), values.clone()))?;
        for is_outer_value in [false, true] {
            let reference = ScalarQueryRef {
                arena_id: 1,
                pos: 7,
                is_outer_value,
            };
            round_trip(&reference)?;
            round_trip(&(reference, DataValue::Null))?;
        }
        for pk in [None, Some(DataValue::new_utf8("primary".into()))] {
            let tuple = Tuple::new(pk.clone(), values.clone());
            round_trip(&tuple)?;
            let (left, right) = values.split_at(values.len() / 2);
            let joined = TupleImpl::concat(
                Tuple::new(None, left.to_vec()).into(),
                Tuple::new(None, right.to_vec()).into(),
                pk,
            );
            round_trip(&joined)?;
            let mut flat_bytes = Vec::new();
            tuple.encode(&mut flat_bytes)?;
            let mut joined_bytes = Vec::new();
            joined.encode(&mut joined_bytes)?;
            assert_eq!(joined_bytes, flat_bytes);
            let row = SortRow {
                sort_values: vec![DataValue::Int32(2), DataValue::Null],
                tuple: joined,
                outer_values: vec![(
                    ScalarQueryRef {
                        arena_id: 0,
                        pos: 3,
                        is_outer_value: true,
                    },
                    DataValue::new_utf8("scalar".into()),
                )],
            };
            let mut bytes = Vec::new();
            row.encode(&mut bytes)?;
            assert_eq!(row.encoded_len()?, bytes.len());
            let mut reader = bytes.as_slice();
            let decoded = SortRow::decode(&mut reader)?;
            assert!(reader.is_empty());
            assert_eq!(decoded.sort_values, row.sort_values);
            assert_eq!(decoded.tuple, row.tuple);
            assert_eq!(decoded.outer_values, row.outer_values);
            assert!(row.estimated_size() >= size_of::<SortRow>());
        }
        assert!(matches!(
            Option::<DataValue>::decode(&mut &[2][..]),
            Err(DatabaseError::InvalidValue(_))
        ));
        assert!(Vec::<DataValue>::decode(&mut &[1, 0, 0, 0][..]).is_err());
        assert!(<(DataValue, DataValue)>::decode(&mut &[0][..]).is_err());
        Ok(())
    }

    #[test]
    fn scalar_reference_spill_rejects_invalid_and_truncated_markers() -> Result<(), DatabaseError> {
        for is_outer_value in [false, true] {
            let reference = ScalarQueryRef {
                arena_id: 2,
                pos: 17,
                is_outer_value,
            };
            let value = (reference, Some(DataValue::Null));
            let mut bytes = Vec::new();
            value.encode(&mut bytes)?;
            assert_eq!(
                <(ScalarQueryRef, Option<DataValue>)>::decode(&mut bytes.as_slice())?,
                value
            );
            assert!(value.estimated_size() >= size_of_val(&value));
            assert!(ScalarQueryRef::decode(&mut &bytes[..16]).is_err());
            bytes[16] = 2;
            assert!(matches!(
                ScalarQueryRef::decode(&mut bytes.as_slice()),
                Err(DatabaseError::InvalidValue(_))
            ));
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
