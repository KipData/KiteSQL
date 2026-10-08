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
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::io::{Read, Write};
use std::mem::size_of;

pub(crate) struct SortRow {
    pub(crate) sort_values: Vec<DataValue>,
    pub(crate) tuple: Tuple,
    pub(crate) outer_values: Vec<(ScalarQueryRef, DataValue)>,
}

impl SortRow {
    pub(crate) fn new(
        sort_fields: &[SortField],
        tuple: Tuple,
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
            tuple: Tuple::decode(reader)?,
            outer_values: Vec::decode(reader)?,
        })
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
                    .saturating_sub(size_of::<Tuple>()),
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

    fn estimated_size(&self) -> usize {
        size_of::<Self>().saturating_add(estimated_dynamic_value_size(self))
    }
}

impl<T: SpillCodec> SpillCodec for Vec<T> {
    fn encode<W: Write>(&self, writer: &mut W) -> Result<(), DatabaseError> {
        let len: u32 = self.len().try_into()?;
        writer.write_all(&len.to_le_bytes())?;
        for value in self {
            value.encode(writer)?;
        }
        Ok(())
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
            None => {
                writer.write_all(&[0])?;
                Ok(())
            }
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

fn estimated_dynamic_value_size(value: &DataValue) -> usize {
    match value {
        DataValue::Utf8 { value, .. } => value.capacity(),
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
