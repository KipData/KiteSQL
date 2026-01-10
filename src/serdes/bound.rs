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
use crate::storage::{TableCache, Transaction};
use std::io::{Read, Write};
use std::ops::Bound;

impl<V> ReferenceSerialization for Bound<V>
where
    V: ReferenceSerialization,
{
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            Bound::Included(v) => {
                writer.write_all(&[0])?;

                v.encode(writer, is_direct, reference_tables)?;
            }
            Bound::Excluded(v) => {
                writer.write_all(&[1])?;

                v.encode(writer, is_direct, reference_tables)?;
            }
            Bound::Unbounded => {
                writer.write_all(&[2])?;
            }
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => Bound::Included(V::decode(reader, drive, reference_tables)?),
            1 => Bound::Excluded(V::decode(reader, drive, reference_tables)?),
            2 => Bound::Unbounded,
            _ => unreachable!(),
        })
    }
}
