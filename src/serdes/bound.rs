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
use crate::planner::MetaArena;
use crate::serdes::ReferenceSerialization;
use crate::storage::StorageTransaction;
use std::io::{Read, Write};
use std::ops::Bound;

impl<V> ReferenceSerialization for Bound<V>
where
    V: ReferenceSerialization,
{
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        match self {
            Bound::Included(v) => {
                writer.write_all(&[0])?;

                v.encode(writer, is_direct, arena)?;
            }
            Bound::Excluded(v) => {
                writer.write_all(&[1])?;

                v.encode(writer, is_direct, arena)?;
            }
            Bound::Unbounded => {
                writer.write_all(&[2])?;
            }
        }

        Ok(())
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut drive: Option<&mut crate::serdes::ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => Bound::Included(V::decode(reader, drive.as_deref_mut(), arena)?),
            1 => Bound::Excluded(V::decode(reader, drive, arena)?),
            2 => Bound::Unbounded,
            _ => unreachable!(),
        })
    }
}
