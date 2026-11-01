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

impl<V> ReferenceSerialization for Vec<V>
where
    V: ReferenceSerialization,
{
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.len().encode(writer, is_direct, arena)?;
        for value in self.iter() {
            value.encode(writer, is_direct, arena)?
        }
        Ok(())
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut drive: Option<&mut crate::serdes::ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let len = <usize as ReferenceSerialization>::decode(reader, drive.as_deref_mut(), arena)?;
        let mut vec = Vec::with_capacity(len);
        for _ in 0..len {
            vec.push(V::decode(reader, drive.as_deref_mut(), arena)?);
        }
        Ok(vec)
    }
}
