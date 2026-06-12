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
use std::io::{Read, Write};

impl<V> ReferenceSerialization for [V; 2]
where
    V: ReferenceSerialization,
{
    fn encode<W: Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self[0].encode(writer, is_direct, reference_tables, arena)?;
        self[1].encode(writer, is_direct, reference_tables, arena)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        Ok([
            V::decode(reader, drive, reference_tables, arena)?,
            V::decode(reader, drive, reference_tables, arena)?,
        ])
    }
}
