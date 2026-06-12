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
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;
use crate::types::index::{IndexMeta, IndexMetaRef};
use std::io::{Read, Write};

impl ReferenceSerialization for IndexMetaRef {
    fn encode<W: Write, A: MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        arena
            .index(*self)
            .encode(writer, is_direct, reference_tables, arena)
    }

    fn decode<T: Transaction, R: Read, A: MetaArena>(
        reader: &mut R,
        drive: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let index = IndexMeta::decode(reader, drive, reference_tables, arena)?;
        Ok(arena.alloc_index(index))
    }
}
