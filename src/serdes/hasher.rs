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
use crate::serdes::stable_hash::StableHasher;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;

impl ReferenceSerialization for StableHasher {
    fn encode<W: std::io::Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        let (key0, key1) = self.keys();
        key0.encode(writer, is_direct, reference_tables, arena)?;
        key1.encode(writer, is_direct, reference_tables, arena)
    }

    fn decode<T: Transaction, R: std::io::Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let key0 = u64::decode(reader, drive, reference_tables, arena)?;
        let key1 = u64::decode(reader, drive, reference_tables, arena)?;

        Ok(StableHasher::new_with_keys(key0, key1))
    }
}
