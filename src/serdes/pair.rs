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

impl<A, B> ReferenceSerialization for (A, B)
where
    A: ReferenceSerialization,
    B: ReferenceSerialization,
{
    fn encode<W: Write, AR: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &AR,
    ) -> Result<(), DatabaseError> {
        let (v1, v2) = self;
        v1.encode(writer, is_direct, reference_tables, arena)?;
        v2.encode(writer, is_direct, reference_tables, arena)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read, AR: crate::planner::MetaArena>(
        reader: &mut R,
        drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut AR,
    ) -> Result<Self, DatabaseError> {
        let v1 = A::decode(reader, drive, reference_tables, arena)?;
        let v2 = B::decode(reader, drive, reference_tables, arena)?;

        Ok((v1, v2))
    }
}
