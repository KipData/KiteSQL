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
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization};
use crate::storage::StorageTransaction;
use std::io::{Read, Write};

/// A definition's position in an Arena namespace, independent of execution-local slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ScalarQueryRef {
    pub(crate) arena_id: usize,
    pub(crate) pos: usize,
    pub(crate) is_outer_value: bool,
}

impl std::fmt::Display for ScalarQueryRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.arena_id, self.pos)
    }
}

impl ReferenceSerialization for ScalarQueryRef {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.arena_id.encode(writer, direct, arena)?;
        self.pos.encode(writer, direct, arena)?;
        self.is_outer_value.encode(writer, direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let reference = Self {
            arena_id: usize::decode(reader, context.as_deref_mut(), arena)?,
            pos: usize::decode(reader, context.as_deref_mut(), arena)?,
            is_outer_value: bool::decode(reader, context, arena)?,
        };
        arena.reserve_scalar_query_ref(reference);
        Ok(reference)
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::planner::TableArena;
    use crate::storage::rocksdb::RocksTransaction;

    #[test]
    fn decoding_preserves_source_reference() -> Result<(), DatabaseError> {
        let mut source = TableArena::default();
        let reference = source.alloc_scalar_query_ref(true);
        let mut bytes = Vec::new();
        reference.encode(&mut bytes, false, &source)?;
        let mut target = TableArena::default();
        let mut context = ReferenceDecodeContext::<RocksTransaction>::new();
        let first = ScalarQueryRef::decode::<RocksTransaction, _, _>(
            &mut bytes.as_slice(),
            Some(&mut context),
            &mut target,
        )?;
        let repeated = ScalarQueryRef::decode::<RocksTransaction, _, _>(
            &mut bytes.as_slice(),
            Some(&mut context),
            &mut target,
        )?;
        assert_eq!(first, repeated);
        assert_eq!(first, reference);
        let mut next_view = ReferenceDecodeContext::<RocksTransaction>::new();
        let second = ScalarQueryRef::decode::<RocksTransaction, _, _>(
            &mut bytes.as_slice(),
            Some(&mut next_view),
            &mut target,
        )?;
        assert_eq!(second, reference);
        assert_ne!(target.alloc_scalar_query_ref(false), reference);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
