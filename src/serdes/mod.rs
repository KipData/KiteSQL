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

mod boolean;
mod bound;
mod btree_map;
mod char;
mod char_length_units;
mod column;
mod data_value;
mod evaluator;
mod expression;
mod function;
mod hasher;
mod index;
mod num;
mod option;
mod pair;
mod path_buf;
mod phantom;
mod physical_option;
mod ptr;
mod slice;
pub(crate) mod stable_hash;
mod string;
mod trim;
mod vec;

use crate::catalog::SourceId;
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::planner::MetaArena;
use crate::storage::StorageTransaction;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::marker::PhantomData;

pub trait ReferenceSerialization {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError>;

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        context: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError>
    where
        Self: Sized;
}

pub struct ReferenceDecodeContext<'a, T: StorageTransaction> {
    _transaction: PhantomData<&'a T>,
    scala_functions: Option<&'a ScalaFunctions>,
    table_functions: Option<&'a TableFunctions>,
    sources: HashMap<SourceId, SourceId>,
}

impl<'a, T: StorageTransaction> Default for ReferenceDecodeContext<'a, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, T: StorageTransaction> ReferenceDecodeContext<'a, T> {
    pub fn new() -> Self {
        Self {
            _transaction: PhantomData,
            scala_functions: None,
            table_functions: None,
            sources: HashMap::new(),
        }
    }

    pub fn with_functions(
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
    ) -> Self {
        Self {
            _transaction: PhantomData,
            scala_functions: Some(scala_functions),
            table_functions: Some(table_functions),
            sources: HashMap::new(),
        }
    }

    pub(crate) fn scala_functions(&self) -> Option<&'a ScalaFunctions> {
        self.scala_functions
    }

    pub(crate) fn table_functions(&self) -> Option<&'a TableFunctions> {
        self.table_functions
    }

    pub(crate) fn source_id(
        &mut self,
        encoded: SourceId,
        alloc: impl FnOnce() -> SourceId,
    ) -> SourceId {
        *self.sources.entry(encoded).or_insert_with(alloc)
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::planner::TableArena;
    use crate::serdes::ReferenceSerialization;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::DataValue;
    use std::collections::{BTreeMap, Bound};
    use std::fmt::Debug;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::marker::PhantomData;
    use std::path::PathBuf;

    fn round_trip<S>(source: S) -> Result<S, DatabaseError>
    where
        S: ReferenceSerialization + PartialEq + Debug,
    {
        let mut arena = TableArena::default();
        let mut cursor = Cursor::new(Vec::new());

        source.encode(&mut cursor, false, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        S::decode::<RocksTransaction, _, _>(&mut cursor, None, &mut arena)
    }

    #[test]
    fn test_round_trip_basic_containers() -> Result<(), DatabaseError> {
        assert_eq!(round_trip([3u32, 5u32])?, [3u32, 5u32]);
        assert_eq!(round_trip(PhantomData::<String>)?, PhantomData::<String>);
        let path = PathBuf::from("kitesql-serde-path.csv");
        assert_eq!(round_trip(path.clone())?, path);

        let mut source = BTreeMap::new();
        source.insert("alpha".to_string(), 11i32);
        source.insert("beta".to_string(), 29i32);
        assert_eq!(round_trip(source.clone())?, source);

        Ok(())
    }

    #[test]
    fn test_generic_range_serialization() -> Result<(), DatabaseError> {
        let range = Range::SortedRanges(vec![
            Range::Eq(DataValue::Int32(7)),
            Range::Scope {
                min: Bound::Included(DataValue::Int32(8)),
                max: Bound::Excluded(DataValue::Int32(12)),
            },
            Range::SortedRanges(vec![Range::Dummy]),
        ]);
        assert_eq!(round_trip(range.clone())?, range);
        let generic = Range::SortedRanges(vec![Range::Eq("prefix".to_string())]);
        assert_eq!(round_trip(generic.clone())?, generic);
        Ok(())
    }

    #[test]
    fn test_round_trip_char_uses_existing_two_byte_encoding() -> Result<(), DatabaseError> {
        assert_eq!(round_trip('a')?, 'a');
        assert_eq!(round_trip('é')?, 'é');
        assert!(round_trip('𐍈').is_err());

        Ok(())
    }
}
// GRCOV_EXCL_STOP
