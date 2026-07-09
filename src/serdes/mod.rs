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
mod function;
mod hasher;
mod index;
mod num;
mod option;
mod pair;
mod path_buf;
mod phantom;
mod ptr;
mod slice;
pub(crate) mod stable_hash;
mod string;
mod trim;
mod vec;

use crate::catalog::TableName;
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::planner::MetaArena;
use crate::storage::{TableCache, Transaction};
use std::io;
use std::io::{Read, Write};

pub trait ReferenceSerialization {
    fn encode<W: Write, A: MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError>;

    fn decode<T: Transaction, R: Read, A: MetaArena>(
        reader: &mut R,
        context: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError>
    where
        Self: Sized;
}

pub struct ReferenceDecodeContext<'a, T: Transaction> {
    drive: Option<(&'a T, &'a TableCache)>,
    scala_functions: Option<&'a ScalaFunctions>,
    table_functions: Option<&'a TableFunctions>,
}

impl<'a, T: Transaction> ReferenceDecodeContext<'a, T> {
    pub fn new(drive: Option<(&'a T, &'a TableCache)>) -> Self {
        Self {
            drive,
            scala_functions: None,
            table_functions: None,
        }
    }

    pub fn with_functions(
        drive: Option<(&'a T, &'a TableCache)>,
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
    ) -> Self {
        Self {
            drive,
            scala_functions: Some(scala_functions),
            table_functions: Some(table_functions),
        }
    }

    pub fn drive(&self) -> Option<(&'a T, &'a TableCache)> {
        self.drive
    }

    pub(crate) fn scala_functions(&self) -> Option<&'a ScalaFunctions> {
        self.scala_functions
    }

    pub(crate) fn table_functions(&self) -> Option<&'a TableFunctions> {
        self.table_functions
    }
}

#[derive(Debug, Default)]
pub struct ReferenceTables {
    tables: Vec<TableName>,
}

impl PartialEq for ReferenceTables {
    fn eq(&self, other: &Self) -> bool {
        self.tables == other.tables
    }
}

impl Eq for ReferenceTables {}

impl ReferenceTables {
    pub fn new() -> Self {
        ReferenceTables { tables: vec![] }
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn clear(&mut self) {
        self.tables.clear();
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn get(&self, i: usize) -> &TableName {
        &self.tables[i]
    }

    pub fn push_or_replace(&mut self, table_name: &TableName) -> usize {
        for (i, item) in self.tables.iter().enumerate() {
            if item == table_name {
                return i;
            }
        }
        self.tables.push(table_name.clone());
        self.tables.len() - 1
    }

    pub fn to_raw<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_all(&(self.tables.len() as u32).to_le_bytes())?;
        for table_name in self.tables.iter() {
            writer.write_all(&(table_name.len() as u32).to_le_bytes())?;
            writer.write_all(table_name.as_bytes())?
        }

        Ok(())
    }

    pub fn from_raw<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut bytes = [0u8; 4];
        reader.read_exact(&mut bytes)?;
        let tables_len = u32::from_le_bytes(bytes) as usize;
        let mut tables = Vec::with_capacity(tables_len);

        for _ in 0..tables_len {
            let mut bytes = [0u8; 4];
            reader.read_exact(&mut bytes)?;
            let len = u32::from_le_bytes(bytes) as usize;
            let mut bytes = vec![0u8; len];
            reader.read_exact(&mut bytes)?;
            tables.push(String::from_utf8(bytes).unwrap().into());
        }

        Ok(ReferenceTables { tables })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use std::collections::BTreeMap;
    use std::fmt::Debug;
    use std::io;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::marker::PhantomData;
    use std::path::PathBuf;

    fn round_trip<S>(source: S) -> Result<S, DatabaseError>
    where
        S: ReferenceSerialization + PartialEq + Debug,
    {
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();
        let mut cursor = Cursor::new(Vec::new());

        source.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        S::decode::<RocksTransaction, _, _>(&mut cursor, None, &reference_tables, &mut arena)
    }

    #[test]
    fn test_to_raw() -> io::Result<()> {
        let mut reference_tables = ReferenceTables::new();
        assert!(reference_tables.is_empty());
        reference_tables.push_or_replace(&"t1".to_string().into());
        reference_tables.push_or_replace(&"t2".to_string().into());
        assert_eq!(
            reference_tables.push_or_replace(&"t1".to_string().into()),
            0
        );
        assert_eq!(reference_tables.len(), 2);
        assert_eq!(reference_tables.get(1).as_ref(), "t2");

        let mut cursor = io::Cursor::new(Vec::new());
        reference_tables.to_raw(&mut cursor)?;

        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(reference_tables, ReferenceTables::from_raw(&mut cursor)?);

        reference_tables.clear();
        assert!(reference_tables.is_empty());

        Ok(())
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
    fn test_round_trip_char_uses_existing_two_byte_encoding() -> Result<(), DatabaseError> {
        assert_eq!(round_trip('a')?, 'a');
        assert_eq!(round_trip('é')?, 'é');
        assert!(round_trip('𐍈').is_err());

        Ok(())
    }
}
