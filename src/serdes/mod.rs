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
mod string;
mod trim;
mod ulid;
mod vec;

use crate::catalog::TableName;
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::planner::MetaArena;
use crate::storage::{TableCache, Transaction};
use std::io;
use std::io::{Read, Write};

#[macro_export]
macro_rules! implement_serialization_by_bincode {
    ($struct_name:ident) => {
        impl $crate::serdes::ReferenceSerialization for $struct_name {
            fn encode<W: std::io::Write, A: $crate::planner::MetaArena>(
                &self,
                writer: &mut W,
                _: bool,
                _: &mut $crate::serdes::ReferenceTables,
                _: &A,
            ) -> Result<(), $crate::errors::DatabaseError> {
                bincode::serialize_into(writer, self)?;

                Ok(())
            }

            fn decode<
                T: $crate::storage::Transaction,
                R: std::io::Read,
                A: $crate::planner::MetaArena,
            >(
                reader: &mut R,
                _: Option<&$crate::serdes::ReferenceDecodeContext<'_, T>>,
                _: &$crate::serdes::ReferenceTables,
                _: &mut A,
            ) -> Result<Self, $crate::errors::DatabaseError> {
                Ok(bincode::deserialize_from(reader)?)
            }
        }
    };
}

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
    use crate::serdes::ReferenceTables;
    use std::io;
    use std::io::{Seek, SeekFrom};

    #[test]
    fn test_to_raw() -> io::Result<()> {
        let mut reference_tables = ReferenceTables::new();
        reference_tables.push_or_replace(&"t1".to_string().into());
        reference_tables.push_or_replace(&"t2".to_string().into());

        let mut cursor = io::Cursor::new(Vec::new());
        reference_tables.to_raw(&mut cursor)?;

        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(reference_tables, ReferenceTables::from_raw(&mut cursor)?);

        Ok(())
    }
}
