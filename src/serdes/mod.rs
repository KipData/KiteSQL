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
use crate::errors::DatabaseError;
use crate::storage::{TableCache, Transaction};
use std::io;
use std::io::{Read, Write};
use std::sync::Arc;

#[macro_export]
macro_rules! implement_serialization_by_bincode {
    ($struct_name:ident) => {
        impl $crate::serdes::ReferenceSerialization for $struct_name {
            fn encode<W: std::io::Write>(
                &self,
                writer: &mut W,
                _: bool,
                _: &mut $crate::serdes::ReferenceTables,
            ) -> Result<(), $crate::errors::DatabaseError> {
                bincode::serialize_into(writer, self)?;

                Ok(())
            }

            fn decode<T: $crate::storage::Transaction, R: std::io::Read>(
                reader: &mut R,
                _: Option<(&T, &$crate::storage::TableCache)>,
                _: &$crate::serdes::ReferenceTables,
            ) -> Result<Self, $crate::errors::DatabaseError> {
                Ok(bincode::deserialize_from(reader)?)
            }
        }
    };
}

pub trait ReferenceSerialization: Sized {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError>;

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError>;
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct ReferenceTables {
    tables: Vec<TableName>,
}

impl ReferenceTables {
    pub fn new() -> Self {
        ReferenceTables { tables: vec![] }
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
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
            tables.push(Arc::new(String::from_utf8(bytes).unwrap()));
        }

        Ok(ReferenceTables { tables })
    }
}

#[cfg(test)]
mod tests {
    use crate::serdes::ReferenceTables;
    use std::io;
    use std::io::{Seek, SeekFrom};
    use std::sync::Arc;

    #[test]
    fn test_to_raw() -> io::Result<()> {
        let reference_tables = ReferenceTables {
            tables: vec![Arc::new("t1".to_string()), Arc::new("t2".to_string())],
        };

        let mut cursor = io::Cursor::new(Vec::new());
        reference_tables.to_raw(&mut cursor)?;

        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(reference_tables, ReferenceTables::from_raw(&mut cursor)?);

        Ok(())
    }
}
