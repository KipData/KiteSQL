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
use std::sync::Arc;

impl ReferenceSerialization for String {
    fn encode<W: Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.len()
            .encode(writer, is_direct, reference_tables, arena)?;
        writer.write_all(self.as_bytes())?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let len = usize::decode(reader, drive, reference_tables, arena)?;
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes)?;

        Ok(String::from_utf8(bytes)?)
    }
}

impl ReferenceSerialization for Arc<str> {
    fn encode<W: Write, A: crate::planner::MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.len()
            .encode(writer, is_direct, reference_tables, arena)?;
        writer.write_all(self.as_bytes())?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read, A: crate::planner::MetaArena>(
        reader: &mut R,
        drive: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let str = String::decode(reader, drive, reference_tables, arena)?;
        Ok(str.into())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();

        let source = "hello".to_string();
        ReferenceSerialization::encode(&source, &mut cursor, true, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            String::decode::<RocksTransaction, _, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut arena,
            )?,
            source
        );

        Ok(())
    }
}
