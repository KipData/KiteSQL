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
use crate::serdes::ReferenceSerialization;
use crate::storage::StorageTransaction;
use std::io::{Read, Write};
use std::sync::Arc;

impl ReferenceSerialization for String {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.len().encode(writer, is_direct, arena)?;
        writer.write_all(self.as_bytes())?;

        Ok(())
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        drive: Option<&mut crate::serdes::ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let len = usize::decode(reader, drive, arena)?;
        let mut bytes = vec![0; len];
        reader.read_exact(&mut bytes)?;

        Ok(String::from_utf8(bytes)?)
    }
}

impl ReferenceSerialization for Arc<str> {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.len().encode(writer, is_direct, arena)?;
        writer.write_all(self.as_bytes())?;

        Ok(())
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        drive: Option<&mut crate::serdes::ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let str = String::decode(reader, drive, arena)?;
        Ok(str.into())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::ReferenceSerialization;
    use crate::storage::rocksdb::RocksTransaction;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);
        let mut arena = crate::planner::TableArena::default();

        let source = "hello".to_string();
        ReferenceSerialization::encode(&source, &mut cursor, true, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            String::decode::<RocksTransaction, _, _>(&mut cursor, None, &mut arena,)?,
            source
        );

        Ok(())
    }
}
// GRCOV_EXCL_STOP
