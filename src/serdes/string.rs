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
use crate::implement_serialization_by_bincode;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use std::sync::Arc;

implement_serialization_by_bincode!(String);

impl ReferenceSerialization for Arc<str> {
    fn encode<W: std::io::Write>(
        &self,
        writer: &mut W,
        _: bool,
        _: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        bincode::serialize_into(writer, self)?;

        Ok(())
    }

    fn decode<T: Transaction, R: std::io::Read>(
        reader: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let str: String = bincode::deserialize_from(reader)?;
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

        let source = "hello".to_string();
        ReferenceSerialization::encode(&source, &mut cursor, true, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            String::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            source
        );

        Ok(())
    }
}
