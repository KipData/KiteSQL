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
use crate::storage::{TableCache, Transaction};
use std::io::Read;
use std::io::Write;
use std::mem::size_of;

#[macro_export]
macro_rules! implement_num_serialization {
    ($struct_name:ident) => {
        impl ReferenceSerialization for $struct_name {
            fn encode<W: Write>(
                &self,
                writer: &mut W,
                _: bool,
                _: &mut ReferenceTables,
            ) -> Result<(), DatabaseError> {
                writer.write_all(&self.to_le_bytes()[..])?;

                Ok(())
            }

            fn decode<T: Transaction, R: Read>(
                reader: &mut R,
                _: Option<(&T, &TableCache)>,
                _: &ReferenceTables,
            ) -> Result<Self, DatabaseError> {
                let mut bytes = [0u8; size_of::<Self>()];
                reader.read_exact(&mut bytes)?;

                Ok(Self::from_le_bytes(bytes))
            }
        }
    };
}

implement_num_serialization!(i8);
implement_num_serialization!(i16);
implement_num_serialization!(i32);
implement_num_serialization!(i64);
implement_num_serialization!(u8);
implement_num_serialization!(u16);
implement_num_serialization!(u32);
implement_num_serialization!(u64);

implement_num_serialization!(f32);
implement_num_serialization!(f64);

impl ReferenceSerialization for usize {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        (*self as u32).encode(writer, is_direct, reference_tables)
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        Ok(u32::decode(reader, drive, reference_tables)? as usize)
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
        let source_0 = 8u8;
        let source_1 = 16u16;
        let source_2 = 32u32;
        let source_3 = 64u64;
        let source_4 = 8i8;
        let source_5 = 16i16;
        let source_6 = 32i32;
        let source_7 = 64i64;
        let source_8 = 32.0f32;
        let source_9 = 64.0f64;
        let source_10 = 32usize;

        let mut reference_tables = ReferenceTables::new();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor, false, &mut reference_tables)?;
        source_1.encode(&mut cursor, false, &mut reference_tables)?;
        source_2.encode(&mut cursor, false, &mut reference_tables)?;
        source_3.encode(&mut cursor, false, &mut reference_tables)?;
        source_4.encode(&mut cursor, false, &mut reference_tables)?;
        source_5.encode(&mut cursor, false, &mut reference_tables)?;
        source_6.encode(&mut cursor, false, &mut reference_tables)?;
        source_7.encode(&mut cursor, false, &mut reference_tables)?;
        source_8.encode(&mut cursor, false, &mut reference_tables)?;
        source_9.encode(&mut cursor, false, &mut reference_tables)?;
        source_10.encode(&mut cursor, false, &mut reference_tables)?;

        cursor.seek(SeekFrom::Start(0))?;

        let decoded_0 =
            u8::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_1 =
            u16::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_2 =
            u32::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_3 =
            u64::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_4 =
            i8::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_5 =
            i16::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_6 =
            i32::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_7 =
            i64::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_8 =
            f32::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_9 =
            f64::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_10 =
            usize::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
        assert_eq!(source_3, decoded_3);
        assert_eq!(source_4, decoded_4);
        assert_eq!(source_5, decoded_5);
        assert_eq!(source_6, decoded_6);
        assert_eq!(source_7, decoded_7);
        assert_eq!(source_8, decoded_8);
        assert_eq!(source_9, decoded_9);
        assert_eq!(source_10, decoded_10);

        Ok(())
    }
}
