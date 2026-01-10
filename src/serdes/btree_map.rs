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
use std::collections::BTreeMap;
use std::io::{Read, Write};

impl<K, V> ReferenceSerialization for BTreeMap<K, V>
where
    K: ReferenceSerialization + Ord,
    V: ReferenceSerialization,
{
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.len().encode(writer, is_direct, reference_tables)?;
        for (key, value) in self.iter() {
            key.encode(writer, is_direct, reference_tables)?;
            value.encode(writer, is_direct, reference_tables)?;
        }
        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let len = <usize as ReferenceSerialization>::decode(reader, drive, reference_tables)?;
        let mut btree_map = BTreeMap::new();
        for _ in 0..len {
            let key = K::decode(reader, drive, reference_tables)?;
            let value = V::decode(reader, drive, reference_tables)?;
            btree_map.insert(key, value);
        }
        Ok(btree_map)
    }
}
