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
