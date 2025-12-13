use crate::implement_serialization_by_bincode;
use crate::types::value::DataValue;

implement_serialization_by_bincode!(DataValue);

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::DataValue;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let source_0 = DataValue::Null;
        let source_1 = DataValue::Int32(32);
        let source_2 = DataValue::Null;
        let source_3 = DataValue::Null;
        let source_4 = DataValue::Null;
        let source_5 = DataValue::Tuple(vec![DataValue::Null, DataValue::Int32(42)], false);

        let mut reference_tables = ReferenceTables::new();
        let mut bytes = Vec::new();
        let mut cursor = Cursor::new(&mut bytes);

        source_0.encode(&mut cursor, false, &mut reference_tables)?;
        source_1.encode(&mut cursor, false, &mut reference_tables)?;
        source_2.encode(&mut cursor, false, &mut reference_tables)?;
        source_3.encode(&mut cursor, false, &mut reference_tables)?;
        source_4.encode(&mut cursor, false, &mut reference_tables)?;
        source_5.encode(&mut cursor, false, &mut reference_tables)?;

        cursor.seek(SeekFrom::Start(0))?;

        let decoded_0 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_1 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_2 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_3 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_4 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();
        let decoded_5 =
            DataValue::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables).unwrap();

        assert_eq!(source_0, decoded_0);
        assert_eq!(source_1, decoded_1);
        assert_eq!(source_2, decoded_2);
        assert_eq!(source_3, decoded_3);
        assert_eq!(source_4, decoded_4);
        assert_eq!(source_5, decoded_5);

        Ok(())
    }
}
