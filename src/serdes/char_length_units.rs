use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use sqlparser::ast::CharLengthUnits;
use std::io::{Read, Write};

impl ReferenceSerialization for CharLengthUnits {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            CharLengthUnits::Characters => 0u8,
            CharLengthUnits::Octets => 1u8,
        }
        .encode(writer, is_direct, reference_tables)?;

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        _: Option<(&T, &TableCache)>,
        _: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut one_byte = [0u8; 1];
        reader.read_exact(&mut one_byte)?;

        Ok(match one_byte[0] {
            0 => CharLengthUnits::Characters,
            1 => CharLengthUnits::Octets,
            _ => unreachable!(),
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::errors::DatabaseError;
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use sqlparser::ast::CharLengthUnits;
    use std::io::{Cursor, Seek, SeekFrom};

    #[test]
    fn test_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        CharLengthUnits::Characters.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            CharLengthUnits::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            CharLengthUnits::Characters
        );
        cursor.seek(SeekFrom::Start(0))?;
        CharLengthUnits::Octets.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;
        assert_eq!(
            CharLengthUnits::decode::<RocksTransaction, _>(&mut cursor, None, &reference_tables)?,
            CharLengthUnits::Octets
        );

        Ok(())
    }
}
