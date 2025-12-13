use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::ColumnId;
use std::io::{Read, Write};
use std::sync::Arc;

impl ReferenceSerialization for ColumnRef {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        self.summary().encode(writer, is_direct, reference_tables)?;
        self.in_join()
            .then(|| self.nullable())
            .encode(writer, is_direct, reference_tables)?;

        if is_direct
            || !matches!(
                self.summary().relation,
                ColumnRelation::Table { is_temp: false, .. }
            )
        {
            self.nullable()
                .encode(writer, is_direct, reference_tables)?;
            self.desc().encode(writer, is_direct, reference_tables)?;
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let summary = ColumnSummary::decode(reader, drive, reference_tables)?;
        let nullable_for_join = Option::<bool>::decode(reader, drive, reference_tables)?;

        if let (
            ColumnRelation::Table {
                column_id,
                table_name,
                is_temp: false,
            },
            Some((transaction, table_cache)),
        ) = (&summary.relation, drive)
        {
            let table = transaction
                .table(table_cache, table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let column = table
                .get_column_by_id(column_id)
                .ok_or(DatabaseError::InvalidColumn(format!(
                    "column id: {column_id} not found"
                )))?;
            Ok(nullable_for_join
                .and_then(|nullable| column.nullable_for_join(nullable))
                .unwrap_or_else(|| column.clone()))
        } else {
            let mut nullable = bool::decode(reader, drive, reference_tables)?;
            let desc = ColumnDesc::decode(reader, drive, reference_tables)?;
            let mut in_join = false;
            if let Some(nullable_for_join) = nullable_for_join {
                in_join = true;
                nullable = nullable_for_join;
            }

            Ok(Self(Arc::new(ColumnCatalog::direct_new(
                summary, nullable, desc, in_join,
            ))))
        }
    }
}

impl ReferenceSerialization for ColumnRelation {
    fn encode<W: Write>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(), DatabaseError> {
        match self {
            ColumnRelation::None => {
                writer.write_all(&[0])?;
            }
            ColumnRelation::Table {
                column_id,
                table_name,
                is_temp,
            } => {
                writer.write_all(&[1])?;
                column_id.encode(writer, is_direct, reference_tables)?;
                is_temp.encode(writer, is_direct, reference_tables)?;

                reference_tables.push_or_replace(table_name).encode(
                    writer,
                    is_direct,
                    reference_tables,
                )?;
            }
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read>(
        reader: &mut R,
        drive: Option<(&T, &TableCache)>,
        reference_tables: &ReferenceTables,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => ColumnRelation::None,
            1 => {
                let column_id = ColumnId::decode(reader, drive, reference_tables)?;
                let is_temp = bool::decode(reader, drive, reference_tables)?;
                let table_name = reference_tables
                    .get(<usize as ReferenceSerialization>::decode(
                        reader,
                        drive,
                        reference_tables,
                    )?)
                    .clone();

                ColumnRelation::Table {
                    column_id,
                    table_name,
                    is_temp,
                }
            }
            _ => unreachable!(),
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::catalog::{
        ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary, TableName,
    };
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::ScalarExpression;
    use crate::serdes::ReferenceSerialization;
    use crate::serdes::ReferenceTables;
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::{StatisticsMetaCache, Storage, Transaction};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use std::hash::RandomState;
    use std::io::{Cursor, Seek, SeekFrom};
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    #[test]
    fn test_column_serialization() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let meta_cache = StatisticsMetaCache::new(4, 1, RandomState::new())?;

        let table_name: TableName = "t1".to_string().into();
        build_table(&table_cache, &mut transaction)?;

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let c3_column_id = {
            let table = transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            *table.get_column_id_by_name("c3").unwrap()
        };

        {
            let ref_column = ColumnRef(Arc::new(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c3_column_id,
                        table_name: table_name.clone(),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
                false,
            )));

            ref_column.encode(&mut cursor, false, &mut reference_tables)?;
            cursor.seek(SeekFrom::Start(0))?;

            assert_eq!(
                ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                    &mut cursor,
                    Some((&transaction, &table_cache)),
                    &reference_tables
                )?,
                ref_column
            );
            cursor.seek(SeekFrom::Start(0))?;

            transaction.drop_column(&table_cache, &meta_cache, &table_name, "c3")?;
            assert!(ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                &mut cursor,
                Some((&transaction, &table_cache)),
                &reference_tables
            )
            .is_err());
            cursor.seek(SeekFrom::Start(0))?;
        }
        {
            let not_ref_column = ColumnRef(Arc::new(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::None,
                },
                false,
                ColumnDesc::new(
                    LogicalType::Integer,
                    None,
                    false,
                    Some(ScalarExpression::Constant(DataValue::UInt64(42))),
                )?,
                false,
            )));
            not_ref_column.encode(&mut cursor, false, &mut reference_tables)?;
            cursor.seek(SeekFrom::Start(0))?;

            assert_eq!(
                ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                    &mut cursor,
                    None,
                    &reference_tables
                )?,
                not_ref_column
            );
        }

        Ok(())
    }

    #[test]
    fn test_column_summary_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let summary = ColumnSummary {
            name: "c1".to_string(),
            relation: ColumnRelation::Table {
                column_id: Ulid::new(),
                table_name: "t1".to_string().into(),
                is_temp: false,
            },
        };
        summary.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            ColumnSummary::decode::<RocksTransaction, Cursor<Vec<u8>>>(
                &mut cursor,
                None,
                &reference_tables
            )?,
            summary
        );

        Ok(())
    }

    #[test]
    fn test_column_relation_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let none_relation = ColumnRelation::None;
        none_relation.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>>(
            &mut cursor,
            None,
            &reference_tables,
        )?;
        assert_eq!(none_relation, decode_relation);
        cursor.seek(SeekFrom::Start(0))?;
        let table_relation = ColumnRelation::Table {
            column_id: Ulid::new(),
            table_name: "t1".to_string().into(),
            is_temp: false,
        };
        table_relation.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>>(
            &mut cursor,
            None,
            &reference_tables,
        )?;
        assert_eq!(table_relation, decode_relation);

        Ok(())
    }

    #[test]
    fn test_column_desc_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let desc = ColumnDesc::new(
            LogicalType::Integer,
            None,
            false,
            Some(ScalarExpression::Constant(DataValue::UInt64(42))),
        )?;
        desc.encode(&mut cursor, false, &mut reference_tables)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_desc = ColumnDesc::decode::<RocksTransaction, Cursor<Vec<u8>>>(
            &mut cursor,
            None,
            &reference_tables,
        )?;
        assert_eq!(desc, decode_desc);

        Ok(())
    }
}
