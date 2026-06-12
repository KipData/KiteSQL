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

use crate::catalog::{ColumnCatalog, ColumnRef, ColumnRelation};
use crate::errors::DatabaseError;
use crate::planner::MetaArena;
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
use crate::storage::Transaction;
use crate::types::ColumnId;
use std::io::{Read, Write};

impl ReferenceSerialization for ColumnRef {
    fn encode<W: Write, A: MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        arena
            .column(*self)
            .encode(writer, is_direct, reference_tables, arena)
    }

    fn decode<T: Transaction, R: Read, A: MetaArena>(
        reader: &mut R,
        drive: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let column = ColumnCatalog::decode(reader, drive, reference_tables, arena)?;

        if let ColumnRelation::Table {
            column_id,
            table_name,
            is_temp: false,
        } = &column.summary().relation
        {
            if let Some((_, table_cache)) = drive.and_then(ReferenceDecodeContext::drive) {
                if let Some(column_ref) = table_cache
                    .get(table_name)
                    .and_then(|table| table.get_column_by_id(column_id))
                {
                    return Ok(column_ref);
                }
            }
        }

        Ok(arena.alloc_column(column))
    }
}

impl ReferenceSerialization for ColumnRelation {
    fn encode<W: Write, A: MetaArena>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut ReferenceTables,
        arena: &A,
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
                column_id.encode(writer, is_direct, reference_tables, arena)?;
                is_temp.encode(writer, is_direct, reference_tables, arena)?;

                reference_tables.push_or_replace(table_name).encode(
                    writer,
                    is_direct,
                    reference_tables,
                    arena,
                )?;
            }
        }

        Ok(())
    }

    fn decode<T: Transaction, R: Read, A: MetaArena>(
        reader: &mut R,
        drive: Option<&ReferenceDecodeContext<'_, T>>,
        reference_tables: &ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => ColumnRelation::None,
            1 => {
                let column_id = ColumnId::decode(reader, drive, reference_tables, arena)?;
                let is_temp = bool::decode(reader, drive, reference_tables, arena)?;
                let table_name = reference_tables
                    .get(<usize as ReferenceSerialization>::decode(
                        reader,
                        drive,
                        reference_tables,
                        arena,
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
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::{Storage, Transaction};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::io::{Cursor, Seek, SeekFrom};
    use tempfile::TempDir;
    use ulid::Ulid;

    #[test]
    fn test_column_serialization() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        let table_name: TableName = "t1".to_string().into();
        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let mut plan_arena = PlanArena::new(&table_arena);

        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let ref_column = {
            let table = transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            table.get_column_by_name("c3").unwrap()
        };

        {
            ref_column.encode(&mut cursor, false, &mut reference_tables, &plan_arena)?;
            cursor.seek(SeekFrom::Start(0))?;

            assert_eq!(
                {
                    let context = ReferenceDecodeContext::new(Some((&transaction, &table_cache)));
                    ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                        &mut cursor,
                        Some(&context),
                        &reference_tables,
                        &mut plan_arena,
                    )?
                },
                ref_column
            );
            cursor.seek(SeekFrom::Start(0))?;

            let mut table_codec = crate::storage::table_codec::TableCodec::default();
            let table =
                transaction.drop_column(&mut table_codec, &mut plan_arena, &table_name, "c3")?;
            let table = table.transplant_to_table_arena(&plan_arena)?;
            table_cache.insert(table.name().clone(), table);
            plan_arena = PlanArena::new(&table_arena);
            let context = ReferenceDecodeContext::new(Some((&transaction, &table_cache)));
            assert_eq!(
                ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                    &mut cursor,
                    Some(&context),
                    &reference_tables,
                    &mut plan_arena,
                )?,
                ref_column
            );
            let table = transaction
                .table(&table_cache, table_name.clone())?
                .expect("table should still exist after dropping one column");
            assert!(table.get_column_id_by_name("c3").is_none());
            cursor.seek(SeekFrom::Start(0))?;
        }
        {
            let not_ref_column = plan_arena.alloc_column(ColumnCatalog::direct_new(
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
            ));
            not_ref_column.encode(&mut cursor, false, &mut reference_tables, &plan_arena)?;
            cursor.seek(SeekFrom::Start(0))?;

            let decoded = ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut plan_arena,
            )?;
            assert_eq!(
                plan_arena.column(decoded),
                plan_arena.column(not_ref_column)
            );
        }

        Ok(())
    }

    #[test]
    fn test_column_summary_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();
        let summary = ColumnSummary {
            name: "c1".to_string(),
            relation: ColumnRelation::Table {
                column_id: Ulid::new(),
                table_name: "t1".to_string().into(),
                is_temp: false,
            },
        };
        summary.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            ColumnSummary::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                &mut cursor,
                None,
                &reference_tables,
                &mut arena,
            )?,
            summary
        );

        Ok(())
    }

    #[test]
    fn test_column_relation_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();
        let none_relation = ColumnRelation::None;
        none_relation.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
            &mut cursor,
            None,
            &reference_tables,
            &mut arena,
        )?;
        assert_eq!(none_relation, decode_relation);
        cursor.seek(SeekFrom::Start(0))?;
        let table_relation = ColumnRelation::Table {
            column_id: Ulid::new(),
            table_name: "t1".to_string().into(),
            is_temp: false,
        };
        table_relation.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
            &mut cursor,
            None,
            &reference_tables,
            &mut arena,
        )?;
        assert_eq!(table_relation, decode_relation);

        Ok(())
    }

    #[test]
    fn test_column_desc_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();
        let mut arena = crate::planner::TableArena::default();
        let desc = ColumnDesc::new(
            LogicalType::Integer,
            None,
            false,
            Some(ScalarExpression::Constant(DataValue::UInt64(42))),
        )?;
        desc.encode(&mut cursor, false, &mut reference_tables, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_desc = ColumnDesc::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
            &mut cursor,
            None,
            &reference_tables,
            &mut arena,
        )?;
        assert_eq!(desc, decode_desc);

        Ok(())
    }
}
