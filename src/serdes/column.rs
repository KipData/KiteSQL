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

use crate::catalog::{ColumnCatalog, ColumnRef, ColumnRelation, SourceId, TableId};
use crate::errors::DatabaseError;
use crate::planner::MetaArena;
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization};
use crate::storage::StorageTransaction;
use crate::types::ColumnId;
use std::io::{Read, Write};

impl ReferenceSerialization for ColumnRef {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        arena.column(*self).encode(writer, is_direct, arena)
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        drive: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let column = ColumnCatalog::decode(reader, drive, arena)?;

        if let ColumnRelation::Table {
            table_id,
            column_id,
        } = &column.summary().relation
        {
            if let Some((_, column_ref)) = arena
                .table(*table_id)
                .and_then(|table| table.get_column_by_id(column_id))
            {
                return Ok(column_ref);
            }
        }

        Ok(arena.alloc_column(column))
    }
}

impl ReferenceSerialization for ColumnRelation {
    fn encode<W: Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        match self {
            ColumnRelation::None => {
                writer.write_all(&[0])?;
            }
            ColumnRelation::Table {
                table_id,
                column_id,
            } => {
                writer.write_all(&[1])?;
                table_id.encode(writer, is_direct, arena)?;
                column_id.encode(writer, is_direct, arena)?;
            }
            ColumnRelation::Source {
                source_id,
                column_id,
                is_temp,
            } => {
                writer.write_all(&[2])?;
                source_id.encode(writer, is_direct, arena)?;
                column_id.encode(writer, is_direct, arena)?;
                is_temp.encode(writer, is_direct, arena)?;
            }
        }

        Ok(())
    }

    fn decode<T: StorageTransaction, R: Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        mut drive: Option<&mut ReferenceDecodeContext<'_, T>>,
        arena: &mut A,
    ) -> Result<Self, DatabaseError> {
        let mut type_bytes = [0u8; 1];
        reader.read_exact(&mut type_bytes)?;

        Ok(match type_bytes[0] {
            0 => ColumnRelation::None,
            1 => ColumnRelation::Table {
                table_id: TableId::decode(reader, drive.as_deref_mut(), arena)?,
                column_id: ColumnId::decode(reader, drive.as_deref_mut(), arena)?,
            },
            2 => {
                let encoded_id = SourceId::decode(reader, drive.as_deref_mut(), arena)?;
                let column_id = ColumnId::decode(reader, drive.as_deref_mut(), arena)?;
                let is_temp = bool::decode(reader, drive.as_deref_mut(), arena)?;
                let source_id = match drive {
                    Some(context) => context.source_id(encoded_id, || arena.alloc_source()),
                    None => arena.alloc_source(),
                };

                ColumnRelation::Source {
                    source_id,
                    column_id,
                    is_temp,
                }
            }
            _ => unreachable!(),
        })
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::catalog::{
        ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary, TableName,
    };
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::ScalarExpression;
    use crate::planner::{MetaArena, PlanArena, TableArenaCell};
    use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::{Storage, StorageTransaction};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::io::{Cursor, Seek, SeekFrom};
    use tempfile::TempDir;

    #[test]
    fn test_column_serialization() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let mut names = crate::storage::NameMapping::default();
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        let table_name: TableName = "t1".to_string();
        build_table(
            &mut names,
            &mut table_cache,
            &mut transaction,
            &mut plan_arena,
        )?;
        let mut plan_arena = PlanArena::new(&table_arena);

        let mut cursor = Cursor::new(Vec::new());
        let ref_column = {
            let table = transaction.table(&names, &table_cache, "t1")?.unwrap();
            table.get_column_by_name("c3").unwrap()
        };

        {
            ref_column.encode(&mut cursor, false, &plan_arena)?;
            cursor.seek(SeekFrom::Start(0))?;

            assert_eq!(
                {
                    let mut context = ReferenceDecodeContext::new();
                    ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                        &mut cursor,
                        Some(&mut context),
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
            names.insert_table(&mut table_cache, table);
            plan_arena = PlanArena::new(&table_arena);
            let mut context = ReferenceDecodeContext::new();
            assert_eq!(
                ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                    &mut cursor,
                    Some(&mut context),
                    &mut plan_arena,
                )?,
                ref_column
            );
            let table = transaction
                .table(&names, &table_cache, &table_name)?
                .expect("table should still exist after dropping one column");
            assert!(table.get_column_id_by_name("c3").is_none());
            cursor.seek(SeekFrom::Start(0))?;
        }
        {
            let default =
                plan_arena.alloc_expression(ScalarExpression::Constant(DataValue::UInt64(42)));
            let not_ref_column = plan_arena.alloc_column(ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, Some(default))?,
            ));
            not_ref_column.encode(&mut cursor, false, &plan_arena)?;
            cursor.seek(SeekFrom::Start(0))?;

            let decoded = ColumnRef::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                &mut cursor,
                None,
                &mut plan_arena,
            )?;
            let decoded = plan_arena.column(decoded);
            let expected = plan_arena.column(not_ref_column);
            assert_eq!(decoded.summary(), expected.summary());
            assert_eq!(decoded.nullable(), expected.nullable());
            assert_eq!(decoded.datatype(), expected.datatype());
            assert_eq!(decoded.desc().primary(), expected.desc().primary());
            assert_eq!(decoded.desc().is_unique(), expected.desc().is_unique());
            let decoded_default = decoded.desc().default.unwrap();
            let expected_default = expected.desc().default.unwrap();
            assert!(decoded_default.eq_ignore_colref_pos(expected_default, &plan_arena));
        }

        Ok(())
    }

    #[test]
    fn test_column_summary_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut arena = crate::planner::TableArena::default();
        let summary = ColumnSummary {
            name: "c1".to_string(),
            relation: ColumnRelation::Table {
                table_id: 0,
                column_id: 1,
            },
        };
        summary.encode(&mut cursor, false, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        assert_eq!(
            ColumnSummary::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
                &mut cursor,
                None,
                &mut arena,
            )?,
            summary
        );

        Ok(())
    }

    #[test]
    fn test_column_relation_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut arena = crate::planner::TableArena::default();
        let none_relation = ColumnRelation::None;
        none_relation.encode(&mut cursor, false, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
            &mut cursor,
            None,
            &mut arena,
        )?;
        assert_eq!(none_relation, decode_relation);
        cursor.seek(SeekFrom::Start(0))?;
        let table_relation = ColumnRelation::Table {
            table_id: 0,
            column_id: 1,
        };
        table_relation.encode(&mut cursor, false, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_relation = ColumnRelation::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
            &mut cursor,
            None,
            &mut arena,
        )?;
        assert_eq!(table_relation, decode_relation);

        Ok(())
    }

    #[test]
    fn test_column_desc_serialization() -> Result<(), DatabaseError> {
        let mut cursor = Cursor::new(Vec::new());
        let mut arena = crate::planner::TableArena::default();
        let desc = ColumnDesc::new(
            LogicalType::Integer,
            None,
            false,
            Some(arena.alloc_expression(ScalarExpression::Constant(DataValue::UInt64(42)))),
        )?;
        desc.encode(&mut cursor, false, &arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        let decode_desc = ColumnDesc::decode::<RocksTransaction, Cursor<Vec<u8>>, _>(
            &mut cursor,
            None,
            &mut arena,
        )?;
        assert_eq!(desc.column_datatype, decode_desc.column_datatype);
        assert_eq!(desc.primary(), decode_desc.primary());
        assert_eq!(desc.is_unique(), decode_desc.is_unique());
        assert_eq!(
            arena.expression(desc.default.unwrap()),
            arena.expression(decode_desc.default.unwrap())
        );

        Ok(())
    }
}
// GRCOV_EXCL_STOP
