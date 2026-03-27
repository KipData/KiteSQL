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

use crate::binder::copy::FileFormat;
use crate::catalog::PrimaryKeyIndices;
use crate::errors::DatabaseError;
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, WriteExecutor};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;
use std::fs::File;
use std::io::BufReader;

pub struct CopyFromFile {
    op: Option<CopyFromFileOperator>,
}

impl From<CopyFromFileOperator> for CopyFromFile {
    fn from(op: CopyFromFileOperator) -> Self {
        CopyFromFile { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CopyFromFile {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::CopyFromFile(self))
    }
}

impl CopyFromFile {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        let Some(op) = self.op.take() else {
            arena.finish();
            return Ok(());
        };
        let serializers = op
            .schema_ref
            .iter()
            .map(|column| column.datatype().serializable())
            .collect_vec();
        let table = arena
            .transaction_mut()
            .table(arena.table_cache(), op.table.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let primary_keys_indices = table.primary_keys_indices().clone();

        let file = File::open(op.source.path)?;
        let mut buf_reader = BufReader::new(file);
        let mut reader = match op.source.format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.map(|c| c as u8))
                .has_headers(header)
                .from_reader(&mut buf_reader),
        };

        let column_count = op.schema_ref.len();
        let tuple_builder = TupleBuilder::new(&op.schema_ref, Some(&primary_keys_indices));
        let mut size = 0_usize;

        for record in reader.records() {
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::MisMatch("columns", "values"));
            }

            let chunk = tuple_builder.build_with_row(record.iter())?;
            arena
                .transaction_mut()
                .append_tuple(table.name(), chunk, &serializers, false)?;
            size += 1;
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), size.to_string());
        arena.resume();
        Ok(())
    }

    #[allow(dead_code)]
    fn read_file_blocking(
        mut self,
        tx: std::sync::mpsc::Sender<Tuple>,
        pk_indices: PrimaryKeyIndices,
    ) -> Result<(), DatabaseError> {
        let Some(op) = self.op.take() else {
            return Ok(());
        };
        let file = File::open(op.source.path)?;
        let mut buf_reader = BufReader::new(file);
        let mut reader = match op.source.format {
            FileFormat::Csv {
                delimiter,
                quote,
                escape,
                header,
            } => csv::ReaderBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .escape(escape.map(|c| c as u8))
                .has_headers(header)
                .from_reader(&mut buf_reader),
        };

        let column_count = op.schema_ref.len();
        let tuple_builder = TupleBuilder::new(&op.schema_ref, Some(&pk_indices));

        for record in reader.records() {
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::MisMatch("columns", "values"));
            }

            tx.send(tuple_builder.build_with_row(record.iter())?)
                .map_err(|_| DatabaseError::ChannelClose)?;
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::DataBaseBuilder;
    use crate::errors::DatabaseError;
    use crate::storage::Storage;
    use crate::types::LogicalType;
    use sqlparser::ast::CharLengthUnits;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    #[test]
    fn read_csv() -> Result<(), DatabaseError> {
        let csv = "1,1.5,one\n2,2.5,two\n";

        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        write!(file, "{csv}").expect("failed to write file");

        let columns = vec![
            ColumnRef::from(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "a".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: Ulid::new(),
                        table_name: "t1".to_string().into(),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?,
                false,
            )),
            ColumnRef::from(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "b".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: Ulid::new(),
                        table_name: "t1".to_string().into(),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(LogicalType::Float, None, false, None)?,
                false,
            )),
            ColumnRef::from(ColumnCatalog::direct_new(
                ColumnSummary {
                    name: "c".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: Ulid::new(),
                        table_name: "t1".to_string().into(),
                        is_temp: false,
                    },
                },
                false,
                ColumnDesc::new(
                    LogicalType::Varchar(Some(10), CharLengthUnits::Characters),
                    None,
                    false,
                    None,
                )?,
                false,
            )),
        ];

        let op = CopyFromFileOperator {
            table: "test_copy".to_string().into(),
            source: ExtSource {
                path: file.path().into(),
                format: FileFormat::Csv {
                    delimiter: ',',
                    quote: '"',
                    escape: None,
                    header: false,
                },
            },
            schema_ref: Arc::new(columns),
        };

        let tmp_dir = TempDir::new()?;
        let db = DataBaseBuilder::path(tmp_dir.path()).build_rocksdb()?;
        db.run("create table test_copy (a int primary key, b float, c varchar(10))")?
            .done()?;

        let storage = db.storage;
        let mut transaction = storage.transaction()?;
        let mut executor = crate::execution::execute_mut(
            CopyFromFile::from(op),
            (
                db.state.table_cache(),
                db.state.view_cache(),
                db.state.meta_cache(),
            ),
            &mut transaction,
        );

        let result = executor.next().expect("copy from file should yield once")?;
        assert_eq!(result.values[0].to_string(), "2");

        Ok(())
    }
}
