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
use crate::execution::{spawn_executor, Executor, WriteExecutor};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;
use std::fs::File;
use std::io::BufReader;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::thread;

pub struct CopyFromFile {
    op: CopyFromFileOperator,
    size: usize,
}

impl From<CopyFromFileOperator> for CopyFromFile {
    fn from(op: CopyFromFileOperator) -> Self {
        CopyFromFile { op, size: 0 }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CopyFromFile {
    fn execute_mut(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let serializers = self
                .op
                .schema_ref
                .iter()
                .map(|column| column.datatype().serializable())
                .collect_vec();
            let (tx, rx) = mpsc::channel();
            let (tx1, rx1) = mpsc::channel();
            let table = throw!(
                co,
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(table_cache, self.op.table.clone())
                )
                .ok_or(DatabaseError::TableNotFound)
            );
            let primary_keys_indices = table.primary_keys_indices().clone();
            let handle = thread::spawn(|| self.read_file_blocking(tx, primary_keys_indices));
            let mut size = 0_usize;
            while let Ok(chunk) = rx.recv() {
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.append_tuple(
                        table.name(),
                        chunk,
                        &serializers,
                        false
                    )
                );
                size += 1;
            }
            throw!(co, handle.join().unwrap());

            let handle = thread::spawn(move || return_result(size, tx1));
            while let Ok(chunk) = rx1.recv() {
                co.yield_(Ok(chunk)).await;
            }
            throw!(co, handle.join().unwrap())
        })
    }
}

impl CopyFromFile {
    /// Read records from file using blocking IO.
    ///
    /// The read data chunks will be sent through `tx`.
    fn read_file_blocking(
        mut self,
        tx: Sender<Tuple>,
        pk_indices: PrimaryKeyIndices,
    ) -> Result<(), DatabaseError> {
        let file = File::open(self.op.source.path)?;
        let mut buf_reader = BufReader::new(file);
        let mut reader = match self.op.source.format {
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

        let column_count = self.op.schema_ref.len();
        let tuple_builder = TupleBuilder::new(&self.op.schema_ref, Some(&pk_indices));

        for record in reader.records() {
            // read records and push raw str rows into data chunk builder
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::MisMatch("columns", "values"));
            }

            self.size += 1;
            tx.send(tuple_builder.build_with_row(record.iter())?)
                .map_err(|_| DatabaseError::ChannelClose)?;
        }
        Ok(())
    }
}

fn return_result(size: usize, tx: Sender<Tuple>) -> Result<(), DatabaseError> {
    let tuple = TupleBuilder::build_result(size.to_string());

    tx.send(tuple).map_err(|_| DatabaseError::ChannelClose)?;
    Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::{DataBaseBuilder, ResultIter};
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
        let executor = CopyFromFile {
            op: op.clone(),
            size: 0,
        };

        let temp_dir = TempDir::new().unwrap();
        let db = DataBaseBuilder::path(temp_dir.path()).build()?;
        db.run("create table test_copy (a int primary key, b float, c varchar(10))")?
            .done()?;
        let storage = db.storage;
        let mut transaction = storage.transaction()?;

        let mut executor_iter = executor.execute_mut(
            (
                db.state.table_cache(),
                db.state.view_cache(),
                db.state.meta_cache(),
            ),
            &mut transaction,
        );
        let tuple = executor_iter
            .next()
            .expect("executor should yield once")
            .unwrap();
        assert_eq!(tuple, TupleBuilder::build_result(2.to_string()));

        Ok(())
    }
}
