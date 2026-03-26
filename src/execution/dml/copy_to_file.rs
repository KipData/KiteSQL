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
use crate::errors::DatabaseError;
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct CopyToFile {
    op: CopyToFileOperator,
    input_plan: Option<LogicalPlan>,
    input: Option<ExecId>,
}

impl From<(CopyToFileOperator, LogicalPlan)> for CopyToFile {
    fn from((op, input): (CopyToFileOperator, LogicalPlan)) -> Self {
        CopyToFile {
            op,
            input_plan: Some(input),
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for CopyToFile {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            self.input_plan
                .take()
                .expect("copy to file input plan initialized"),
            cache,
            transaction,
        ));
        arena.push(ExecNode::CopyToFile(self))
    }
}

impl CopyToFile {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let mut writer = self.create_writer()?;
        while arena.next_tuple(input)? {
            let tuple = arena.result_tuple();
            writer.write_record(
                tuple
                    .values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>(),
            )?;
        }
        writer.flush().map_err(DatabaseError::from)?;

        TupleBuilder::build_result_into(arena.result_tuple_mut(), format!("{}", self.op));
        arena.resume();
        Ok(())
    }

    fn create_writer(&self) -> Result<csv::Writer<std::fs::File>, DatabaseError> {
        let mut writer = match self.op.target.format {
            FileFormat::Csv {
                delimiter,
                quote,
                header,
                ..
            } => csv::WriterBuilder::new()
                .delimiter(delimiter as u8)
                .quote(quote as u8)
                .has_headers(header)
                .from_path(self.op.target.path.clone())?,
        };

        if let FileFormat::Csv { header: true, .. } = self.op.target.format {
            let headers = self
                .op
                .schema_ref
                .iter()
                .map(|c| c.name())
                .collect::<Vec<_>>();
            writer.write_record(headers)?;
        }

        Ok(writer)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::DataBaseBuilder;
    use crate::errors::DatabaseError;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::storage::Storage;
    use crate::types::LogicalType;
    use sqlparser::ast::CharLengthUnits;
    use std::sync::Arc;
    use tempfile::TempDir;
    use ulid::Ulid;

    #[test]
    fn read_csv() -> Result<(), DatabaseError> {
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

        let tmp_dir = TempDir::new()?;
        let file_path = tmp_dir.path().join("test.csv");

        let op = CopyToFileOperator {
            target: ExtSource {
                path: file_path.clone(),
                format: FileFormat::Csv {
                    delimiter: ',',
                    quote: '"',
                    escape: None,
                    header: true,
                },
            },
            schema_ref: Arc::new(columns),
        };

        let temp_dir = TempDir::new().unwrap();
        let db = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        db.run("create table t1 (a int primary key, b float, c varchar(10))")?
            .done()?;
        db.run("insert into t1 values (1, 1.1, 'foo')")?.done()?;
        db.run("insert into t1 values (2, 2.0, 'fooo')")?.done()?;
        db.run("insert into t1 values (3, 2.1, 'Kite')")?.done()?;

        let storage = db.storage;
        let mut transaction = storage.transaction()?;
        let table = transaction
            .table(db.state.table_cache(), "t1".to_string().into())?
            .unwrap();

        let executor = CopyToFile {
            op: op.clone(),
            input_plan: Some(TableScanOperator::build(
                "t1".to_string().into(),
                table,
                true,
            )?),
            input: None,
        };
        let mut executor = executor.execute(
            (
                db.state.table_cache(),
                db.state.view_cache(),
                db.state.meta_cache(),
            ),
            &mut transaction,
        );

        let tuple = executor.next().expect("executor should yield once")?;

        let mut rdr = csv::Reader::from_path(file_path)?;
        let headers = rdr.headers()?.clone();
        assert_eq!(headers, vec!["a", "b", "c"]);

        let mut records = rdr.records();
        let record1 = records.next().unwrap()?;
        assert_eq!(record1, vec!["1", "1.1", "foo"]);

        let record2 = records.next().unwrap()?;
        assert_eq!(record2, vec!["2", "2.0", "fooo"]);

        let record3 = records.next().unwrap()?;
        assert_eq!(record3, vec!["3", "2.1", "Kite"]);

        assert_eq!(tuple.values[0].to_string(), format!("{op}"));
        Ok(())
    }
}
