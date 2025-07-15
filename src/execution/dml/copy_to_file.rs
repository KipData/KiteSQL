use crate::binder::copy::FileFormat;
use crate::errors::DatabaseError;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct CopyToFile {
    op: CopyToFileOperator,
    input: LogicalPlan,
}

impl From<(CopyToFileOperator, LogicalPlan)> for CopyToFile {
    fn from((op, input): (CopyToFileOperator, LogicalPlan)) -> Self {
        CopyToFile { op, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for CopyToFile {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let mut writer = throw!(self.create_writer());
                let CopyToFile { input, .. } = self;

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);

                    throw!(writer
                        .write_record(
                            tuple
                                .values
                                .iter()
                                .map(|v| v.to_string())
                                .collect::<Vec<_>>()
                        )
                        .map_err(DatabaseError::from));
                }

                throw!(writer.flush().map_err(DatabaseError::from));

                yield Ok(TupleBuilder::build_result(format!("{}", self.op)));
            },
        )
    }
}

impl CopyToFile {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::errors::DatabaseError;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::storage::Storage;
    use crate::types::LogicalType;
    use sqlparser::ast::CharLengthUnits;
    use std::ops::{Coroutine, CoroutineState};
    use std::pin::Pin;
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
                        table_name: Arc::new("t1".to_string()),
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
                        table_name: Arc::new("t1".to_string()),
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
                        table_name: Arc::new("t1".to_string()),
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
        let db = DataBaseBuilder::path(temp_dir.path()).build()?;
        db.run("create table t1 (a int primary key, b float, c varchar(10))")?
            .done()?;
        db.run("insert into t1 values (1, 1.1, 'foo')")?.done()?;
        db.run("insert into t1 values (2, 2.0, 'fooo')")?.done()?;
        db.run("insert into t1 values (3, 2.1, 'Kite')")?.done()?;

        let storage = db.storage;
        let mut transaction = storage.transaction()?;
        let table = transaction
            .table(&db.state.table_cache(), Arc::new("t1".to_string()))?
            .unwrap();

        let executor = CopyToFile {
            op: op.clone(),
            input: TableScanOperator::build(Arc::new("t1".to_string()), table, true),
        };
        let mut coroutine = executor.execute(
            (
                db.state.table_cache(),
                db.state.view_cache(),
                db.state.meta_cache(),
            ),
            &mut transaction,
        );

        let tuple = match Pin::new(&mut coroutine).resume(()) {
            CoroutineState::Yielded(tuple) => tuple,
            CoroutineState::Complete(()) => unreachable!(),
        }?;

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

        assert!(records.next().is_none());

        assert_eq!(tuple, TupleBuilder::build_result(format!("{}", op)));

        Ok(())
    }
}
