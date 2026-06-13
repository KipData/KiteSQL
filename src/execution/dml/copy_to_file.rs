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
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;

pub struct CopyToFile {
    op: CopyToFileOperator,
    input_plan: LogicalPlan,
    column_names: Vec<String>,
    input: Option<ExecId>,
}

impl From<(CopyToFileOperator, LogicalPlan)> for CopyToFile {
    fn from((op, input): (CopyToFileOperator, LogicalPlan)) -> Self {
        CopyToFile {
            op,
            input_plan: input,
            column_names: Default::default(),
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for CopyToFile {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut executor = input;
        executor.column_names = executor
            .input_plan
            .take_schema(plan_arena)
            .into_iter()
            .map(|column| plan_arena.column(column).name().to_string())
            .collect_vec();
        executor.input = Some(build_read(
            arena,
            plan_arena,
            executor.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::CopyToFile(executor))
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
            writer.write_record(&self.column_names)?;
        }

        Ok(writer)
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CopyToFile {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let mut writer = self.create_writer()?;
        while arena.next_tuple(input, plan_arena)? {
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

        let message = if self.column_names.is_empty() {
            format!("{}", self.op)
        } else {
            format!("{} [{}]", self.op, self.column_names.iter().format(", "))
        };
        TupleBuilder::build_result_into(arena.result_tuple_mut(), message);
        arena.resume();
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::errors::DatabaseError;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::storage::Storage;
    use tempfile::TempDir;

    #[test]
    fn read_csv() -> Result<(), DatabaseError> {
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
        };

        let temp_dir = TempDir::new().unwrap();
        let mut db = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        db.ddl("create table t1 (a int primary key, b float, c varchar(10))")?;
        db.load(CatalogKind::Table("t1".to_string().into()))?;
        db.run("insert into t1 values (1, 1.1, 'foo')")?.done()?;
        db.run("insert into t1 values (2, 2.0, 'fooo')")?.done()?;
        db.run("insert into t1 values (3, 2.1, 'Kite')")?.done()?;

        let plan_arena = crate::planner::PlanArena::new(db.state.table_arena());
        let transaction = db.storage.transaction()?;
        let table = transaction
            .table(db.state.table_cache(), "t1".to_string().into())?
            .unwrap();

        let executor = CopyToFile {
            op: op.clone(),
            input_plan: TableScanOperator::build(
                "t1".to_string().into(),
                table,
                true,
                &plan_arena,
            )?,
            column_names: Default::default(),
            input: None,
        };
        let mut executor = crate::execution::execute(
            executor,
            (
                db.state.table_cache(),
                db.state.view_cache(),
                db.state.meta_cache(),
            ),
            plan_arena,
            &transaction,
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

        assert_eq!(tuple.values[0].to_string(), format!("{op} [a, b, c]"));
        Ok(())
    }
}
