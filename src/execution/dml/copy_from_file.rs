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
    ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, WriteExecutor,
};
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::storage::Transaction;
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
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::CopyFromFile(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CopyFromFile {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(op) = self.op.take() else {
            arena.finish();
            return Ok(());
        };
        let column_types = op
            .schema_ref
            .iter()
            .map(|column| plan_arena.column(*column).datatype().clone())
            .collect_vec();
        let serializers = column_types
            .iter()
            .map(|ty| ty.serializable())
            .collect_vec();
        let table_cache = arena.context().table_cache();
        let transaction = arena.transaction();
        let table = transaction
            .table(table_cache, op.table.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let table_name = table.name().to_string();

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
        let tuple_builder = TupleBuilder::new(column_types, Some(table.primary_key_indices()));
        let mut size = 0_usize;

        for record in reader.records() {
            let record = record?;

            if !(record.len() == column_count
                || record.len() == column_count + 1 && record.get(column_count) == Some(""))
            {
                return Err(DatabaseError::MisMatch("columns", "values"));
            }

            let chunk = tuple_builder.build_with_row(record.iter())?;
            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec_mut();
            transaction.append_tuple(table_codec, &table_name, chunk, &serializers, false)?;
            size += 1;
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), size.to_string());
        arena.resume();
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::copy::ExtSource;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::errors::DatabaseError;
    use crate::storage::Storage;
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use std::io::Write;
    use tempfile::TempDir;
    use ulid::Ulid;

    #[test]
    fn read_csv() -> Result<(), DatabaseError> {
        let csv = "1,1.5,one\n2,2.5,two\n";

        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        write!(file, "{csv}").expect("failed to write file");

        let tmp_dir = TempDir::new()?;
        let mut db = DataBaseBuilder::path(tmp_dir.path()).build_rocksdb()?;
        db.ddl("create table test_copy (a int primary key, b float, c varchar(10))")?;
        db.load(CatalogKind::Table("test_copy".to_string().into()))?;

        fn test_column(
            name: &str,
            ty: LogicalType,
            primary_key: Option<usize>,
        ) -> Result<ColumnCatalog, DatabaseError> {
            let mut column = ColumnCatalog::new(
                name.to_string(),
                false,
                ColumnDesc::new(ty, primary_key, false, None)?,
            );
            column.set_ref_table("t1".to_string().into(), Ulid::new(), false);
            Ok(column)
        }

        let mut plan_arena = crate::planner::PlanArena::new(db.state.table_arena());
        let columns = vec![
            plan_arena.alloc_column(test_column("a", LogicalType::Integer, Some(0))?),
            plan_arena.alloc_column(test_column("b", LogicalType::Float, None)?),
            plan_arena.alloc_column(test_column(
                "c",
                LogicalType::Varchar(Some(10), CharLengthUnits::Characters),
                None,
            )?),
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
            schema_ref: columns,
        };

        let transaction = db.storage.transaction()?;
        let mut executor = crate::execution::execute_mut(
            CopyFromFile::from(op),
            crate::execution::test_utils::empty_context(
                db.state.table_cache(),
                db.state.view_cache(),
                db.state.meta_cache(),
            ),
            plan_arena,
            &transaction,
        );

        let result = executor.next().expect("copy from file should yield once")?;
        assert_eq!(result.values[0].to_string(), "2");

        Ok(())
    }
}
