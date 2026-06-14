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

use super::{is_valid_identifier, Binder};
use crate::catalog::{ColumnCatalog, TableName};
use crate::errors::DatabaseError;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use std::collections::HashSet;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    // TODO: TableConstraint
    pub(crate) fn bind_create_table(
        &mut self,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut names = HashSet::new();
        for column in &columns {
            if !names.insert(column.name()) {
                return Err(DatabaseError::DuplicateColumn(column.name().to_string()));
            }
            if !is_valid_identifier(column.name()) {
                return Err(DatabaseError::invalid_column(
                    "illegal column naming".to_string(),
                ));
            }
        }

        if columns.iter().filter(|col| col.desc().is_primary()).count() == 0 {
            return Err(DatabaseError::invalid_table(
                "the primary key field must exist and have at least one".to_string(),
            ));
        }

        Ok(LogicalPlan::new(
            Operator::CreateTable(CreateTableOperator {
                table_name,
                columns,
                if_not_exists,
            }),
            Childrens::None,
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::BinderContext;
    use crate::catalog::ColumnDesc;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::Storage;
    use crate::types::CharLengthUnits;
    use crate::types::LogicalType;
    use tempfile::TempDir;

    #[test]
    fn test_create_bind() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let table_cache = crate::storage::TableCache::default();
        let view_cache = crate::storage::ViewCache::default();
        let scala_functions = Default::default();
        let table_functions = Default::default();

        let sql = "create table t1 (id int primary key, name varchar(10) null)";
        let mut binder = Binder::new(
            BinderContext::new(
                &table_cache,
                &view_cache,
                &transaction,
                &scala_functions,
                &table_functions,
            ),
            &[],
            None,
        );
        let stmt = crate::parser::parse_sql(sql).unwrap();
        let stmt = stmt.into_iter().next().unwrap();
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let plan1 = binder.bind(&stmt, &mut plan_arena).unwrap();

        match plan1.operator {
            Operator::CreateTable(op) => {
                assert_eq!(op.table_name.as_ref(), "t1");
                assert_eq!(op.columns[0].name(), "id");
                assert!(!op.columns[0].nullable());
                assert_eq!(
                    op.columns[0].desc(),
                    &ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?
                );
                assert_eq!(op.columns[1].name(), "name");
                assert!(op.columns[1].nullable());
                assert_eq!(
                    op.columns[1].desc(),
                    &ColumnDesc::new(
                        LogicalType::Varchar(Some(10), CharLengthUnits::Characters),
                        None,
                        false,
                        None
                    )?
                );
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
