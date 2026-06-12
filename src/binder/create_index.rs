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

use crate::binder::{Binder, Source};
use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::index::IndexType;
use crate::types::value::DataValue;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_create_index_source(
        &mut self,
        table_name: TableName,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let source = self
            .context
            .source_and_bind(table_name.clone(), None, None, false)?
            .ok_or(DatabaseError::SourceNotFound)?;
        match source {
            Source::Table(table) => {
                TableScanOperator::build(table_name.clone(), table, true, arena)
            }
            Source::View(view) => Ok(LogicalPlan::clone(&view.plan)),
            Source::Schema(_) => Err(DatabaseError::UnsupportedStmt(
                "derived source cannot be rebound as a base relation".to_string(),
            )),
        }
    }

    pub(crate) fn bind_create_index(
        &mut self,
        table_name: TableName,
        index_name: String,
        columns: Vec<ColumnRef>,
        if_not_exists: bool,
        is_unique: bool,
        input: LogicalPlan,
    ) -> Result<LogicalPlan, DatabaseError> {
        let ty = if is_unique {
            IndexType::Unique
        } else if columns.len() == 1 {
            IndexType::Normal
        } else {
            IndexType::Composite
        };

        Ok(LogicalPlan::new(
            Operator::CreateIndex(CreateIndexOperator {
                table_name,
                columns,
                index_name,
                if_not_exists,
                ty,
            }),
            Childrens::Only(Box::new(input)),
        ))
    }
}
