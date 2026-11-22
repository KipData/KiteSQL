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
use crate::errors::DatabaseError;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::LogicalPlan;
use crate::storage::StorageTransaction;
use crate::types::LogicalType;

impl<T: StorageTransaction, A: AsRef<[(usize, LogicalType)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_create_index_source(
        &mut self,
        table_name: &str,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let source = self
            .context
            .source_and_bind(table_name, None, None, false, arena)?
            .ok_or(DatabaseError::SourceNotFound)?;
        match source {
            Source::Table(table) => TableScanOperator::build(table, true, arena),
            Source::View(view) => view.plan.clone_plan(arena),
            Source::Schema(_) => Err(DatabaseError::UnsupportedStmt(
                "derived source cannot be rebound as a base relation".to_string(),
            )),
        }
    }
}
