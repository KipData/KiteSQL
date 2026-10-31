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

use crate::binder::Binder;
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::storage::StorageTransaction;
use crate::types::LogicalType;

impl<T: StorageTransaction, A: AsRef<[(usize, LogicalType)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_update(
        &mut self,
        table_name: &str,
        assignments: Vec<(ColumnRef, ExprRef)>,
        input: LogicalPlan,
        arena: &mut PlanArena<'_>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_catalog = self
            .context
            .table(table_name)?
            .ok_or(DatabaseError::TableNotFound)?;
        let mut value_exprs = Vec::with_capacity(assignments.len());
        for (column, expr) in assignments {
            let column = arena.column(column);
            let (position, _) = column
                .id()
                .and_then(|column_id| table_catalog.get_column_by_id(&column_id))
                .ok_or_else(|| DatabaseError::column_not_found(column.name().to_string()))?;
            if value_exprs.iter().any(|(updated, _)| *updated == position) {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "multiple assignments to the same column `{}`",
                    column.name()
                )));
            }
            value_exprs.push((position, expr));
        }

        let is_updated =
            |position: usize| value_exprs.iter().any(|(updated, _)| *updated == position);
        let updates_primary_key = table_catalog
            .primary_key_indices()
            .iter()
            .any(|position| is_updated(*position));
        // Only the indexes over an updated column, or all of them when the primary key
        // changes, need their keys rewritten.
        let table = table_catalog.dml_table(arena, |index| {
            updates_primary_key
                || index.column_ids.iter().any(|column_id| {
                    table_catalog
                        .get_column_by_id(column_id)
                        .is_some_and(|(position, _)| is_updated(position))
                })
        })?;
        Ok(LogicalPlan::new(
            Operator::Update(UpdateOperator { table, value_exprs }),
            Childrens::Only(Box::new(input)),
        ))
    }
}
