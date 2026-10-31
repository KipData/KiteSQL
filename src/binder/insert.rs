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
use crate::catalog::table::DmlTable;
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::storage::StorageTransaction;
use crate::types::tuple::Schema;
use crate::types::value::DataValue;
use crate::types::LogicalType;

impl<T: StorageTransaction, A: AsRef<[(usize, LogicalType)]>> Binder<'_, '_, T, A> {
    pub(crate) fn dml_table(
        &self,
        table_name: &str,
        arena: &mut PlanArena<'_>,
    ) -> Result<DmlTable, DatabaseError> {
        self.context
            .table(table_name)?
            .ok_or(DatabaseError::TableNotFound)?
            .dml_table(arena, |_| true)
    }

    pub(crate) fn bind_insert_values(
        &mut self,
        table_name: &str,
        targets: Schema,
        rows: Vec<ExprRef>,
        row_count: usize,
        is_overwrite: bool,
        arena: &mut PlanArena<'_>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table = self.dml_table(table_name, arena)?;
        let mut values = Vec::with_capacity(row_count * table.columns.len());
        for row in rows.chunks(targets.len()) {
            for &column in table.columns.iter() {
                values.push(match Self::insert_target(&targets, column, arena) {
                    Some(position) => row[position],
                    None => Self::insert_default(column, arena)?,
                });
            }
        }
        let values_plan = self.bind_values(values, row_count, table.columns.clone());

        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table,
                is_overwrite,
            }),
            Childrens::Only(Box::new(values_plan)),
        ))
    }

    pub(crate) fn bind_insert_query(
        &mut self,
        table_name: &str,
        mut input_plan: LogicalPlan,
        targets: &[ColumnRef],
        is_overwrite: bool,
        arena: &mut PlanArena<'_>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table = self.dml_table(table_name, arena)?;
        // Project the input to all columns in table order, filling in defaults.
        let mut projection = Vec::with_capacity(table.columns.len());
        for &column in table.columns.iter() {
            projection.push(match Self::insert_target(targets, column, arena) {
                Some(position) => {
                    let input = input_plan.output_schema(arena)[position];
                    arena.alloc_expression(ScalarExpression::column_expr(input, position))
                }
                None => Self::insert_default(column, arena)?,
            });
        }
        let input_plan = self.bind_project(input_plan, projection, arena)?;

        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table,
                is_overwrite,
            }),
            Childrens::Only(Box::new(input_plan)),
        ))
    }

    fn insert_target(
        targets: &[ColumnRef],
        column: ColumnRef,
        arena: &PlanArena<'_>,
    ) -> Option<usize> {
        let column_id = arena.column(column).id();
        targets
            .iter()
            .rposition(|target| arena.column(*target).id() == column_id)
    }

    fn insert_default(
        column: ColumnRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<ExprRef, DatabaseError> {
        match arena.column(column).desc().default {
            Some(default) => default.clone_expression(arena),
            None => Ok(arena.alloc_expression(ScalarExpression::Constant(DataValue::Null))),
        }
    }

    pub(crate) fn bind_values(
        &mut self,
        rows: Vec<ExprRef>,
        row_count: usize,
        schema_ref: Schema,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Values(ValuesOperator::new(rows, row_count, schema_ref)),
            Childrens::None,
        )
    }
}
