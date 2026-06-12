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

use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::{Schema, Tuple};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use itertools::Itertools;
use std::collections::HashMap;

pub struct Update {
    table_name: TableName,
    value_exprs: Vec<(ColumnRef, ScalarExpression)>,
    input_schema: Schema,
    input_plan: LogicalPlan,
    input: Option<ExecId>,
}

impl From<(UpdateOperator, LogicalPlan)> for Update {
    fn from(
        (
            UpdateOperator {
                table_name,
                value_exprs,
            },
            input,
        ): (UpdateOperator, LogicalPlan),
    ) -> Self {
        Update {
            table_name,
            value_exprs,
            input_schema: Default::default(),
            input_plan: input,
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Update {
    type Input = (
        crate::planner::operator::update::UpdateOperator,
        LogicalPlan,
    );

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut exec = Self::from(input);
        exec.input_schema = exec.input_plan.take_schema(plan_arena);
        exec.input = Some(build_read(
            arena,
            plan_arena,
            exec.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Update(exec))
    }
}
impl<'a> ExecutorNode<'a> for Update {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            runtime.finish();
            return Ok(());
        };

        let mut exprs_map = HashMap::with_capacity(self.value_exprs.len());
        for (column, expr) in self.value_exprs.drain(..) {
            exprs_map.insert(plan_arena.column(column).id(), expr);
        }

        let table_snapshot = {
            runtime
                .transaction_table(self.table_name.clone())?
                .map(|table| table.dml_snapshot(plan_arena))
                .transpose()?
        };
        if let Some(table_snapshot) = table_snapshot {
            let serializers = self
                .input_schema
                .iter()
                .map(|column| plan_arena.column(*column).datatype().serializable())
                .collect_vec();

            let mut updated_count = 0;

            while runtime.next_tuple(input, plan_arena)? {
                let mut tuple = runtime.result_tuple().clone();
                let mut is_overwrite = true;

                let Some(old_pk) = tuple.pk.clone() else {
                    continue;
                };
                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    let values = Projection::projection(&tuple, exprs)?;
                    let Some(value) = DataValue::values_to_tuple(values) else {
                        continue;
                    };
                    let index = Index::new(index_meta.id, &value, index_meta.ty);
                    runtime.transaction_del_index(&self.table_name, &index, &old_pk)?;
                }
                for (i, column) in self.input_schema.iter().enumerate() {
                    if let Some(expr) = exprs_map.get(&plan_arena.column(*column).id()) {
                        let value = expr.eval(Some(&tuple))?;
                        tuple.values[i] = value;
                    }
                }

                tuple.pk = Some(Tuple::primary_projection(
                    &table_snapshot.primary_key_indices,
                    &tuple.values,
                ));
                let new_pk = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;

                if new_pk != &old_pk {
                    runtime.transaction_remove_tuple(&self.table_name, &old_pk)?;
                    is_overwrite = false;
                }
                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    let values = Projection::projection(&tuple, exprs)?;
                    let Some(value) = DataValue::values_to_tuple(values) else {
                        continue;
                    };
                    let index = Index::new(index_meta.id, &value, index_meta.ty);
                    runtime.transaction_add_index(&self.table_name, index, new_pk)?;
                }

                runtime.transaction_append_tuple(
                    &self.table_name,
                    tuple,
                    &serializers,
                    is_overwrite,
                )?;
                updated_count += 1;
            }

            TupleBuilder::build_result_into(runtime.result_tuple_mut(), updated_count.to_string());
            runtime.resume();
            Ok(())
        } else {
            TupleBuilder::build_result_into(runtime.result_tuple_mut(), "0".to_string());
            runtime.resume();
            Ok(())
        }
    }
}
