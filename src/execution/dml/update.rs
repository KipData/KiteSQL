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
use crate::execution::{
    build_read, with_projection_tmp_value, ExecArena, ExecId, ExecNode, ExecutionContext,
    ExecutorNode, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::iter_ext::Itertools;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::{Schema, Tuple};
use crate::types::tuple_builder::TupleBuilder;
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
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut executor = input;
        executor.input_schema = executor.input_plan.take_schema(plan_arena);
        executor.input = Some(build_read(
            arena,
            plan_arena,
            executor.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Update(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Update {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let mut exprs_map = HashMap::with_capacity(self.value_exprs.len());
        for (column, expr) in self.value_exprs.drain(..) {
            exprs_map.insert(plan_arena.column(column).id(), expr);
        }

        let table_cache = arena.context().table_cache();
        let transaction = arena.transaction();
        let table_snapshot = {
            transaction
                .table(table_cache, self.table_name.clone())?
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

            while arena.next_tuple(input, plan_arena)? {
                let mut tuple = arena.result_tuple().clone();
                let mut is_overwrite = true;

                let Some(old_pk) = tuple.pk.clone() else {
                    continue;
                };
                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    with_projection_tmp_value(arena, Some(&tuple), exprs, |arena, value| {
                        let mut state = arena.local_state(plan_arena);
                        let (transaction, table_codec) = state.transaction_codec_mut();
                        let index = Index::new(index_meta.id, &value, index_meta.ty);
                        transaction.del_index(table_codec, &self.table_name, &index, &old_pk)
                    })?;
                }
                for (i, column) in self.input_schema.iter().enumerate() {
                    if let Some(expr) = exprs_map.get(&plan_arena.column(*column).id()) {
                        let value = expr.eval(Some(&tuple))?;
                        tuple.values[i] = value;
                    }
                }

                tuple.pk = Some(Tuple::primary_projection(
                    table_snapshot.primary_key_indices,
                    &tuple.values,
                ));
                let new_pk = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;

                if new_pk != &old_pk {
                    let mut state = arena.local_state(plan_arena);
                    let (transaction, table_codec) = state.transaction_codec_mut();
                    transaction.remove_tuple(table_codec, &self.table_name, &old_pk)?;
                    is_overwrite = false;
                }
                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    with_projection_tmp_value(arena, Some(&tuple), exprs, |arena, value| {
                        let mut state = arena.local_state(plan_arena);
                        let (transaction, table_codec) = state.transaction_codec_mut();
                        let index = Index::new(index_meta.id, &value, index_meta.ty);
                        transaction.add_index(table_codec, &self.table_name, index, new_pk)
                    })?;
                }

                let mut state = arena.local_state(plan_arena);
                let (transaction, table_codec) = state.transaction_codec_mut();
                transaction.append_tuple(
                    table_codec,
                    &self.table_name,
                    tuple,
                    &serializers,
                    is_overwrite,
                )?;
                updated_count += 1;
            }

            TupleBuilder::build_result_into(arena.result_tuple_mut(), updated_count.to_string());
            arena.resume();
            Ok(())
        } else {
            TupleBuilder::build_result_into(arena.result_tuple_mut(), "0".to_string());
            arena.resume();
            Ok(())
        }
    }
}
