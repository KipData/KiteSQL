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
use crate::types::index::{Index, IndexMeta, IndexType};
use crate::types::tuple::{Schema, Tuple};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::ColumnId;
use std::{
    collections::{HashMap, HashSet},
    mem,
};

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

impl Update {
    fn index_needs_update(
        index_meta: &IndexMeta,
        updated_column_ids: &HashSet<ColumnId>,
        updates_primary_key: bool,
    ) -> bool {
        if matches!(index_meta.ty, IndexType::PrimaryKey { .. }) {
            return false;
        }

        updates_primary_key
            || index_meta
                .column_ids
                .iter()
                .any(|column_id| updated_column_ids.contains(column_id))
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
        let mut updated_column_ids = HashSet::with_capacity(self.value_exprs.len());
        for (column, expr) in self.value_exprs.drain(..) {
            let column = plan_arena.column(column);
            let column_id = column
                .id()
                .ok_or_else(|| DatabaseError::column_not_found(column.name().to_string()))?;
            updated_column_ids.insert(column_id);
            exprs_map.insert(column_id, expr);
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
            let updates_primary_key = table_snapshot.primary_key_indices.iter().any(|index| {
                table_snapshot
                    .columns
                    .get(*index)
                    .and_then(|column| plan_arena.column(*column).id())
                    .is_some_and(|column_id| updated_column_ids.contains(&column_id))
            });
            let serializers = self
                .input_schema
                .iter()
                .map(|column| plan_arena.column(*column).datatype().serializable())
                .collect_vec();

            let mut updated_count = 0;

            while arena.next_tuple(input, plan_arena)? {
                let mut is_overwrite = true;

                let Some(old_pk) = arena.result_tuple().pk.clone() else {
                    continue;
                };

                let mut old_index_values = Vec::new();
                for (index_offset, (index_meta, exprs)) in
                    table_snapshot.index_metas.iter().enumerate()
                {
                    let index_meta = plan_arena.index(*index_meta);
                    if !Self::index_needs_update(
                        index_meta,
                        &updated_column_ids,
                        updates_primary_key,
                    ) {
                        continue;
                    }

                    with_projection_tmp_value(arena, None, exprs, |_, value| {
                        old_index_values.push((index_offset, value));
                        Ok(())
                    })?;
                }
                for (i, column) in self.input_schema.iter().enumerate() {
                    let Some(column_id) = plan_arena.column(*column).id() else {
                        continue;
                    };
                    if let Some(expr) = exprs_map.get(&column_id) {
                        let value = expr.eval(Some(arena.result_tuple()))?;
                        arena.result_tuple_mut().values[i] = value;
                    }
                }

                let new_pk = Tuple::primary_projection(
                    table_snapshot.primary_key_indices,
                    &arena.result_tuple().values,
                );
                arena.result_tuple_mut().pk = Some(new_pk.clone());

                let primary_key_changed = new_pk != old_pk;
                if primary_key_changed {
                    let mut state = arena.local_state(plan_arena);
                    let (transaction, table_codec) = state.transaction_codec_mut();
                    transaction.remove_tuple(table_codec, &self.table_name, &old_pk)?;
                    is_overwrite = false;
                }

                for (index_offset, old_value) in old_index_values {
                    let (index_meta, exprs) = &table_snapshot.index_metas[index_offset];
                    let index_meta = plan_arena.index(*index_meta);
                    let index_id = index_meta.id;
                    let index_ty = index_meta.ty;
                    with_projection_tmp_value(arena, None, exprs, |arena, value| {
                        if !primary_key_changed && old_value == value {
                            return Ok(());
                        }

                        let mut state = arena.local_state(plan_arena);
                        let (transaction, table_codec) = state.transaction_codec_mut();
                        let old_index = Index::new(index_id, &old_value, index_ty);
                        transaction.del_index(
                            table_codec,
                            &self.table_name,
                            &old_index,
                            &old_pk,
                        )?;
                        let new_index = Index::new(index_id, &value, index_ty);
                        transaction.add_index(table_codec, &self.table_name, new_index, &new_pk)
                    })?;
                }

                let tuple = mem::take(arena.result_tuple_mut());
                let mut state = arena.local_state(plan_arena);
                let (transaction, table_codec) = state.transaction_codec_mut();
                transaction.append_tuple(
                    table_codec,
                    &self.table_name,
                    &tuple,
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
