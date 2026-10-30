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
use crate::execution::{build_read, ExecArena, ExecBuilder, ExecNode, ExecutorNode, WriteExecutor};
use crate::iter_ext::Itertools;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::index::{Index, IndexMeta, IndexType};
use crate::types::tuple::{Schema, Tuple};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::ColumnId;
use std::collections::{HashMap, HashSet};
use std::mem;

pub struct Update<'a> {
    table_name: &'a TableName,
    value_exprs: &'a [(ColumnRef, ExprRef)],
    input_schema: Schema,
    input: ExecNode<'a>,
    // Set once the statement ran; cleared when the executor is reset.
    done: bool,
}

impl<'a> WriteExecutor<'a> for Update<'a> {
    type Input = (&'a UpdateOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (
            UpdateOperator {
                table_name,
                value_exprs,
            },
            input_plan,
        ): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input_schema = input_plan.read_schema().clone();
        let input = build_read(arena, plan_arena, input_plan);
        ExecNode::Update(Box::new(Update {
            table_name,
            value_exprs,
            input_schema,
            input,
            done: false,
        }))
    }
}

impl Update<'_> {
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

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Update<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.done = false;
        self.input.reset(arena, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if mem::replace(&mut self.done, true) {
            arena.finish();
            return Ok(());
        }
        let input = &mut self.input;

        let mut exprs_map = HashMap::with_capacity(self.value_exprs.len());
        let mut updated_column_ids = HashSet::with_capacity(self.value_exprs.len());
        for &(column, expr) in self.value_exprs {
            let column = plan_arena.column(column);
            let column_id = column
                .id()
                .ok_or_else(|| DatabaseError::column_not_found(column.name().to_string()))?;
            updated_column_ids.insert(column_id);
            exprs_map.insert(column_id, expr);
        }

        let table_cache = arena.table_cache();
        let transaction = arena.transaction();
        let table_snapshot = {
            transaction
                .table(table_cache, self.table_name.clone())?
                .map(|table| table.dml_snapshot(plan_arena))
                .transpose()?
        };
        let mut updated_count = 0;

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
                .map(|column: &ColumnRef| plan_arena.column(*column).datatype().serializable())
                .collect_vec();

            while input.next_tuple(arena, plan_arena)? {
                let mut is_overwrite = true;

                let mut tuple = arena.take_tuple();
                let Some(old_pk) = tuple.pk().cloned() else {
                    continue;
                };

                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    if !Self::index_needs_update(
                        index_meta,
                        &updated_column_ids,
                        updates_primary_key,
                    ) {
                        continue;
                    }

                    arena.rewrite(exprs, plan_arena, Some(&tuple))?;
                    let mut state = arena.local_state(plan_arena);
                    let (values, transaction, table_codec) = state.tuple_transaction_codec_mut();
                    let old_index = Index::new(index_meta.id, values, index_meta.ty);
                    transaction.del_index(table_codec, self.table_name, &old_index, &old_pk)?;
                }
                for (i, column) in self.input_schema.iter().enumerate() {
                    let Some(column_id) = plan_arena.column(*column).id() else {
                        continue;
                    };
                    if let Some(expr) = exprs_map.get(&column_id) {
                        let value = plan_arena
                            .expression(*expr)
                            .eval(plan_arena, Some(&tuple))?;
                        tuple[i] = value.into_owned();
                    }
                }

                let new_pk = Tuple::primary_projection(table_snapshot.primary_key_indices, &tuple);
                let primary_key_changed = new_pk != old_pk;
                if primary_key_changed {
                    let mut state = arena.local_state(plan_arena);
                    let (transaction, table_codec) = state.transaction_codec_mut();
                    transaction.remove_tuple(table_codec, self.table_name, &old_pk)?;
                    is_overwrite = false;
                }

                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    if !Self::index_needs_update(
                        index_meta,
                        &updated_column_ids,
                        updates_primary_key,
                    ) {
                        continue;
                    }
                    arena.rewrite(exprs, plan_arena, Some(&tuple))?;
                    let mut state = arena.local_state(plan_arena);
                    let (values, transaction, table_codec) = state.tuple_transaction_codec_mut();
                    let new_index = Index::new(index_meta.id, values, index_meta.ty);
                    transaction.add_index(table_codec, self.table_name, new_index, &new_pk)?;
                }

                tuple.set_pk(Some(new_pk));
                let mut state = arena.local_state(plan_arena);
                let (transaction, table_codec) = state.transaction_codec_mut();
                let stamp = if is_overwrite { 0 } else { table_codec.stamp() };
                table_codec.with_stamp(stamp, |table_codec| {
                    transaction.append_tuple(
                        table_codec,
                        self.table_name,
                        &tuple,
                        &serializers,
                        is_overwrite,
                    )
                })?;
                updated_count += 1;
            }
        }
        arena.produce(TupleBuilder::build_result(updated_count.to_string()).into());
        arena.resume();
        Ok(())
    }
}
