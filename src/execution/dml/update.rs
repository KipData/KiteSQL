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
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, WriteExecutor,
};
use crate::iter_ext::Itertools;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::MetaArena;
use crate::storage::Transaction;
use crate::types::index::{Index, IndexMeta, IndexType};
use crate::types::tuple::{Schema, Tuple};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::ColumnId;
use std::collections::{HashMap, HashSet};

pub struct Update<'a> {
    table_name: &'a TableName,
    value_exprs: &'a [(ColumnRef, ExprRef)],
    input_schema: Schema,
    input: Option<ExecId>,
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Update<'a> {
    type Input = (&'a UpdateOperator, &'a LogicalPlan);

    fn into_executor(
        (
            UpdateOperator {
                table_name,
                value_exprs,
            },
            input_plan,
        ): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input_schema = input_plan.read_schema().clone();
        let input = Some(build_read(
            arena,
            plan_arena,
            input_plan,
            cache,
            transaction,
        ));
        arena.push(ExecNode::Update(Update {
            table_name,
            value_exprs,
            input_schema,
            input,
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

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Update<'a> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

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

        let table_cache = arena.context().table_cache();
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

            while arena.next_tuple(input, plan_arena)? {
                let mut is_overwrite = true;

                let mut tuple = arena.materialize_tuple();
                let Some(old_pk) = tuple.pk.clone() else {
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
                    let (values, transaction, table_codec) =
                        state.index_values_transaction_codec_mut();
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
                        tuple.values[i] = value.into_owned();
                    }
                }

                let new_pk =
                    Tuple::primary_projection(table_snapshot.primary_key_indices, &tuple.values);
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
                    let (values, transaction, table_codec) =
                        state.index_values_transaction_codec_mut();
                    let new_index = Index::new(index_meta.id, values, index_meta.ty);
                    transaction.add_index(table_codec, self.table_name, new_index, &new_pk)?;
                }

                tuple.pk = Some(new_pk);
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
        arena.produce_tuple(TupleBuilder::build_result(updated_count.to_string()));
        arena.resume();
        Ok(())
    }
}
