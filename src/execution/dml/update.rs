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

use crate::catalog::table::DmlTable;
use crate::errors::DatabaseError;
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, WriteExecutor,
};
use crate::planner::operator::update::UpdateOperator;
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use std::mem;

pub struct Update<'a> {
    table: &'a DmlTable,
    value_exprs: &'a [(usize, ExprRef)],
    input: ExecNode<'a>,
    // Set once the statement ran; cleared when the executor is reset.
    done: bool,
}

impl<'a> WriteExecutor<'a> for Update<'a> {
    type Input = (&'a UpdateOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (UpdateOperator { table, value_exprs }, input_plan): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input = build_read(arena, plan_arena, input_plan);
        ExecNode::Update(Box::new(Update {
            table,
            value_exprs,
            input,
            done: false,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Update<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.done = false;
        self.input.reset(arena, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if mem::replace(&mut self.done, true) {
            arena.finish();
            return Ok(());
        }
        let Update {
            table,
            value_exprs,
            input,
            ..
        } = self;
        let mut updated_count = 0;
        let mut values = Vec::with_capacity(value_exprs.len());

        while input.next_tuple(arena, plan_arena)? {
            let mut is_overwrite = true;

            let mut tuple = arena.take_tuple();
            let Some(old_pk) = tuple.pk().cloned() else {
                continue;
            };

            for (index_meta, exprs) in table.index_exprs.iter() {
                let index_meta = plan_arena.index(*index_meta);
                arena.rewrite(exprs, plan_arena, Some(&tuple))?;
                let mut state = arena.local_state(plan_arena);
                let (values, transaction, table_codec) = state.tuple_transaction_codec_mut();
                let old_index = Index::new(index_meta.id, values, index_meta.ty);
                transaction.del_index(table_codec, table.table_id, &old_index, &old_pk)?;
            }
            // Every expression reads the old row, so evaluate them all before writing.
            for (_, expr) in value_exprs.iter() {
                let value = plan_arena
                    .expression(*expr)
                    .eval(plan_arena, Some(&tuple))?;
                values.push(value.into_owned());
            }
            for ((position, _), value) in value_exprs.iter().zip(values.drain(..)) {
                tuple[*position] = value;
            }

            let new_pk = Tuple::primary_projection(&table.primary_key_indices, &tuple);
            let primary_key_changed = new_pk != old_pk;
            if primary_key_changed {
                let mut state = arena.local_state(plan_arena);
                let (transaction, table_codec) = state.transaction_codec_mut();
                transaction.remove_tuple(table_codec, table.table_id, &old_pk)?;
                is_overwrite = false;
            }

            for (index_meta, exprs) in table.index_exprs.iter() {
                let index_meta = plan_arena.index(*index_meta);
                arena.rewrite(exprs, plan_arena, Some(&tuple))?;
                let mut state = arena.local_state(plan_arena);
                let (values, transaction, table_codec) = state.tuple_transaction_codec_mut();
                let new_index = Index::new(index_meta.id, values, index_meta.ty);
                transaction.add_index(table_codec, table.table_id, new_index, &new_pk)?;
            }

            tuple.set_pk(Some(new_pk));
            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec_mut();
            let stamp = if is_overwrite { 0 } else { table_codec.stamp() };
            table_codec.with_stamp(stamp, |table_codec| {
                transaction.append_tuple(
                    table_codec,
                    table.table_id,
                    &tuple,
                    &table.serializers,
                    is_overwrite,
                )
            })?;
            updated_count += 1;
        }
        arena.produce(TupleBuilder::build_result(updated_count.to_string()).into());
        arena.resume();
        Ok(())
    }
}
