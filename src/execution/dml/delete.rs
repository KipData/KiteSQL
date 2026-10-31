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

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, WriteExecutor,
};
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::index::{Index, IndexType};
use crate::types::tuple_builder::TupleBuilder;
use std::mem;

pub struct Delete<'a> {
    table_name: &'a TableName,
    input: ExecNode<'a>,
    // Set once the statement ran; cleared when the executor is reset.
    done: bool,
}

impl<'a> WriteExecutor<'a> for Delete<'a> {
    type Input = (&'a DeleteOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (DeleteOperator { table_name, .. }, input_plan): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input = build_read(arena, plan_arena, input_plan);
        ExecNode::Delete(Box::new(Delete {
            table_name,
            input,
            done: false,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Delete<'a> {
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
        let input = &mut self.input;

        let (table_id, index_templates) = {
            let table = arena
                .transaction()
                .table(arena.table_cache(), self.table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let index_templates = table
                .indexes()
                .filter(|index_meta| {
                    !matches!(
                        plan_arena.index(**index_meta).ty,
                        IndexType::PrimaryKey { .. }
                    )
                })
                .map(|index_meta| {
                    let index_meta = plan_arena.index(*index_meta);
                    Ok((
                        index_meta.id,
                        index_meta.ty,
                        index_meta
                            .column_exprs(table)
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?;
            (table.id(), index_templates)
        };
        let mut deleted_count = 0;

        while input.next_tuple(arena, plan_arena)? {
            let tuple = arena.take_tuple();
            let Some(tuple_id) = tuple.pk() else {
                continue;
            };
            for (index_id, index_ty, exprs) in index_templates.iter() {
                arena.rewrite(exprs, plan_arena, Some(&tuple))?;
                let mut state = arena.local_state(plan_arena);
                let (values, transaction, table_codec) = state.tuple_transaction_codec_mut();
                let index = Index::new(*index_id, values, *index_ty);
                transaction.del_index(table_codec, table_id, &index, tuple_id)?;
            }

            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec_mut();
            transaction.remove_tuple(table_codec, table_id, tuple_id)?;
            deleted_count += 1;
        }

        arena.produce(TupleBuilder::build_result(deleted_count.to_string()).into());
        arena.resume();
        Ok(())
    }
}
