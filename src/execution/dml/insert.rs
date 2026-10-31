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
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::index::Index;
use crate::types::tuple::{Tuple, TupleImpl};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::mem;

pub struct Insert<'a> {
    table: &'a DmlTable,
    // Its rows hold all columns of the table, in table order.
    input: ExecNode<'a>,
    // Reused by every row of every execution.
    tuple: TupleImpl,
    // Set once the statement ran; cleared when the executor is reset.
    done: bool,
    is_overwrite: bool,
}

impl<'a> WriteExecutor<'a> for Insert<'a> {
    type Input = (&'a InsertOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (
            InsertOperator {
                table,
                is_overwrite,
            },
            input_plan,
        ): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input = build_read(arena, plan_arena, input_plan);
        ExecNode::Insert(Box::new(Insert {
            table,
            input,
            tuple: TupleImpl::from(Tuple::new(None, Vec::with_capacity(table.columns_len))),
            done: false,
            is_overwrite: *is_overwrite,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Insert<'a> {
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
        let Insert {
            table,
            input,
            tuple,
            is_overwrite,
            ..
        } = self;
        if table.primary_key_indices.is_empty() {
            return Err(DatabaseError::not_null());
        }
        let mut inserted_count = 0;

        while input.next_tuple(arena, plan_arena)? {
            tuple.clear();
            arena.result_mut().drain_values(|value| tuple.push(value));
            for (i, column) in table.columns.iter().enumerate() {
                let column = plan_arena.column(*column);
                let value = mem::replace(&mut tuple[i], DataValue::Null).cast(column.datatype())?;
                value.check_len(column.datatype())?;
                if value.is_null() && !column.nullable() {
                    return Err(DatabaseError::not_null_column(column.name().to_string()));
                }
                tuple[i] = value;
            }
            tuple.set_pk(Some(Tuple::primary_projection(
                &table.primary_key_indices,
                &*tuple,
            )));
            for (index_meta, exprs) in table.index_exprs.iter() {
                let index_meta = plan_arena.index(*index_meta);
                let tuple_id = tuple.pk().ok_or(DatabaseError::PrimaryKeyNotFound)?;
                arena.rewrite(exprs, plan_arena, Some(&*tuple))?;
                let mut state = arena.local_state(plan_arena);
                let (values, transaction, table_codec) = state.tuple_transaction_codec_mut();
                let index = Index::new(index_meta.id, values, index_meta.ty);
                transaction.add_index(table_codec, table.table_id, index, tuple_id)?;
            }
            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec_mut();
            transaction.append_tuple(
                table_codec,
                table.table_id,
                &*tuple,
                &table.serializers,
                *is_overwrite,
            )?;
            inserted_count += 1;
        }
        arena.produce(TupleBuilder::build_result(inserted_count.to_string()).into());
        arena.resume();
        Ok(())
    }
}
