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

use crate::errors::DatabaseError;
use crate::execution::{
    build_read, DDLApply, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode,
    WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::planner::MetaArena;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Schema;
use crate::types::ColumnId;

pub struct CreateIndex<'a> {
    op: &'a CreateIndexOperator,
    input_schema: Schema,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateIndex<'a> {
    type Input = (&'a CreateIndexOperator, &'a LogicalPlan);

    fn into_executor(
        (op, input_plan): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input_schema = input_plan.read_schema().clone();
        let input = build_read(arena, plan_arena, input_plan, cache, transaction);
        arena.push(ExecNode::CreateIndex(CreateIndex {
            op,
            input_schema,
            input,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CreateIndex<'a> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        let CreateIndexOperator {
            table_name,
            index_name,
            columns,
            if_not_exists,
            ty,
        } = self.op;

        if *if_not_exists
            && arena.table_cache().get(table_name).is_some_and(|table| {
                table
                    .indexes()
                    .any(|index| plan_arena.index(*index).name == *index_name)
            })
        {
            arena.finish();
            return Ok(());
        }

        let (column_ids, column_exprs): (Vec<ColumnId>, Vec<ScalarExpression>) = columns
            .iter()
            .copied()
            .filter_map(|column| {
                plan_arena.column(column).id().and_then(|id| {
                    self.input_schema
                        .iter()
                        .position(|schema_column| schema_column == &column)
                        .map(|position| (id, ScalarExpression::column_expr(column, position)))
                })
            })
            .unzip();
        let index_id_result = {
            let (transaction, table_codec) = arena.transaction_codec_mut();
            let (table, index_id) = transaction.add_index_meta(
                table_codec,
                plan_arena,
                table_name,
                index_name.clone(),
                column_ids,
                *ty,
            )?;
            arena.push_ddl_apply(DDLApply::upsert_table(table, false));
            Ok(index_id)
        };
        let index_id = match index_id_result {
            Ok(index_id) => index_id,
            Err(DatabaseError::DuplicateIndex(index_name)) => {
                if *if_not_exists {
                    arena.finish();
                    return Ok(());
                } else {
                    return Err(DatabaseError::DuplicateIndex(index_name));
                }
            }
            Err(err) => return Err(err),
        };

        while arena.next_tuple(self.input, plan_arena)? {
            if arena.result_tuple().pk.is_none() {
                continue;
            }
            arena.rewrite(&column_exprs, plan_arena, None)?;
            {
                let mut state = arena.local_state(plan_arena);
                let (tuple, transaction, table_codec) = state.tuple_transaction_codec_mut();
                let tuple_pk = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;
                let index = Index::new(index_id, &tuple.values, *ty);
                transaction.add_index(table_codec, table_name.as_ref(), index, tuple_pk)?;
            }
        }

        arena.finish();
        Ok(())
    }
}
