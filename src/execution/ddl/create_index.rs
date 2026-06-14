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
    build_read, with_projection_tmp_value, DDLApply, ExecArena, ExecId, ExecNode, ExecutionContext,
    ExecutorNode, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Schema;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::ColumnId;

pub struct CreateIndex {
    op: Option<CreateIndexOperator>,
    input_schema: Schema,
    input_plan: LogicalPlan,
    input: ExecId,
}

impl From<(CreateIndexOperator, LogicalPlan)> for CreateIndex {
    fn from((op, input): (CreateIndexOperator, LogicalPlan)) -> Self {
        Self {
            op: Some(op),
            input_schema: Default::default(),
            input_plan: input,
            input: 0,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateIndex {
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
        executor.input = build_read(
            arena,
            plan_arena,
            executor.input_plan.take(),
            cache,
            transaction,
        );
        arena.push(ExecNode::CreateIndex(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CreateIndex {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(CreateIndexOperator {
            table_name,
            index_name,
            columns,
            if_not_exists,
            ty,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        if if_not_exists
            && arena.table_cache().get(&table_name).is_some_and(|table| {
                table
                    .indexes()
                    .any(|index| plan_arena.index(*index).name == index_name)
            })
        {
            arena.finish();
            return Ok(());
        }

        let (column_ids, column_exprs): (Vec<ColumnId>, Vec<ScalarExpression>) = columns
            .into_iter()
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
                &table_name,
                index_name,
                column_ids,
                ty,
            )?;
            arena.push_ddl_apply(DDLApply::upsert_table(table, false));
            Ok(index_id)
        };
        let index_id = match index_id_result {
            Ok(index_id) => index_id,
            Err(DatabaseError::DuplicateIndex(index_name)) => {
                if if_not_exists {
                    arena.finish();
                    return Ok(());
                } else {
                    return Err(DatabaseError::DuplicateIndex(index_name));
                }
            }
            Err(err) => return Err(err),
        };

        while arena.next_tuple(self.input, plan_arena)? {
            let Some(tuple_pk) = arena.result_tuple().pk.clone() else {
                continue;
            };
            with_projection_tmp_value(arena, None, &column_exprs, |arena, value| {
                let mut state = arena.local_state(plan_arena);
                let (transaction, table_codec) = state.transaction_codec_mut();
                let index = Index::new(index_id, &value, ty);
                transaction.add_index(table_codec, table_name.as_ref(), index, &tuple_pk)
            })?;
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), "1".to_string());
        arena.resume();
        Ok(())
    }
}
