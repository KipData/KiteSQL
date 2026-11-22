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
    build_read, DDLApply, ExecBuilder, ExecContext, ExecNode, ExecutorNode, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::index::Index;
use crate::types::tuple::Schema;
use crate::types::value::DataValue;

pub struct CreateIndex<'a> {
    op: &'a CreateIndexOperator,
    input_schema: Schema,
    input: ExecNode<'a>,
}

impl<'a> WriteExecutor<'a> for CreateIndex<'a> {
    type Input = (&'a CreateIndexOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (op, input_plan): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input_schema = input_plan.read_schema().clone();
        let input = build_read(arena, plan_arena, input_plan);
        ExecNode::CreateIndex(Box::new(CreateIndex {
            op,
            input_schema,
            input,
        }))
    }
}

#[maybe_async]
impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for CreateIndex<'a> {
    async fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.input.reset(arena, plan_arena).await
    }

    async fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let CreateIndexOperator {
            table_id,
            index_name,
            columns,
            index_val_ty,
            if_not_exists,
            ty,
        } = self.op;

        let table_catalog = arena
            .table_cache()
            .get(table_id)
            .ok_or(DatabaseError::TableNotFound)?;
        if *if_not_exists
            && table_catalog
                .indexes()
                .any(|index| plan_arena.index(*index).name == *index_name)
        {
            arena.finish();
            return Ok(());
        }

        let mut column_exprs = Vec::with_capacity(columns.len());
        for &column in columns {
            let position = self
                .input_schema
                .iter()
                .position(|c| *c == column)
                .ok_or(DatabaseError::InvalidIndex)?;
            column_exprs.push(ScalarExpression::column_expr(column, position));
        }
        let index_id_result = {
            let mut decode = arena.decode_context();
            let (transaction, table_codec) = arena.transaction_codec_mut();
            let (table, index_meta) = transaction
                .add_index_meta(
                    table_codec,
                    plan_arena,
                    table_catalog.name(),
                    index_name.clone(),
                    columns,
                    index_val_ty.clone(),
                    *ty,
                    &mut decode,
                )
                .await?;
            let table_id = table.id();
            arena.push_ddl_apply(DDLApply::upsert_table(table, false));
            Ok((table_id, index_meta))
        };
        let (table_id, index_meta) = match index_id_result {
            Ok(ids) => ids,
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

        while self.input.next_tuple(arena, plan_arena).await? {
            if arena.result_tuple().pk().is_none() {
                continue;
            }
            arena.rewrite(&column_exprs, plan_arena, None::<&[DataValue]>)?;
            {
                let mut state = arena.local_state(plan_arena);
                let (tuple, transaction, table_codec) = state.tuple_transaction_codec_mut();
                let tuple_pk = tuple.pk().ok_or(DatabaseError::PrimaryKeyNotFound)?;
                let index = Index::from_meta(plan_arena.index(index_meta), tuple);
                transaction
                    .add_index(table_codec, table_id, index, tuple_pk)
                    .await?;
            }
        }

        arena.finish();
        Ok(())
    }
}
