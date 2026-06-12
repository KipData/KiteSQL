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
use crate::execution::dql::projection::Projection;
use crate::execution::{
    build_read, DDLApply, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode,
    ReadExecutionContext, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::Schema;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
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
    type Input = (
        crate::planner::operator::create_index::CreateIndexOperator,
        LogicalPlan,
    );

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut exec = Self::from(input);
        exec.input_schema = exec.input_plan.take_schema(plan_arena);
        exec.input = build_read(
            arena,
            plan_arena,
            exec.input_plan.take(),
            cache,
            transaction,
        );
        arena.push(ExecNode::CreateIndex(exec))
    }
}
impl<'a> ExecutorNode<'a> for CreateIndex {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
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
            runtime.finish();
            return Ok(());
        };

        if if_not_exists
            && runtime
                .transaction_table(table_name.clone())?
                .is_some_and(|table| {
                    table
                        .indexes()
                        .any(|index| plan_arena.index(*index).name == index_name)
                })
        {
            runtime.finish();
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
        let index_id_result = runtime
            .transaction_add_index_meta(plan_arena, &table_name, index_name, column_ids, ty)
            .map(|(table, index_id)| {
                runtime.push_ddl_apply(DDLApply::upsert_table(table, false));
                index_id
            });
        let index_id = match index_id_result {
            Ok(index_id) => index_id,
            Err(DatabaseError::DuplicateIndex(index_name)) => {
                if if_not_exists {
                    runtime.finish();
                    return Ok(());
                } else {
                    return Err(DatabaseError::DuplicateIndex(index_name));
                }
            }
            Err(err) => return Err(err),
        };

        while runtime.next_tuple(self.input, plan_arena)? {
            let (value, tuple_pk) = {
                let tuple = runtime.result_tuple();
                let Some(value) =
                    DataValue::values_to_tuple(Projection::projection(tuple, &column_exprs)?)
                else {
                    continue;
                };
                let Some(tuple_pk) = tuple.pk.clone() else {
                    continue;
                };
                (value, tuple_pk)
            };
            let index = Index::new(index_id, &value, ty);
            runtime.transaction_add_index(table_name.as_ref(), index, &tuple_pk)?;
        }

        TupleBuilder::build_result_into(runtime.result_tuple_mut(), "1".to_string());
        runtime.resume();
        Ok(())
    }
}
