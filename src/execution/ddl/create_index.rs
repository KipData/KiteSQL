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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;

pub struct CreateIndex {
    op: Option<CreateIndexOperator>,
    input_schema: SchemaRef,
    input_plan: Option<LogicalPlan>,
    input: ExecId,
}

impl From<(CreateIndexOperator, LogicalPlan)> for CreateIndex {
    fn from((op, mut input): (CreateIndexOperator, LogicalPlan)) -> Self {
        Self {
            op: Some(op),
            input_schema: input.output_schema().clone(),
            input_plan: Some(input),
            input: 0,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateIndex {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = build_read(
            arena,
            self.input_plan
                .take()
                .expect("create index input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::CreateIndex(self))
    }
}

impl CreateIndex {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let table_cache = arena.table_cache();

        let Some(CreateIndexOperator {
            table_name,
            index_name,
            columns,
            if_not_exists,
            ty,
        }) = self.op.take()
        else {
            return Ok(None);
        };

        let (column_ids, column_exprs): (Vec<ColumnId>, Vec<ScalarExpression>) = columns
            .into_iter()
            .filter_map(|column| {
                column.id().and_then(|id| {
                    self.input_schema
                        .iter()
                        .position(|schema_column| schema_column == &column)
                        .map(|position| (id, ScalarExpression::column_expr(column, position)))
                })
            })
            .unzip();
        let index_id = match arena.transaction_mut().add_index_meta(
            table_cache,
            &table_name,
            index_name,
            column_ids,
            ty,
        ) {
            Ok(index_id) => index_id,
            Err(DatabaseError::DuplicateIndex(index_name)) => {
                if if_not_exists {
                    return Ok(None);
                } else {
                    return Err(DatabaseError::DuplicateIndex(index_name));
                }
            }
            Err(err) => return Err(err),
        };

        while let Some(tuple) = arena.next_tuple(self.input)? {
            let Some(value) = DataValue::values_to_tuple(Projection::projection(
                &tuple,
                &column_exprs,
                &self.input_schema,
            )?) else {
                continue;
            };
            let tuple_id = if let Some(tuple_id) = tuple.pk.as_ref() {
                tuple_id
            } else {
                continue;
            };
            let index = Index::new(index_id, &value, ty);
            arena
                .transaction_mut()
                .add_index(table_name.as_ref(), index, tuple_id)?;
        }

        Ok(Some(TupleBuilder::build_result("1".to_string())))
    }
}
