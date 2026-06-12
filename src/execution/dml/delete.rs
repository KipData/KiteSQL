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
use crate::execution::dql::projection::Projection;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ReadExecutionContext, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::{Index, IndexId, IndexType};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::collections::HashMap;

pub struct Delete {
    table_name: TableName,
    input_plan: LogicalPlan,
    input: Option<ExecId>,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete {
            table_name,
            input_plan: input,
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Delete {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            plan_arena,
            self.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Delete(self))
    }
}

impl Delete {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let index_templates = {
            let table = arena
                .transaction()
                .table(arena.table_cache(), self.table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            table
                .indexes()
                .map(|index_meta| {
                    let index_meta = plan_arena.index(*index_meta);
                    Ok((
                        index_meta.id,
                        index_meta.ty,
                        index_meta.column_exprs(table, plan_arena)?,
                    ))
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?
        };
        let mut indexes: HashMap<IndexId, Value> = HashMap::new();

        let mut deleted_count = 0;

        while arena.next_tuple(input, plan_arena)? {
            let tuple = arena.result_tuple().clone();
            for (index_id, index_ty, exprs) in index_templates.iter() {
                if let Some(Value { exprs, values, .. }) = indexes.get_mut(index_id) {
                    let Some(data_value) =
                        DataValue::values_to_tuple(Projection::projection(&tuple, exprs)?)
                    else {
                        continue;
                    };
                    values.push(data_value);
                } else {
                    let Some(data_value) =
                        DataValue::values_to_tuple(Projection::projection(&tuple, exprs)?)
                    else {
                        continue;
                    };
                    let mut values = Vec::with_capacity(index_templates.len());
                    values.push(data_value);

                    indexes.insert(
                        *index_id,
                        Value {
                            exprs: exprs.clone(),
                            values,
                            index_ty: *index_ty,
                        },
                    );
                }
            }
            if let Some(tuple_id) = &tuple.pk {
                for (
                    index_id,
                    Value {
                        values, index_ty, ..
                    },
                ) in indexes.iter_mut()
                {
                    for value in values {
                        let mut state = arena.local_state(plan_arena);
                        let (transaction, table_codec) = state.transaction_codec_mut();
                        transaction.del_index(
                            table_codec,
                            &self.table_name,
                            &Index::new(*index_id, value, *index_ty),
                            tuple_id,
                        )?;
                    }
                }

                let mut state = arena.local_state(plan_arena);
                let (transaction, table_codec) = state.transaction_codec_mut();
                transaction.remove_tuple(table_codec, &self.table_name, tuple_id)?;
                deleted_count += 1;
            }
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), deleted_count.to_string());
        arena.resume();
        Ok(())
    }
}

struct Value {
    exprs: Vec<ScalarExpression>,
    values: Vec<DataValue>,
    index_ty: IndexType,
}
