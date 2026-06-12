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
    build_read, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    WriteExecutor,
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
    type Input = (
        crate::planner::operator::delete::DeleteOperator,
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
        exec.input = Some(build_read(
            arena,
            plan_arena,
            exec.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Delete(exec))
    }
}
impl<'a> ExecutorNode<'a> for Delete {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            runtime.finish();
            return Ok(());
        };

        let index_templates = {
            let table = runtime
                .transaction_table(self.table_name.clone())?
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

        while runtime.next_tuple(input, plan_arena)? {
            let tuple = runtime.result_tuple().clone();
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
                        runtime.transaction_del_index(
                            &self.table_name,
                            &Index::new(*index_id, value, *index_ty),
                            tuple_id,
                        )?;
                    }
                }

                runtime.transaction_remove_tuple(&self.table_name, tuple_id)?;
                deleted_count += 1;
            }
        }

        TupleBuilder::build_result_into(runtime.result_tuple_mut(), deleted_count.to_string());
        runtime.resume();
        Ok(())
    }
}

struct Value {
    exprs: Vec<ScalarExpression>,
    values: Vec<DataValue>,
    index_ty: IndexType,
}
