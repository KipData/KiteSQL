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
    build_read, take_plan, ExecArena, ExecId, ExecNode, ExecutionCaches, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::{Index, IndexId, IndexType};
use crate::types::tuple::SchemaRef;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::collections::HashMap;

pub struct Delete {
    table_name: TableName,
    input_schema: SchemaRef,
    input_plan: LogicalPlan,
    input: Option<ExecId>,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, mut input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete {
            table_name,
            input_schema: input.output_schema().clone(),
            input_plan: input,
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Delete {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            take_plan(&mut self.input_plan),
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
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let table = arena
            .transaction_mut()
            .table(arena.table_cache(), self.table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let mut indexes: HashMap<IndexId, Value> = HashMap::new();

        let mut deleted_count = 0;

        while arena.next_tuple(input)? {
            let tuple = arena.result_tuple().clone();
            for index_meta in table.indexes() {
                if let Some(Value { exprs, values, .. }) = indexes.get_mut(&index_meta.id) {
                    let Some(data_value) = DataValue::values_to_tuple(Projection::projection(
                        &tuple,
                        exprs,
                        &self.input_schema,
                    )?) else {
                        continue;
                    };
                    values.push(data_value);
                } else {
                    let mut values = Vec::with_capacity(table.indexes().len());
                    let exprs = index_meta.column_exprs(table)?;
                    let Some(data_value) = DataValue::values_to_tuple(Projection::projection(
                        &tuple,
                        &exprs,
                        &self.input_schema,
                    )?) else {
                        continue;
                    };
                    values.push(data_value);

                    indexes.insert(
                        index_meta.id,
                        Value {
                            exprs,
                            values,
                            index_ty: index_meta.ty,
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
                        arena.transaction_mut().del_index(
                            &self.table_name,
                            &Index::new(*index_id, value, *index_ty),
                            tuple_id,
                        )?;
                    }
                }

                arena
                    .transaction_mut()
                    .remove_tuple(&self.table_name, tuple_id)?;
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
