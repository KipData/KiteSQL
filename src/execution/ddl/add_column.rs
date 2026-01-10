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
use crate::execution::{build_read, spawn_executor, Executor, WriteExecutor};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, ViewCache};
use crate::types::index::{Index, IndexType};
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::{
    planner::operator::alter_table::add_column::AddColumnOperator, storage::Transaction, throw,
};
use itertools::Itertools;

pub struct AddColumn {
    op: AddColumnOperator,
    input: LogicalPlan,
}

impl From<(AddColumnOperator, LogicalPlan)> for AddColumn {
    fn from((op, input): (AddColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for AddColumn {
    fn execute_mut(
        mut self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let AddColumnOperator {
                table_name,
                column,
                if_not_exists,
            } = &self.op;

            let mut unique_values = column.desc().is_unique().then(Vec::new);
            let mut tuples = Vec::new();
            let schema = self.input.output_schema();
            let mut types = Vec::with_capacity(schema.len() + 1);

            for column_ref in schema.iter() {
                types.push(column_ref.datatype().clone());
            }
            types.push(column.datatype().clone());

            let mut coroutine = build_read(self.input, cache, transaction);

            for tuple in coroutine.by_ref() {
                let mut tuple: Tuple = throw!(co, tuple);

                if let Some(value) = throw!(co, column.default_value()) {
                    if let Some(unique_values) = &mut unique_values {
                        unique_values.push((
                            throw!(
                                co,
                                tuple.pk.clone().ok_or(DatabaseError::PrimaryKeyNotFound)
                            ),
                            value.clone(),
                        ));
                    }
                    tuple.values.push(value);
                } else {
                    tuple.values.push(DataValue::Null);
                }
                tuples.push(tuple);
            }
            drop(coroutine);

            let serializers = types.iter().map(|ty| ty.serializable()).collect_vec();
            for tuple in tuples {
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.append_tuple(
                        table_name,
                        tuple,
                        &serializers,
                        true
                    )
                );
            }
            let col_id = throw!(
                co,
                unsafe { &mut (*transaction) }.add_column(
                    cache.0,
                    table_name,
                    column,
                    *if_not_exists
                )
            );

            // Unique Index
            if let (Some(unique_values), Some(unique_meta)) = (
                unique_values,
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .and_then(|table| table.get_unique_index(&col_id))
                .cloned(),
            ) {
                for (tuple_id, value) in unique_values {
                    let index = Index::new(unique_meta.id, &value, IndexType::Unique);
                    throw!(
                        co,
                        unsafe { &mut (*transaction) }.add_index(table_name, index, &tuple_id)
                    );
                }
            }

            co.yield_(Ok(TupleBuilder::build_result("1".to_string())))
                .await;
        })
    }
}
