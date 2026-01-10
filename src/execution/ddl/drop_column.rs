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
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;

pub struct DropColumn {
    op: DropColumnOperator,
    input: LogicalPlan,
}

impl From<(DropColumnOperator, LogicalPlan)> for DropColumn {
    fn from((op, input): (DropColumnOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropColumn {
    fn execute_mut(
        mut self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let DropColumnOperator {
                table_name,
                column_name,
                if_exists,
            } = self.op;

            let tuple_columns = self.input.output_schema();
            if let Some((column_index, is_primary)) = tuple_columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name() == column_name)
                .map(|(i, column)| (i, column.desc().is_primary()))
            {
                if is_primary {
                    throw!(
                        co,
                        Err(DatabaseError::InvalidColumn(
                            "drop of primary key column is not allowed.".to_owned(),
                        ))
                    );
                }
                let mut tuples = Vec::new();
                let mut types = Vec::with_capacity(tuple_columns.len() - 1);

                for (i, column_ref) in tuple_columns.iter().enumerate() {
                    if i == column_index {
                        continue;
                    }
                    types.push(column_ref.datatype().clone());
                }
                let mut coroutine = build_read(self.input, cache, transaction);

                for tuple in coroutine.by_ref() {
                    let mut tuple: Tuple = throw!(co, tuple);
                    let _ = tuple.values.remove(column_index);

                    tuples.push(tuple);
                }
                drop(coroutine);
                let serializers = types.iter().map(|ty| ty.serializable()).collect_vec();
                for tuple in tuples {
                    throw!(
                        co,
                        unsafe { &mut (*transaction) }.append_tuple(
                            &table_name,
                            tuple,
                            &serializers,
                            true
                        )
                    );
                }
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.drop_column(
                        cache.0,
                        cache.2,
                        &table_name,
                        &column_name
                    )
                );

                co.yield_(Ok(TupleBuilder::build_result("1".to_string())))
                    .await;
            } else if !if_exists {
                co.yield_(Err(DatabaseError::ColumnNotFound(column_name)))
                    .await;
            }
        })
    }
}
