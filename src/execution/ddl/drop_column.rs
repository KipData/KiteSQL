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
use super::rewrite_table_in_batches;
use crate::execution::{spawn_executor, Executor, WriteExecutor};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;

pub struct DropColumn {
    op: DropColumnOperator,
}

impl From<DropColumnOperator> for DropColumn {
    fn from(op: DropColumnOperator) -> Self {
        Self { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropColumn {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let DropColumnOperator {
                table_name,
                column_name,
                if_exists,
            } = self.op;

            let table_catalog = throw!(
                co,
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .cloned()
                .ok_or(DatabaseError::TableNotFound)
            );
            let tuple_columns = table_catalog.schema_ref().clone();
            if let Some((column_index, is_primary)) = tuple_columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name() == column_name)
                .map(|(i, column)| (i, column.desc().is_primary()))
            {
                if is_primary {
                    throw!(
                        co,
                        Err(DatabaseError::invalid_column(
                            "drop of primary key column is not allowed.".to_owned(),
                        ))
                    );
                }
                let old_deserializers = tuple_columns
                    .iter()
                    .map(|column| column.datatype().serializable())
                    .collect_vec();
                let serializers = tuple_columns
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != column_index)
                    .map(|(_, column)| column.datatype().serializable())
                    .collect_vec();
                let pk_ty = table_catalog.primary_keys_type().clone();
                throw!(
                    co,
                    rewrite_table_in_batches(
                        unsafe { &mut (*transaction) },
                        &table_name,
                        &pk_ty,
                        &old_deserializers,
                        tuple_columns.len(),
                        tuple_columns.len(),
                        &serializers,
                        |mut tuple| {
                            let _ = tuple.values.remove(column_index);
                            Ok(tuple)
                        },
                        |_, _| Ok(()),
                    )
                );
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
                co.yield_(Err(DatabaseError::column_not_found(column_name)))
                    .await;
            }
        })
    }
}
