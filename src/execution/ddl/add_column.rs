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

use super::rewrite_table_in_batches;
use crate::errors::DatabaseError;
use crate::execution::{spawn_executor, Executor, WriteExecutor};
use crate::storage::{StatisticsMetaCache, TableCache, ViewCache};
use crate::types::index::{Index, IndexType};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::{
    planner::operator::alter_table::add_column::AddColumnOperator, storage::Transaction, throw,
};
use itertools::Itertools;

pub struct AddColumn {
    op: AddColumnOperator,
}

impl From<AddColumnOperator> for AddColumn {
    fn from(op: AddColumnOperator) -> Self {
        Self { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for AddColumn {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let AddColumnOperator {
                table_name,
                column,
                if_not_exists,
            } = &self.op;

            let table_catalog = throw!(
                co,
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .cloned()
                .ok_or(DatabaseError::TableNotFound)
            );
            if table_catalog.get_column_by_name(column.name()).is_some() {
                if *if_not_exists {
                    co.yield_(Ok(TupleBuilder::build_result("1".to_string())))
                        .await;
                    return;
                }
                co.yield_(Err(DatabaseError::DuplicateColumn(
                    column.name().to_string(),
                )))
                .await;
                return;
            }

            let schema = table_catalog.schema_ref().clone();
            let old_deserializers = schema
                .iter()
                .map(|column_ref| column_ref.datatype().serializable())
                .collect_vec();
            let serializers = schema
                .iter()
                .map(|column_ref| column_ref.datatype().serializable())
                .chain(::std::iter::once(column.datatype().serializable()))
                .collect_vec();
            let pk_ty = table_catalog.primary_keys_type().clone();
            let default_value = throw!(co, column.default_value());

            let col_id = throw!(
                co,
                unsafe { &mut (*transaction) }.add_column(
                    cache.0,
                    table_name,
                    column,
                    *if_not_exists
                )
            );
            let unique_meta = if column.desc().is_unique() {
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .and_then(|table| table.get_unique_index(&col_id))
                .cloned()
            } else {
                None
            };
            let default_for_index = default_value.clone();

            throw!(
                co,
                rewrite_table_in_batches(
                    unsafe { &mut (*transaction) },
                    table_name,
                    &pk_ty,
                    &old_deserializers,
                    schema.len(),
                    schema.len(),
                    &serializers,
                    |mut tuple| {
                        if let Some(value) = &default_value {
                            tuple.values.push(value.clone());
                        } else {
                            tuple.values.push(DataValue::Null);
                        }
                        Ok(tuple)
                    },
                    |transaction, tuple| {
                        if let (Some(unique_meta), Some(value), Some(tuple_id)) = (
                            unique_meta.as_ref(),
                            default_for_index.as_ref(),
                            tuple.pk.as_ref(),
                        ) {
                            let index = Index::new(unique_meta.id, value, IndexType::Unique);
                            transaction.add_index(table_name, index, tuple_id)?;
                        }
                        Ok(())
                    },
                )
            );

            co.yield_(Ok(TupleBuilder::build_result("1".to_string())))
                .await;
        })
    }
}
