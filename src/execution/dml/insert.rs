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

use crate::catalog::{ColumnCatalog, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, spawn_executor, Executor, WriteExecutor};
use crate::expression::BindPosition;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct Insert {
    table_name: TableName,
    input: LogicalPlan,
    is_overwrite: bool,
    is_mapping_by_name: bool,
}

impl From<(InsertOperator, LogicalPlan)> for Insert {
    fn from(
        (
            InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name,
            },
            input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input,
            is_overwrite,
            is_mapping_by_name,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum MappingKey<'a> {
    Name(&'a str),
    Id(Option<ColumnId>),
}

impl ColumnCatalog {
    fn key(&self, is_mapping_by_name: bool) -> MappingKey<'_> {
        if is_mapping_by_name {
            MappingKey::Name(self.name())
        } else {
            MappingKey::Id(self.id())
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Insert {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Insert {
                table_name,
                mut input,
                is_overwrite,
                is_mapping_by_name,
            } = self;

            let schema = input.output_schema().clone();

            let primary_keys = schema
                .iter()
                .filter_map(|column| column.desc().primary().map(|i| (i, column)))
                .sorted_by_key(|(i, _)| *i)
                .map(|(_, col)| col.key(is_mapping_by_name))
                .collect_vec();
            if primary_keys.is_empty() {
                throw!(co, Err(DatabaseError::NotNull))
            }

            if let Some(table_catalog) = throw!(
                co,
                unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
            )
            .cloned()
            {
                let mut index_metas = Vec::new();
                for index_meta in table_catalog.indexes() {
                    let mut exprs = throw!(co, index_meta.column_exprs(&table_catalog));
                    throw!(
                        co,
                        BindPosition::bind_exprs(
                            exprs.iter_mut(),
                            || schema.iter().map(Cow::Borrowed),
                            |a, b| {
                                if is_mapping_by_name {
                                    a.name == b.name
                                } else {
                                    a == b
                                }
                            }
                        )
                    );
                    index_metas.push((index_meta, exprs));
                }

                let serializers = table_catalog
                    .columns()
                    .map(|column| column.datatype().serializable())
                    .collect_vec();
                let pk_indices = table_catalog.primary_keys_indices();
                let mut coroutine = build_read(input, cache, transaction);
                let mut inserted_count = 0;

                for tuple in coroutine.by_ref() {
                    let Tuple { values, .. } = throw!(co, tuple);

                    let mut tuple_map = HashMap::new();
                    for (i, value) in values.into_iter().enumerate() {
                        tuple_map.insert(schema[i].key(is_mapping_by_name), value);
                    }
                    let mut values = Vec::with_capacity(table_catalog.columns_len());

                    for col in table_catalog.columns() {
                        let value = {
                            let mut value = tuple_map.remove(&col.key(is_mapping_by_name));

                            if value.is_none() {
                                value = throw!(co, col.default_value());
                            }
                            value.unwrap_or(DataValue::Null)
                        };
                        if value.is_null() && !col.nullable() {
                            co.yield_(Err(DatabaseError::NotNull)).await;
                            return;
                        }
                        values.push(value)
                    }
                    let pk = Tuple::primary_projection(pk_indices, &values);
                    let tuple = Tuple::new(Some(pk), values);

                    for (index_meta, exprs) in index_metas.iter() {
                        let values = throw!(co, Projection::projection(&tuple, exprs, &schema));
                        let Some(value) = DataValue::values_to_tuple(values) else {
                            continue;
                        };
                        let tuple_id = throw!(
                            co,
                            tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)
                        );
                        let index = Index::new(index_meta.id, &value, index_meta.ty);
                        throw!(
                            co,
                            unsafe { &mut (*transaction) }.add_index(&table_name, index, tuple_id)
                        );
                    }
                    throw!(
                        co,
                        unsafe { &mut (*transaction) }.append_tuple(
                            &table_name,
                            tuple,
                            &serializers,
                            is_overwrite
                        )
                    );
                    inserted_count += 1;
                }
                drop(coroutine);

                co.yield_(Ok(TupleBuilder::build_result(inserted_count.to_string())))
                    .await;
            } else {
                co.yield_(Ok(TupleBuilder::build_result("0".to_string())))
                    .await;
            }
        })
    }
}
