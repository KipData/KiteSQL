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

use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, spawn_executor, Executor, WriteExecutor};
use crate::expression::{BindPosition, ScalarExpression};
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct Update {
    table_name: TableName,
    value_exprs: Vec<(ColumnRef, ScalarExpression)>,
    input: LogicalPlan,
}

impl From<(UpdateOperator, LogicalPlan)> for Update {
    fn from(
        (
            UpdateOperator {
                table_name,
                value_exprs,
            },
            input,
        ): (UpdateOperator, LogicalPlan),
    ) -> Self {
        Update {
            table_name,
            value_exprs,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Update {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Update {
                table_name,
                value_exprs,
                mut input,
            } = self;

            let mut exprs_map = HashMap::with_capacity(value_exprs.len());
            for (column, expr) in value_exprs {
                exprs_map.insert(column.id(), expr);
            }

            let input_schema = input.output_schema().clone();

            if let Some(table_catalog) = throw!(
                co,
                unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
            )
            .cloned()
            {
                let serializers = input_schema
                    .iter()
                    .map(|column| column.datatype().serializable())
                    .collect_vec();
                let mut index_metas = Vec::new();
                for index_meta in table_catalog.indexes() {
                    let mut exprs = throw!(co, index_meta.column_exprs(&table_catalog));
                    throw!(
                        co,
                        BindPosition::bind_exprs(
                            exprs.iter_mut(),
                            || input_schema.iter().map(Cow::Borrowed),
                            |a, b| a == b
                        )
                    );
                    index_metas.push((index_meta, exprs));
                }

                let mut coroutine = build_read(input, cache, transaction);
                let mut updated_count = 0;

                for tuple in coroutine.by_ref() {
                    let mut tuple: Tuple = throw!(co, tuple);

                    let mut is_overwrite = true;

                    let old_pk = throw!(
                        co,
                        tuple.pk.clone().ok_or(DatabaseError::PrimaryKeyNotFound)
                    );
                    for (index_meta, exprs) in index_metas.iter() {
                        let values =
                            throw!(co, Projection::projection(&tuple, exprs, &input_schema));
                        let Some(value) = DataValue::values_to_tuple(values) else {
                            continue;
                        };
                        let index = Index::new(index_meta.id, &value, index_meta.ty);
                        throw!(
                            co,
                            unsafe { &mut (*transaction) }.del_index(&table_name, &index, &old_pk)
                        );
                    }
                    for (i, column) in input_schema.iter().enumerate() {
                        if let Some(expr) = exprs_map.get(&column.id()) {
                            tuple.values[i] = throw!(co, expr.eval(Some((&tuple, &input_schema))));
                        }
                    }

                    tuple.pk = Some(Tuple::primary_projection(
                        table_catalog.primary_keys_indices(),
                        &tuple.values,
                    ));
                    let new_pk = throw!(
                        co,
                        tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)
                    );

                    if new_pk != &old_pk {
                        throw!(
                            co,
                            unsafe { &mut (*transaction) }.remove_tuple(&table_name, &old_pk)
                        );
                        is_overwrite = false;
                    }
                    for (index_meta, exprs) in index_metas.iter() {
                        let values =
                            throw!(co, Projection::projection(&tuple, exprs, &input_schema));
                        let Some(value) = DataValue::values_to_tuple(values) else {
                            continue;
                        };
                        let index = Index::new(index_meta.id, &value, index_meta.ty);
                        throw!(
                            co,
                            unsafe { &mut (*transaction) }.add_index(&table_name, index, new_pk)
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
                    updated_count += 1;
                }
                drop(coroutine);

                co.yield_(Ok(TupleBuilder::build_result(updated_count.to_string())))
                    .await;
            } else {
                co.yield_(Ok(TupleBuilder::build_result("0".to_string())))
                    .await;
            }
        })
    }
}
