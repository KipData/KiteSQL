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
use crate::execution::{build_read, spawn_executor, Executor, WriteExecutor};
use crate::expression::{BindPosition, ScalarExpression};
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::{Index, IndexId, IndexType};
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct Delete {
    table_name: TableName,
    input: LogicalPlan,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete { table_name, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Delete {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Delete {
                table_name,
                mut input,
            } = self;

            let schema = input.output_schema().clone();
            let table = throw!(
                co,
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .ok_or(DatabaseError::TableNotFound)
            );
            let mut indexes: HashMap<IndexId, Value> = HashMap::new();

            let mut deleted_count = 0;
            let mut coroutine = build_read(input, cache, transaction);

            for tuple in coroutine.by_ref() {
                let tuple: Tuple = throw!(co, tuple);

                for index_meta in table.indexes() {
                    if let Some(Value { exprs, values, .. }) = indexes.get_mut(&index_meta.id) {
                        let Some(data_value) = DataValue::values_to_tuple(throw!(
                            co,
                            Projection::projection(&tuple, exprs, &schema)
                        )) else {
                            continue;
                        };
                        values.push(data_value);
                    } else {
                        let mut values = Vec::with_capacity(table.indexes().len());
                        let mut exprs = throw!(co, index_meta.column_exprs(table));
                        throw!(
                            co,
                            BindPosition::bind_exprs(
                                exprs.iter_mut(),
                                || schema.iter().map(Cow::Borrowed),
                                |a, b| a == b
                            )
                        );
                        let Some(data_value) = DataValue::values_to_tuple(throw!(
                            co,
                            Projection::projection(&tuple, &exprs, &schema)
                        )) else {
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
                            throw!(
                                co,
                                unsafe { &mut (*transaction) }.del_index(
                                    &table_name,
                                    &Index::new(*index_id, value, *index_ty),
                                    tuple_id,
                                )
                            );
                        }
                    }

                    throw!(
                        co,
                        unsafe { &mut (*transaction) }.remove_tuple(&table_name, tuple_id)
                    );
                    deleted_count += 1;
                }
            }
            drop(coroutine);
            co.yield_(Ok(TupleBuilder::build_result(deleted_count.to_string())))
                .await;
        })
    }
}

struct Value {
    exprs: Vec<ScalarExpression>,
    values: Vec<DataValue>,
    index_ty: IndexType,
}
