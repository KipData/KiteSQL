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

use super::{rewrite_table_in_batches, visit_table_in_batches};
use crate::errors::DatabaseError;
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, WriteExecutor};
use crate::planner::operator::alter_table::change_column::{ChangeColumnOperator, NotNullChange};
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;

pub struct ChangeColumn {
    op: Option<ChangeColumnOperator>,
}

impl From<ChangeColumnOperator> for ChangeColumn {
    fn from(op: ChangeColumnOperator) -> Self {
        Self { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for ChangeColumn {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::ChangeColumn(self))
    }
}

impl ChangeColumn {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        let table_cache = arena.table_cache();
        let Some(ChangeColumnOperator {
            table_name,
            old_column_name,
            new_column_name,
            data_type,
            default_change,
            not_null_change,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        let table_catalog = arena
            .transaction_mut()
            .table(table_cache, table_name.clone())?
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;
        let schema = table_catalog.schema_ref().clone();
        let (column_index, old_column) = schema
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == old_column_name)
            .map(|(index, column)| (index, column.clone()))
            .ok_or_else(|| DatabaseError::column_not_found(old_column_name.clone()))?;
        let needs_data_rewrite = old_column.datatype() != &data_type;
        let needs_not_null_validation = matches!(not_null_change, NotNullChange::Set);

        if needs_data_rewrite {
            let Some(column_id) = old_column.id() else {
                return Err(DatabaseError::column_not_found(old_column_name.clone()));
            };
            let affected_index = table_catalog
                .indexes()
                .find(|index_meta| index_meta.column_ids.contains(&column_id));
            if let Some(index_meta) = affected_index {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "cannot alter type of indexed column `{}`; drop index `{}` first",
                    old_column_name, index_meta.name
                )));
            }
        }

        let old_deserializers = schema
            .iter()
            .map(|column| column.datatype().serializable())
            .collect_vec();
        let pk_ty = table_catalog.primary_keys_type().clone();

        if needs_data_rewrite {
            let serializers = schema
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    if index == column_index {
                        data_type.serializable()
                    } else {
                        column.datatype().serializable()
                    }
                })
                .collect_vec();
            let target_column_name = new_column_name.clone();
            let target_data_type = data_type.clone();
            rewrite_table_in_batches(
                arena.transaction_mut(),
                &table_name,
                &pk_ty,
                &old_deserializers,
                schema.len(),
                schema.len(),
                &serializers,
                |tuple| {
                    tuple.values[column_index] =
                        tuple.values[column_index].clone().cast(&target_data_type)?;
                    if needs_not_null_validation && tuple.values[column_index].is_null() {
                        return Err(DatabaseError::not_null_column(target_column_name.clone()));
                    }
                    Ok(())
                },
                |_, _| Ok(()),
            )?;
        } else if needs_not_null_validation {
            let target_column_name = new_column_name.clone();
            visit_table_in_batches(
                arena.transaction(),
                &table_name,
                &pk_ty,
                &old_deserializers,
                schema.len(),
                schema.len(),
                |tuple| {
                    if tuple.values[column_index].is_null() {
                        return Err(DatabaseError::not_null_column(target_column_name.clone()));
                    }
                    Ok(())
                },
            )?;
        }

        arena.transaction_mut().change_column(
            table_cache,
            &table_name,
            &old_column_name,
            &new_column_name,
            &data_type,
            &default_change,
            &not_null_change,
        )?;

        TupleBuilder::build_result_into(arena.result_tuple_mut(), format!("{table_name}"));
        arena.resume();
        Ok(())
    }
}
