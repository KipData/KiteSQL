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
use crate::execution::{
    DDLApply, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, WriteExecutor,
};
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
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::ChangeColumn(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ChangeColumn {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
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

        let (old_schema, pk_ty, column_index, old_column_type, old_column_id, affected_index_name) = {
            let table_catalog = arena
                .transaction()
                .table(table_cache, table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let (column_index, old_column) = table_catalog
                .columns()
                .enumerate()
                .find_map(|(index, column)| {
                    let column = plan_arena.column(*column);
                    (column.name() == old_column_name).then_some((index, column))
                })
                .ok_or_else(|| DatabaseError::column_not_found(old_column_name.clone()))?;
            let old_column_id = old_column.id();
            let affected_index_name = old_column_id.and_then(|column_id| {
                table_catalog
                    .indexes()
                    .map(|index_meta| plan_arena.index(*index_meta))
                    .find(|index_meta| index_meta.column_ids.contains(&column_id))
                    .map(|index_meta| index_meta.name.clone())
            });
            (
                table_catalog.columns().copied().collect_vec(),
                table_catalog.primary_keys_type().clone(),
                column_index,
                old_column.datatype().clone(),
                old_column_id,
                affected_index_name,
            )
        };
        let needs_data_rewrite = old_column_type != data_type;
        let needs_not_null_validation = matches!(not_null_change, NotNullChange::Set);

        if needs_data_rewrite {
            let Some(_) = old_column_id else {
                return Err(DatabaseError::column_not_found(old_column_name.clone()));
            };
            if let Some(index_name) = affected_index_name {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "cannot alter type of indexed column `{old_column_name}`; drop index `{index_name}` first"
                )));
            }
        }

        if needs_data_rewrite {
            let target_column_name = new_column_name.clone();
            let target_data_type = data_type.clone();
            let mut state = arena.local_state(plan_arena);
            let plan_arena = state.plan_arena;
            let (transaction, table_codec) = state.transaction_codec_mut();
            rewrite_table_in_batches(
                transaction,
                table_codec,
                &table_name,
                &pk_ty,
                old_schema.len(),
                || {
                    old_schema
                        .iter()
                        .map(|column| plan_arena.column(*column).datatype().serializable())
                },
                || {
                    old_schema.iter().enumerate().map(|(index, column)| {
                        if index == column_index {
                            data_type.serializable()
                        } else {
                            plan_arena.column(*column).datatype().serializable()
                        }
                    })
                },
                |tuple| {
                    tuple.values[column_index] =
                        tuple.values[column_index].clone().cast(&target_data_type)?;
                    if needs_not_null_validation && tuple.values[column_index].is_null() {
                        return Err(DatabaseError::not_null_column(target_column_name.clone()));
                    }
                    Ok(())
                },
                |_, _, _| Ok(()),
            )?;
        } else if needs_not_null_validation {
            let target_column_name = new_column_name.clone();
            let mut state = arena.local_state(plan_arena);
            let plan_arena = state.plan_arena;
            let (transaction, table_codec) = state.transaction_codec_mut();
            visit_table_in_batches(
                transaction,
                table_codec,
                &table_name,
                &pk_ty,
                old_schema.len(),
                || {
                    old_schema
                        .iter()
                        .map(|column| plan_arena.column(*column).datatype().serializable())
                },
                |tuple| {
                    if tuple.values[column_index].is_null() {
                        return Err(DatabaseError::not_null_column(target_column_name.clone()));
                    }
                    Ok(())
                },
            )?;
        }

        let apply = {
            let (transaction, table_codec) = arena.transaction_codec_mut();
            let table = transaction.change_column(
                table_codec,
                plan_arena,
                &table_name,
                &old_column_name,
                &new_column_name,
                &data_type,
                &default_change,
                &not_null_change,
            )?;
            DDLApply::upsert_table(table, true)
        };
        arena.push_ddl_apply(apply);

        TupleBuilder::build_result_into(arena.result_tuple_mut(), format!("{table_name}"));
        arena.resume();
        Ok(())
    }
}
