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
use crate::execution::{
    DDLApply, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, WriteExecutor,
};
use crate::iter_ext::Itertools;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropColumn {
    op: Option<DropColumnOperator>,
}

impl From<DropColumnOperator> for DropColumn {
    fn from(op: DropColumnOperator) -> Self {
        Self { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropColumn {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::DropColumn(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for DropColumn {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let table_cache = arena.table_cache();
        let Some(DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        let (old_schema, pk_ty, column_info) = {
            let table_catalog = arena
                .transaction()
                .table(table_cache, table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let column_info = table_catalog
                .columns()
                .enumerate()
                .find_map(|(index, column)| {
                    let column = plan_arena.column(*column);
                    (column.name() == column_name).then(|| (index, column.desc().is_primary()))
                });
            (
                table_catalog.columns().copied().collect_vec(),
                table_catalog.primary_keys_type().clone(),
                column_info,
            )
        };
        if let Some((column_index, is_primary)) = column_info {
            if is_primary {
                return Err(DatabaseError::invalid_column(
                    "drop of primary key column is not allowed.".to_owned(),
                ));
            }
            {
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
                        old_schema
                            .iter()
                            .enumerate()
                            .filter(|(index, _)| *index != column_index)
                            .map(|(_, column)| plan_arena.column(*column).datatype().serializable())
                    },
                    |tuple| {
                        let _ = tuple.values.remove(column_index);
                        Ok(())
                    },
                    |_, _, _| Ok(()),
                )?;
            }
            {
                let (transaction, table_codec) = arena.transaction_codec_mut();
                let table =
                    transaction.drop_column(table_codec, plan_arena, &table_name, &column_name)?;
                arena.push_ddl_apply(DDLApply::upsert_table(table, true));
            }

            TupleBuilder::build_result_into(arena.result_tuple_mut(), "1".to_string());
            arena.resume();
            Ok(())
        } else if !if_exists {
            Err(DatabaseError::column_not_found(column_name))
        } else {
            arena.finish();
            Ok(())
        }
    }
}
