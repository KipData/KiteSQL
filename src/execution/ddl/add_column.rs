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
    DDLApply, ExecArena, ExecId, ExecNode, ReadExecutionContext, WriteExecutor,
};
use crate::planner::operator::alter_table::add_column::AddColumnOperator;
use crate::storage::Transaction;
use crate::types::index::{Index, IndexType};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use itertools::Itertools;

pub struct AddColumn {
    op: Option<AddColumnOperator>,
}

impl From<AddColumnOperator> for AddColumn {
    fn from(op: AddColumnOperator) -> Self {
        Self { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for AddColumn {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::AddColumn(self))
    }
}

impl AddColumn {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let table_cache = arena.table_cache();
        let Some(AddColumnOperator {
            table_name,
            column,
            if_not_exists,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        let (old_schema, pk_ty, column_exists) = {
            let table_catalog = arena
                .transaction()
                .table(table_cache, table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            (
                table_catalog.columns().copied().collect_vec(),
                table_catalog.primary_keys_type().clone(),
                table_catalog.get_column_by_name(column.name()).is_some(),
            )
        };
        if column_exists {
            if if_not_exists {
                TupleBuilder::build_result_into(arena.result_tuple_mut(), "1".to_string());
                arena.resume();
                return Ok(());
            }
            return Err(DatabaseError::DuplicateColumn(column.name().to_string()));
        }

        let default_value = column.default_value()?;

        let (unique_index_id, apply) = {
            let (transaction, table_codec) = arena.transaction_codec_mut();
            let (table, col_id) = transaction.add_column(
                table_codec,
                plan_arena,
                &table_name,
                &column,
                if_not_exists,
            )?;
            let unique_meta = if column.desc().is_unique() {
                table
                    .get_unique_index(&col_id, plan_arena)
                    .map(|index| plan_arena.index(index).id)
            } else {
                None
            };
            (unique_meta, DDLApply::upsert_table(table, false))
        };
        arena.push_ddl_apply(apply);
        let default_for_index = default_value.clone();

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
                    .map(|column| plan_arena.column(*column).datatype().serializable())
                    .chain(::std::iter::once(column.datatype().serializable()))
            },
            |tuple| {
                if let Some(value) = &default_value {
                    tuple.values.push(value.clone());
                } else {
                    tuple.values.push(DataValue::Null);
                }
                Ok(())
            },
            |transaction, table_codec, tuple| {
                if let (Some(unique_index_id), Some(value), Some(tuple_id)) = (
                    unique_index_id.as_ref(),
                    default_for_index.as_ref(),
                    tuple.pk.as_ref(),
                ) {
                    let index = Index::new(*unique_index_id, value, IndexType::Unique);
                    transaction.add_index(table_codec, &table_name, index, tuple_id)?;
                }
                Ok(())
            },
        )?;

        TupleBuilder::build_result_into(arena.result_tuple_mut(), "1".to_string());
        arena.resume();
        Ok(())
    }
}
