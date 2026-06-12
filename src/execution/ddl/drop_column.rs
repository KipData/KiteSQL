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
use crate::execution::{ExecArena, ExecId, ExecNode, ReadExecutionContext, WriteExecutor};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;
use itertools::Itertools;

pub struct DropColumn {
    op: Option<DropColumnOperator>,
}

impl From<DropColumnOperator> for DropColumn {
    fn from(op: DropColumnOperator) -> Self {
        Self { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropColumn {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::DropColumn(self))
    }
}

impl DropColumn {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
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

        let (tuple_columns, pk_ty) = {
            let table_catalog = arena
                .transaction()
                .table(table_cache, table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            (
                table_catalog.schema_ref().clone(),
                table_catalog.primary_keys_type().clone(),
            )
        };
        if let Some((column_index, is_primary)) = tuple_columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == column_name)
            .map(|(i, column)| (i, column.desc().is_primary()))
        {
            if is_primary {
                return Err(DatabaseError::invalid_column(
                    "drop of primary key column is not allowed.".to_owned(),
                ));
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
            let mut state = arena.local_state();
            let (transaction, table_codec) = state.transaction_codec_mut();
            rewrite_table_in_batches(
                transaction,
                table_codec,
                &table_name,
                &pk_ty,
                &old_deserializers,
                tuple_columns.len(),
                &serializers,
                |tuple| {
                    let _ = tuple.values.remove(column_index);
                    Ok(())
                },
                |_, _, _| Ok(()),
            )?;
            let mut state = arena.local_state();
            let (transaction, table_codec, context) = state.write_context_mut();
            let (table_cache, meta_cache) = context.table_meta_cache_mut();
            transaction.drop_column(
                table_codec,
                table_cache,
                meta_cache,
                &table_name,
                &column_name,
            )?;

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
