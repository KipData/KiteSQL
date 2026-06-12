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

        let (schema, pk_ty, column_exists) = {
            let table_catalog = arena
                .transaction()
                .table(table_cache, table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            (
                table_catalog.schema_ref().clone(),
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

        let old_deserializers = schema
            .iter()
            .map(|column_ref| column_ref.datatype().serializable())
            .collect_vec();
        let serializers = schema
            .iter()
            .map(|column_ref| column_ref.datatype().serializable())
            .chain(::std::iter::once(column.datatype().serializable()))
            .collect_vec();
        let default_value = column.default_value()?;

        let unique_meta = {
            let (transaction, context) = arena.write_context_mut();
            let table_cache = context.table_cache_mut();
            let col_id =
                transaction.add_column(table_cache, &table_name, &column, if_not_exists)?;
            if column.desc().is_unique() {
                table_cache
                    .get(&table_name)
                    .and_then(|table| table.get_unique_index(&col_id))
                    .cloned()
            } else {
                None
            }
        };
        let default_for_index = default_value.clone();

        rewrite_table_in_batches(
            arena.transaction_mut(),
            &table_name,
            &pk_ty,
            &old_deserializers,
            schema.len(),
            &serializers,
            |tuple| {
                if let Some(value) = &default_value {
                    tuple.values.push(value.clone());
                } else {
                    tuple.values.push(DataValue::Null);
                }
                Ok(())
            },
            |transaction, tuple| {
                if let (Some(unique_meta), Some(value), Some(tuple_id)) = (
                    unique_meta.as_ref(),
                    default_for_index.as_ref(),
                    tuple.pk.as_ref(),
                ) {
                    let index = Index::new(unique_meta.id, value, IndexType::Unique);
                    transaction.add_index(&table_name, index, tuple_id)?;
                }
                Ok(())
            },
        )?;

        TupleBuilder::build_result_into(arena.result_tuple_mut(), "1".to_string());
        arena.resume();
        Ok(())
    }
}
