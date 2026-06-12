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

use crate::errors::DatabaseError;
use crate::execution::{
    DDLApply, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    TupleValueSerializerIter, WriteExecutor,
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
    type Input = crate::planner::operator::alter_table::add_column::AddColumnOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::AddColumn(Self::from(input)))
    }
}
impl<'a> ExecutorNode<'a> for AddColumn {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(AddColumnOperator {
            table_name,
            column,
            if_not_exists,
        }) = self.op.take()
        else {
            runtime.finish();
            return Ok(());
        };

        let (old_schema, pk_ty, column_exists) = {
            let table_catalog = runtime
                .transaction_table(table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            (
                table_catalog.columns().copied().collect_vec(),
                table_catalog.primary_keys_type().clone(),
                table_catalog.get_column_by_name(column.name()).is_some(),
            )
        };
        if column_exists {
            if if_not_exists {
                TupleBuilder::build_result_into(runtime.result_tuple_mut(), "1".to_string());
                runtime.resume();
                return Ok(());
            }
            return Err(DatabaseError::DuplicateColumn(column.name().to_string()));
        }

        let default_value = column.default_value()?;

        let (unique_index_id, apply) = {
            let (table, col_id) =
                runtime.transaction_add_column(plan_arena, &table_name, &column, if_not_exists)?;
            let unique_meta = if column.desc().is_unique() {
                table
                    .get_unique_index(&col_id, plan_arena)
                    .map(|index| plan_arena.index(index).id)
            } else {
                None
            };
            (unique_meta, DDLApply::upsert_table(table, false))
        };
        runtime.push_ddl_apply(apply);
        let default_for_index = default_value.clone();
        runtime.transaction_rewrite_table_in_batches(
            &table_name,
            &pk_ty,
            old_schema.len(),
            &mut || -> TupleValueSerializerIter<'_> {
                Box::new(
                    old_schema
                        .iter()
                        .map(|column| plan_arena.column(*column).datatype().serializable()),
                )
            },
            &mut || -> TupleValueSerializerIter<'_> {
                Box::new(
                    old_schema
                        .iter()
                        .map(|column| plan_arena.column(*column).datatype().serializable())
                        .chain(::std::iter::once(column.datatype().serializable())),
                )
            },
            &mut |tuple| {
                if let Some(value) = &default_value {
                    tuple.values.push(value.clone());
                } else {
                    tuple.values.push(DataValue::Null);
                }
                Ok(())
            },
            &mut |runtime, tuple| {
                if let (Some(unique_index_id), Some(value), Some(tuple_id)) = (
                    unique_index_id.as_ref(),
                    default_for_index.as_ref(),
                    tuple.pk.as_ref(),
                ) {
                    let index = Index::new(*unique_index_id, value, IndexType::Unique);
                    runtime.transaction_add_index(&table_name, index, tuple_id)?;
                }
                Ok(())
            },
        )?;

        TupleBuilder::build_result_into(runtime.result_tuple_mut(), "1".to_string());
        runtime.resume();
        Ok(())
    }
}
