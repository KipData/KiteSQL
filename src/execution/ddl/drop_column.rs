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
    type Input = crate::planner::operator::alter_table::drop_column::DropColumnOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::DropColumn(Self::from(input)))
    }
}
impl<'a> ExecutorNode<'a> for DropColumn {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(DropColumnOperator {
            table_name,
            column_name,
            if_exists,
        }) = self.op.take()
        else {
            runtime.finish();
            return Ok(());
        };

        let (old_schema, pk_ty, column_info) = {
            let table_catalog = runtime
                .transaction_table(table_name.clone())?
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
                                .enumerate()
                                .filter(|(index, _)| *index != column_index)
                                .map(|(_, column)| {
                                    plan_arena.column(*column).datatype().serializable()
                                }),
                        )
                    },
                    &mut |tuple| {
                        let _ = tuple.values.remove(column_index);
                        Ok(())
                    },
                    &mut |_, _| Ok(()),
                )?;
            }
            {
                let table =
                    runtime.transaction_drop_column(plan_arena, &table_name, &column_name)?;
                runtime.push_ddl_apply(DDLApply::upsert_table(table, true));
            }

            TupleBuilder::build_result_into(runtime.result_tuple_mut(), "1".to_string());
            runtime.resume();
            Ok(())
        } else if !if_exists {
            Err(DatabaseError::column_not_found(column_name))
        } else {
            runtime.finish();
            Ok(())
        }
    }
}
