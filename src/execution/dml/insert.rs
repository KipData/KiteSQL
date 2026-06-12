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
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    WriteExecutor,
};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::{Schema, Tuple};
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;
use itertools::Itertools;
use std::collections::HashMap;

pub struct Insert {
    table_name: TableName,
    input_schema: Schema,
    input_plan: LogicalPlan,
    input: Option<ExecId>,
    is_overwrite: bool,
    is_mapping_by_name: bool,
}

impl From<(InsertOperator, LogicalPlan)> for Insert {
    fn from(
        (
            InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name,
            },
            input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input_schema: Default::default(),
            input_plan: input,
            input: None,
            is_overwrite,
            is_mapping_by_name,
        }
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum MappingKey<'a> {
    Name(&'a str),
    Id(Option<ColumnId>),
}

impl Insert {
    fn column_key<'a>(
        column: &'a crate::catalog::ColumnCatalog,
        is_mapping_by_name: bool,
    ) -> MappingKey<'a> {
        if is_mapping_by_name {
            MappingKey::Name(column.name())
        } else {
            MappingKey::Id(column.id())
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Insert {
    type Input = (
        crate::planner::operator::insert::InsertOperator,
        LogicalPlan,
    );

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut exec = Self::from(input);
        exec.input_schema = exec.input_plan.take_schema(plan_arena);
        exec.input = Some(build_read(
            arena,
            plan_arena,
            exec.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Insert(exec))
    }
}
impl<'a> ExecutorNode<'a> for Insert {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            runtime.finish();
            return Ok(());
        };

        let table_snapshot = {
            runtime
                .transaction_table(self.table_name.clone())?
                .map(|table| table.dml_snapshot(plan_arena))
                .transpose()?
        };
        if let Some(table_snapshot) = table_snapshot {
            if table_snapshot.primary_key_indices.is_empty() {
                return Err(DatabaseError::not_null());
            }

            let serializers = table_snapshot
                .columns
                .iter()
                .map(|column| plan_arena.column(*column).datatype().serializable())
                .collect_vec();
            let mut inserted_count = 0;

            while runtime.next_tuple(input, plan_arena)? {
                let values = runtime.result_tuple().values.clone();

                let mut tuple_map = HashMap::new();
                for (i, value) in values.into_iter().enumerate() {
                    let column = plan_arena.column(self.input_schema[i]);
                    tuple_map.insert(Self::column_key(column, self.is_mapping_by_name), value);
                }
                let mut values = Vec::with_capacity(table_snapshot.columns_len);

                for column in table_snapshot.columns.iter() {
                    let column = plan_arena.column(*column);
                    let mut value = {
                        let mut value =
                            tuple_map.remove(&Self::column_key(column, self.is_mapping_by_name));

                        if value.is_none() {
                            value = column.default_value()?;
                        }
                        value.unwrap_or(DataValue::Null)
                    };
                    value = value.cast(column.datatype())?;
                    value.check_len(column.datatype())?;
                    if value.is_null() && !column.nullable() {
                        return Err(DatabaseError::not_null_column(column.name().to_string()));
                    }
                    values.push(value)
                }
                let pk = Tuple::primary_projection(&table_snapshot.primary_key_indices, &values);
                let tuple = Tuple::new(Some(pk), values);

                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let index_meta = plan_arena.index(*index_meta);
                    let values = Projection::projection(&tuple, exprs)?;
                    let Some(value) = DataValue::values_to_tuple(values) else {
                        continue;
                    };
                    let tuple_id = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;
                    let index = Index::new(index_meta.id, &value, index_meta.ty);
                    runtime.transaction_add_index(&self.table_name, index, tuple_id)?;
                }
                runtime.transaction_append_tuple(
                    &self.table_name,
                    tuple,
                    &serializers,
                    self.is_overwrite,
                )?;
                inserted_count += 1;
            }

            TupleBuilder::build_result_into(runtime.result_tuple_mut(), inserted_count.to_string());
            runtime.resume();
            Ok(())
        } else {
            TupleBuilder::build_result_into(runtime.result_tuple_mut(), "0".to_string());
            runtime.resume();
            Ok(())
        }
    }
}
