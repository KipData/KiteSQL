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

use crate::catalog::{ColumnCatalog, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ReadExecutionContext, WriteExecutor,
};
use crate::planner::operator::insert::InsertOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::index::Index;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;
use itertools::Itertools;
use std::collections::HashMap;

pub struct Insert {
    table_name: TableName,
    input_schema: SchemaRef,
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
            mut input,
        ): (InsertOperator, LogicalPlan),
    ) -> Self {
        Insert {
            table_name,
            input_schema: input.output_schema().clone(),
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

impl ColumnCatalog {
    fn key(&self, is_mapping_by_name: bool) -> MappingKey<'_> {
        if is_mapping_by_name {
            MappingKey::Name(self.name())
        } else {
            MappingKey::Id(self.id())
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Insert {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            self.input_plan.take(),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Insert(self))
    }
}

impl Insert {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        if let Some(table_snapshot) = arena
            .transaction()
            .table(arena.table_cache(), self.table_name.clone())?
            .map(|table| table.dml_snapshot())
            .transpose()?
        {
            if table_snapshot.primary_key_indices.is_empty() {
                return Err(DatabaseError::not_null());
            }

            let serializers = table_snapshot
                .schema_ref
                .iter()
                .map(|column| column.datatype().serializable())
                .collect_vec();
            let mut inserted_count = 0;

            while arena.next_tuple(input)? {
                let values = arena.result_tuple().values.clone();

                let mut tuple_map = HashMap::new();
                for (i, value) in values.into_iter().enumerate() {
                    tuple_map.insert(self.input_schema[i].key(self.is_mapping_by_name), value);
                }
                let mut values = Vec::with_capacity(table_snapshot.columns_len);

                for col in table_snapshot.schema_ref.iter() {
                    let mut value = {
                        let mut value = tuple_map.remove(&col.key(self.is_mapping_by_name));

                        if value.is_none() {
                            value = col.default_value()?;
                        }
                        value.unwrap_or(DataValue::Null)
                    };
                    value = value.cast(col.datatype())?;
                    value.check_len(col.datatype())?;
                    if value.is_null() && !col.nullable() {
                        return Err(DatabaseError::not_null_column(col.name().to_string()));
                    }
                    values.push(value)
                }
                let pk = Tuple::primary_projection(&table_snapshot.primary_key_indices, &values);
                let tuple = Tuple::new(Some(pk), values);

                for (index_meta, exprs) in table_snapshot.index_metas.iter() {
                    let values = Projection::projection(&tuple, exprs)?;
                    let Some(value) = DataValue::values_to_tuple(values) else {
                        continue;
                    };
                    let tuple_id = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;
                    let index = Index::new(index_meta.id, &value, index_meta.ty);
                    arena
                        .transaction_mut()
                        .add_index(&self.table_name, index, tuple_id)?;
                }
                arena.transaction_mut().append_tuple(
                    &self.table_name,
                    tuple,
                    &serializers,
                    self.is_overwrite,
                )?;
                inserted_count += 1;
            }

            TupleBuilder::build_result_into(arena.result_tuple_mut(), inserted_count.to_string());
            arena.resume();
            Ok(())
        } else {
            TupleBuilder::build_result_into(arena.result_tuple_mut(), "0".to_string());
            arena.resume();
            Ok(())
        }
    }
}
