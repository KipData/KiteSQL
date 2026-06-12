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

use crate::binder::Binder;
use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::value::DataValue;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_insert_values(
        &mut self,
        table_name: TableName,
        schema_ref: Schema,
        rows: Vec<Vec<DataValue>>,
        is_overwrite: bool,
        is_mapping_by_name: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let values_plan = self.bind_values(rows, schema_ref);

        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name,
            }),
            Childrens::Only(Box::new(values_plan)),
        ))
    }

    pub(crate) fn bind_insert_query(
        &mut self,
        table_name: TableName,
        input_plan: LogicalPlan,
        is_overwrite: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name: true,
            }),
            Childrens::Only(Box::new(input_plan)),
        ))
    }

    pub(crate) fn bind_values(
        &mut self,
        rows: Vec<Vec<DataValue>>,
        schema_ref: Schema,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Values(ValuesOperator { rows, schema_ref }),
            Childrens::None,
        )
    }
}
