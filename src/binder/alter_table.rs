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

use super::Binder;
use crate::catalog::{ColumnCatalog, TableName};
use crate::errors::DatabaseError;
use crate::planner::operator::alter_table::add_column::AddColumnOperator;
use crate::planner::operator::alter_table::change_column::{
    ChangeColumnOperator, DefaultChange, NotNullChange,
};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_add_column(
        &mut self,
        table_name: TableName,
        column: ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(
            Operator::AddColumn(AddColumnOperator {
                table_name,
                if_not_exists,
                column,
            }),
            Childrens::None,
        ))
    }

    pub(crate) fn bind_drop_column(
        &mut self,
        table_name: TableName,
        column_name: String,
        if_exists: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(
            Operator::DropColumn(DropColumnOperator {
                table_name,
                if_exists,
                column_name,
            }),
            Childrens::None,
        ))
    }

    pub(crate) fn bind_change_column(
        &mut self,
        table_name: TableName,
        old_column_name: String,
        new_column_name: String,
        data_type: LogicalType,
        default_change: DefaultChange,
        not_null_change: NotNullChange,
    ) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(
            Operator::ChangeColumn(ChangeColumnOperator {
                table_name,
                old_column_name,
                new_column_name,
                data_type,
                default_change,
                not_null_change,
            }),
            Childrens::None,
        ))
    }
}
