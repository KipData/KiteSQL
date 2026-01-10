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

use sqlparser::ast::{AlterTableOperation, ObjectName};

use std::sync::Arc;

use super::{is_valid_identifier, Binder};
use crate::binder::lower_case_name;
use crate::errors::DatabaseError;
use crate::planner::operator::alter_table::add_column::AddColumnOperator;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name: Arc<str> = lower_case_name(name)?.into();
        let table = self
            .context
            .table(table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let plan = match operation {
            AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists,
                column_def,
            } => {
                let plan = TableScanOperator::build(table_name.clone(), table, true);
                let column = self.bind_column(column_def, None)?;

                if !is_valid_identifier(column.name()) {
                    return Err(DatabaseError::InvalidColumn(
                        "illegal column naming".to_string(),
                    ));
                }
                LogicalPlan::new(
                    Operator::AddColumn(AddColumnOperator {
                        table_name,
                        if_not_exists: *if_not_exists,
                        column,
                    }),
                    Childrens::Only(Box::new(plan)),
                )
            }
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
                ..
            } => {
                let plan = TableScanOperator::build(table_name.clone(), table, true);
                let column_name = column_name.value.clone();

                LogicalPlan::new(
                    Operator::DropColumn(DropColumnOperator {
                        table_name,
                        if_exists: *if_exists,
                        column_name,
                    }),
                    Childrens::Only(Box::new(plan)),
                )
            }
            op => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "AlertOperation: {op:?}"
                )))
            }
        };

        Ok(plan)
    }
}
