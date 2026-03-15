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

use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, ObjectName};

use std::sync::Arc;

use super::{attach_span_if_absent, is_valid_identifier, Binder};
use crate::binder::lower_case_name;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::alter_table::add_column::AddColumnOperator;
use crate::planner::operator::alter_table::change_column::{
    ChangeColumnOperator, DefaultChange, NotNullChange,
};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    fn bind_alter_default_expr(
        &mut self,
        expr: &sqlparser::ast::Expr,
        ty: &LogicalType,
    ) -> Result<ScalarExpression, DatabaseError> {
        let mut expr = self.bind_expr(expr)?;

        if !expr.referenced_columns(true).is_empty() {
            return Err(DatabaseError::UnsupportedStmt(
                "column is not allowed to exist in default".to_string(),
            ));
        }
        if expr.return_type() != *ty {
            expr = ScalarExpression::TypeCast {
                expr: Box::new(expr),
                ty: ty.clone(),
            };
        }

        Ok(expr)
    }

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
                ..
            } => {
                let plan = TableScanOperator::build(table_name.clone(), table, true)?;
                let column = self.bind_column(column_def, None)?;

                if !is_valid_identifier(column.name()) {
                    return Err(attach_span_if_absent(
                        DatabaseError::invalid_column("illegal column naming".to_string()),
                        column_def,
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
                column_names,
                if_exists,
                ..
            } => {
                let plan = TableScanOperator::build(table_name.clone(), table, true)?;
                if column_names.len() != 1 {
                    return Err(DatabaseError::UnsupportedStmt(
                        "only dropping a single column is supported".to_string(),
                    ));
                }
                let column_name = column_names[0].value.clone();

                LogicalPlan::new(
                    Operator::DropColumn(DropColumnOperator {
                        table_name,
                        if_exists: *if_exists,
                        column_name,
                    }),
                    Childrens::Only(Box::new(plan)),
                )
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old_column_name = old_column_name.value.to_lowercase();
                let new_column_name = new_column_name.value.to_lowercase();
                let old_column = table
                    .get_column_by_name(&old_column_name)
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.clone()))?;
                let plan = TableScanOperator::build(table_name.clone(), table, true)?;

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        old_column_name,
                        new_column_name,
                        data_type: old_column.datatype().clone(),
                        default_change: DefaultChange::NoChange,
                        not_null_change: NotNullChange::NoChange,
                    }),
                    Childrens::Only(Box::new(plan)),
                )
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let old_column_name = column_name.value.to_lowercase();
                let old_column = table
                    .get_column_by_name(&old_column_name)
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.clone()))?;
                let old_data_type = old_column.datatype().clone();
                let plan = TableScanOperator::build(table_name.clone(), table, true)?;

                let (data_type, default_change, not_null_change) = match op {
                    AlterColumnOperation::SetDataType {
                        data_type, using, ..
                    } => {
                        if using.is_some() {
                            return Err(DatabaseError::UnsupportedStmt(
                                "ALTER COLUMN TYPE USING is not supported".to_string(),
                            ));
                        }
                        (
                            LogicalType::try_from(data_type.clone())?,
                            DefaultChange::NoChange,
                            NotNullChange::NoChange,
                        )
                    }
                    AlterColumnOperation::SetDefault { value } => (
                        old_data_type.clone(),
                        DefaultChange::Set(self.bind_alter_default_expr(value, &old_data_type)?),
                        NotNullChange::NoChange,
                    ),
                    AlterColumnOperation::DropDefault => (
                        old_data_type.clone(),
                        DefaultChange::Drop,
                        NotNullChange::NoChange,
                    ),
                    AlterColumnOperation::SetNotNull => (
                        old_data_type.clone(),
                        DefaultChange::NoChange,
                        NotNullChange::Set,
                    ),
                    AlterColumnOperation::DropNotNull => (
                        old_data_type.clone(),
                        DefaultChange::NoChange,
                        NotNullChange::Drop,
                    ),
                    _ => {
                        return Err(DatabaseError::UnsupportedStmt(format!(
                            "unsupported alter column operation: {op:?}"
                        )))
                    }
                };

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        new_column_name: old_column_name.clone(),
                        old_column_name,
                        data_type,
                        default_change,
                        not_null_change,
                    }),
                    Childrens::Only(Box::new(plan)),
                )
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                if !options.is_empty() || column_position.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "MODIFY COLUMN currently only supports changing the data type".to_string(),
                    ));
                }
                let old_column_name = col_name.value.to_lowercase();
                let _ = table
                    .get_column_by_name(&old_column_name)
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.clone()))?;
                let plan = TableScanOperator::build(table_name.clone(), table, true)?;

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        new_column_name: old_column_name.clone(),
                        old_column_name,
                        data_type: LogicalType::try_from(data_type.clone())?,
                        default_change: DefaultChange::NoChange,
                        not_null_change: NotNullChange::NoChange,
                    }),
                    Childrens::Only(Box::new(plan)),
                )
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                if !options.is_empty() || column_position.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "CHANGE COLUMN currently only supports renaming and changing the data type"
                            .to_string(),
                    ));
                }
                let old_column_name = old_name.value.to_lowercase();
                let new_column_name = new_name.value.to_lowercase();
                let _ = table
                    .get_column_by_name(&old_column_name)
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.clone()))?;
                let plan = TableScanOperator::build(table_name.clone(), table, true)?;

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        old_column_name,
                        new_column_name,
                        data_type: LogicalType::try_from(data_type.clone())?,
                        default_change: DefaultChange::NoChange,
                        not_null_change: NotNullChange::NoChange,
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
