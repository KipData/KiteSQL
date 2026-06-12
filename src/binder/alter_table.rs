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

use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, ColumnOption, ObjectName};

use std::borrow::Cow;

use super::{attach_span_if_absent, is_valid_identifier, Binder};
use crate::binder::{lower_case_name, lower_ident};
use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::alter_table::add_column::AddColumnOperator;
use crate::planner::operator::alter_table::change_column::{
    ChangeColumnOperator, DefaultChange, NotNullChange,
};
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::Operator;
use crate::planner::Childrens;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    fn bind_alter_default_expr(
        &mut self,
        expr: &sqlparser::ast::Expr,
        ty: &LogicalType,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let mut expr = self.bind_expr(expr, arena)?;

        if expr.any_referenced_column(arena, |_, _| true) {
            return Err(DatabaseError::UnsupportedStmt(
                "column is not allowed to exist in default".to_string(),
            ));
        }
        expr = ScalarExpression::type_cast(expr, Cow::Borrowed(ty), arena)?;

        Ok(expr)
    }

    fn bind_change_column_options(
        &mut self,
        options: &[ColumnOption],
        data_type: &LogicalType,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(DefaultChange, NotNullChange), DatabaseError> {
        let mut default_change = DefaultChange::NoChange;
        let mut not_null_change = NotNullChange::NoChange;

        for option in options {
            match option {
                ColumnOption::Null => not_null_change = NotNullChange::Drop,
                ColumnOption::NotNull => not_null_change = NotNullChange::Set,
                ColumnOption::Default(expr) => {
                    default_change =
                        DefaultChange::Set(self.bind_alter_default_expr(expr, data_type, arena)?);
                }
                option => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "CHANGE/MODIFY COLUMN does not currently support this option: {option:?}"
                    )))
                }
            }
        }

        Ok((default_change, not_null_change))
    }

    pub(crate) fn bind_alter_table(
        &mut self,
        name: &ObjectName,
        operation: &AlterTableOperation,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name: TableName = lower_case_name(name)?.into();
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
                let column = self.bind_column(column_def, None, arena)?;

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
                    Childrens::None,
                )
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
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
                    Childrens::None,
                )
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old_column_name = lower_ident(old_column_name);
                let new_column_name = lower_ident(new_column_name).into_owned();
                let old_column = table
                    .get_column_by_name(old_column_name.as_ref())
                    .map(|column| arena.column(column))
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.to_string()))?;

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        old_column_name: old_column_name.into_owned(),
                        new_column_name,
                        data_type: old_column.datatype().clone(),
                        default_change: DefaultChange::NoChange,
                        not_null_change: NotNullChange::NoChange,
                    }),
                    Childrens::None,
                )
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let old_column_name = lower_ident(column_name);
                let old_column = table
                    .get_column_by_name(old_column_name.as_ref())
                    .map(|column| arena.column(column))
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.to_string()))?;
                let old_data_type = old_column.datatype().clone();

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
                        DefaultChange::Set(self.bind_alter_default_expr(
                            value,
                            &old_data_type,
                            arena,
                        )?),
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
                        new_column_name: old_column_name.to_string(),
                        old_column_name: old_column_name.into_owned(),
                        data_type,
                        default_change,
                        not_null_change,
                    }),
                    Childrens::None,
                )
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                if column_position.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "MODIFY COLUMN does not currently support column positions".to_string(),
                    ));
                }
                let old_column_name = lower_ident(col_name);
                let _ = table
                    .get_column_by_name(old_column_name.as_ref())
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.to_string()))?;
                let old_column_name = old_column_name.into_owned();
                let data_type = LogicalType::try_from(data_type.clone())?;
                let (default_change, not_null_change) =
                    self.bind_change_column_options(options, &data_type, arena)?;

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        new_column_name: old_column_name.clone(),
                        old_column_name,
                        data_type,
                        default_change,
                        not_null_change,
                    }),
                    Childrens::None,
                )
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                if column_position.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "CHANGE COLUMN does not currently support column positions".to_string(),
                    ));
                }
                let old_column_name = lower_ident(old_name);
                let new_column_name = lower_ident(new_name).into_owned();
                let _ = table
                    .get_column_by_name(old_column_name.as_ref())
                    .ok_or_else(|| DatabaseError::column_not_found(old_column_name.to_string()))?;

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }
                let data_type = LogicalType::try_from(data_type.clone())?;
                let (default_change, not_null_change) =
                    self.bind_change_column_options(options, &data_type, arena)?;

                LogicalPlan::new(
                    Operator::ChangeColumn(ChangeColumnOperator {
                        table_name,
                        old_column_name: old_column_name.into_owned(),
                        new_column_name,
                        data_type,
                        default_change,
                        not_null_change,
                    }),
                    Childrens::None,
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
