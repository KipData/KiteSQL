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

use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::{Assignment, Expr, TableFactor, TableWithJoins};
use std::slice;
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_update(
        &mut self,
        to: &TableWithJoins,
        selection: &Option<Expr>,
        assignments: &[Assignment],
    ) -> Result<LogicalPlan, DatabaseError> {
        // FIXME: Make it better to detect the current BindStep
        self.context.allow_default = true;
        if let TableFactor::Table { name, .. } = &to.relation {
            let table_name: Arc<str> = lower_case_name(name)?.into();
            self.with_pk(table_name.clone());

            let mut plan = self.bind_table_ref(to)?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }
            let mut value_exprs = Vec::with_capacity(assignments.len());

            if assignments.is_empty() {
                return Err(DatabaseError::ColumnsEmpty);
            }
            for Assignment { id, value } in assignments {
                let expression = self.bind_expr(value)?;

                for ident in id {
                    match self.bind_column_ref_from_identifiers(
                        slice::from_ref(ident),
                        Some(table_name.to_string()),
                    )? {
                        ScalarExpression::ColumnRef { column, .. } => {
                            let mut expr = if matches!(expression, ScalarExpression::Empty) {
                                let default_value = column
                                    .default_value()?
                                    .ok_or(DatabaseError::DefaultNotExist)?;
                                ScalarExpression::Constant(default_value)
                            } else {
                                expression.clone()
                            };
                            if &expr.return_type() != column.datatype() {
                                expr = ScalarExpression::TypeCast {
                                    expr: Box::new(expr),
                                    ty: column.datatype().clone(),
                                }
                            }
                            value_exprs.push((column, expr));
                        }
                        _ => return Err(DatabaseError::InvalidColumn(ident.to_string())),
                    }
                }
            }
            self.context.allow_default = false;
            Ok(LogicalPlan::new(
                Operator::Update(UpdateOperator {
                    table_name,
                    value_exprs,
                }),
                Childrens::Only(Box::new(plan)),
            ))
        } else {
            unreachable!("only table")
        }
    }
}
