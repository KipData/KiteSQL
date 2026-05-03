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

use crate::binder::{
    attach_span_from_sqlparser_span_if_absent, attach_span_if_absent, lower_case_name, Binder,
};
use crate::errors::DatabaseError;
use crate::expression::visitor_mut::VisitorMut;
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::{
    Assignment, AssignmentTarget, Expr, Ident, ObjectName, TableFactor, TableWithJoins,
};
use std::borrow::Cow;
use std::slice;
use std::sync::Arc;

struct UpdateExprTargetRemapper<'a> {
    target_schema: &'a [crate::catalog::ColumnRef],
}

impl VisitorMut<'_> for UpdateExprTargetRemapper<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut crate::catalog::ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        let Some(target_position) = self
            .target_schema
            .iter()
            .position(|target_column| target_column.same_column(column))
        else {
            return Err(DatabaseError::UnsupportedStmt(
                "joined UPDATE SET expressions can only reference target table columns".to_string(),
            ));
        };
        *position = target_position;
        Ok(())
    }
}

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    fn single_ident_from_object_name(name: &ObjectName) -> Result<&Ident, DatabaseError> {
        if name.0.len() != 1 {
            return Err(attach_span_if_absent(
                DatabaseError::invalid_column(name.to_string()),
                name,
            ));
        }
        name.0[0].as_ident().ok_or_else(|| {
            attach_span_if_absent(DatabaseError::invalid_column(name.to_string()), name)
        })
    }

    pub(crate) fn bind_update(
        &mut self,
        to: &TableWithJoins,
        selection: &Option<Expr>,
        assignments: &[Assignment],
    ) -> Result<LogicalPlan, DatabaseError> {
        // FIXME: Make it better to detect the current BindStep
        self.context.allow_default = true;
        if let TableFactor::Table { name, .. } = &to.relation {
            let is_joined_update = !to.joins.is_empty();
            let table_name: Arc<str> = lower_case_name(name)?.into();
            self.with_pk(table_name.clone());

            let mut plan = self.bind_table_ref(to)?;
            let (target_schema, target_offset) = Self::resolve_source_columns_in_scope(
                &self.context,
                &mut self.table_schema_buf,
                &table_name,
            )?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }
            let mut value_exprs = Vec::with_capacity(assignments.len());

            if assignments.is_empty() {
                return Err(DatabaseError::ColumnsEmpty);
            }
            for Assignment { target, value } in assignments {
                let expression = self.bind_expr(value)?;
                let mut idents = vec![];
                match target {
                    AssignmentTarget::ColumnName(name) => {
                        idents.push(Self::single_ident_from_object_name(name)?);
                    }
                    AssignmentTarget::Tuple(_) => {
                        return Err(DatabaseError::UnsupportedStmt(
                            "UPDATE assignment tuple target is not supported".to_string(),
                        ))
                    }
                }

                for ident in idents {
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
                            expr = ScalarExpression::type_cast(
                                expr,
                                Cow::Borrowed(column.datatype()),
                            )?;
                            if is_joined_update {
                                UpdateExprTargetRemapper {
                                    target_schema: &target_schema,
                                }
                                .visit(&mut expr)?;
                            }
                            value_exprs.push((column, expr));
                        }
                        _ => {
                            return Err(attach_span_from_sqlparser_span_if_absent(
                                DatabaseError::invalid_column(ident.to_string()),
                                ident.span,
                            ))
                        }
                    }
                }
            }
            self.context.allow_default = false;
            if is_joined_update {
                let exprs = target_schema
                    .iter()
                    .enumerate()
                    .map(|(index, column)| {
                        ScalarExpression::column_expr(column.clone(), target_offset + index)
                    })
                    .collect();
                plan = LogicalPlan::new(
                    Operator::Project(ProjectOperator { exprs }),
                    Childrens::Only(Box::new(plan)),
                );
            }
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
