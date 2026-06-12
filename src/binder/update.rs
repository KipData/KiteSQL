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

use crate::binder::{attach_span_if_absent, lower_case_name, Binder};
use crate::catalog::ColumnRef;
use crate::catalog::TableName;
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
    Assignment, AssignmentTarget, Expr, Ident, ObjectName, ObjectNamePart, TableFactor,
    TableWithJoins,
};
use std::borrow::Cow;
use std::slice;

struct UpdateExprTargetRemapper<'a, 'p> {
    target_schema: &'a [ColumnRef],
    arena: &'a crate::planner::PlanArena<'p>,
}

impl VisitorMut<'_> for UpdateExprTargetRemapper<'_, '_> {
    fn visit_column_ref(
        &mut self,
        column: &mut crate::catalog::ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        let Some(target_position) = self
            .target_schema
            .iter()
            .copied()
            .position(|target_column| self.arena.same_column(target_column, *column))
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
    fn single_ident_from_object_name(name: ObjectName) -> Result<Ident, DatabaseError> {
        if name.0.len() != 1 {
            return Err(attach_span_if_absent(
                DatabaseError::invalid_column(name.to_string()),
                &name,
            ));
        }
        match name.0.into_iter().next() {
            Some(ObjectNamePart::Identifier(ident)) => Ok(ident),
            Some(part) => Err(DatabaseError::invalid_column(part.to_string())),
            None => Err(DatabaseError::invalid_column(String::new())),
        }
    }

    pub(crate) fn bind_update(
        &mut self,
        to: TableWithJoins,
        selection: Option<Expr>,
        assignments: Vec<Assignment>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        // FIXME: Make it better to detect the current BindStep
        self.context.allow_default = true;
        if let TableFactor::Table { name, .. } = &to.relation {
            let is_joined_update = !to.joins.is_empty();
            let table_name: TableName = lower_case_name(name)?.into();
            self.with_pk(table_name.clone());

            let mut plan = self.bind_table_ref(to, arena)?;
            let (target_source, target_offset) =
                Self::resolve_source_columns_in_scope(&self.context, &table_name)?;
            let target_schema = target_source.schema().to_vec();

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate, arena)?;
            }
            let mut value_exprs = Vec::with_capacity(assignments.len());

            if assignments.is_empty() {
                return Err(DatabaseError::ColumnsEmpty);
            }
            for Assignment { target, value } in assignments {
                let expression = self.bind_expr(value, arena)?;
                let mut bind_assignment =
                    |name: ObjectName, expression: ScalarExpression| -> Result<(), DatabaseError> {
                        let ident = Self::single_ident_from_object_name(name)?;

                        let column = {
                            match self.bind_column_ref_from_identifiers(
                                slice::from_ref(&ident),
                                Some(table_name.as_ref()),
                                arena,
                            )? {
                                ScalarExpression::ColumnRef { column, .. } => column,
                                _ => {
                                    return Err(attach_span_if_absent(
                                        DatabaseError::invalid_column(ident.to_string()),
                                        ident.span,
                                    ))
                                }
                            }
                        };

                        let mut expr = if matches!(expression, ScalarExpression::Empty) {
                            let column_catalog = arena.column(column);
                            let default_value = column_catalog
                                .default_value()?
                                .ok_or(DatabaseError::DefaultNotExist)?;
                            ScalarExpression::Constant(default_value)
                        } else {
                            expression
                        };
                        let column_catalog = arena.column(column);
                        expr = ScalarExpression::type_cast(
                            expr,
                            Cow::Borrowed(column_catalog.datatype()),
                            arena,
                        )?;
                        if is_joined_update {
                            UpdateExprTargetRemapper {
                                target_schema: &target_schema,
                                arena,
                            }
                            .visit(&mut expr)?;
                        }
                        value_exprs.push((column, expr));
                        Ok(())
                    };

                match target {
                    AssignmentTarget::ColumnName(name) => bind_assignment(name, expression)?,
                    AssignmentTarget::Tuple(names) => {
                        let expected = names.len();
                        let ScalarExpression::Tuple(exprs) = expression else {
                            return Err(DatabaseError::ValuesLenMismatch(expected, 1));
                        };
                        let got = exprs.len();
                        let mut names = names.into_iter();
                        let mut exprs = exprs.into_iter();

                        loop {
                            match (names.next(), exprs.next()) {
                                (Some(name), Some(expression)) => {
                                    bind_assignment(name, expression)?
                                }
                                (None, None) => break,
                                _ => return Err(DatabaseError::ValuesLenMismatch(expected, got)),
                            }
                        }
                    }
                }
            }
            self.context.allow_default = false;
            if is_joined_update {
                let exprs = target_schema
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(index, column)| {
                        ScalarExpression::column_expr(column, target_offset + index)
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
            Err(DatabaseError::UnsupportedStmt(format!(
                "UPDATE target must be a table: {:?}",
                to.relation
            )))
        }
    }
}
