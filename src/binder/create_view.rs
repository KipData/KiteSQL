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

use crate::binder::{lower_case_name, lower_ident, Binder};
use crate::catalog::view::View;
use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::operator::create_view::CreateViewOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use itertools::Itertools;
use sqlparser::ast::{ObjectName, Query, ViewColumnDef};
use std::sync::Arc;
use ulid::Ulid;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_create_view(
        &mut self,
        or_replace: &bool,
        name: &ObjectName,
        columns: &[ViewColumnDef],
        query: &Query,
    ) -> Result<LogicalPlan, DatabaseError> {
        fn projection_exprs(
            view_name: &Arc<str>,
            mapping_schema: &[ColumnRef],
            column_names: impl Iterator<Item = String>,
        ) -> Vec<ScalarExpression> {
            column_names
                .enumerate()
                .map(|(i, column_name)| {
                    let mapping_column = &mapping_schema[i];
                    let mut column = ColumnCatalog::new(
                        column_name,
                        mapping_column.nullable(),
                        mapping_column.desc().clone(),
                    );
                    column.set_ref_table(view_name.clone(), Ulid::new(), true);

                    ScalarExpression::Alias {
                        expr: Box::new(ScalarExpression::column_expr(mapping_column.clone(), i)),
                        alias: AliasType::Expr(Box::new(ScalarExpression::column_expr(
                            ColumnRef::from(column),
                            i,
                        ))),
                    }
                })
                .collect_vec()
        }

        let view_name: Arc<str> = lower_case_name(name)?.into();
        let mut plan = self.bind_query(query)?;

        let mapping_schema = plan.output_schema();

        let exprs: Vec<ScalarExpression> = if columns.is_empty() {
            projection_exprs(
                &view_name,
                mapping_schema,
                mapping_schema
                    .iter()
                    .map(|column| column.name().to_string()),
            )
        } else {
            projection_exprs(
                &view_name,
                mapping_schema,
                columns.iter().map(|column| lower_ident(&column.name)),
            )
        };
        plan = self.bind_project(plan, exprs)?;

        Ok(LogicalPlan::new(
            Operator::CreateView(CreateViewOperator {
                view: View {
                    name: view_name,
                    plan: Box::new(plan),
                },
                or_replace: *or_replace,
            }),
            Childrens::None,
        ))
    }
}
