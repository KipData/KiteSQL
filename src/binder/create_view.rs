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
use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::operator::create_view::CreateViewOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::{CreateView, Query, SelectItem, SetExpr};
use ulid::Ulid;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_create_view(
        &mut self,
        create: CreateView,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let CreateView {
            or_replace,
            name,
            columns,
            query,
            ..
        } = create;
        fn projection_exprs(
            view_name: &TableName,
            mapping_schema: &[ColumnRef],
            arena: &mut crate::planner::PlanArena,
            mut column_name: impl FnMut(usize, ColumnRef, &crate::planner::PlanArena) -> String,
        ) -> Vec<ScalarExpression> {
            let mapping_schema_len = mapping_schema.len();
            let mut exprs = Vec::with_capacity(mapping_schema_len);
            for (i, mapping_column) in mapping_schema.iter().copied().enumerate() {
                let output_name = column_name(i, mapping_column, arena);
                let (nullable, desc) = {
                    let mapping_column_catalog = arena.column(mapping_column);
                    (
                        mapping_column_catalog.nullable(),
                        mapping_column_catalog.desc().clone(),
                    )
                };
                let mut column = ColumnCatalog::new(output_name, nullable, desc);
                column.set_ref_table(view_name.clone(), Ulid::new(), true);
                let output_column = arena.alloc_column(column);

                exprs.push(ScalarExpression::Alias {
                    expr: Box::new(ScalarExpression::column_expr(mapping_column, i)),
                    alias: AliasType::Expr(Box::new(ScalarExpression::column_expr(
                        output_column,
                        i,
                    ))),
                });
            }
            exprs
        }

        let output_aliases = query_output_aliases(&query);
        let view_name: TableName = lower_case_name(&name)?.into();
        let mut plan = self.bind_query(*query, arena)?;

        let mapping_schema = plan.output_schema(arena);

        if !columns.is_empty() && columns.len() > mapping_schema.len() {
            return Err(DatabaseError::UnsupportedStmt(format!(
                "view column count {} exceeds query output count {}",
                columns.len(),
                mapping_schema.len()
            )));
        }

        let exprs: Vec<ScalarExpression> = if columns.is_empty() {
            projection_exprs(&view_name, mapping_schema, arena, |i, column, arena| {
                output_aliases
                    .get(i)
                    .and_then(Clone::clone)
                    .unwrap_or_else(|| arena.column(column).name().to_string())
            })
        } else {
            projection_exprs(
                &view_name,
                &mapping_schema[..columns.len()],
                arena,
                |i, _, _| lower_ident(&columns[i].name).into_owned(),
            )
        };
        plan = self.bind_project(plan, exprs, arena)?;
        let schema = plan.output_schema(arena).clone();

        Ok(LogicalPlan::new(
            Operator::CreateView(CreateViewOperator {
                view: View {
                    name: view_name,
                    plan: Box::new(plan),
                    schema,
                },
                or_replace,
            }),
            Childrens::None,
        ))
    }
}

fn query_output_aliases(query: &Query) -> Vec<Option<String>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };

    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(lower_ident(alias).into_owned()),
            _ => None,
        })
        .collect()
}
