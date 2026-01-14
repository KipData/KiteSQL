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

use crate::binder::{lower_case_name, Binder, Source};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::index::IndexType;
use crate::types::value::DataValue;
use sqlparser::ast::{ObjectName, OrderByExpr};
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_create_index(
        &mut self,
        table_name: &ObjectName,
        name: &ObjectName,
        exprs: &[OrderByExpr],
        if_not_exists: bool,
        is_unique: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name: Arc<str> = lower_case_name(table_name)?.into();
        let index_name = lower_case_name(name)?;
        let ty = if is_unique {
            IndexType::Unique
        } else if exprs.len() == 1 {
            IndexType::Normal
        } else {
            IndexType::Composite
        };

        let source = self
            .context
            .source_and_bind(table_name.clone(), None, None, false)?
            .ok_or(DatabaseError::SourceNotFound)?;
        let plan = match source {
            Source::Table(table) => TableScanOperator::build(table_name.clone(), table, true)?,
            Source::View(view) => LogicalPlan::clone(&view.plan),
        };
        let mut columns = Vec::with_capacity(exprs.len());

        for expr in exprs {
            // TODO: Expression Index
            match self.bind_expr(&expr.expr)? {
                ScalarExpression::ColumnRef { column, .. } => columns.push(column),
                expr => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "'CREATE INDEX' by {expr}"
                    )))
                }
            }
        }

        Ok(LogicalPlan::new(
            Operator::CreateIndex(CreateIndexOperator {
                table_name,
                columns,
                index_name,
                if_not_exists,
                ty,
            }),
            Childrens::Only(Box::new(plan)),
        ))
    }
}
