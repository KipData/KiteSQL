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
use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::ObjectName;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_analyze(
        &mut self,
        name: &ObjectName,
        arena: &crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name: TableName = lower_case_name(name)?.into();

        let table = self
            .context
            .source_and_bind(table_name.clone(), None, None, true)?
            .and_then(|source| {
                if let Source::Table(table) = source {
                    Some(table)
                } else {
                    None
                }
            })
            .ok_or(DatabaseError::TableNotFound)?;
        let index_metas = table.indexes.clone();

        let scan_op = TableScanOperator::build(table_name.clone(), table, false, arena)?;
        Ok(LogicalPlan::new(
            Operator::Analyze(AnalyzeOperator {
                table_name,
                index_metas,
                histogram_buckets: None,
            }),
            Childrens::Only(Box::new(scan_op)),
        ))
    }
}
