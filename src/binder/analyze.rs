use crate::binder::{lower_case_name, Binder, Source};
use crate::errors::DatabaseError;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_analyze(&mut self, name: &ObjectName) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(lower_case_name(name)?);

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

        let scan_op = TableScanOperator::build(table_name.clone(), table, false);
        Ok(LogicalPlan::new(
            Operator::Analyze(AnalyzeOperator {
                table_name,
                index_metas,
            }),
            Childrens::Only(scan_op),
        ))
    }
}
