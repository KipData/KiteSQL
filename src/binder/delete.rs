use crate::binder::{lower_case_name, Binder, Source};
use crate::errors::DatabaseError;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use itertools::Itertools;
use sqlparser::ast::{Expr, TableAlias, TableFactor, TableWithJoins};
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_delete(
        &mut self,
        from: &TableWithJoins,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, DatabaseError> {
        if let TableFactor::Table { name, alias, .. } = &from.relation {
            let table_name = Arc::new(lower_case_name(name)?);
            let mut table_alias = None;
            let mut alias_idents = None;

            if let Some(TableAlias { name, columns }) = alias {
                table_alias = Some(Arc::new(name.value.to_lowercase()));
                alias_idents = Some(columns);
            }
            let Source::Table(table) = self
                .context
                .source_and_bind(table_name.clone(), table_alias.as_ref(), None, true)?
                .ok_or(DatabaseError::TableNotFound)?
            else {
                unreachable!()
            };
            let primary_keys = table
                .primary_keys()
                .iter()
                .map(|(_, column)| column.clone())
                .collect_vec();
            let mut plan = TableScanOperator::build(table_name.clone(), table, true);

            if let Some(alias_idents) = alias_idents {
                plan = self.bind_alias(
                    plan,
                    alias_idents,
                    table_alias.unwrap(),
                    Some(table_name.clone()),
                )?;
            }

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            Ok(LogicalPlan::new(
                Operator::Delete(DeleteOperator {
                    table_name,
                    primary_keys,
                }),
                Childrens::Only(plan),
            ))
        } else {
            unreachable!("only table")
        }
    }
}
