use crate::binder::{lower_ident, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use sqlparser::ast::ObjectName;
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_drop_index(
        &mut self,
        name: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = name
            .0
            .first()
            .ok_or(DatabaseError::InvalidTable(name.to_string()))?;
        let index_name = name.0.get(1).ok_or(DatabaseError::InvalidIndex)?;

        let table_name = Arc::new(lower_ident(table_name));
        let index_name = lower_ident(index_name);

        Ok(LogicalPlan::new(
            Operator::DropIndex(DropIndexOperator {
                table_name,
                index_name,
                if_exists: *if_exists,
            }),
            Childrens::None,
        ))
    }
}
