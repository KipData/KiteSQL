use crate::binder::Binder;
use crate::errors::DatabaseError;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_show_tables(&mut self) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(Operator::ShowTable, Childrens::None))
    }
}
