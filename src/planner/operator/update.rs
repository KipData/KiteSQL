use crate::catalog::{ColumnRef, TableName};
use crate::expression::ScalarExpression;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct UpdateOperator {
    pub table_name: TableName,
    pub value_exprs: Vec<(ColumnRef, ScalarExpression)>,
}

impl fmt::Display for UpdateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let values = self
            .value_exprs
            .iter()
            .map(|(column, expr)| format!("{} -> {}", column.full_name(), expr))
            .join(", ");
        write!(f, "Update {} set {}", self.table_name, values)?;

        Ok(())
    }
}
