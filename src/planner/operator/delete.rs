use crate::catalog::{ColumnRef, TableName};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DeleteOperator {
    pub table_name: TableName,
    // for column pruning
    pub primary_keys: Vec<ColumnRef>,
}

impl fmt::Display for DeleteOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Delete {}", self.table_name)?;

        Ok(())
    }
}
