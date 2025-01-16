use crate::catalog::{ColumnCatalog, TableName};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CreateTableOperator {
    /// Table name to insert to
    pub table_name: TableName,
    /// List of columns of the table
    pub columns: Vec<ColumnCatalog>,
    pub if_not_exists: bool,
}

impl fmt::Display for CreateTableOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");
        write!(
            f,
            "Create {} -> [{}], If Not Exists: {}",
            self.table_name, columns, self.if_not_exists
        )?;

        Ok(())
    }
}
