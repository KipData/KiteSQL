use crate::catalog::{ColumnCatalog, TableName};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct AddColumnOperator {
    pub table_name: TableName,
    pub if_not_exists: bool,
    pub column: ColumnCatalog,
}

impl fmt::Display for AddColumnOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Add {} -> {}, If Not Exists: {}",
            self.column.name(),
            self.table_name,
            self.if_not_exists
        )?;

        Ok(())
    }
}
