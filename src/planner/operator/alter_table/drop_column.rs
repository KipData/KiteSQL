use crate::catalog::TableName;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DropColumnOperator {
    pub table_name: TableName,
    pub column_name: String,
    pub if_exists: bool,
}

impl fmt::Display for DropColumnOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Drop {} -> {}, If Exists: {}",
            self.column_name, self.table_name, self.if_exists
        )?;

        Ok(())
    }
}
