use crate::catalog::{ColumnRef, TableName};
use crate::types::index::IndexType;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CreateIndexOperator {
    pub table_name: TableName,
    /// List of columns of the index
    pub columns: Vec<ColumnRef>,
    pub index_name: String,
    pub if_not_exists: bool,
    pub ty: IndexType,
}

impl fmt::Display for CreateIndexOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .columns
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");
        write!(
            f,
            "Create Index On {} -> [{}], If Not Exists: {}",
            self.table_name, columns, self.if_not_exists
        )?;

        Ok(())
    }
}
