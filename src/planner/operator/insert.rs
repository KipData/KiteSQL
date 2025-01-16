use crate::catalog::TableName;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct InsertOperator {
    pub table_name: TableName,
    pub is_overwrite: bool,
    pub is_mapping_by_name: bool,
}

impl fmt::Display for InsertOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Insert {}, Is Overwrite: {}, Is Mapping By Name: {}",
            self.table_name, self.is_overwrite, self.is_mapping_by_name
        )?;

        Ok(())
    }
}
