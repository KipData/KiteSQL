use crate::catalog::TableName;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DescribeOperator {
    pub table_name: TableName,
}

impl fmt::Display for DescribeOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Describe {}", self.table_name)?;

        Ok(())
    }
}
