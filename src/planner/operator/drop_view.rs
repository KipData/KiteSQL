use crate::catalog::TableName;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DropViewOperator {
    /// Table name to insert to
    pub view_name: TableName,
    pub if_exists: bool,
}

impl fmt::Display for DropViewOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Drop View {}, If Exists: {}",
            self.view_name, self.if_exists
        )?;

        Ok(())
    }
}
