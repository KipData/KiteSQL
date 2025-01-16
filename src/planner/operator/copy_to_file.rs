use crate::binder::copy::ExtSource;
use crate::types::tuple::SchemaRef;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CopyToFileOperator {
    pub target: ExtSource,
    pub schema_ref: SchemaRef,
}

impl fmt::Display for CopyToFileOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self
            .schema_ref
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");
        write!(f, "Copy To {} [{}]", self.target.path.display(), columns)?;

        Ok(())
    }
}
