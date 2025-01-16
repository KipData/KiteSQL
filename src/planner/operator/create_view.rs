use crate::catalog::view::View;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CreateViewOperator {
    pub view: View,
    pub or_replace: bool,
}

impl fmt::Display for CreateViewOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Create View as {}, Or Replace: {}",
            self.view, self.or_replace
        )?;

        Ok(())
    }
}
