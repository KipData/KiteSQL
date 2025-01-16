use crate::expression::ScalarExpression;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct ProjectOperator {
    pub exprs: Vec<ScalarExpression>,
}

impl fmt::Display for ProjectOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let exprs = self.exprs.iter().map(|expr| format!("{}", expr)).join(", ");

        write!(f, "Projection [{}]", exprs)?;

        Ok(())
    }
}
