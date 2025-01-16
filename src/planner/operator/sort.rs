use crate::expression::ScalarExpression;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct SortField {
    pub expr: ScalarExpression,
    pub asc: bool,
    pub nulls_first: bool,
}

impl SortField {
    pub fn new(expr: ScalarExpression, asc: bool, nulls_first: bool) -> Self {
        SortField {
            expr,
            asc,
            nulls_first,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct SortOperator {
    pub sort_fields: Vec<SortField>,
    /// Support push down limit to sort plan.
    pub limit: Option<usize>,
}

impl fmt::Display for SortOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let sort_fields = self
            .sort_fields
            .iter()
            .map(|sort_field| format!("{}", sort_field))
            .join(", ");
        write!(f, "Sort By {}", sort_fields)?;

        if let Some(limit) = self.limit {
            write!(f, ", Limit {}", limit)?;
        }

        Ok(())
    }
}

impl fmt::Display for SortField {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " Asc")?;
        } else {
            write!(f, " Desc")?;
        }
        if self.nulls_first {
            write!(f, " Nulls First")?;
        } else {
            write!(f, " Nulls Last")?;
        }

        Ok(())
    }
}
