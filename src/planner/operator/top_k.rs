use super::Operator;
use crate::planner::operator::sort::SortField;
use crate::planner::{Childrens, LogicalPlan};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct TopKOperator {
    pub sort_fields: Vec<SortField>,
    pub limit: usize,
    pub offset: Option<usize>,
}

impl TopKOperator {
    pub fn build(
        sort_fields: Vec<SortField>,
        limit: usize,
        offset: Option<usize>,
        children: LogicalPlan,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::TopK(TopKOperator {
                sort_fields,
                limit,
                offset,
            }),
            Childrens::Only(Box::new(children)),
        )
    }
}

impl fmt::Display for TopKOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Top {}, ", self.limit)?;

        if let Some(offset) = self.offset {
            write!(f, "Offset {offset}, ")?;
        }

        let sort_fields = self
            .sort_fields
            .iter()
            .map(|sort_field| format!("{sort_field}"))
            .join(", ");
        write!(f, "Sort By {sort_fields}")?;

        Ok(())
    }
}
