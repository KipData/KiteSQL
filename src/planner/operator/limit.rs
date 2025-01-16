use super::Operator;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct LimitOperator {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

impl LimitOperator {
    pub fn build(
        offset: Option<usize>,
        limit: Option<usize>,
        children: LogicalPlan,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Limit(LimitOperator { offset, limit }),
            Childrens::Only(children),
        )
    }
}

impl fmt::Display for LimitOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Some(limit) = self.limit {
            write!(f, "Limit {}", limit)?;
        }
        if self.limit.is_some() && self.offset.is_some() {
            write!(f, ", ")?;
        }
        if let Some(offset) = self.offset {
            write!(f, "Offset {}", offset)?;
        }

        Ok(())
    }
}
