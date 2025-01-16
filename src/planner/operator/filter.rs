use crate::expression::ScalarExpression;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

use super::Operator;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct FilterOperator {
    pub predicate: ScalarExpression,
    pub is_optimized: bool,
    pub having: bool,
}

impl FilterOperator {
    pub fn build(predicate: ScalarExpression, children: LogicalPlan, having: bool) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                is_optimized: false,
                having,
            }),
            Childrens::Only(children),
        )
    }
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Filter {}, Is Having: {}", self.predicate, self.having)?;

        Ok(())
    }
}
