use crate::catalog::TableName;
use crate::planner::LogicalPlan;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, Clone, Hash, Eq, PartialEq, ReferenceSerialization)]
pub struct View {
    pub name: TableName,
    pub plan: Box<LogicalPlan>,
}

impl fmt::Display for View {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "View {}: {}", self.name, self.plan.explain(0))?;

        Ok(())
    }
}
