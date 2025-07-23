use crate::catalog::TableName;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DropIndexOperator {
    pub table_name: TableName,
    pub index_name: String,
    pub if_exists: bool,
}

impl DropIndexOperator {
    pub fn build(
        table_name: TableName,
        index_name: String,
        if_exists: bool,
        childrens: Childrens,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::DropIndex(DropIndexOperator {
                table_name,
                index_name,
                if_exists,
            }),
            childrens,
        )
    }
}

impl fmt::Display for DropIndexOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Drop Index {} On {}, If Exists: {}",
            self.index_name, self.table_name, self.if_exists
        )?;

        Ok(())
    }
}
