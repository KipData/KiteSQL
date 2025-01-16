use crate::expression::function::table::TableFunction;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct FunctionScanOperator {
    pub table_function: TableFunction,
}

impl FunctionScanOperator {
    pub fn build(table_function: TableFunction) -> LogicalPlan {
        LogicalPlan::new(
            Operator::FunctionScan(FunctionScanOperator { table_function }),
            Childrens::None,
        )
    }
}

impl fmt::Display for FunctionScanOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Function Scan: {}", self.table_function.summary().name)?;

        Ok(())
    }
}
