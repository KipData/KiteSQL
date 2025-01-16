use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::types::tuple::SchemaRef;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct UnionOperator {
    pub left_schema_ref: SchemaRef,
    // mainly use `left_schema` as output and `right_schema` for `column pruning`
    pub _right_schema_ref: SchemaRef,
}

impl UnionOperator {
    pub fn build(
        left_schema_ref: SchemaRef,
        right_schema_ref: SchemaRef,
        left_plan: LogicalPlan,
        right_plan: LogicalPlan,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Union(UnionOperator {
                left_schema_ref,
                _right_schema_ref: right_schema_ref,
            }),
            Childrens::Twins {
                left: left_plan,
                right: right_plan,
            },
        )
    }
}

impl fmt::Display for UnionOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let schema = self
            .left_schema_ref
            .iter()
            .map(|column| column.name().to_string())
            .join(", ");

        write!(f, "Union: [{}]", schema)?;

        Ok(())
    }
}
