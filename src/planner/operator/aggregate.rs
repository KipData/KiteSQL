// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::planner::{Childrens, LogicalPlan};
use crate::{expression::ScalarExpression, planner::operator::Operator};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct AggregateOperator {
    pub groupby_exprs: Vec<ScalarExpression>,
    pub agg_calls: Vec<ScalarExpression>,
    pub is_distinct: bool,
}

impl AggregateOperator {
    pub fn build(
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
        is_distinct: bool,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Aggregate(Self {
                groupby_exprs,
                agg_calls,
                is_distinct,
            }),
            Childrens::Only(Box::new(children)),
        )
    }
}

impl fmt::Display for AggregateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let calls = self
            .agg_calls
            .iter()
            .map(|call| format!("{call}"))
            .join(", ");
        write!(f, "Aggregate [{calls}]")?;

        if !self.groupby_exprs.is_empty() {
            let groupbys = self
                .groupby_exprs
                .iter()
                .map(|groupby| format!("{groupby}"))
                .join(", ");
            write!(f, " -> Group By [{groupbys}]")?;
        }

        Ok(())
    }
}
