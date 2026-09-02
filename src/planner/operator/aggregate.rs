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

use crate::planner::operator::Operator;
use crate::planner::{fmt_explain_list, Childrens, Explain, ExprRef, LogicalPlan, PlanArena};
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct AggregateOperator {
    pub groupby_exprs: Vec<ExprRef>,
    pub agg_calls: Vec<ExprRef>,
    pub is_distinct: bool,
    pub force_spill: bool,
}

impl AggregateOperator {
    pub fn build(
        children: LogicalPlan,
        agg_calls: Vec<ExprRef>,
        groupby_exprs: Vec<ExprRef>,
        is_distinct: bool,
        force_spill: bool,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Aggregate(Self {
                groupby_exprs,
                agg_calls,
                is_distinct,
                force_spill,
            }),
            Childrens::Only(Box::new(children)),
        )
    }
}

impl Explain for AggregateOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Aggregate [")?;
        fmt_explain_list(&self.agg_calls, ", ", arena, f)?;
        f.write_str("]")?;
        if !self.groupby_exprs.is_empty() {
            f.write_str(" -> Group By [")?;
            fmt_explain_list(&self.groupby_exprs, ", ", arena, f)?;
            f.write_str("]")?;
        }
        Ok(())
    }
}
