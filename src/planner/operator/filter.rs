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

use crate::planner::{Childrens, Explain, ExprRef, LogicalPlan, PlanArena};
use kite_sql_serde_macros::ReferenceSerialization;

use super::Operator;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct FilterOperator {
    pub predicate: ExprRef,
    pub is_optimized: bool,
    pub having: bool,
}

impl FilterOperator {
    pub fn build(predicate: ExprRef, children: LogicalPlan, having: bool) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                is_optimized: false,
                having,
            }),
            Childrens::Only(Box::new(children)),
        )
    }
}

impl Explain for FilterOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Filter {}, Is Having: {}",
            self.predicate.explain(arena),
            self.having
        )
    }
}
