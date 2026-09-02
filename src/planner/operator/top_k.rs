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

use super::Operator;
use crate::planner::operator::sort::SortField;
use crate::planner::{fmt_explain_list, Childrens, Explain, LogicalPlan, PlanArena};
use kite_sql_serde_macros::ReferenceSerialization;

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

impl Explain for TopKOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Top {}, ", self.limit)?;
        if let Some(offset) = self.offset {
            write!(f, "Offset {offset}, ")?;
        }
        f.write_str("Sort By ")?;
        fmt_explain_list(&self.sort_fields, ", ", arena, f)
    }
}
