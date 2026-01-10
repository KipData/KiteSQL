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

use crate::errors::DatabaseError;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child};
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use std::sync::LazyLock;

static TOP_K_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Limit(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Sort(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

pub struct TopK;

impl MatchPattern for TopK {
    fn pattern(&self) -> &Pattern {
        &TOP_K_RULE
    }
}

impl NormalizationRule for TopK {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let (offset, limit) = match &plan.operator {
            Operator::Limit(op) => match op.limit {
                Some(limit) => (op.offset, limit),
                None => return Ok(false),
            },
            _ => return Ok(false),
        };

        let sort_fields = {
            let child = match only_child_mut(plan) {
                Some(child) => child,
                None => return Ok(false),
            };

            match &child.operator {
                Operator::Sort(sort_op) => {
                    let fields = sort_op.sort_fields.clone();
                    let removed = replace_with_only_child(child);
                    debug_assert!(removed);
                    fields
                }
                _ => return Ok(false),
            }
        };

        plan.operator = Operator::TopK(TopKOperator {
            sort_fields,
            limit,
            offset,
        });
        Ok(true)
    }
}
