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

use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate, PatternMatcher};
use crate::planner::LogicalPlan;

/// Use pattern to determine which rule can be applied on a [`LogicalPlan`] subtree.
pub struct PlanMatcher<'a> {
    pattern: &'a Pattern,
    plan: &'a LogicalPlan,
}

impl<'a> PlanMatcher<'a> {
    pub(crate) fn new(pattern: &'a Pattern, plan: &'a LogicalPlan) -> Self {
        Self { pattern, plan }
    }
}

impl PatternMatcher for PlanMatcher<'_> {
    fn match_opt_expr(&self) -> bool {
        if !(self.pattern.predicate)(&self.plan.operator) {
            return false;
        }

        match &self.pattern.children {
            PatternChildrenPredicate::Recursive => {
                for child in self.plan.childrens.iter() {
                    if !(self.pattern.predicate)(&child.operator) {
                        return false;
                    }
                    if !PlanMatcher::new(self.pattern, child).match_opt_expr() {
                        return false;
                    }
                }
            }
            PatternChildrenPredicate::Predicate(patterns) => {
                for child in self.plan.childrens.iter() {
                    for pattern in patterns {
                        if !PlanMatcher::new(pattern, child).match_opt_expr() {
                            return false;
                        }
                    }
                }
            }
            PatternChildrenPredicate::None => {}
        }

        true
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate, PatternMatcher};
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};

    #[test]
    fn test_predicate() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1")?;

        let project_into_table_scan_pattern = Pattern {
            predicate: |p| matches!(p, Operator::Project(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| matches!(p, Operator::TableScan(_)),
                children: PatternChildrenPredicate::None,
            }]),
        };

        assert!(PlanMatcher::new(&project_into_table_scan_pattern, &plan).match_opt_expr());

        Ok(())
    }

    #[test]
    fn test_recursive() {
        let all_dummy_plan = LogicalPlan {
            operator: Operator::Dummy,
            childrens: Box::new(Childrens::Twins {
                left: Box::new(LogicalPlan {
                    operator: Operator::Dummy,
                    childrens: Box::new(Childrens::Only(Box::new(LogicalPlan {
                        operator: Operator::Dummy,
                        childrens: Box::new(Childrens::None),
                        physical_option: None,
                        _output_schema_ref: None,
                    }))),
                    physical_option: None,
                    _output_schema_ref: None,
                }),
                right: Box::new(LogicalPlan {
                    operator: Operator::Dummy,
                    childrens: Box::new(Childrens::None),
                    physical_option: None,
                    _output_schema_ref: None,
                }),
            }),
            physical_option: None,
            _output_schema_ref: None,
        };

        let only_dummy_pattern = Pattern {
            predicate: |p| matches!(p, Operator::Dummy),
            children: PatternChildrenPredicate::Recursive,
        };

        assert!(PlanMatcher::new(&only_dummy_pattern, &all_dummy_plan).match_opt_expr());
    }
}
