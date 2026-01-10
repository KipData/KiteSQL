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
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};

/// Use pattern to determines which rule can be applied
pub struct HepMatcher<'a, 'b> {
    pattern: &'a Pattern,
    start_id: HepNodeId,
    graph: &'b HepGraph,
}

impl<'a, 'b> HepMatcher<'a, 'b> {
    pub(crate) fn new(pattern: &'a Pattern, start_id: HepNodeId, graph: &'b HepGraph) -> Self {
        Self {
            pattern,
            start_id,
            graph,
        }
    }
}

impl PatternMatcher for HepMatcher<'_, '_> {
    fn match_opt_expr(&self) -> bool {
        let op = self.graph.operator(self.start_id);
        // check the root node predicate
        if !(self.pattern.predicate)(op) {
            return false;
        }

        match &self.pattern.children {
            PatternChildrenPredicate::Recursive => {
                // check
                for node_id in self.graph.nodes_iter(Some(self.start_id)) {
                    if !(self.pattern.predicate)(self.graph.operator(node_id)) {
                        return false;
                    }
                }
            }
            PatternChildrenPredicate::Predicate(patterns) => {
                for node_id in self.graph.children_at(self.start_id) {
                    for pattern in patterns {
                        if !HepMatcher::new(pattern, node_id, self.graph).match_opt_expr() {
                            return false;
                        }
                    }
                }
            }
            PatternChildrenPredicate::None => (),
        }

        true
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate, PatternMatcher};
    use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
    use crate::optimizer::heuristic::matcher::HepMatcher;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};

    #[test]
    fn test_predicate() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1")?;
        let graph = HepGraph::new(plan.clone());

        let project_into_table_scan_pattern = Pattern {
            predicate: |p| match p {
                Operator::Project(_) => true,
                _ => false,
            },
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| match p {
                    Operator::TableScan(_) => true,
                    _ => false,
                },
                children: PatternChildrenPredicate::None,
            }]),
        };

        assert!(
            HepMatcher::new(&project_into_table_scan_pattern, HepNodeId::new(0), &graph)
                .match_opt_expr()
        );

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
        let graph = HepGraph::new(all_dummy_plan.clone());

        let only_dummy_pattern = Pattern {
            predicate: |p| match p {
                Operator::Dummy => true,
                _ => false,
            },
            children: PatternChildrenPredicate::Recursive,
        };

        assert!(HepMatcher::new(&only_dummy_pattern, HepNodeId::new(0), &graph).match_opt_expr());
    }
}
