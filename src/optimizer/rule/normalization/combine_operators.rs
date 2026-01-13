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
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child};
use crate::optimizer::rule::normalization::{is_subset_exprs, strip_alias};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::types::LogicalType;
use std::collections::HashSet;
use std::sync::LazyLock;

static COLLAPSE_PROJECT_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Project(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Project(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

static COMBINE_FILTERS_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Filter(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Filter(_)) || is_passthrough_project_operator(op),
        children: PatternChildrenPredicate::None,
    }]),
});

static COLLAPSE_GROUP_BY_AGG: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| match op {
        Operator::Aggregate(agg_op) => !agg_op.groupby_exprs.is_empty(),
        _ => false,
    },
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| match op {
            Operator::Aggregate(agg_op) => !agg_op.groupby_exprs.is_empty(),
            _ => false,
        },
        children: PatternChildrenPredicate::None,
    }]),
});

fn is_passthrough_project(op: &ProjectOperator) -> bool {
    op.exprs
        .iter()
        .all(|expr| matches!(strip_alias(expr), ScalarExpression::ColumnRef { .. }))
}

fn is_passthrough_project_operator(op: &Operator) -> bool {
    matches!(op, Operator::Project(project_op) if is_passthrough_project(project_op))
}

/// Combine two adjacent project operators into one.
pub struct CollapseProject;

impl MatchPattern for CollapseProject {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_PROJECT_RULE
    }
}

impl NormalizationRule for CollapseProject {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let parent_exprs = match &plan.operator {
            Operator::Project(op) => op.exprs.clone(),
            _ => return Ok(false),
        };

        let mut removed = false;
        while let Some(child) = only_child_mut(plan) {
            match &child.operator {
                Operator::Project(child_op)
                    if is_passthrough_project(child_op)
                        && is_subset_exprs(&parent_exprs, &child_op.exprs) =>
                {
                    removed |= replace_with_only_child(child);
                }
                _ => break,
            }
        }

        Ok(removed)
    }
}

/// Combine two adjacent filter operators into one.
pub struct CombineFilter;

impl MatchPattern for CombineFilter {
    fn pattern(&self) -> &Pattern {
        &COMBINE_FILTERS_RULE
    }
}

impl NormalizationRule for CombineFilter {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let (parent_predicate, parent_having) = match &plan.operator {
            Operator::Filter(op) => (op.predicate.clone(), op.having),
            _ => return Ok(false),
        };

        let cursor = match only_child_mut(plan) {
            Some(child) => child,
            None => return Ok(false),
        };

        loop {
            match &mut cursor.operator {
                Operator::Filter(child_op) => {
                    child_op.predicate = ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(parent_predicate),
                        right_expr: Box::new(child_op.predicate.clone()),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    };
                    child_op.having = parent_having || child_op.having;

                    return Ok(replace_with_only_child(plan));
                }
                Operator::Project(project_op) if is_passthrough_project(project_op) => {
                    if replace_with_only_child(cursor) {
                        continue;
                    }
                    return Ok(false);
                }
                _ => return Ok(false),
            }
        }
    }
}

pub struct CollapseGroupByAgg;

impl MatchPattern for CollapseGroupByAgg {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_GROUP_BY_AGG
    }
}

impl NormalizationRule for CollapseGroupByAgg {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        if let Operator::Aggregate(op) = plan.operator.clone() {
            if !op.agg_calls.is_empty() {
                return Ok(false);
            }

            if let Some(child) = only_child_mut(plan) {
                if let Operator::Aggregate(child_op) = child.operator.clone() {
                    if op.groupby_exprs.len() != child_op.groupby_exprs.len() {
                        return Ok(false);
                    }
                    let mut expr_set = HashSet::new();

                    for expr in op.groupby_exprs.iter() {
                        expr_set.insert(expr);
                    }
                    for expr in child_op.groupby_exprs.iter() {
                        expr_set.remove(expr);
                    }
                    if expr_set.is_empty() {
                        return Ok(replace_with_only_child(plan));
                    }
                }
            }
        }

        Ok(false)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::planner::Childrens;
    use crate::storage::rocksdb::RocksTransaction;

    #[test]
    fn test_collapse_project() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select c1 from (select c1, c2 from t1) t")?;

        let optimizer = HepOptimizer::new(plan).before_batch(
            "test_collapse_project".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CollapseProject],
        );
        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;

        if let Operator::Project(op) = &best_plan.operator {
            assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator")
        }

        let scan_op = best_plan.childrens.pop_only();
        if let Operator::TableScan(_) = &scan_op.operator {
            assert!(matches!(scan_op.childrens.as_ref(), Childrens::None));
        } else {
            unreachable!("Should be a scan operator")
        }

        Ok(())
    }

    #[test]
    fn test_collapse_project_with_alias() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select t.x from (select c1 as x from t1) t")?;
        let original = plan.clone();
        let original_child = original.childrens.pop_only();
        assert!(matches!(original_child.operator, Operator::Project(_)));
        let original_grandchild = original_child.childrens.pop_only();
        assert!(matches!(original_grandchild.operator, Operator::Project(_)));

        let optimizer = HepOptimizer::new(plan).before_batch(
            "test_collapse_project_with_alias".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CollapseProject],
        );
        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;
        if let Operator::Project(op) = &best_plan.operator {
            assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator");
        }

        let scan_op = best_plan.childrens.pop_only();
        assert!(matches!(scan_op.operator, Operator::Project(_)));
        let scan_child = scan_op.childrens.pop_only();
        assert!(
            !matches!(scan_child.operator, Operator::Project(_)),
            "Child project should be collapsed"
        );

        Ok(())
    }

    #[test]
    fn test_combine_filter() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan =
            table_state.plan("select * from (select * from t1 where c1 > 1) t where 1 = 1")?;

        let optimizer = HepOptimizer::new(plan).before_batch(
            "test_combine_filter".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CombineFilter],
        );
        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(op) = &filter_op.operator {
            if let ScalarExpression::Binary { op, .. } = &op.predicate {
                assert_eq!(op, &BinaryOperator::And);
            } else {
                unreachable!("Should be a and operator")
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_collapse_group_by_agg() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select distinct c1, c2 from t1 group by c1, c2")?;

        let optimizer = HepOptimizer::new(plan.clone()).before_batch(
            "test_collapse_group_by_agg".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::CollapseGroupByAgg],
        );

        let best_plan = optimizer.find_best::<RocksTransaction>(None)?;

        let agg_op = best_plan.childrens.pop_only();
        if let Operator::Aggregate(_) = &agg_op.operator {
            let inner_agg_op = agg_op.childrens.pop_only();
            if let Operator::Aggregate(_) = &inner_agg_op.operator {
                unreachable!("Should not be a agg operator")
            } else {
                return Ok(());
            }
        }
        unreachable!("Should be a agg operator")
    }
}
