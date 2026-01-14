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
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child, wrap_child_with};
use crate::planner::operator::join::JoinType;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use std::sync::LazyLock;

static LIMIT_PROJECT_TRANSPOSE_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Limit(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Project(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

static PUSH_LIMIT_THROUGH_JOIN_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Limit(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Join(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

static PUSH_LIMIT_INTO_TABLE_SCAN_RULE: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Limit(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::TableScan(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

pub struct LimitProjectTranspose;

impl MatchPattern for LimitProjectTranspose {
    fn pattern(&self) -> &Pattern {
        &LIMIT_PROJECT_TRANSPOSE_RULE
    }
}

impl NormalizationRule for LimitProjectTranspose {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let operator = std::mem::replace(&mut plan.operator, Operator::Dummy);

        let limit_op = match operator {
            Operator::Limit(op) => op,
            other => {
                plan.operator = other;
                return Ok(false);
            }
        };

        let mut project_op = None;

        if let Some(child) = only_child_mut(plan) {
            if matches!(child.operator, Operator::Project(_)) {
                project_op = Some(std::mem::replace(
                    &mut child.operator,
                    Operator::Limit(limit_op.clone()),
                ));
            }
        }

        if let Some(project_op) = project_op {
            plan.operator = project_op;
            return Ok(true);
        }

        plan.operator = Operator::Limit(limit_op);
        Ok(false)
    }
}

/// Add extra limits below JOIN:
/// 1. For LEFT OUTER and RIGHT OUTER JOIN, we push limits to the left and right sides,
///    respectively.
///
/// TODO: 2. For INNER and CROSS JOIN, we push limits to both the left and right sides
/// TODO: if join condition is empty.
pub struct PushLimitThroughJoin;

impl MatchPattern for PushLimitThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_THROUGH_JOIN_RULE
    }
}

impl NormalizationRule for PushLimitThroughJoin {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let limit_op = match &plan.operator {
            Operator::Limit(op) => op.clone(),
            _ => return Ok(false),
        };

        if let Some(child) = only_child_mut(plan) {
            if let Operator::Join(join_op) = &child.operator {
                let mut applied = false;
                match join_op.join_type {
                    JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => {
                        applied |= wrap_child_with(child, 0, Operator::Limit(limit_op.clone()));
                    }
                    JoinType::RightOuter => {
                        applied |= wrap_child_with(child, 1, Operator::Limit(limit_op));
                    }
                    _ => {}
                }
                return Ok(applied);
            }
        }

        Ok(false)
    }
}

/// Push down `Limit` past a `Scan`.
pub struct PushLimitIntoScan;

impl MatchPattern for PushLimitIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_INTO_TABLE_SCAN_RULE
    }
}

impl NormalizationRule for PushLimitIntoScan {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let (offset, limit) = match &plan.operator {
            Operator::Limit(limit_op) => (limit_op.offset, limit_op.limit),
            _ => return Ok(false),
        };

        if let Some(child) = only_child_mut(plan) {
            if let Operator::TableScan(scan_op) = &mut child.operator {
                scan_op.limit = (offset, limit);
                let removed = replace_with_only_child(plan);
                return Ok(removed);
            }
        }

        Ok(false)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::storage::rocksdb::RocksTransaction;

    #[test]
    fn test_limit_project_transpose() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select c1, c2 from t1 limit 1")?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_limit_project_transpose".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::LimitProjectTranspose],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

        if let Operator::Project(_) = &best_plan.operator {
        } else {
            unreachable!("Should be a project operator")
        }

        let limit_op = best_plan.childrens.pop_only();
        if let Operator::Limit(_) = &limit_op.operator {
        } else {
            unreachable!("Should be a limit operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_limit_through_join() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 left join t2 on c1 = c3 limit 1")?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_push_limit_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::LimitProjectTranspose,
                    NormalizationRuleImpl::PushLimitThroughJoin,
                ],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

        let join_op = best_plan.childrens.pop_only().childrens.pop_only();
        if let Operator::Join(_) = &join_op.operator {
        } else {
            unreachable!("Should be a join operator")
        }

        let limit_op = join_op.childrens.pop_twins().0;
        if let Operator::Limit(op) = &limit_op.operator {
            assert_eq!(op.limit, Some(1));
        } else {
            unreachable!("Should be a limit operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_limit_into_table_scan() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select * from t1 limit 1 offset 1")?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_push_limit_into_table_scan".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::LimitProjectTranspose,
                    NormalizationRuleImpl::PushLimitIntoTableScan,
                ],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

        let scan_op = best_plan.childrens.pop_only();
        if let Operator::TableScan(op) = &scan_op.operator {
            assert_eq!(op.limit, (Some(1), Some(1)))
        } else {
            unreachable!("Should be a project operator")
        }

        Ok(())
    }
}
