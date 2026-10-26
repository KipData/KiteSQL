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
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child};
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::types::LogicalType;
use std::mem;

/// Combine two adjacent filter operators into one.
pub struct CombineFilter;

impl NormalizationRule for CombineFilter {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let parent_filter = match mem::replace(&mut plan.operator, Operator::Dummy) {
            Operator::Filter(op) => op,
            operator => {
                plan.operator = operator;
                return Ok(false);
            }
        };
        let cursor = match only_child_mut(plan) {
            Some(child) => child,
            None => {
                plan.operator = Operator::Filter(parent_filter);
                return Ok(false);
            }
        };

        match &mut cursor.operator {
            Operator::Filter(child_op) => {
                let FilterOperator {
                    predicate,
                    having,
                    is_optimized: _,
                } = parent_filter;
                let child_predicate = child_op.predicate;
                child_op.predicate = arena.alloc_expression(ScalarExpression::Binary {
                    op: BinaryOperator::And,
                    left_expr: predicate,
                    right_expr: child_predicate,
                    evaluator: None,
                    ty: LogicalType::Boolean,
                });
                child_op.having = having || child_op.having;
                child_op.is_optimized = false;

                Ok(replace_with_only_child(plan))
            }
            _ => {
                plan.operator = Operator::Filter(parent_filter);
                Ok(false)
            }
        }
    }
}

fn groupby_exprs_match(
    parent_exprs: &[ExprRef],
    child_exprs: &[ExprRef],
    arena: &PlanArena<'_>,
) -> bool {
    parent_exprs.len() == child_exprs.len()
        && parent_exprs
            .iter()
            .zip(child_exprs)
            .all(|(parent, child)| parent.eq_ignore_colref_pos(*child, arena))
}

pub struct CollapseGroupByAgg;

impl NormalizationRule for CollapseGroupByAgg {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let can_collapse = {
            let LogicalPlan {
                operator,
                childrens,
                ..
            } = plan;
            let Operator::Aggregate(op) = operator else {
                return Ok(false);
            };
            if !op.agg_calls.is_empty() {
                return Ok(false);
            }

            let Childrens::Only(child) = childrens.as_ref() else {
                return Ok(false);
            };
            let Operator::Aggregate(child_op) = &child.operator else {
                return Ok(false);
            };
            groupby_exprs_match(&op.groupby_exprs, &child_op.groupby_exprs, arena)
        };

        if can_collapse {
            return Ok(replace_with_only_child(plan));
        }

        Ok(false)
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::catalog::ColumnCatalog;
    use crate::errors::DatabaseError;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::combine_operators::CollapseGroupByAgg;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};

    fn column_expr(arena: &mut PlanArena, name: &str, position: usize) -> ExprRef {
        let column = arena.alloc_column(ColumnCatalog::new_dummy(name.to_string()));
        arena.alloc_expression(ScalarExpression::column_expr(column, position))
    }

    #[test]
    fn test_combine_filter_keeps_reordered_projection() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state.plan_with_arena("select * from t1 c where c.c1 > 1", &mut arena)?;
        let mut filter = plan.childrens.pop_only();
        if let Childrens::Only(child) = filter.childrens.as_mut() {
            if let Operator::Project(project) = &mut child.operator {
                project.exprs.reverse();
            }
        }
        assert!(!super::CombineFilter.apply(&mut filter, &mut arena, None)?);
        assert!(matches!(
            filter.childrens.pop_only().operator,
            Operator::Project(_)
        ));
        Ok(())
    }

    #[test]
    fn test_combine_filter() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state.plan_with_arena(
            "select * from (select * from t1 where c1 > 1) t where 1 = 1",
            &mut arena,
        )?;

        // Projects are eliminated before filters are combined.
        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "Collapse Project",
                HepBatchStrategy::fix_point_topdown(10),
                &[NormalizationRuleImpl::EliminateProject],
            )
            .before_batch(
                "test_combine_filter",
                HepBatchStrategy::once_topdown(),
                &[NormalizationRuleImpl::CombineFilter],
            )
            .build();
        let best_plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(op) = &filter_op.operator {
            if let ScalarExpression::Binary { op, .. } = arena.expression(op.predicate) {
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
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state
            .plan_with_arena("select distinct c1, c2 from t1 group by c1, c2", &mut arena)?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_collapse_group_by_agg",
                HepBatchStrategy::once_topdown(),
                &[NormalizationRuleImpl::CollapseGroupByAgg],
            )
            .build();

        let best_plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;

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

    #[test]
    fn test_collapse_group_by_agg_ignores_columnref_position() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let child = AggregateOperator::build(
            LogicalPlan::new(Operator::Dummy, Childrens::None),
            vec![],
            vec![column_expr(&mut arena, "c2", 1)],
            false,
            false,
        );
        let expr = column_expr(&mut arena, "c2", 0);
        let mut plan = AggregateOperator::build(child, vec![], vec![expr], true, false);

        assert!(CollapseGroupByAgg.apply(&mut plan, &mut arena, None)?);
        let Operator::Aggregate(op) = &plan.operator else {
            unreachable!("expected aggregate");
        };
        let ScalarExpression::ColumnRef { position, .. } = arena.expression(op.groupby_exprs[0])
        else {
            unreachable!("expected column ref");
        };
        assert_eq!(*position, 1);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
