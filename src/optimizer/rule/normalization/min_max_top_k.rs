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
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::plan_utils::{only_child, wrap_child_with};
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use std::sync::LazyLock;

static MIN_MAX_TOPK_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Aggregate(_)),
    children: PatternChildrenPredicate::None,
});

pub struct MinMaxToTopK;

impl MatchPattern for MinMaxToTopK {
    fn pattern(&self) -> &Pattern {
        &MIN_MAX_TOPK_PATTERN
    }
}

impl NormalizationRule for MinMaxToTopK {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let Operator::Aggregate(op) = &plan.operator else {
            return Ok(false);
        };
        if !op.groupby_exprs.is_empty() || op.agg_calls.len() != 1 {
            return Ok(false);
        }

        let ScalarExpression::AggCall { kind, args, .. } = &op.agg_calls[0] else {
            return Ok(false);
        };
        if args.len() != 1 {
            return Ok(false);
        }

        let asc = match kind {
            AggKind::Min => true,
            AggKind::Max => false,
            _ => return Ok(false),
        };

        let sort_field = SortField::new(args[0].clone(), asc, true);
        let already_topk = match only_child(plan) {
            Some(child) => match &child.operator {
                Operator::TopK(topk) => {
                    topk.limit == 1
                        && topk.offset.is_none()
                        && topk.sort_fields == vec![sort_field.clone()]
                }
                _ => false,
            },
            None => return Ok(false),
        };

        if already_topk {
            return Ok(false);
        }

        // IndexScan prioritizes indexed columns as null first.
        // Therefore, to ensure Top K is eliminated when an index exists,
        // we set it to null first and filter null rows.
        let predicate = ScalarExpression::IsNull {
            negated: true,
            expr: Box::new(args[0].clone()),
        };
        let filter = Operator::Filter(FilterOperator {
            predicate,
            is_optimized: false,
            having: false,
        });

        if !wrap_child_with(plan, 0, filter) {
            return Ok(false);
        }

        // Agg do not remove, because when the table is empty, MIN/MAX should return a NULL row.
        Ok(wrap_child_with(
            plan,
            0,
            Operator::TopK(TopKOperator {
                sort_fields: vec![sort_field],
                limit: 1,
                offset: None,
            }),
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::MinMaxToTopK;
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::planner::operator::Operator;
    use crate::planner::Childrens;

    fn find_aggregate<'a>(
        plan: &'a crate::planner::LogicalPlan,
    ) -> &'a crate::planner::LogicalPlan {
        if matches!(plan.operator, Operator::Aggregate(_)) {
            return plan;
        }
        match plan.childrens.as_ref() {
            Childrens::Only(child) => find_aggregate(child),
            _ => panic!("Aggregate operator not found"),
        }
    }

    fn find_aggregate_mut<'a>(
        plan: &'a mut crate::planner::LogicalPlan,
    ) -> &'a mut crate::planner::LogicalPlan {
        if matches!(plan.operator, Operator::Aggregate(_)) {
            return plan;
        }
        match plan.childrens.as_mut() {
            Childrens::Only(child) => find_aggregate_mut(child),
            _ => panic!("Aggregate operator not found"),
        }
    }

    #[test]
    fn test_min_to_topk() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut plan = table_state.plan("select min(c1) from t1")?;

        let agg_plan = find_aggregate_mut(&mut plan);
        assert!(MinMaxToTopK.apply(agg_plan)?);

        let agg_plan = find_aggregate(&plan);
        let Operator::Aggregate(op) = &agg_plan.operator else {
            unreachable!("Aggregate operator not found");
        };
        let child = match agg_plan.childrens.as_ref() {
            Childrens::Only(child) => child.as_ref(),
            _ => unreachable!("Aggregate should have one child"),
        };
        let topk_plan = match &child.operator {
            Operator::TopK(_) => child,
            _ => unreachable!("Expected TopK under Aggregate"),
        };
        let Operator::TopK(topk) = &topk_plan.operator else {
            unreachable!("Expected TopK under Aggregate");
        };
        assert_eq!(topk.limit, 1);
        assert!(topk.offset.is_none());
        assert_eq!(topk.sort_fields.len(), 1);
        assert!(topk.sort_fields[0].asc);
        assert!(topk.sort_fields[0].nulls_first);
        let args = match &op.agg_calls[0] {
            crate::expression::ScalarExpression::AggCall { args, .. } => args,
            _ => unreachable!("Aggregate should use AggCall"),
        };
        assert_eq!(topk.sort_fields[0].expr, args[0]);

        let filter_plan = match topk_plan.childrens.as_ref() {
            Childrens::Only(child) => child.as_ref(),
            _ => unreachable!("TopK should have one child"),
        };
        match &filter_plan.operator {
            Operator::Filter(filter_op) => match &filter_op.predicate {
                crate::expression::ScalarExpression::IsNull { negated, expr } => {
                    assert!(*negated);
                    assert_eq!(**expr, args[0]);
                }
                _ => unreachable!("Expected IS NOT NULL filter under TopK"),
            },
            _ => unreachable!("Expected Filter under TopK"),
        }

        Ok(())
    }

    #[test]
    fn test_max_to_topk() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut plan = table_state.plan("select max(c2) from t1")?;

        let agg_plan = find_aggregate_mut(&mut plan);
        assert!(MinMaxToTopK.apply(agg_plan)?);

        let agg_plan = find_aggregate(&plan);
        let child = match agg_plan.childrens.as_ref() {
            Childrens::Only(child) => child.as_ref(),
            _ => unreachable!("Aggregate should have one child"),
        };
        let topk_plan = match &child.operator {
            Operator::TopK(_) => child,
            _ => unreachable!("Expected TopK under Aggregate"),
        };
        let Operator::TopK(topk) = &topk_plan.operator else {
            unreachable!("Expected TopK under Aggregate");
        };
        assert_eq!(topk.limit, 1);
        assert!(topk.offset.is_none());
        assert_eq!(topk.sort_fields.len(), 1);
        assert!(!topk.sort_fields[0].asc);
        assert!(topk.sort_fields[0].nulls_first);

        let filter_plan = match topk_plan.childrens.as_ref() {
            Childrens::Only(child) => child.as_ref(),
            _ => unreachable!("TopK should have one child"),
        };
        match &filter_plan.operator {
            Operator::Filter(filter_op) => match &filter_op.predicate {
                crate::expression::ScalarExpression::IsNull { negated, .. } => {
                    assert!(*negated);
                }
                _ => unreachable!("Expected IS NOT NULL filter under TopK"),
            },
            _ => unreachable!("Expected Filter under TopK"),
        }

        Ok(())
    }
}
