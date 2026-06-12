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
use crate::expression::simplify::{ConstantCalculator, Simplify};
use crate::expression::visitor_mut::VisitorMut;
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};

#[derive(Copy, Clone)]
pub struct ConstantCalculation;

pub(crate) fn constant_calculation_current(
    plan: &mut LogicalPlan,
    arena: &crate::planner::PlanArena,
) -> Result<(), DatabaseError> {
    let operator = &mut plan.operator;
    let mut calculator = ConstantCalculator::new(arena);

    match operator {
        Operator::Aggregate(op) => {
            for expr in op.agg_calls.iter_mut().chain(op.groupby_exprs.iter_mut()) {
                calculator.visit(expr)?;
            }
        }
        Operator::Filter(op) => {
            calculator.visit(&mut op.predicate)?;
        }
        Operator::Join(op) => {
            if let JoinCondition::On { on, filter } = &mut op.on {
                for (left_expr, right_expr) in on {
                    calculator.visit(left_expr)?;
                    calculator.visit(right_expr)?;
                }
                if let Some(expr) = filter {
                    calculator.visit(expr)?;
                }
            }
        }
        Operator::Project(op) => {
            for expr in &mut op.exprs {
                calculator.visit(expr)?;
            }
        }
        Operator::Sort(op) => {
            for field in &mut op.sort_fields {
                calculator.visit(&mut field.expr)?;
            }
        }
        _ => (),
    }

    Ok(())
}

impl ConstantCalculation {
    fn _apply(
        plan: &mut LogicalPlan,
        arena: &crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        constant_calculation_current(plan, arena)?;
        match plan.childrens.as_mut() {
            Childrens::Only(child) => Self::_apply(child.as_mut(), arena)?,
            Childrens::Twins { left, right } => {
                Self::_apply(left.as_mut(), arena)?;
                Self::_apply(right.as_mut(), arena)?;
            }
            Childrens::None => (),
        }

        Ok(())
    }
}

impl NormalizationRule for ConstantCalculation {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        Self::_apply(plan, arena)?;
        Ok(true)
    }
}

#[derive(Copy, Clone)]
pub struct SimplifyFilter;

fn has_aggregate_descendant(plan: &LogicalPlan) -> bool {
    if matches!(plan.operator, Operator::Aggregate(_)) {
        return true;
    }

    match plan.childrens.as_ref() {
        Childrens::Only(child) => has_aggregate_descendant(child),
        Childrens::Twins { left, right } => {
            has_aggregate_descendant(left) || has_aggregate_descendant(right)
        }
        Childrens::None => false,
    }
}

impl NormalizationRule for SimplifyFilter {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        if let Operator::Filter(filter_op) = &mut plan.operator {
            if filter_op.is_optimized {
                return Ok(false);
            }
            if let Some(child) = plan.childrens.iter().next() {
                if has_aggregate_descendant(child) {
                    return Ok(false);
                }
            }
            ConstantCalculator::new(arena).visit(&mut filter_op.predicate)?;
            Simplify::default().visit(&mut filter_op.predicate)?;
            filter_op.is_optimized = true;
            return Ok(true);
        }

        Ok(false)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::binder::test::build_t1_table;

    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::{Range, RangeDetacher};
    use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::planner::{LogicalPlan, PlanArena};
    use crate::types::value::DataValue;
    use crate::types::{ColumnId, LogicalType};
    use std::collections::Bound;

    fn run_with_single_batch(
        plan: LogicalPlan,
        name: &str,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        HepOptimizerPipeline::builder()
            .before_batch(name.to_string(), strategy, rules)
            .build()
            .instantiate(plan)
            .find_best(None, arena)
    }

    #[test]
    fn test_constant_calculation_omitted() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        // (2 + (-1)) < -(c1 + 1)
        let plan = table_state.plan_with_arena(
            "select c1 + (2 + 1), 2 + 1 from t1 where (2 + (-1)) < -(c1 + 1)",
            &mut arena,
        )?;

        let best_plan = HepOptimizerPipeline::builder()
            .before_batch(
                "test_simplification".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::SimplifyFilter,
                    NormalizationRuleImpl::ConstantCalculation,
                ],
            )
            .build()
            .instantiate(plan)
            .find_best(None, &mut arena)?;
        if let Operator::Project(project_op) = best_plan.clone().operator {
            let constant_expr = ScalarExpression::Constant(DataValue::Int32(3));
            if let ScalarExpression::Binary { right_expr, .. } = &project_op.exprs[0] {
                assert_eq!(right_expr.as_ref(), &constant_expr);
            } else {
                unreachable!();
            }
            assert_eq!(&project_op.exprs[1], &constant_expr);
        } else {
            unreachable!();
        }
        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(filter_op) = filter_op.operator {
            let range = RangeDetacher::new("t1", table_state.column_id_by_name("c1"), &arena)
                .detach(&filter_op.predicate)?
                .unwrap();
            assert_eq!(
                range,
                Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Excluded(DataValue::Int32(-2)),
                }
            );
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[test]
    fn test_constant_cast_elimination() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state.plan_with_arena(
            "select cast(1 as int), cast(2 as int) + 1 from t1 where cast(3 as int) = 3",
            &mut arena,
        )?;

        let best_plan = run_with_single_batch(
            plan,
            "test_constant_cast_elimination",
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::ConstantCalculation],
            &mut arena,
        )?;

        if let Operator::Project(project_op) = best_plan.operator {
            assert_eq!(
                project_op.exprs[0],
                ScalarExpression::Constant(DataValue::Int32(1))
            );
            assert_eq!(
                project_op.exprs[1],
                ScalarExpression::Constant(DataValue::Int32(3))
            );
        } else {
            unreachable!();
        }

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(filter_op) = filter_op.operator {
            assert_eq!(
                filter_op.predicate,
                ScalarExpression::Constant(DataValue::Boolean(true))
            );
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[test]
    fn test_simplify_filter_single_column() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        // c1 + 1 < -1 => c1 < -2
        let plan_1 =
            table_state.plan_with_arena("select * from t1 where -(c1 + 1) > 1", &mut arena)?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 =
            table_state.plan_with_arena("select * from t1 where -(1 - c1) > 1", &mut arena)?;
        // c1 < -1
        let plan_3 = table_state.plan_with_arena("select * from t1 where -c1 > 1", &mut arena)?;
        // c1 > 0
        let plan_4 =
            table_state.plan_with_arena("select * from t1 where c1 + 1 > 1", &mut arena)?;

        // c1 + 1 < -1 => c1 < -2
        let plan_5 =
            table_state.plan_with_arena("select * from t1 where 1 < -(c1 + 1)", &mut arena)?;
        // 1 - c1 < -1 => c1 > 2
        let plan_6 =
            table_state.plan_with_arena("select * from t1 where 1 < -(1 - c1)", &mut arena)?;
        // c1 < -1
        let plan_7 = table_state.plan_with_arena("select * from t1 where 1 < -c1", &mut arena)?;
        // c1 > 0
        let plan_8 =
            table_state.plan_with_arena("select * from t1 where 1 < c1 + 1", &mut arena)?;
        // c1 < 24
        let plan_9 =
            table_state.plan_with_arena("select * from t1 where (-1 - c1) + 1 > 24", &mut arena)?;
        // c1 < 24
        let plan_10 =
            table_state.plan_with_arena("select * from t1 where 24 < (-1 - c1) + 1", &mut arena)?;

        let mut op = |plan: LogicalPlan| -> Result<Option<Range>, DatabaseError> {
            let best_plan = run_with_single_batch(
                plan,
                "test_simplify_filter",
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
                &mut arena,
            )?;

            let filter_op = best_plan.childrens.pop_only();
            if let Operator::Filter(filter_op) = filter_op.operator {
                Ok(
                    RangeDetacher::new("t1", table_state.column_id_by_name("c1"), &arena)
                        .detach(&filter_op.predicate)?,
                )
            } else {
                Ok(None)
            }
        };

        let op_1 = op(plan_1)?;
        let op_2 = op(plan_2)?;
        let op_3 = op(plan_3)?;
        let op_4 = op(plan_4)?;
        let op_5 = op(plan_9)?;

        assert!(op_1.is_some());
        assert!(op_2.is_some());
        assert!(op_3.is_some());
        assert!(op_4.is_some());
        assert!(op_5.is_some());

        assert_eq!(op_1, op(plan_5)?);
        assert_eq!(op_2, op(plan_6)?);
        assert_eq!(op_3, op(plan_7)?);
        assert_eq!(op_4, op(plan_8)?);
        assert_eq!(op_5, op(plan_10)?);

        Ok(())
    }

    #[test]
    fn test_simplify_filter_boolean_wrapped_range_comparison() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let expected = Some(Range::Scope {
            min: Bound::Unbounded,
            max: Bound::Included(DataValue::Int32(10)),
        });

        for sql in [
            "select * from t1 where (c1 > 10) = false",
            "select * from t1 where (c1 > 10) != true",
            "select * from t1 where not (c1 > 10)",
        ] {
            let plan = table_state.plan_with_arena(sql, &mut arena)?;
            assert_eq!(
                plan_filter(&plan, table_state.column_id_by_name("c1"), &mut arena)?,
                expected
            );
        }

        Ok(())
    }

    #[test]
    fn test_simplify_filter_repeating_column() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan =
            table_state.plan_with_arena("select * from t1 where -(c1 + 1) > c2", &mut arena)?;

        let best_plan = run_with_single_batch(
            plan,
            "test_simplify_filter",
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::SimplifyFilter],
            &mut arena,
        )?;

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(filter_op) = filter_op.operator {
            let c1_ref = table_state.table.get_column_by_name("c1").unwrap();
            let c2_ref = table_state.table.get_column_by_name("c2").unwrap();

            // -(c1 + 1) > c2 => c1 < -c2 - 1
            assert_eq!(
                filter_op.predicate,
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    left_expr: Box::new(ScalarExpression::Unary {
                        op: UnaryOperator::Minus,
                        expr: Box::new(ScalarExpression::Binary {
                            op: BinaryOperator::Plus,
                            left_expr: Box::new(ScalarExpression::column_expr(c1_ref, 0)),
                            right_expr: Box::new(ScalarExpression::Constant(DataValue::Int32(1))),
                            evaluator: None,
                            ty: LogicalType::Integer,
                        }),
                        evaluator: None,
                        ty: LogicalType::Integer,
                    }),
                    right_expr: Box::new(ScalarExpression::column_expr(c2_ref, 1)),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                }
            )
        } else {
            unreachable!()
        }

        Ok(())
    }

    fn plan_filter(
        plan: &LogicalPlan,
        column_id: &ColumnId,
        arena: &mut PlanArena,
    ) -> Result<Option<Range>, DatabaseError> {
        let best_plan = run_with_single_batch(
            plan.clone(),
            "test_simplify_filter",
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::SimplifyFilter],
            arena,
        )?;

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(filter_op) = filter_op.operator {
            Ok(RangeDetacher::new("t1", column_id, arena).detach(&filter_op.predicate)?)
        } else {
            Ok(None)
        }
    }

    #[test]
    fn test_simplify_filter_multiple_column() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        // c1 + 1 < -1 => c1 < -2
        let plan_1 = table_state.plan_with_arena(
            "select * from t1 where -(c1 + 1) > 1 and -(1 - c2) > 1",
            &mut arena,
        )?;
        // 1 - c1 < -1 => c1 > 2
        let plan_2 = table_state.plan_with_arena(
            "select * from t1 where -(1 - c1) > 1 and -(c2 + 1) > 1",
            &mut arena,
        )?;
        // c1 < -1
        let plan_3 = table_state
            .plan_with_arena("select * from t1 where -c1 > 1 and c2 + 1 > 1", &mut arena)?;
        // c1 > 0
        let plan_4 = table_state
            .plan_with_arena("select * from t1 where c1 + 1 > 1 and -c2 > 1", &mut arena)?;

        let range_1_c1 =
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?.unwrap();
        let range_1_c2 =
            plan_filter(&plan_1, table_state.column_id_by_name("c2"), &mut arena)?.unwrap();

        let range_2_c1 =
            plan_filter(&plan_2, table_state.column_id_by_name("c1"), &mut arena)?.unwrap();
        let range_2_c2 =
            plan_filter(&plan_2, table_state.column_id_by_name("c2"), &mut arena)?.unwrap();

        let range_3_c1 =
            plan_filter(&plan_3, table_state.column_id_by_name("c1"), &mut arena)?.unwrap();
        let range_3_c2 =
            plan_filter(&plan_3, table_state.column_id_by_name("c2"), &mut arena)?.unwrap();

        let range_4_c1 =
            plan_filter(&plan_4, table_state.column_id_by_name("c1"), &mut arena)?.unwrap();
        let range_4_c2 =
            plan_filter(&plan_4, table_state.column_id_by_name("c2"), &mut arena)?.unwrap();

        assert_eq!(
            range_1_c1,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(-2))
            }
        );
        assert_eq!(
            range_1_c2,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(2)),
                max: Bound::Unbounded
            }
        );
        assert_eq!(
            range_2_c1,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(2)),
                max: Bound::Unbounded
            }
        );
        assert_eq!(
            range_2_c2,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(-2))
            }
        );
        assert_eq!(
            range_3_c1,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(-1))
            }
        );
        assert_eq!(
            range_3_c2,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Unbounded
            }
        );
        assert_eq!(
            range_4_c1,
            Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Unbounded
            }
        );
        assert_eq!(
            range_4_c2,
            Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Excluded(DataValue::Int32(-1))
            }
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_multiple_column_in_or() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        // c1 > c2 or c1 > 1
        let plan_1 =
            table_state.plan_with_arena("select * from t1 where c1 > c2 or c1 > 1", &mut arena)?;

        assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?,
            None
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_multiple_dispersed_same_column_in_or() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan_1 = table_state.plan_with_arena(
            "select * from t1 where c1 = 4 and c1 > c2 or c1 > 1",
            &mut arena,
        )?;

        assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?,
            Some(Range::Scope {
                min: Bound::Excluded(DataValue::Int32(1)),
                max: Bound::Unbounded,
            })
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_is_null() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan_1 =
            table_state.plan_with_arena("select * from t1 where c1 is null", &mut arena)?;

        assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?,
            Some(Range::Eq(DataValue::Null))
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_is_not_null() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan_1 =
            table_state.plan_with_arena("select * from t1 where c1 is not null", &mut arena)?;

        assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?,
            None
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_in() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan_1 =
            table_state.plan_with_arena("select * from t1 where c1 in (1, 2, 3)", &mut arena)?;

        assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?,
            Some(Range::SortedRanges(vec![
                Range::Eq(DataValue::Int32(1)),
                Range::Eq(DataValue::Int32(2)),
                Range::Eq(DataValue::Int32(3)),
            ]))
        );

        Ok(())
    }

    #[test]
    fn test_simplify_filter_column_not_in() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan_1 = table_state
            .plan_with_arena("select * from t1 where c1 not in (1, 2, 3)", &mut arena)?;

        assert_eq!(
            plan_filter(&plan_1, table_state.column_id_by_name("c1"), &mut arena)?,
            None
        );

        Ok(())
    }
}
