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
use crate::expression::{AliasType, BinaryOperator, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child};
use crate::optimizer::rule::normalization::{is_subset_exprs, strip_alias};
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::mem;

fn is_passthrough_project(op: &ProjectOperator) -> bool {
    op.exprs
        .iter()
        .all(|expr| matches!(strip_alias(expr), ScalarExpression::ColumnRef { .. }))
}

fn passthrough_source_position(expr: &ScalarExpression) -> Option<usize> {
    match strip_alias(expr) {
        ScalarExpression::ColumnRef { position, .. } => Some(*position),
        _ => None,
    }
}

fn rewrite_column_position(expr: &mut ScalarExpression, new_position: usize) {
    match expr {
        ScalarExpression::ColumnRef { position, .. } => {
            *position = new_position;
        }
        ScalarExpression::Alias { expr, alias } => {
            rewrite_column_position(expr, new_position);
            if let AliasType::Expr(alias_expr) = alias {
                rewrite_column_position(alias_expr, new_position);
            }
        }
        _ => {}
    }
}

fn remap_passthrough_project_exprs(
    parent_exprs: &mut [ScalarExpression],
    child_exprs: &[ScalarExpression],
) -> bool {
    let mut remapped_positions = Vec::with_capacity(parent_exprs.len());

    for parent_expr in parent_exprs.iter() {
        let Some(position) = child_exprs
            .iter()
            .find(|child_expr| parent_expr.eq_ignore_colref_pos(child_expr))
            .and_then(passthrough_source_position)
        else {
            return false;
        };
        remapped_positions.push(position);
    }

    for (parent_expr, position) in parent_exprs.iter_mut().zip(remapped_positions) {
        rewrite_column_position(parent_expr, position);
    }

    true
}

fn groupby_exprs_match(
    parent_exprs: &[ScalarExpression],
    child_exprs: &[ScalarExpression],
) -> bool {
    parent_exprs.len() == child_exprs.len()
        && parent_exprs
            .iter()
            .zip(child_exprs.iter())
            .all(|(parent_expr, child_expr)| parent_expr.eq_ignore_colref_pos(child_expr))
}

/// Combine two adjacent project operators into one.
pub struct CollapseProject;

impl NormalizationRule for CollapseProject {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let Operator::Project(parent_op) = &mut plan.operator else {
            return Ok(false);
        };

        let mut removed = false;
        loop {
            let Childrens::Only(child) = plan.childrens.as_mut() else {
                break;
            };
            match &child.operator {
                Operator::Project(child_op)
                    if is_passthrough_project(child_op)
                        && is_subset_exprs(&parent_op.exprs, &child_op.exprs)
                        && remap_passthrough_project_exprs(
                            &mut parent_op.exprs,
                            &child_op.exprs,
                        ) =>
                {
                    removed |= replace_with_only_child(child.as_mut());
                }
                _ => break,
            }
        }

        Ok(removed)
    }
}

/// Combine two adjacent filter operators into one.
pub struct CombineFilter;

impl NormalizationRule for CombineFilter {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let parent_filter = match mem::replace(&mut plan.operator, Operator::Dummy) {
            Operator::Filter(op) => op,
            operator => {
                plan.operator = operator;
                return Ok(false);
            }
        };
        let parent_filter = parent_filter;

        let cursor = match only_child_mut(plan) {
            Some(child) => child,
            None => {
                plan.operator = Operator::Filter(parent_filter);
                return Ok(false);
            }
        };

        loop {
            match &mut cursor.operator {
                Operator::Filter(child_op) => {
                    let FilterOperator {
                        predicate,
                        having,
                        is_optimized: _,
                    } = parent_filter;
                    let child_predicate = mem::replace(
                        &mut child_op.predicate,
                        ScalarExpression::Constant(DataValue::Boolean(true)),
                    );
                    child_op.predicate = ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(predicate),
                        right_expr: Box::new(child_predicate),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    };
                    child_op.having = having || child_op.having;

                    return Ok(replace_with_only_child(plan));
                }
                Operator::Project(project_op) if is_passthrough_project(project_op) => {
                    if replace_with_only_child(cursor) {
                        continue;
                    }
                    plan.operator = Operator::Filter(parent_filter);
                    return Ok(false);
                }
                _ => {
                    plan.operator = Operator::Filter(parent_filter);
                    return Ok(false);
                }
            }
        }
    }
}

pub struct CollapseGroupByAgg;

impl NormalizationRule for CollapseGroupByAgg {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
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
            groupby_exprs_match(&op.groupby_exprs, &child_op.groupby_exprs)
        };

        if can_collapse {
            return Ok(replace_with_only_child(plan));
        }

        Ok(false)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::catalog::{ColumnCatalog, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::combine_operators::{
        CollapseGroupByAgg, CollapseProject,
    };
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksTransaction;

    fn column_expr(name: &str, position: usize) -> ScalarExpression {
        ScalarExpression::column_expr(
            ColumnRef::from(ColumnCatalog::new_dummy(name.to_string())),
            position,
        )
    }

    #[test]
    fn test_collapse_project() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select c1 from (select c1, c2 from t1) t")?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_collapse_project".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::CollapseProject],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

        if let Operator::Project(op) = &best_plan.operator {
            assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator")
        }

        let alias_project = best_plan.childrens.pop_only();
        assert!(
            matches!(alias_project.operator, Operator::Project(_)),
            "Derived-table alias projection should be preserved"
        );
        let scan_op = alias_project.childrens.pop_only();
        assert!(matches!(scan_op.operator, Operator::TableScan(_)));
        assert!(matches!(scan_op.childrens.as_ref(), Childrens::None));

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

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_collapse_project_with_alias".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::CollapseProject],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;
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
    fn test_collapse_project_remaps_reordered_passthrough_positions() -> Result<(), DatabaseError> {
        let child = LogicalPlan::new(
            Operator::Project(ProjectOperator {
                exprs: vec![column_expr("c2", 1), column_expr("c1", 0)],
            }),
            Childrens::Only(Box::new(LogicalPlan::new(Operator::Dummy, Childrens::None))),
        );
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator {
                exprs: vec![column_expr("c2", 0)],
            }),
            Childrens::Only(Box::new(child)),
        );

        assert!(CollapseProject.apply(&mut plan)?);

        let Operator::Project(op) = &plan.operator else {
            unreachable!("expected project");
        };
        let ScalarExpression::ColumnRef { position, .. } = &op.exprs[0] else {
            unreachable!("expected column ref");
        };
        assert_eq!(*position, 1);
        assert!(matches!(
            plan.childrens.pop_only().operator,
            Operator::Dummy
        ));
        Ok(())
    }

    #[test]
    fn test_combine_filter() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan =
            table_state.plan("select * from (select * from t1 where c1 > 1) t where 1 = 1")?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_combine_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::CombineFilter],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

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

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_collapse_group_by_agg".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::CollapseGroupByAgg],
            )
            .build();

        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

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
        let child = AggregateOperator::build(
            LogicalPlan::new(Operator::Dummy, Childrens::None),
            vec![],
            vec![column_expr("c2", 1)],
            false,
        );
        let mut plan = AggregateOperator::build(child, vec![], vec![column_expr("c2", 0)], true);

        assert!(CollapseGroupByAgg.apply(&mut plan)?);
        let Operator::Aggregate(op) = &plan.operator else {
            unreachable!("expected aggregate");
        };
        let ScalarExpression::ColumnRef { position, .. } = &op.groupby_exprs[0] else {
            unreachable!("expected column ref");
        };
        assert_eq!(*position, 1);
        Ok(())
    }
}
