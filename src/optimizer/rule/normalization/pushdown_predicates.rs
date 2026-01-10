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

use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::range_detacher::{Range, RangeDetacher};
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::plan_utils::{
    left_child, only_child_mut, replace_with_only_child, right_child, wrap_child_with,
};
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::types::index::{IndexInfo, IndexMetaRef, IndexType};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use itertools::Itertools;
use std::ops::Bound;
use std::sync::LazyLock;
use std::{mem, slice};

static PUSH_PREDICATE_THROUGH_JOIN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Filter(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Join(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

static PUSH_PREDICATE_INTO_SCAN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Filter(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::TableScan(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

// TODO: 感觉是只是处理projection中的alias反向替换为filter中表达式
#[allow(dead_code)]
static PUSH_PREDICATE_THROUGH_NON_JOIN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Filter(_)),
    children: PatternChildrenPredicate::Predicate(vec![Pattern {
        predicate: |op| matches!(op, Operator::Project(_)),
        children: PatternChildrenPredicate::None,
    }]),
});

fn split_conjunctive_predicates(expr: &ScalarExpression) -> Vec<ScalarExpression> {
    match expr {
        ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr,
            right_expr,
            ..
        } => split_conjunctive_predicates(left_expr)
            .into_iter()
            .chain(split_conjunctive_predicates(right_expr))
            .collect_vec(),
        _ => vec![expr.clone()],
    }
}

/// reduce filters into a filter, and then build a new LogicalFilter node with input child.
/// if filters is empty, return the input child.
fn reduce_filters(filters: Vec<ScalarExpression>, having: bool) -> Option<FilterOperator> {
    filters
        .into_iter()
        .reduce(|a, b| ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr: Box::new(a),
            right_expr: Box::new(b),
            evaluator: None,
            ty: LogicalType::Boolean,
        })
        .map(|f| FilterOperator {
            predicate: f,
            is_optimized: false,
            having,
        })
}

/// Return true when left is subset of right, only compare table_id and column_id, so it's safe to
/// used for join output cols with nullable columns.
/// If left equals right, return true.
pub fn is_subset_cols(left: &[ColumnRef], right: &[ColumnRef]) -> bool {
    left.iter().all(|l| right.contains(l))
}

/// Comments copied from Spark Catalyst PushPredicateThroughJoin
///
/// Pushes down `Filter` operators where the `condition` can be
/// evaluated using only the attributes of the left or right side of a join.  Other
/// `Filter` conditions are moved into the `condition` of the `Join`.
///
/// And also pushes down the join filter, where the `condition` can be evaluated using only the
/// attributes of the left or right side of sub query when applicable.
pub struct PushPredicateThroughJoin;

impl MatchPattern for PushPredicateThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_THROUGH_JOIN
    }
}

impl NormalizationRule for PushPredicateThroughJoin {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let filter_op = match &plan.operator {
            Operator::Filter(op) => op.clone(),
            _ => return Ok(false),
        };

        let mut applied = false;

        let parent_replacement = {
            let join_plan = match only_child_mut(plan) {
                Some(child) => child,
                None => return Ok(false),
            };

            let join_op = match &join_plan.operator {
                Operator::Join(op) => op,
                _ => return Ok(false),
            };

            if !matches!(
                join_op.join_type,
                JoinType::Inner
                    | JoinType::LeftOuter
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightOuter
            ) {
                return Ok(false);
            }

            let left_columns = left_child(join_plan)
                .map(|child| child.operator.referenced_columns(true))
                .unwrap_or_default();
            let right_columns = right_child(join_plan)
                .map(|child| child.operator.referenced_columns(true))
                .unwrap_or_default();

            let filter_exprs = split_conjunctive_predicates(&filter_op.predicate);
            let (left_filters, rest): (Vec<_>, Vec<_>) = filter_exprs
                .into_iter()
                .partition(|f| is_subset_cols(&f.referenced_columns(true), &left_columns));
            let (right_filters, common_filters): (Vec<_>, Vec<_>) = rest
                .into_iter()
                .partition(|f| is_subset_cols(&f.referenced_columns(true), &right_columns));

            let mut new_ops = (None, None, None);
            let replace_filters = match join_op.join_type {
                JoinType::Inner => {
                    if let Some(left_filter_op) = reduce_filters(left_filters, filter_op.having) {
                        new_ops.0 = Some(Operator::Filter(left_filter_op));
                    }

                    if let Some(right_filter_op) = reduce_filters(right_filters, filter_op.having) {
                        new_ops.1 = Some(Operator::Filter(right_filter_op));
                    }

                    common_filters
                }
                JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => {
                    if let Some(left_filter_op) = reduce_filters(left_filters, filter_op.having) {
                        new_ops.0 = Some(Operator::Filter(left_filter_op));
                    }

                    common_filters
                        .into_iter()
                        .chain(right_filters)
                        .collect_vec()
                }
                JoinType::RightOuter => {
                    if let Some(right_filter_op) = reduce_filters(right_filters, filter_op.having) {
                        new_ops.1 = Some(Operator::Filter(right_filter_op));
                    }

                    common_filters.into_iter().chain(left_filters).collect_vec()
                }
                _ => vec![],
            };

            if let Some(replace_filter_op) = reduce_filters(replace_filters, filter_op.having) {
                new_ops.2 = Some(Operator::Filter(replace_filter_op));
            }

            if let Some(left_op) = new_ops.0 {
                applied |= wrap_child_with(join_plan, 0, left_op);
            }

            if let Some(right_op) = new_ops.1 {
                applied |= wrap_child_with(join_plan, 1, right_op);
            }

            new_ops.2
        };

        match parent_replacement {
            Some(common_op) => {
                plan.operator = common_op;
                applied = true;
            }
            None if applied => {
                applied |= replace_with_only_child(plan);
            }
            _ => {}
        }

        Ok(applied)
    }
}

pub struct PushPredicateIntoScan;

impl MatchPattern for PushPredicateIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_INTO_SCAN
    }
}

impl NormalizationRule for PushPredicateIntoScan {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        if let Operator::Filter(op) = plan.operator.clone() {
            if let Some(child) = only_child_mut(plan) {
                if let Operator::TableScan(scan_op) = &mut child.operator {
                    let mut changed = false;
                    for IndexInfo {
                        meta,
                        range,
                        covered_deserializers,
                    } in &mut scan_op.index_infos
                    {
                        if range.is_some() {
                            continue;
                        }
                        *range = match meta.ty {
                            IndexType::PrimaryKey { is_multiple: false }
                            | IndexType::Unique
                            | IndexType::Normal => {
                                RangeDetacher::new(meta.table_name.as_ref(), &meta.column_ids[0])
                                    .detach(&op.predicate)?
                            }
                            IndexType::PrimaryKey { is_multiple: true } | IndexType::Composite => {
                                Self::composite_range(&op, meta)?
                            }
                        };
                        if range.is_none() {
                            continue;
                        }
                        changed = true;

                        let mut deserializers = Vec::with_capacity(meta.column_ids.len());
                        let mut cover_count = 0;
                        let index_column_types = match &meta.value_ty {
                            LogicalType::Tuple(tys) => tys,
                            ty => slice::from_ref(ty),
                        };
                        for (i, column_id) in meta.column_ids.iter().enumerate() {
                            for column in scan_op.columns.values() {
                                deserializers.push(
                                    if column.id().map(|id| id == *column_id).unwrap_or(false) {
                                        cover_count += 1;
                                        column.datatype().serializable()
                                    } else {
                                        index_column_types[i].skip_serializable()
                                    },
                                );
                            }
                        }
                        if cover_count == scan_op.columns.len() {
                            *covered_deserializers = Some(deserializers);
                        }
                    }
                    return Ok(changed);
                }
            }
        }

        Ok(false)
    }
}

impl PushPredicateIntoScan {
    fn composite_range(
        op: &FilterOperator,
        meta: &mut IndexMetaRef,
    ) -> Result<Option<Range>, DatabaseError> {
        let mut res = None;
        let mut eq_ranges = Vec::with_capacity(meta.column_ids.len());
        let mut apply_column_count = 0;

        for column_id in meta.column_ids.iter() {
            if let Some(range) =
                RangeDetacher::new(meta.table_name.as_ref(), column_id).detach(&op.predicate)?
            {
                apply_column_count += 1;

                if range.only_eq() {
                    eq_ranges.push(range);
                    continue;
                }
                res = range.combining_eqs(&eq_ranges);
            }
            break;
        }
        if res.is_none() {
            if let Some(range) = eq_ranges.pop() {
                res = range.combining_eqs(&eq_ranges);
            }
        }
        Ok(res.map(|range| {
            if range.only_eq() && apply_column_count != meta.column_ids.len() {
                fn eq_to_scope(range: Range) -> Range {
                    match range {
                        Range::Eq(DataValue::Tuple(values, _)) => {
                            let min = Bound::Excluded(DataValue::Tuple(values.clone(), false));
                            let max = Bound::Excluded(DataValue::Tuple(values, true));

                            Range::Scope { min, max }
                        }
                        Range::SortedRanges(mut ranges) => {
                            for range in ranges.iter_mut() {
                                let tmp = mem::replace(range, Range::Dummy);
                                *range = eq_to_scope(tmp);
                            }
                            Range::SortedRanges(ranges)
                        }
                        range => range,
                    }
                }
                return eq_to_scope(range);
            }
            range
        }))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::Operator;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;

    #[test]
    fn test_push_predicate_into_scan() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        // 1 - c2 < 0 => c2 > 1
        let plan = table_state.plan("select * from t1 where -(1 - c2) > 0")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "simplify_filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .batch(
                "test_push_predicate_into_scan".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateIntoScan],
            )
            .find_best::<RocksTransaction>(None)?;

        let scan_op = best_plan.childrens.pop_only().childrens.pop_only();
        if let Operator::TableScan(op) = &scan_op.operator {
            let mock_range = Range::Scope {
                min: Bound::Excluded(DataValue::Int32(1)),
                max: Bound::Unbounded,
            };

            assert_eq!(op.index_infos[0].range, Some(mock_range));
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_left_join() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan =
            table_state.plan("select * from t1 left join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .find_best::<RocksTransaction>(None)?;

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(op) = &filter_op.operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        let filter_op = filter_op.childrens.pop_only().childrens.pop_twins().0;
        if let Operator::Filter(op) = &filter_op.operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_right_join() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state
            .plan("select * from t1 right join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .find_best::<RocksTransaction>(None)?;

        let filter_op = best_plan.childrens.pop_only();
        if let Operator::Filter(op) = &filter_op.operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        let filter_op = filter_op.childrens.pop_only().childrens.pop_twins().1;
        if let Operator::Filter(op) = &filter_op.operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }

    #[test]
    fn test_push_predicate_through_join_in_inner_join() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state
            .plan("select * from t1 inner join t2 on c1 = c3 where c1 > 1 and c3 < 2")?;

        let best_plan = HepOptimizer::new(plan)
            .batch(
                "test_push_predicate_through_join".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .find_best::<RocksTransaction>(None)?;

        let join_op = best_plan.childrens.pop_only();
        if let Operator::Join(_) = &join_op.operator {
        } else {
            unreachable!("Should be a filter operator")
        }

        let (left_filter_op, right_filter_op) = join_op.childrens.pop_twins();
        if let Operator::Filter(op) = &left_filter_op.operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Gt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        if let Operator::Filter(op) = &right_filter_op.operator {
            match op.predicate {
                ScalarExpression::Binary {
                    op: BinaryOperator::Lt,
                    ty: LogicalType::Boolean,
                    ..
                } => (),
                _ => unreachable!(),
            }
        } else {
            unreachable!("Should be a filter operator")
        }

        Ok(())
    }
}
