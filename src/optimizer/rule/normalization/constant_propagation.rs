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

//! Constant propagation over equality predicates.
//!
//! `a = b AND a = 1` implies `b = 1`. The derived predicate is added (never replaces anything),
//! so later rules can turn it into a static index range on the other column.
//!
//! - [`PropagateFilterConstants`] derives constants inside one `Filter`'s top-level conjuncts.
//! - [`PropagateJoinConstants`] derives constants across a join's equi-keys and wraps the
//!   target input with a new `Filter`. Only the sides whose rows may be filtered without
//!   changing the join result are targeted: both sides of an inner join, the right side of a
//!   left join and the left side of a right join. Constants come from filters below the join
//!   and, for outer joins, also from the preserved side's ON conjuncts that stay on the join.
//!
//! Only plain table columns of identical types participate, and NULL constants are ignored:
//! `col = NULL` is never true, so it carries no value to propagate.

use crate::catalog::{ColumnRef, ColumnRelation};
use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::plan_utils::wrap_child_with;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::{JoinCondition, JoinType};
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::types::LogicalType;

#[derive(Clone, Copy)]
struct Fact {
    column: ColumnRef,
    position: usize,
    constant: ExprRef,
    left: bool,
}

fn lookup(facts: &[Fact], arena: &PlanArena<'_>, column: ColumnRef, left: bool) -> Option<ExprRef> {
    facts
        .iter()
        .find(|fact| fact.left == left && arena.same_column(fact.column, column))
        .map(|fact| fact.constant)
}

fn for_each_conjunct(expr: ExprRef, arena: &PlanArena<'_>, f: &mut impl FnMut(ExprRef)) {
    match arena.expression(expr) {
        ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr,
            right_expr,
            ..
        } => {
            let (left_expr, right_expr) = (*left_expr, *right_expr);
            for_each_conjunct(left_expr, arena, f);
            for_each_conjunct(right_expr, arena, f);
        }
        _ => f(expr),
    }
}

fn table_column(expr: ExprRef, arena: &PlanArena<'_>) -> Option<(ColumnRef, usize)> {
    match expr.unpack_alias_ref(arena) {
        ScalarExpression::ColumnRef { column, position }
            if matches!(
                arena.column(*column).summary().relation,
                ColumnRelation::Table { .. }
            ) =>
        {
            Some((*column, *position))
        }
        _ => None,
    }
}

fn eq_operands(expr: ExprRef, arena: &PlanArena<'_>) -> Option<(ExprRef, ExprRef)> {
    match expr.unpack_alias_ref(arena) {
        ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr,
            right_expr,
            ..
        } => Some((*left_expr, *right_expr)),
        _ => None,
    }
}

fn as_fact(
    expr: ExprRef,
    arena: &PlanArena<'_>,
    left: bool,
    has_col_connection: &mut bool,
) -> Option<Fact> {
    let (lhs, rhs) = eq_operands(expr, arena)?;
    let ((column, position), constant) = match (table_column(lhs, arena), table_column(rhs, arena))
    {
        (Some(column), None) => (column, rhs.unpack_alias(arena)),
        (None, Some(column)) => (column, lhs.unpack_alias(arena)),
        (Some(_), Some(_)) => {
            *has_col_connection = true;
            return None;
        }
        (None, None) => return None,
    };
    matches!(arena.expression(constant), ScalarExpression::Constant(value) if !value.is_null())
        .then_some(Fact {
            column,
            position,
            constant,
            left,
        })
}

type ColumnSlot = (ColumnRef, usize);

fn as_column_pair(
    lhs: ExprRef,
    rhs: ExprRef,
    arena: &PlanArena<'_>,
) -> Option<(ColumnSlot, ColumnSlot)> {
    let lhs = table_column(lhs, arena)?;
    let rhs = table_column(rhs, arena)?;
    (arena.column(lhs.0).datatype() == arena.column(rhs.0).datatype()
        && !arena.same_column(lhs.0, rhs.0))
    .then_some((lhs, rhs))
}

fn and_fact(
    acc: Option<ExprRef>,
    fact: Fact,
    arena: &mut PlanArena<'_>,
) -> Result<ExprRef, DatabaseError> {
    let left_expr = arena.alloc_expression(ScalarExpression::ColumnRef {
        column: fact.column,
        position: fact.position,
    });
    let right_expr = fact.constant.clone_expression(arena)?;
    let eq = arena.alloc_expression(ScalarExpression::Binary {
        op: BinaryOperator::Eq,
        left_expr,
        right_expr,
        evaluator: None,
        ty: LogicalType::Boolean,
    });
    Ok(match acc {
        Some(left_expr) => arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr,
            right_expr: eq,
            evaluator: None,
            ty: LogicalType::Boolean,
        }),
        None => eq,
    })
}

pub struct PropagateFilterConstants;

impl NormalizationRule for PropagateFilterConstants {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let Operator::Filter(filter) = &plan.operator else {
            return Ok(false);
        };
        let predicate = filter.predicate;

        let mut facts = Vec::new();
        let mut has_col_connection = false;
        for_each_conjunct(predicate, arena, &mut |conjunct| {
            facts.extend(as_fact(conjunct, arena, false, &mut has_col_connection));
        });
        // Without both a constant and a column pair nothing can be derived.
        if facts.is_empty() || !has_col_connection {
            return Ok(false);
        }

        let base = facts.len();
        loop {
            let before = facts.len();
            for_each_conjunct(predicate, arena, &mut |conjunct| {
                let Some((lhs, rhs)) = eq_operands(conjunct, arena) else {
                    return;
                };
                let Some((lhs, rhs)) = as_column_pair(lhs, rhs, arena) else {
                    return;
                };
                for (from, to) in [(lhs, rhs), (rhs, lhs)] {
                    if lookup(&facts, arena, to.0, false).is_some() {
                        continue;
                    }
                    if let Some(constant) = lookup(&facts, arena, from.0, false) {
                        facts.push(Fact {
                            column: to.0,
                            position: to.1,
                            constant,
                            left: false,
                        });
                    }
                }
            });
            if facts.len() == before {
                break;
            }
        }
        if facts.len() == base {
            return Ok(false);
        }

        let mut acc = predicate;
        for &fact in &facts[base..] {
            acc = and_fact(Some(acc), fact, arena)?;
        }
        if let Operator::Filter(filter) = &mut plan.operator {
            filter.predicate = acc;
        }
        Ok(true)
    }
}

fn collect_facts(plan: &LogicalPlan, arena: &PlanArena<'_>, left: bool, facts: &mut Vec<Fact>) {
    match (&plan.operator, plan.childrens.as_ref()) {
        (Operator::Filter(filter), Childrens::Only(child)) => {
            for_each_conjunct(filter.predicate, arena, &mut |conjunct| {
                facts.extend(as_fact(conjunct, arena, left, &mut false));
            });
            collect_facts(child, arena, left, facts);
        }
        (Operator::Join(join), Childrens::Twins { left: l, right: r }) => match join.join_type {
            JoinType::Inner | JoinType::Cross => {
                collect_facts(l, arena, left, facts);
                collect_facts(r, arena, left, facts);
            }
            JoinType::LeftOuter => collect_facts(l, arena, left, facts),
            JoinType::RightOuter => collect_facts(r, arena, left, facts),
            JoinType::Full => {}
        },
        _ => {}
    }
}

pub struct PropagateJoinConstants;

impl NormalizationRule for PropagateJoinConstants {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let Operator::Join(join) = &plan.operator else {
            return Ok(false);
        };
        let (to_left, to_right) = match join.join_type {
            JoinType::Inner => (true, true),
            JoinType::LeftOuter => (false, true),
            JoinType::RightOuter => (true, false),
            JoinType::Full | JoinType::Cross => return Ok(false),
        };
        let JoinCondition::On { on, filter } = &join.on else {
            return Ok(false);
        };
        if on.is_empty() {
            return Ok(false);
        }
        let join_filter = *filter;

        let mut facts = Vec::new();
        if let (Some(join_filter), true) = (join_filter, to_left != to_right) {
            let preserved_left = to_right;
            let Childrens::Twins { left, right } = plan.childrens.as_mut() else {
                return Ok(false);
            };
            let preserved = if preserved_left { left } else { right };
            let preserved_columns = preserved.output_schema(arena);
            for_each_conjunct(join_filter, arena, &mut |conjunct| {
                facts.extend(
                    as_fact(conjunct, arena, preserved_left, &mut false)
                        .filter(|fact| preserved_columns.contains(&fact.column)),
                );
            });
        }
        let Childrens::Twins { left, right } = plan.childrens.as_ref() else {
            return Ok(false);
        };
        collect_facts(left, arena, true, &mut facts);
        collect_facts(right, arena, false, &mut facts);
        if facts.is_empty() {
            return Ok(false);
        }

        let base = facts.len();
        for &(left_key, right_key) in on {
            let Some((l, r)) = as_column_pair(left_key, right_key, arena) else {
                continue;
            };
            for (allowed, from, to, to_left_side) in
                [(to_right, l, r, false), (to_left, r, l, true)]
            {
                if !allowed || lookup(&facts, arena, to.0, to_left_side).is_some() {
                    continue;
                }
                if let Some(constant) = lookup(&facts, arena, from.0, !to_left_side) {
                    facts.push(Fact {
                        column: to.0,
                        position: to.1,
                        constant,
                        left: to_left_side,
                    });
                }
            }
        }
        if facts.len() == base {
            return Ok(false);
        }

        let (mut left_acc, mut right_acc) = (None, None);
        for &fact in &facts[base..] {
            let acc = if fact.left {
                &mut left_acc
            } else {
                &mut right_acc
            };
            *acc = Some(and_fact(acc.take(), fact, arena)?);
        }
        let mut applied = false;
        for (child_idx, acc) in [(0, left_acc), (1, right_acc)] {
            if let Some(predicate) = acc {
                applied |= wrap_child_with(
                    plan,
                    child_idx,
                    Operator::Filter(FilterOperator {
                        predicate,
                        is_optimized: false,
                        having: false,
                    }),
                );
            }
        }
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::{eq_operands, for_each_conjunct, table_column};
    use super::{PropagateFilterConstants, PropagateJoinConstants};
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::expression::ScalarExpression;
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::plan_utils::wrap_child_with;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan, PlanArena};

    /// `column = constant` conjuncts of the filters stacked directly on `plan`, as
    /// sorted `"column = value"` strings.
    fn facts(plan: &LogicalPlan, arena: &PlanArena<'_>) -> Vec<String> {
        let mut out = Vec::new();
        let mut node = plan;
        while let (Operator::Filter(filter), Childrens::Only(child)) =
            (&node.operator, node.childrens.as_ref())
        {
            for_each_conjunct(filter.predicate, arena, &mut |conjunct| {
                let Some((lhs, rhs)) = eq_operands(conjunct, arena) else {
                    return;
                };
                let (column, constant) = match (table_column(lhs, arena), table_column(rhs, arena))
                {
                    (Some((column, _)), None) => (column, rhs),
                    (None, Some((column, _))) => (column, lhs),
                    _ => return,
                };
                if let ScalarExpression::Constant(value) = constant.unpack_alias_ref(arena) {
                    out.push(format!("{} = {}", arena.column(column).name(), value));
                }
            });
            node = child;
        }
        out.sort();
        out
    }

    fn find_filter(plan: &LogicalPlan) -> &LogicalPlan {
        match (&plan.operator, plan.childrens.as_ref()) {
            (Operator::Filter(_), _) => plan,
            (_, Childrens::Only(child)) => find_filter(child),
            _ => unreachable!("expected a filter"),
        }
    }

    fn find_join(plan: &LogicalPlan) -> (&LogicalPlan, &LogicalPlan) {
        match (&plan.operator, plan.childrens.as_ref()) {
            (Operator::Join(_), Childrens::Twins { left, right }) => (left, right),
            (_, Childrens::Only(child)) => find_join(child),
            _ => unreachable!("expected a join"),
        }
    }

    fn find_join_mut(plan: &mut LogicalPlan) -> &mut LogicalPlan {
        if matches!(plan.operator, Operator::Join(_)) {
            return plan;
        }
        match plan.childrens.as_mut() {
            Childrens::Only(child) => find_join_mut(child),
            _ => unreachable!("expected a join"),
        }
    }

    /// Runs the rules of the default "Predicate Pushdown" batch (same order as `db.rs`).
    fn propagate_join(sql: &str) -> Result<(Vec<String>, Vec<String>), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state.plan_with_arena(sql, &mut arena)?;
        let mut plan = HepOptimizerPipeline::builder()
            .before_batch(
                "test_propagate_join_constants",
                HepBatchStrategy::fix_point_topdown(30),
                &[
                    NormalizationRuleImpl::PropagateFilterConstants,
                    NormalizationRuleImpl::PropagateJoinConstants,
                    NormalizationRuleImpl::PushPredicateThroughJoin,
                    NormalizationRuleImpl::PushJoinPredicateIntoScan,
                ],
            )
            .build()
            .instantiate(plan)
            .find_best(None, &mut arena)?;

        // Fix point reached: a second application derives nothing new.
        assert!(!PropagateJoinConstants.apply(find_join_mut(&mut plan), &mut arena, None)?);

        let (left, right) = find_join(&plan);
        Ok((facts(left, &arena), facts(right, &arena)))
    }

    #[test]
    fn filter_derives_constant_through_column_equality() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let mut plan =
            table_state.plan_with_arena("select * from t1 where c1 = c2 and c2 = 1", &mut arena)?;
        let filter = find_filter_mut(&mut plan);

        assert!(PropagateFilterConstants.apply(filter, &mut arena, None)?);
        assert_eq!(facts(filter, &arena), ["c1 = 1", "c2 = 1"]);
        // Idempotent: the derived fact is now known, so nothing is added again.
        assert!(!PropagateFilterConstants.apply(filter, &mut arena, None)?);
        Ok(())
    }

    #[test]
    fn filter_closes_equality_chains() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let mut plan = table_state.plan_with_arena(
            "select * from t1, t2 where c1 = c3 and c3 = c2 and c2 = 1",
            &mut arena,
        )?;
        let filter = find_filter_mut(&mut plan);

        assert!(PropagateFilterConstants.apply(filter, &mut arena, None)?);
        assert_eq!(facts(filter, &arena), ["c1 = 1", "c2 = 1", "c3 = 1"]);
        Ok(())
    }

    #[test]
    fn filter_ignores_null_and_non_equality() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        for sql in [
            "select * from t1 where c1 = c2 and c2 = null",
            "select * from t1 where c1 > c2 and c2 = 1",
            "select * from t1 where c1 = c2 + 1 and c2 = 1",
        ] {
            let mut arena = PlanArena::new(&table_state.table_arena);
            let mut plan = table_state.plan_with_arena(sql, &mut arena)?;
            let filter = find_filter_mut(&mut plan);
            assert!(
                !PropagateFilterConstants.apply(filter, &mut arena, None)?,
                "{sql}"
            );
        }
        Ok(())
    }

    #[test]
    fn inner_join_propagates_both_ways() -> Result<(), DatabaseError> {
        assert_eq!(
            propagate_join("select * from t1 join t2 on c1 = c3 where c1 = 1")?,
            (vec!["c1 = 1".into()], vec!["c3 = 1".into()])
        );
        assert_eq!(
            propagate_join("select * from t1 join t2 on c1 = c3 where c3 = 1")?,
            (vec!["c1 = 1".into()], vec!["c3 = 1".into()])
        );
        Ok(())
    }

    #[test]
    fn left_join_only_constrains_right_side() -> Result<(), DatabaseError> {
        assert_eq!(
            propagate_join("select * from t1 left join t2 on c1 = c3 where c1 = 1")?,
            (vec!["c1 = 1".into()], vec!["c3 = 1".into()])
        );
        // The non-preserved side's ON constant is pushed down, never derived onto the left.
        assert_eq!(
            propagate_join("select * from t1 left join t2 on c1 = c3 and c3 = 1")?,
            (vec![], vec!["c3 = 1".into()])
        );
        // The preserved side's ON constant stays on the join but still derives the right side.
        assert_eq!(
            propagate_join("select * from t1 left join t2 on c1 = c3 and c1 = 1")?,
            (vec![], vec!["c3 = 1".into()])
        );
        Ok(())
    }

    #[test]
    fn right_join_only_constrains_left_side() -> Result<(), DatabaseError> {
        assert_eq!(
            propagate_join("select * from t1 right join t2 on c1 = c3 where c3 = 1")?,
            (vec!["c1 = 1".into()], vec!["c3 = 1".into()])
        );
        assert_eq!(
            propagate_join("select * from t1 right join t2 on c1 = c3 and c1 = 1")?,
            (vec!["c1 = 1".into()], vec![])
        );
        assert_eq!(
            propagate_join("select * from t1 right join t2 on c1 = c3 and c3 = 1")?,
            (vec!["c1 = 1".into()], vec![])
        );
        Ok(())
    }

    fn propagate_from_side(
        join: &str,
        fact_on_left: bool,
    ) -> Result<(bool, Vec<String>, Vec<String>), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let mut plan = table_state
            .plan_with_arena(format!("select * from t1 {join} t2 on c1 = c3"), &mut arena)?;
        let (fact_sql, child_idx) = if fact_on_left {
            ("select * from t1 where c1 = 1", 0)
        } else {
            ("select * from t2 where c3 = 1", 1)
        };
        // Same table arena, so the fact's column has the input's summary and position.
        let fact_plan = table_state.plan_with_arena(fact_sql, &mut arena)?;
        let Operator::Filter(filter) = &find_filter(&fact_plan).operator else {
            unreachable!()
        };
        let predicate = filter.predicate;

        let join_plan = find_join_mut(&mut plan);
        assert!(wrap_child_with(
            join_plan,
            child_idx,
            Operator::Filter(FilterOperator {
                predicate,
                is_optimized: false,
                having: false,
            }),
        ));
        let applied = PropagateJoinConstants.apply(join_plan, &mut arena, None)?;
        let (left, right) = find_join(&plan);
        Ok((applied, facts(left, &arena), facts(right, &arena)))
    }

    #[test]
    fn join_type_limits_propagation_direction() -> Result<(), DatabaseError> {
        let one = |column: &str| vec![format!("{column} = 1")];
        // (join, fact on left, expected (applied, left facts, right facts))
        let cases = [
            ("join", true, (true, one("c1"), one("c3"))),
            ("join", false, (true, one("c1"), one("c3"))),
            ("left join", true, (true, one("c1"), one("c3"))),
            // Filtering the preserved left side would drop unmatched rows.
            ("left join", false, (false, vec![], one("c3"))),
            ("right join", true, (false, one("c1"), vec![])),
            ("right join", false, (true, one("c1"), one("c3"))),
            ("full join", true, (false, one("c1"), vec![])),
            ("full join", false, (false, vec![], one("c3"))),
        ];
        for (join, fact_on_left, expected) in cases {
            assert_eq!(
                propagate_from_side(join, fact_on_left)?,
                expected,
                "{join}, fact on left: {fact_on_left}"
            );
        }
        Ok(())
    }

    fn find_filter_mut(plan: &mut LogicalPlan) -> &mut LogicalPlan {
        if matches!(plan.operator, Operator::Filter(_)) {
            return plan;
        }
        match plan.childrens.as_mut() {
            Childrens::Only(child) => find_filter_mut(child),
            _ => unreachable!("expected a filter"),
        }
    }
}
