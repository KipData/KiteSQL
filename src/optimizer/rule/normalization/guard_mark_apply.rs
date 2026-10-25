// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::visitor::ExprVisitor;
use crate::expression::{BinaryOperator, ScalarExpression, TypeCast, UnaryOperator};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::plan_utils::{only_child, only_child_mut};
use crate::planner::operator::mark_apply::MarkApplyKind;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::types::evaluator::BinaryEvaluatorRef;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::iter::successors;

pub struct GuardMarkApply;

impl NormalizationRule for GuardMarkApply {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        if !matches!(plan.operator, Operator::Project(_)) {
            return Ok(false);
        }
        let Some(filter) = skip_row_order(plan) else {
            return Ok(false);
        };
        let (Operator::Filter(filter_op), Childrens::Only(mark)) =
            (&filter.operator, filter.childrens.as_ref())
        else {
            return Ok(false);
        };
        let predicate = filter_op.predicate;
        let mut markers = Vec::new();
        let mut input = mark.as_ref();
        while let (Operator::MarkApply(op), Childrens::Twins { left, .. }) =
            (&input.operator, input.childrens.as_ref())
        {
            if matches!(op.kind, MarkApplyKind::InnerJoin) {
                break;
            }
            markers.push(*op.output_column());
            input = left;
        }
        if markers.is_empty() {
            return Ok(false);
        }
        // No marker in this segment may be read above the filter.
        let segment = successors(Some(&*plan), |node| only_child(node))
            .take_while(|node| !matches!(node.operator, Operator::Filter(_)));
        for node in segment {
            if !node
                .operator
                .visit_referenced_columns(arena, &mut |arena, column| {
                    !markers
                        .iter()
                        .any(|marker| arena.same_column(*column, *marker))
                })?
            {
                return Ok(false);
            }
        }
        // Do not skip values consumed by another apply, even if Filter does not need them.
        let mut consumed = Vec::new();
        let mut columns = Vec::new();
        let mut input = mark.as_ref();
        for (index, _) in markers.iter().enumerate() {
            columns.clear();
            let (Operator::MarkApply(op), Childrens::Twins { left, right }) =
                (&input.operator, input.childrens.as_ref())
            else {
                unreachable!()
            };
            for expr in op.predicates.iter().chain(op.parameterized_probe.iter()) {
                expr.all_referenced_columns(arena, |_, column, _| {
                    columns.push(*column);
                    true
                })?;
            }
            right.visit_column_refs(arena, &mut |column| columns.push(*column))?;
            for marker in &markers[index + 1..] {
                if columns
                    .iter()
                    .any(|column| arena.same_column(*column, *marker))
                {
                    consumed.push(*marker);
                }
            }
            input = left;
        }
        let mut changed = false;
        let mut guards = Vec::new();
        let Some(mut input) = mark_below_filter(plan) else {
            return Ok(false);
        };
        for (index, marker) in markers.iter().enumerate() {
            let (Operator::MarkApply(op), Childrens::Twins { left, .. }) =
                (&mut input.operator, input.childrens.as_mut())
            else {
                unreachable!()
            };
            if op.guard.is_none()
                && !consumed
                    .iter()
                    .any(|column| arena.same_column(*column, *marker))
            {
                // Top-down plan order is the reverse of execution order.
                let unavailable = &markers[..=index];
                guards.clear();
                let mut visitor = GuardVisitor {
                    marker: *marker,
                    guards: &mut guards,
                };
                visitor.visit(predicate, arena)?;
                let mut guard = None;
                for &(sibling, when_true) in &guards {
                    let skip = decisive_condition(sibling, when_true, unavailable, arena)?;
                    let needed = skip.map(|expr| negate(expr, arena));
                    guard = combine_conditions(BinaryOperator::And, guard, needed, true, arena);
                }
                if let Some(guard) = guard {
                    let schema = left.output_schema(arena);
                    if guard.all_referenced_columns(arena, |arena, column, _| {
                        !unavailable
                            .iter()
                            .any(|marker| arena.same_column(*marker, *column))
                            && schema
                                .iter()
                                .any(|candidate| arena.same_column(*candidate, *column))
                    })? {
                        op.guard = Some(guard.clone_expression(arena)?);
                        changed = true;
                    }
                }
            }
            input = left;
        }
        Ok(changed)
    }
}

// Sort/Limit/TopK between the projection and the filter only reorder or cut rows.
fn skip_row_order(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    let mut child = only_child(plan)?;
    while matches!(
        child.operator,
        Operator::Sort(_) | Operator::Limit(_) | Operator::TopK(_)
    ) {
        child = only_child(child)?;
    }
    Some(child)
}

fn mark_below_filter(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    let mut node = plan;
    while !matches!(node.operator, Operator::Filter(_)) {
        node = only_child_mut(node)?;
    }
    only_child_mut(node)
}

struct GuardVisitor<'a> {
    marker: ColumnRef,
    guards: &'a mut Vec<(ExprRef, bool)>,
}

impl ExprVisitor<PlanArena<'_>> for GuardVisitor<'_> {
    fn visit_expression_ref(&mut self, _: ExprRef, _: &PlanArena) -> Result<bool, DatabaseError> {
        Ok(true)
    }

    fn visit_expression(
        &mut self,
        expr: &ScalarExpression,
        _: &PlanArena,
    ) -> Result<bool, DatabaseError> {
        Ok(matches!(
            expr,
            ScalarExpression::Binary {
                op: BinaryOperator::And | BinaryOperator::Or,
                ..
            } | ScalarExpression::Unary {
                op: UnaryOperator::Not,
                ..
            } | ScalarExpression::TypeCast { .. }
                | ScalarExpression::Alias { .. }
        ))
    }

    fn visit_binary(
        &mut self,
        op: &BinaryOperator,
        left: ExprRef,
        right: ExprRef,
        _: Option<&BinaryEvaluatorRef>,
        _: &LogicalType,
        arena: &PlanArena,
    ) -> Result<(), DatabaseError> {
        let left_marker = left.any_referenced_column(arena, |arena, column| {
            arena.same_column(*column, self.marker)
        })?;
        let right_marker = right.any_referenced_column(arena, |arena, column| {
            arena.same_column(*column, self.marker)
        })?;
        let (branch, sibling) = match (left_marker, right_marker) {
            (true, false) => (left, right),
            (false, true) => (right, left),
            // Repeated occurrences can only use conditions collected above this node.
            _ => return Ok(()),
        };
        self.guards
            .push((sibling, matches!(op, BinaryOperator::Or)));
        self.visit(branch, arena)
    }
}

// Return only sufficient TRUE/FALSE conditions; unavailable marks and NULL are not evidence.
fn decisive_condition(
    expr: ExprRef,
    when_true: bool,
    unavailable: &[ColumnRef],
    arena: &mut PlanArena,
) -> Result<Option<ExprRef>, DatabaseError> {
    if !expr.any_referenced_column(arena, |arena, column| {
        unavailable
            .iter()
            .any(|marker| arena.same_column(*column, *marker))
    })? {
        let mut visitor = SimpleGuardVisitor {
            valid: true,
            context: GuardContext::Predicate,
        };
        visitor.visit(expr, arena)?;
        if visitor.valid {
            return Ok(Some(if when_true { expr } else { negate(expr, arena) }));
        }
    }
    match arena.expression(expr.unpack_alias(arena)).clone() {
        ScalarExpression::Binary {
            op: op @ (BinaryOperator::And | BinaryOperator::Or),
            left_expr,
            right_expr,
            ..
        } => {
            let left = decisive_condition(left_expr, when_true, unavailable, arena)?;
            let right = decisive_condition(right_expr, when_true, unavailable, arena)?;
            let either = matches!(op, BinaryOperator::Or) == when_true;
            Ok(combine_conditions(
                if either {
                    BinaryOperator::Or
                } else {
                    BinaryOperator::And
                },
                left,
                right,
                either,
                arena,
            ))
        }
        ScalarExpression::Unary {
            op: UnaryOperator::Not,
            expr,
            ..
        } => decisive_condition(expr, !when_true, unavailable, arena),
        ScalarExpression::TypeCast {
            expr,
            ty: LogicalType::Boolean,
            ..
        } if *expr.return_type(arena) == LogicalType::Boolean => {
            decisive_condition(expr, when_true, unavailable, arena)
        }
        _ => Ok(None),
    }
}

fn combine_conditions(
    op: BinaryOperator,
    left: Option<ExprRef>,
    right: Option<ExprRef>,
    allow_one: bool,
    arena: &mut PlanArena,
) -> Option<ExprRef> {
    match (left, right) {
        (Some(left_expr), Some(right_expr)) => {
            Some(arena.alloc_expression(ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                evaluator: None,
                ty: LogicalType::Boolean,
            }))
        }
        (Some(expr), None) | (None, Some(expr)) if allow_one => Some(expr),
        _ => None,
    }
}

fn negate(expr: ExprRef, arena: &mut PlanArena) -> ExprRef {
    if let ScalarExpression::Unary {
        op: UnaryOperator::Not,
        expr,
        ..
    } = arena.expression(expr)
    {
        return *expr;
    }
    arena.alloc_expression(ScalarExpression::Unary {
        op: UnaryOperator::Not,
        expr,
        evaluator: None,
        ty: LogicalType::Boolean,
    })
}

#[derive(Clone, Copy)]
enum GuardContext {
    Predicate,
    NullOperand,
    NumericOperand,
}

struct SimpleGuardVisitor {
    valid: bool,
    context: GuardContext,
}

impl ExprVisitor<PlanArena<'_>> for SimpleGuardVisitor {
    fn visit_expression_ref(
        &mut self,
        expr: ExprRef,
        arena: &PlanArena,
    ) -> Result<bool, DatabaseError> {
        if !self.valid {
            return Ok(false);
        }
        let unaliased = expr.unpack_alias(arena);
        if unaliased != expr {
            self.visit(unaliased, arena)?;
            return Ok(false);
        }
        Ok(true)
    }

    fn visit_expression(
        &mut self,
        expr: &ScalarExpression,
        arena: &PlanArena,
    ) -> Result<bool, DatabaseError> {
        self.valid = match (self.context, expr) {
            (
                GuardContext::NullOperand,
                ScalarExpression::Constant(_) | ScalarExpression::ColumnRef { .. },
            ) => true,
            (GuardContext::Predicate, ScalarExpression::Constant(DataValue::Boolean(_))) => true,
            (GuardContext::Predicate, ScalarExpression::ColumnRef { column, .. }) => {
                arena.column(*column).datatype() == &LogicalType::Boolean
            }
            (
                GuardContext::Predicate,
                ScalarExpression::IsNull { .. }
                | ScalarExpression::Unary {
                    op: UnaryOperator::Not,
                    ..
                }
                | ScalarExpression::Binary {
                    op:
                        BinaryOperator::And
                        | BinaryOperator::Or
                        | BinaryOperator::Eq
                        | BinaryOperator::NotEq
                        | BinaryOperator::Gt
                        | BinaryOperator::GtEq
                        | BinaryOperator::Lt
                        | BinaryOperator::LtEq,
                    ..
                },
            ) => true,
            (GuardContext::NumericOperand, ScalarExpression::Constant(value)) => {
                value.is_null() || value.logical_type().is_numeric()
            }
            (GuardContext::NumericOperand, ScalarExpression::ColumnRef { column, .. }) => {
                arena.column(*column).datatype().is_numeric()
            }
            (GuardContext::NumericOperand, ScalarExpression::TypeCast { expr, ty, .. }) => {
                let from = expr.return_type(arena);
                // Implicit numeric widening cannot fail; bounded Decimal and narrowing casts can.
                (from.is_numeric() || *from == LogicalType::SqlNull)
                    && ty.is_numeric()
                    && !matches!(ty, LogicalType::Decimal(..))
                    && LogicalType::can_implicit_cast(&from, ty)
            }
            // Do not move arbitrary functions or potentially failing calculations earlier.
            _ => false,
        };
        Ok(self.valid)
    }

    fn visit_is_null(
        &mut self,
        _: bool,
        expr: ExprRef,
        arena: &PlanArena,
    ) -> Result<(), DatabaseError> {
        let previous = self.context;
        self.context = GuardContext::NullOperand;
        self.visit(expr, arena)?;
        self.context = previous;
        Ok(())
    }

    fn visit_binary(
        &mut self,
        op: &BinaryOperator,
        left: ExprRef,
        right: ExprRef,
        _: Option<&BinaryEvaluatorRef>,
        _: &LogicalType,
        arena: &PlanArena,
    ) -> Result<(), DatabaseError> {
        let previous = self.context;
        self.context = if matches!(op, BinaryOperator::And | BinaryOperator::Or) {
            GuardContext::Predicate
        } else {
            GuardContext::NumericOperand
        };
        self.visit(left, arena)?;
        self.visit(right, arena)?;
        self.context = previous;
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::AliasType;
    use crate::optimizer::rule::normalization::compilation_in_advance::EvaluatorBind;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::mark_apply::MarkApplyOperator;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::TableArenaCell;
    use crate::types::evaluator::cast_create;
    use crate::types::tuple::Tuple;

    struct Fixture {
        plan: LogicalPlan,
        flag: ExprRef,
        marker: ExprRef,
    }

    impl Fixture {
        fn new(
            arena: &mut PlanArena,
            op: BinaryOperator,
            marker_first: bool,
        ) -> Result<Self, DatabaseError> {
            let flag_column = arena.alloc_column(ColumnCatalog::new(
                "flag".to_string(),
                true,
                ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
            ));
            let marker_column = arena.alloc_column(ColumnCatalog::new(
                "marker".to_string(),
                true,
                ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
            ));
            let value = arena.alloc_expression(DataValue::Boolean(true).into());
            let left = LogicalPlan::new(
                Operator::Values(ValuesOperator::new(vec![value], 1, vec![flag_column])),
                Childrens::None,
            );
            let right = LogicalPlan::new(
                Operator::Values(ValuesOperator::new(vec![value], 1, vec![flag_column])),
                Childrens::None,
            );
            let mark = MarkApplyOperator::build_exists(left, right, marker_column, vec![]);
            let flag = arena.alloc_expression(ScalarExpression::column_expr(flag_column, 0));
            let marker = arena.alloc_expression(ScalarExpression::column_expr(marker_column, 1));
            let (left_expr, right_expr) = if marker_first {
                (marker, flag)
            } else {
                (flag, marker)
            };
            let predicate = arena.alloc_expression(ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                evaluator: None,
                ty: LogicalType::Boolean,
            });
            let filter = FilterOperator::build(predicate, mark, false);
            let plan = LogicalPlan::new(
                Operator::Project(ProjectOperator::new(vec![flag])),
                Childrens::Only(Box::new(filter)),
            );
            Ok(Self { plan, flag, marker })
        }

        fn add_mark(
            &mut self,
            arena: &mut PlanArena,
            op: BinaryOperator,
        ) -> Result<ExprRef, DatabaseError> {
            let column = arena.alloc_column(ColumnCatalog::new(
                "upper_marker".into(),
                true,
                ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
            ));
            let upper = arena.alloc_expression(ScalarExpression::column_expr(column, 2));
            let value = arena.alloc_expression(DataValue::Boolean(true).into());
            let right = LogicalPlan::new(
                Operator::Values(ValuesOperator::new(vec![value], 1, vec![column])),
                Childrens::None,
            );
            let Childrens::Only(filter) = self.plan.childrens.as_mut() else {
                unreachable!()
            };
            let Childrens::Only(mark) = filter.childrens.as_mut() else {
                unreachable!()
            };
            let lower = mark.take();
            **mark = MarkApplyOperator::build_exists(lower, right, column, vec![]);
            let markers = arena.alloc_expression(ScalarExpression::Binary {
                op: if matches!(op, BinaryOperator::And) {
                    BinaryOperator::Or
                } else {
                    BinaryOperator::And
                },
                left_expr: self.marker,
                right_expr: upper,
                evaluator: None,
                ty: LogicalType::Boolean,
            });
            let predicate = arena.alloc_expression(ScalarExpression::Binary {
                op,
                left_expr: self.flag,
                right_expr: markers,
                evaluator: None,
                ty: LogicalType::Boolean,
            });
            let Operator::Filter(filter) = &mut filter.operator else {
                unreachable!()
            };
            filter.predicate = predicate;
            Ok(upper)
        }

        fn mark(&mut self) -> &mut MarkApplyOperator {
            let Childrens::Only(filter) = self.plan.childrens.as_mut() else {
                unreachable!()
            };
            let Childrens::Only(mark) = filter.childrens.as_mut() else {
                unreachable!()
            };
            let Operator::MarkApply(op) = &mut mark.operator else {
                unreachable!()
            };
            op
        }

        fn predicate(&mut self) -> ExprRef {
            let filter = self.plan.childrens.only();
            let Operator::Filter(op) = &filter.operator else {
                unreachable!()
            };
            op.predicate
        }
    }

    #[test]
    fn independent_guards_preserve_all_marker_combinations() -> Result<(), DatabaseError> {
        for op in [BinaryOperator::And, BinaryOperator::Or] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let mut fixture = Fixture::new(&mut arena, op, false)?;
            fixture.add_mark(&mut arena, op)?;
            assert!(GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
            let upper = fixture.mark().guard.unwrap();
            let Childrens::Only(filter) = fixture.plan.childrens.as_mut() else {
                unreachable!()
            };
            let Childrens::Only(mark) = filter.childrens.as_mut() else {
                unreachable!()
            };
            let Childrens::Twins { left, .. } = mark.childrens.as_mut() else {
                unreachable!()
            };
            let Operator::MarkApply(lower) = &left.operator else {
                unreachable!()
            };
            let lower = lower.guard.unwrap();
            assert_ne!(upper, lower);
            let predicate = fixture.predicate();
            EvaluatorBind.apply(&mut fixture.plan, &mut arena, None)?;
            for flag in [
                DataValue::Boolean(false),
                DataValue::Boolean(true),
                DataValue::Null,
            ] {
                let input = Tuple::new(None, vec![flag.clone()]);
                let skip_lower = *arena.expression(lower).eval(&arena, Some(&input))?
                    == DataValue::Boolean(false);
                for a in [
                    DataValue::Boolean(false),
                    DataValue::Boolean(true),
                    DataValue::Null,
                ] {
                    for b in [
                        DataValue::Boolean(false),
                        DataValue::Boolean(true),
                        DataValue::Null,
                    ] {
                        let original = Tuple::new(None, vec![flag.clone(), a.clone(), b.clone()]);
                        let a_out = if skip_lower {
                            DataValue::Null
                        } else {
                            a.clone()
                        };
                        let input = Tuple::new(None, vec![flag.clone(), a_out.clone()]);
                        let skip_upper = *arena.expression(upper).eval(&arena, Some(&input))?
                            == DataValue::Boolean(false);
                        let guarded = Tuple::new(
                            None,
                            vec![
                                flag.clone(),
                                a_out,
                                if skip_upper {
                                    DataValue::Null
                                } else {
                                    b.clone()
                                },
                            ],
                        );
                        assert_eq!(
                            arena
                                .expression(predicate)
                                .eval(&arena, Some(&original))?
                                .into_owned(),
                            arena
                                .expression(predicate)
                                .eval(&arena, Some(&guarded))?
                                .into_owned()
                        );
                    }
                }
            }
            assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
        }
        Ok(())
    }

    #[test]
    fn independent_protection_keeps_projected_markers_unguarded() -> Result<(), DatabaseError> {
        for escaped in [0, 1, 2] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let mut fixture = Fixture::new(&mut arena, BinaryOperator::And, false)?;
            let upper = fixture.add_mark(&mut arena, BinaryOperator::And)?;
            if escaped == 0 {
                // (P AND lower) OR upper cannot skip both marks when P=false.
                let protected = arena.alloc_expression(ScalarExpression::Binary {
                    op: BinaryOperator::And,
                    left_expr: fixture.flag,
                    right_expr: fixture.marker,
                    evaluator: None,
                    ty: LogicalType::Boolean,
                });
                let predicate = arena.alloc_expression(ScalarExpression::Binary {
                    op: BinaryOperator::Or,
                    left_expr: protected,
                    right_expr: upper,
                    evaluator: None,
                    ty: LogicalType::Boolean,
                });
                let Childrens::Only(filter) = fixture.plan.childrens.as_mut() else {
                    unreachable!()
                };
                let Operator::Filter(filter) = &mut filter.operator else {
                    unreachable!()
                };
                filter.predicate = predicate;
            } else {
                let Operator::Project(project) = &mut fixture.plan.operator else {
                    unreachable!()
                };
                project
                    .exprs
                    .push(if escaped == 1 { fixture.marker } else { upper });
            }
            assert_eq!(
                GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?,
                escaped == 0
            );
            assert_eq!(fixture.mark().guard.is_some(), escaped == 0);
            let Childrens::Only(filter) = fixture.plan.childrens.as_mut() else {
                unreachable!()
            };
            let Childrens::Only(mark) = filter.childrens.as_mut() else {
                unreachable!()
            };
            let Childrens::Twins { left, .. } = mark.childrens.as_mut() else {
                unreachable!()
            };
            let Operator::MarkApply(lower) = &left.operator else {
                unreachable!()
            };
            assert_eq!(lower.guard.is_some(), escaped == 0);
        }
        Ok(())
    }

    #[test]
    fn and_or_guards_preserve_three_valued_results() -> Result<(), DatabaseError> {
        for op in [BinaryOperator::And, BinaryOperator::Or] {
            for marker_first in [false, true] {
                let tables = TableArenaCell::default();
                let mut arena = PlanArena::new(&tables);
                let mut fixture = Fixture::new(&mut arena, op, marker_first)?;
                assert!(GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
                let guard = fixture.mark().guard.unwrap();
                assert_ne!(guard, fixture.flag);
                assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
                EvaluatorBind.apply(&mut fixture.plan, &mut arena, None)?;
                let predicate = fixture.predicate();
                for flag in [
                    DataValue::Boolean(false),
                    DataValue::Boolean(true),
                    DataValue::Null,
                ] {
                    let left = Tuple::new(None, vec![flag.clone()]);
                    let guard_value = arena
                        .expression(guard)
                        .eval(&arena, Some(&left))?
                        .into_owned();
                    let expected = match (&op, &flag) {
                        (BinaryOperator::Or, DataValue::Boolean(value)) => {
                            DataValue::Boolean(!value)
                        }
                        _ => flag.clone(),
                    };
                    assert_eq!(guard_value, expected);
                    for marker in [
                        DataValue::Boolean(false),
                        DataValue::Boolean(true),
                        DataValue::Null,
                    ] {
                        let original = Tuple::new(None, vec![flag.clone(), marker.clone()]);
                        let guarded = Tuple::new(
                            None,
                            vec![
                                flag.clone(),
                                if guard_value == DataValue::Boolean(false) {
                                    DataValue::Null
                                } else {
                                    marker
                                },
                            ],
                        );
                        assert_eq!(
                            arena.expression(predicate).eval(&arena, Some(&original))?,
                            arena.expression(predicate).eval(&arena, Some(&guarded))?,
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn marker_needed_by_projection_or_another_branch_is_not_guarded() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut fixture = Fixture::new(&mut arena, BinaryOperator::Or, false)?;
        let Operator::Project(project) = &mut fixture.plan.operator else {
            unreachable!()
        };
        project.exprs.push(fixture.marker);
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
        assert!(fixture.mark().guard.is_none());
        let Operator::Project(project) = &mut fixture.plan.operator else {
            unreachable!()
        };
        project.exprs.pop();
        let guarded_branch = fixture.predicate();
        let predicate = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Or,
            left_expr: guarded_branch,
            right_expr: fixture.marker,
            evaluator: None,
            ty: LogicalType::Boolean,
        });
        let Childrens::Only(filter) = fixture.plan.childrens.as_mut() else {
            unreachable!()
        };
        let Operator::Filter(op) = &mut filter.operator else {
            unreachable!()
        };
        op.predicate = predicate;
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
        assert!(fixture.mark().guard.is_none());
        Ok(())
    }

    #[test]
    fn three_mark_guards_preserve_all_729_truth_combinations() -> Result<(), DatabaseError> {
        use crate::expression::visitor_mut::ExprVisitorMut;
        use crate::expression::BindEvaluator;
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut columns = Vec::new();
        let mut exprs = Vec::new();
        for (position, name) in ["q2", "p2", "q3", "m1", "m2", "m3"].iter().enumerate() {
            let column = arena.alloc_column(ColumnCatalog::new(
                (*name).into(),
                true,
                ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
            ));
            columns.push(column);
            exprs.push(arena.alloc_expression(ScalarExpression::column_expr(column, position)));
        }
        let binary = |op, left_expr, right_expr, arena: &mut PlanArena| {
            arena.alloc_expression(ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                evaluator: None,
                ty: LogicalType::Boolean,
            })
        };
        let p2_or_m2 = binary(BinaryOperator::Or, exprs[1], exprs[4], &mut arena);
        let b2 = binary(BinaryOperator::And, exprs[0], p2_or_m2, &mut arena);
        let b3 = binary(BinaryOperator::And, exprs[2], exprs[5], &mut arena);
        let first = binary(BinaryOperator::Or, exprs[3], b2, &mut arena);
        let mut predicate = binary(BinaryOperator::Or, first, b3, &mut arena);
        let mut guards = Vec::new();
        let mut conditions = Vec::new();
        for index in 3..6 {
            conditions.clear();
            let mut visitor = GuardVisitor {
                marker: columns[index],
                guards: &mut conditions,
            };
            visitor.visit(predicate, &arena)?;
            let mut guard = None;
            for &(sibling, when_true) in &conditions {
                let skip = decisive_condition(sibling, when_true, &columns[index..], &mut arena)?;
                let needed = skip.map(|expr| negate(expr, &mut arena));
                guard = combine_conditions(BinaryOperator::And, guard, needed, true, &mut arena);
            }
            let mut guard = guard.unwrap();
            BindEvaluator.visit(&mut guard, &mut arena)?;
            guards.push(guard);
        }
        BindEvaluator.visit(&mut predicate, &mut arena)?;
        let values = [
            DataValue::Boolean(false),
            DataValue::Boolean(true),
            DataValue::Null,
        ];
        let mut skipped = [0; 3];
        for combination in 0..729 {
            let mut digits = combination;
            let original = Tuple::new(
                None,
                (0..6)
                    .map(|_| {
                        let value = values[digits % 3].clone();
                        digits /= 3;
                        value
                    })
                    .collect(),
            );
            let mut guarded = Tuple::new(None, original.values[..3].to_vec());
            for index in 0..3 {
                let skip = *arena
                    .expression(guards[index])
                    .eval(&arena, Some(&guarded))?
                    == DataValue::Boolean(false);
                skipped[index] += usize::from(skip);
                guarded.values.push(if skip {
                    DataValue::Null
                } else {
                    original.values[index + 3].clone()
                });
            }
            assert_eq!(
                arena.expression(predicate).eval(&arena, Some(&original))?,
                arena.expression(predicate).eval(&arena, Some(&guarded))?,
                "combination {combination}"
            );
        }
        assert!(skipped[1] > 0 && skipped[2] > 0);
        Ok(())
    }

    #[test]
    fn unsafe_sibling_is_not_evaluated_before_a_matching_marker() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut fixture = Fixture::new(&mut arena, BinaryOperator::Or, true)?;
        let value = DataValue::from("not a boolean".to_string());
        let evaluator = cast_create(&value.logical_type(), &LogicalType::Boolean)?;
        let expr = arena.alloc_expression(value.into());
        let failing = arena.alloc_expression(ScalarExpression::TypeCast {
            expr,
            ty: LogicalType::Boolean,
            evaluator: Some(evaluator),
        });
        let predicate = fixture.predicate();
        let ScalarExpression::Binary { right_expr, .. } = &mut *arena.expression_mut(predicate)
        else {
            unreachable!()
        };
        *right_expr = failing;
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
        assert!(fixture.mark().guard.is_none());
        EvaluatorBind.apply(&mut fixture.plan, &mut arena, None)?;
        let matched = Tuple::new(
            None,
            vec![DataValue::Boolean(false), DataValue::Boolean(true)],
        );
        assert_eq!(
            *arena.expression(predicate).eval(&arena, Some(&matched))?,
            DataValue::Boolean(true)
        );
        let unmatched = Tuple::new(
            None,
            vec![DataValue::Boolean(false), DataValue::Boolean(false)],
        );
        assert!(matches!(
            arena.expression(predicate).eval(&arena, Some(&unmatched)),
            Err(DatabaseError::ParseBool(_))
        ));
        Ok(())
    }

    #[test]
    fn numeric_guards_accept_only_safe_operands() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let number = arena.alloc_column(ColumnCatalog::new(
            "number".into(),
            true,
            ColumnDesc::new(LogicalType::Bigint, None, false, None)?,
        ));
        let number = arena.alloc_expression(ScalarExpression::column_expr(number, 0));
        let zero = arena.alloc_expression(DataValue::Int64(0).into());
        let widening = arena.alloc_expression(ScalarExpression::TypeCast {
            expr: number,
            ty: LogicalType::Double,
            evaluator: None,
        });
        let narrowing = arena.alloc_expression(ScalarExpression::TypeCast {
            expr: number,
            ty: LogicalType::Integer,
            evaluator: None,
        });
        let arithmetic = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Divide,
            left_expr: number,
            right_expr: zero,
            evaluator: None,
            ty: LogicalType::Bigint,
        });
        for (operand, valid) in [
            (number, true),
            (widening, true),
            (narrowing, false),
            (arithmetic, false),
        ] {
            for op in [
                BinaryOperator::Eq,
                BinaryOperator::NotEq,
                BinaryOperator::Gt,
                BinaryOperator::GtEq,
                BinaryOperator::Lt,
                BinaryOperator::LtEq,
            ] {
                let predicate = arena.alloc_expression(ScalarExpression::Binary {
                    op,
                    left_expr: operand,
                    right_expr: zero,
                    evaluator: None,
                    ty: LogicalType::Boolean,
                });
                let mut visitor = SimpleGuardVisitor {
                    valid: true,
                    context: GuardContext::Predicate,
                };
                visitor.visit(predicate, &arena)?;
                assert_eq!(visitor.valid, valid);
            }
        }
        Ok(())
    }

    #[test]
    fn null_checks_and_aliases_keep_the_guard_whitelist() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut fixture = Fixture::new(&mut arena, BinaryOperator::And, false)?;
        let number_column = arena.alloc_column(ColumnCatalog::new(
            "number".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let number = arena.alloc_expression(ScalarExpression::column_expr(number_column, 0));
        let is_null = arena.alloc_expression(ScalarExpression::IsNull {
            negated: false,
            expr: number,
        });
        let is_not_null = arena.alloc_expression(ScalarExpression::IsNull {
            negated: true,
            expr: number,
        });
        let not_flag = arena.alloc_expression(ScalarExpression::Unary {
            op: UnaryOperator::Not,
            expr: fixture.flag,
            evaluator: None,
            ty: LogicalType::Boolean,
        });
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: fixture.flag,
            alias: AliasType::Expr(number),
        });
        let not_number = arena.alloc_expression(ScalarExpression::Unary {
            op: UnaryOperator::Not,
            expr: number,
            evaluator: None,
            ty: LogicalType::Integer,
        });
        let nested = arena.alloc_expression(ScalarExpression::IsNull {
            negated: false,
            expr: not_number,
        });
        let assert_case = |expr, expected| -> Result<(), DatabaseError> {
            let mut visitor = SimpleGuardVisitor {
                valid: true,
                context: GuardContext::Predicate,
            };
            visitor.visit(expr, &arena)?;
            assert_eq!(visitor.valid, expected);
            Ok(())
        };
        assert_case(fixture.flag, true)?;
        assert_case(number, false)?;
        assert_case(is_null, true)?;
        assert_case(is_not_null, true)?;
        assert_case(not_flag, true)?;
        assert_case(alias, true)?;
        assert_case(nested, false)?;
        let predicate = fixture.predicate();
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: fixture.flag,
            alias: AliasType::Name("flag_alias".to_string()),
        });
        let ScalarExpression::Binary { left_expr, .. } = &mut *arena.expression_mut(predicate)
        else {
            unreachable!()
        };
        *left_expr = alias;
        assert!(GuardMarkApply.apply(&mut fixture.plan, &mut arena, None)?);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
