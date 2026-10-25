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

use super::strip_alias;
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::visitor::ExprVisitor;
use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::mark_apply::MarkApplyKind;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::types::evaluator::BinaryEvaluatorRef;
use crate::types::value::DataValue;
use crate::types::LogicalType;

pub struct GuardMarkApply;

impl NormalizationRule for GuardMarkApply {
    fn apply(&self, plan: &mut LogicalPlan, arena: &mut PlanArena) -> Result<bool, DatabaseError> {
        let (Operator::Project(project), Childrens::Only(filter)) =
            (&plan.operator, plan.childrens.as_mut())
        else {
            return Ok(false);
        };
        let (Operator::Filter(filter_op), Childrens::Only(mark)) =
            (&filter.operator, filter.childrens.as_mut())
        else {
            return Ok(false);
        };
        let (Operator::MarkApply(op), Childrens::Twins { left, .. }) =
            (&mut mark.operator, mark.childrens.as_mut())
        else {
            return Ok(false);
        };
        if op.guard.is_some() || matches!(op.kind, MarkApplyKind::InnerJoin) {
            return Ok(false);
        }
        let marker = *op.output_column();
        // The projection must hide the marker: downstream consumers need its real value otherwise.
        for expr in &project.exprs {
            if expr
                .any_referenced_column(arena, |arena, column| arena.same_column(*column, marker))?
            {
                return Ok(false);
            }
        }
        let mut visitor = GuardVisitor {
            marker,
            guard: None,
        };
        visitor.visit(filter_op.predicate, arena)?;
        let Some((guard, negated)) = visitor.guard else {
            return Ok(false);
        };
        let schema = left.output_schema(arena);
        if !guard.all_referenced_columns(arena, |arena, column| {
            schema
                .iter()
                .any(|candidate| arena.same_column(*candidate, *column))
        })? {
            return Ok(false);
        }
        // The guard has its own mutable expression references, but uses the same left positions.
        let guard = guard.clone_expression(arena)?;
        op.guard = Some(if negated {
            arena.alloc_expression(ScalarExpression::Unary {
                op: UnaryOperator::Not,
                expr: guard,
                evaluator: None,
                ty: LogicalType::Boolean,
            })
        } else {
            guard
        });
        Ok(true)
    }
}

struct GuardVisitor {
    marker: ColumnRef,
    guard: Option<(ExprRef, bool)>,
}

impl ExprVisitor<PlanArena<'_>> for GuardVisitor {
    fn visit_expression_ref(&mut self, _: ExprRef, _: &PlanArena) -> Result<bool, DatabaseError> {
        Ok(self.guard.is_none())
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

    fn visit_alias(
        &mut self,
        expr: ExprRef,
        alias: &AliasType,
        arena: &PlanArena,
    ) -> Result<(), DatabaseError> {
        self.visit(
            match alias {
                AliasType::Expr(expr) => *expr,
                AliasType::Name(_) => expr,
            },
            arena,
        )
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
            _ => return Ok(()),
        };
        let mut visitor = SimpleGuardVisitor {
            valid: true,
            null_operand: false,
        };
        visitor.visit(sibling, arena)?;
        if visitor.valid {
            self.guard = Some((sibling, matches!(op, BinaryOperator::Or)));
            return Ok(());
        }
        self.visit(branch, arena)
    }
}

struct SimpleGuardVisitor {
    valid: bool,
    null_operand: bool,
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
        let unaliased = strip_alias(expr, arena);
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
        self.valid = match expr {
            ScalarExpression::Constant(value) => {
                self.null_operand || matches!(value, DataValue::Boolean(_))
            }
            ScalarExpression::ColumnRef { column, .. } => {
                self.null_operand || arena.column(*column).datatype() == &LogicalType::Boolean
            }
            ScalarExpression::IsNull { .. }
            | ScalarExpression::Unary {
                op: UnaryOperator::Not,
                ..
            } => !self.null_operand,
            // Do not move arbitrary functions, casts or potentially failing calculations earlier.
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
        self.null_operand = true;
        self.visit(expr, arena)?;
        self.null_operand = false;
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
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
                Operator::Project(ProjectOperator { exprs: vec![flag] }),
                Childrens::Only(Box::new(filter)),
            );
            Ok(Self { plan, flag, marker })
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
    fn and_or_guards_preserve_three_valued_results() -> Result<(), DatabaseError> {
        for op in [BinaryOperator::And, BinaryOperator::Or] {
            for marker_first in [false, true] {
                let tables = TableArenaCell::default();
                let mut arena = PlanArena::new(&tables);
                let mut fixture = Fixture::new(&mut arena, op, marker_first)?;
                assert!(GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
                let guard = fixture.mark().guard.unwrap();
                assert_ne!(guard, fixture.flag);
                assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
                EvaluatorBind.apply(&mut fixture.plan, &mut arena)?;
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
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
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
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
        assert!(fixture.mark().guard.is_none());
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
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
        assert!(fixture.mark().guard.is_none());
        EvaluatorBind.apply(&mut fixture.plan, &mut arena)?;
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
            expr: number,
            alias: AliasType::Expr(fixture.flag),
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
                null_operand: false,
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
        // Passing the whitelist does not waive the rule's left-input dependency check.
        let predicate = fixture.predicate();
        let ScalarExpression::Binary { left_expr, .. } = &mut *arena.expression_mut(predicate)
        else {
            unreachable!()
        };
        *left_expr = alias;
        assert!(!GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
        assert!(fixture.mark().guard.is_none());
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: fixture.flag,
            alias: AliasType::Name("flag_alias".to_string()),
        });
        let ScalarExpression::Binary { left_expr, .. } = &mut *arena.expression_mut(predicate)
        else {
            unreachable!()
        };
        *left_expr = alias;
        assert!(GuardMarkApply.apply(&mut fixture.plan, &mut arena)?);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
