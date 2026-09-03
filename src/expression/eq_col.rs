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
use crate::expression::agg::AggKind;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::function::table::TableFunction;
use crate::expression::visitor::{walk_expr, ExprVisitor};
use crate::expression::window::WindowCall;
use crate::expression::{BinaryOperator, ScalarExpression, TrimWhereField, UnaryOperator};
use crate::planner::{ExprRef, PlanArena};
use crate::types::evaluator::{BinaryEvaluatorRef, CastEvaluatorRef, UnaryEvaluatorRef};
use crate::types::value::DataValue;
use crate::types::LogicalType;

pub(super) fn eq_ignore_colref_pos(lhs: ExprRef, rhs: ExprRef, arena: &PlanArena<'_>) -> bool {
    EqIgnoreColRefPosVisitor::equals(lhs, rhs, arena)
}

struct EqIgnoreColRefPosVisitor<'a, 'arena> {
    rhs: ExprRef,
    arena: &'a PlanArena<'arena>,
    equal: bool,
}

impl<'a, 'arena> EqIgnoreColRefPosVisitor<'a, 'arena> {
    fn equals(lhs: ExprRef, rhs: ExprRef, arena: &'a PlanArena<'arena>) -> bool {
        let mut visitor = Self {
            rhs,
            arena,
            equal: true,
        };
        visitor.visit(lhs, arena).is_ok() && visitor.equal
    }

    fn rhs(&self) -> &ScalarExpression {
        self.arena.expression(self.rhs.unpack_alias(self.arena))
    }

    fn refs_equal(&self, lhs: &[ExprRef], rhs: &[ExprRef]) -> bool {
        lhs.len() == rhs.len()
            && lhs
                .iter()
                .zip(rhs)
                .all(|(lhs, rhs)| Self::equals(*lhs, *rhs, self.arena))
    }

    fn optional_refs_equal(&self, lhs: Option<ExprRef>, rhs: Option<ExprRef>) -> bool {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => Self::equals(lhs, rhs, self.arena),
            (None, None) => true,
            _ => false,
        }
    }
}

impl ExprVisitor<PlanArena<'_>> for EqIgnoreColRefPosVisitor<'_, '_> {
    fn visit(&mut self, lhs: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
        let lhs = lhs.unpack_alias(arena);
        self.rhs = self.rhs.unpack_alias(arena);
        if lhs == self.rhs {
            return Ok(());
        }
        walk_expr(self, lhs, arena)
    }

    fn visit_constant(&mut self, lhs: &DataValue) -> Result<(), DatabaseError> {
        self.equal = matches!(self.rhs(), ScalarExpression::Constant(rhs) if lhs == rhs);
        Ok(())
    }

    fn visit_column_ref(&mut self, lhs: &ColumnRef) -> Result<(), DatabaseError> {
        self.equal = matches!(
            self.rhs(),
            ScalarExpression::ColumnRef { column: rhs, .. }
                if self.arena.same_column(*lhs, *rhs)
        );
        Ok(())
    }

    fn visit_type_cast(
        &mut self,
        lhs_expr: ExprRef,
        lhs_ty: &LogicalType,
        lhs_evaluator: Option<&CastEvaluatorRef>,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::TypeCast {
                expr: rhs_expr,
                ty: rhs_ty,
                evaluator: rhs_evaluator,
            } => {
                lhs_ty == rhs_ty
                    && lhs_evaluator == rhs_evaluator.as_ref()
                    && Self::equals(lhs_expr, *rhs_expr, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_is_null(
        &mut self,
        lhs_negated: bool,
        lhs_expr: ExprRef,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::IsNull {
                negated: rhs_negated,
                expr: rhs_expr,
            } => lhs_negated == *rhs_negated && Self::equals(lhs_expr, *rhs_expr, self.arena),
            _ => false,
        };
        Ok(())
    }

    fn visit_unary(
        &mut self,
        lhs_op: &UnaryOperator,
        lhs_expr: ExprRef,
        lhs_evaluator: Option<&UnaryEvaluatorRef>,
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::Unary {
                op: rhs_op,
                expr: rhs_expr,
                evaluator: rhs_evaluator,
                ty: rhs_ty,
            } => {
                lhs_op == rhs_op
                    && lhs_evaluator == rhs_evaluator.as_ref()
                    && lhs_ty == rhs_ty
                    && Self::equals(lhs_expr, *rhs_expr, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_binary(
        &mut self,
        lhs_op: &BinaryOperator,
        lhs_left: ExprRef,
        lhs_right: ExprRef,
        lhs_evaluator: Option<&BinaryEvaluatorRef>,
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::Binary {
                op: rhs_op,
                left_expr: rhs_left,
                right_expr: rhs_right,
                evaluator: rhs_evaluator,
                ty: rhs_ty,
            } => {
                lhs_op == rhs_op
                    && lhs_evaluator == rhs_evaluator.as_ref()
                    && lhs_ty == rhs_ty
                    && Self::equals(lhs_left, *rhs_left, self.arena)
                    && Self::equals(lhs_right, *rhs_right, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_agg(
        &mut self,
        lhs_distinct: bool,
        lhs_kind: &AggKind,
        lhs_args: &[ExprRef],
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::AggCall {
                distinct: rhs_distinct,
                kind: rhs_kind,
                args: rhs_args,
                ty: rhs_ty,
            } => {
                lhs_distinct == *rhs_distinct
                    && lhs_kind == rhs_kind
                    && lhs_ty == rhs_ty
                    && self.refs_equal(lhs_args, rhs_args)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_window(
        &mut self,
        lhs: &WindowCall,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::WindowCall(rhs) => {
                lhs.function.kind == rhs.function.kind
                    && lhs.function.ty == rhs.function.ty
                    && self.refs_equal(&lhs.function.args, &rhs.function.args)
                    && self.refs_equal(&lhs.spec.partition_by, &rhs.spec.partition_by)
                    && lhs.spec.order_by.len() == rhs.spec.order_by.len()
                    && lhs.spec.order_by.iter().zip(&rhs.spec.order_by).all(
                        |(lhs_field, rhs_field)| {
                            lhs_field.asc == rhs_field.asc
                                && lhs_field.nulls_first == rhs_field.nulls_first
                                && Self::equals(lhs_field.expr, rhs_field.expr, self.arena)
                        },
                    )
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_in(
        &mut self,
        lhs_negated: bool,
        lhs_expr: ExprRef,
        lhs_args: &[ExprRef],
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::In {
                negated: rhs_negated,
                expr: rhs_expr,
                args: rhs_args,
            } => {
                lhs_negated == *rhs_negated
                    && Self::equals(lhs_expr, *rhs_expr, self.arena)
                    && self.refs_equal(lhs_args, rhs_args)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_between(
        &mut self,
        lhs_negated: bool,
        lhs_expr: ExprRef,
        lhs_left: ExprRef,
        lhs_right: ExprRef,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::Between {
                negated: rhs_negated,
                expr: rhs_expr,
                left_expr: rhs_left,
                right_expr: rhs_right,
            } => {
                lhs_negated == *rhs_negated
                    && Self::equals(lhs_expr, *rhs_expr, self.arena)
                    && Self::equals(lhs_left, *rhs_left, self.arena)
                    && Self::equals(lhs_right, *rhs_right, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_substring(
        &mut self,
        lhs_expr: ExprRef,
        lhs_for: Option<ExprRef>,
        lhs_from: Option<ExprRef>,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::SubString {
                expr: rhs_expr,
                for_expr: rhs_for,
                from_expr: rhs_from,
            } => {
                Self::equals(lhs_expr, *rhs_expr, self.arena)
                    && self.optional_refs_equal(lhs_for, *rhs_for)
                    && self.optional_refs_equal(lhs_from, *rhs_from)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_position(
        &mut self,
        lhs_expr: ExprRef,
        lhs_in: ExprRef,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::Position {
                expr: rhs_expr,
                in_expr: rhs_in,
            } => {
                Self::equals(lhs_expr, *rhs_expr, self.arena)
                    && Self::equals(lhs_in, *rhs_in, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_trim(
        &mut self,
        lhs_expr: ExprRef,
        lhs_what: Option<ExprRef>,
        lhs_where: Option<&TrimWhereField>,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::Trim {
                expr: rhs_expr,
                trim_what_expr: rhs_what,
                trim_where: rhs_where,
            } => {
                lhs_where == rhs_where.as_ref()
                    && Self::equals(lhs_expr, *rhs_expr, self.arena)
                    && self.optional_refs_equal(lhs_what, *rhs_what)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_empty(&mut self) -> Result<(), DatabaseError> {
        self.equal = matches!(self.rhs(), ScalarExpression::Empty);
        Ok(())
    }

    fn visit_tuple(
        &mut self,
        lhs: &[ExprRef],
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal =
            matches!(self.rhs(), ScalarExpression::Tuple(rhs) if self.refs_equal(lhs, rhs));
        Ok(())
    }

    fn visit_scala_function(
        &mut self,
        lhs: &ScalarFunction,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = matches!(
            self.rhs(),
            ScalarExpression::ScalaFunction(rhs)
                if lhs.summary() == rhs.summary() && self.refs_equal(&lhs.args, &rhs.args)
        );
        Ok(())
    }

    fn visit_table_function(
        &mut self,
        lhs: &TableFunction,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = matches!(
            self.rhs(),
            ScalarExpression::TableFunction(rhs)
                if lhs.summary() == rhs.summary() && self.refs_equal(&lhs.args, &rhs.args)
        );
        Ok(())
    }

    fn visit_if(
        &mut self,
        lhs_condition: ExprRef,
        lhs_left: ExprRef,
        lhs_right: ExprRef,
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::If {
                condition: rhs_condition,
                left_expr: rhs_left,
                right_expr: rhs_right,
                ty: rhs_ty,
            } => {
                lhs_ty == rhs_ty
                    && Self::equals(lhs_condition, *rhs_condition, self.arena)
                    && Self::equals(lhs_left, *rhs_left, self.arena)
                    && Self::equals(lhs_right, *rhs_right, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_if_null(
        &mut self,
        lhs_left: ExprRef,
        lhs_right: ExprRef,
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::IfNull {
                left_expr: rhs_left,
                right_expr: rhs_right,
                ty: rhs_ty,
            } => {
                lhs_ty == rhs_ty
                    && Self::equals(lhs_left, *rhs_left, self.arena)
                    && Self::equals(lhs_right, *rhs_right, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_null_if(
        &mut self,
        lhs_left: ExprRef,
        lhs_right: ExprRef,
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::NullIf {
                left_expr: rhs_left,
                right_expr: rhs_right,
                ty: rhs_ty,
            } => {
                lhs_ty == rhs_ty
                    && Self::equals(lhs_left, *rhs_left, self.arena)
                    && Self::equals(lhs_right, *rhs_right, self.arena)
            }
            _ => false,
        };
        Ok(())
    }

    fn visit_coalesce(
        &mut self,
        lhs_exprs: &[ExprRef],
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::Coalesce {
                exprs: rhs_exprs,
                ty: rhs_ty,
            } => lhs_ty == rhs_ty && self.refs_equal(lhs_exprs, rhs_exprs),
            _ => false,
        };
        Ok(())
    }

    fn visit_case_when(
        &mut self,
        lhs_operand: Option<ExprRef>,
        lhs_pairs: &[(ExprRef, ExprRef)],
        lhs_else: Option<ExprRef>,
        lhs_ty: &LogicalType,
        _arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.equal = match self.rhs() {
            ScalarExpression::CaseWhen {
                operand_expr: rhs_operand,
                expr_pairs: rhs_pairs,
                else_expr: rhs_else,
                ty: rhs_ty,
            } => {
                lhs_ty == rhs_ty
                    && self.optional_refs_equal(lhs_operand, *rhs_operand)
                    && lhs_pairs.len() == rhs_pairs.len()
                    && lhs_pairs.iter().zip(rhs_pairs).all(
                        |((lhs_when, lhs_then), (rhs_when, rhs_then))| {
                            Self::equals(*lhs_when, *rhs_when, self.arena)
                                && Self::equals(*lhs_then, *rhs_then, self.arena)
                        },
                    )
                    && self.optional_refs_equal(lhs_else, *rhs_else)
            }
            _ => false,
        };
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::function::scala::ArcScalarFunctionImpl;
    use crate::expression::function::table::{ArcTableFunctionImpl, TableFunctionCatalog};
    use crate::expression::window::{WindowFunction, WindowFunctionKind, WindowSpec};
    use crate::expression::AliasType;
    use crate::function::current_date::CurrentDate;
    use crate::function::numbers::Numbers;
    use crate::planner::operator::sort::SortField;
    use crate::planner::TableArenaCell;

    fn assert_case(
        arena: &mut PlanArena<'_>,
        lhs: ScalarExpression,
        rhs: ScalarExpression,
        different: ScalarExpression,
    ) {
        let lhs = arena.alloc_expression(lhs);
        let rhs = arena.alloc_expression(rhs);
        let different = arena.alloc_expression(different);

        assert!(
            eq_ignore_colref_pos(lhs, rhs, arena),
            "lhs={lhs:?}, rhs={rhs:?}"
        );
        assert!(
            eq_ignore_colref_pos(rhs, lhs, arena),
            "rhs={rhs:?}, lhs={lhs:?}"
        );
        assert!(
            eq_ignore_colref_pos(lhs, lhs, arena),
            "self comparison failed: {lhs:?}"
        );
        assert!(
            !eq_ignore_colref_pos(lhs, different, arena),
            "unexpected equality: lhs={lhs:?}, different={different:?}"
        );
        assert!(
            !eq_ignore_colref_pos(different, lhs, arena),
            "unexpected reverse equality: different={different:?}, lhs={lhs:?}"
        );
    }

    #[test]
    fn compares_every_scalar_expression_variant() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);

        let lhs_column = arena.alloc_column(ColumnCatalog::new(
            "c1".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let rhs_column = arena.alloc_column(ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Bigint, None, false, None)?,
        ));
        let different_column = arena.alloc_column(ColumnCatalog::new(
            "c2".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));

        let lhs_child = arena.alloc_expression(ScalarExpression::column_expr(lhs_column, 0));
        let rhs_child = arena.alloc_expression(ScalarExpression::column_expr(rhs_column, 99));
        let different_child = arena.alloc_expression(ScalarExpression::Constant(2.into()));
        let lhs_one = arena.alloc_expression(ScalarExpression::Constant(1.into()));
        let rhs_one = arena.alloc_expression(ScalarExpression::Constant(1.into()));
        let lhs_three = arena.alloc_expression(ScalarExpression::Constant(3.into()));
        let rhs_three = arena.alloc_expression(ScalarExpression::Constant(3.into()));

        assert_case(
            &mut arena,
            ScalarExpression::Constant(1.into()),
            ScalarExpression::Constant(1.into()),
            ScalarExpression::Constant(2.into()),
        );
        assert_case(
            &mut arena,
            ScalarExpression::column_expr(lhs_column, 0),
            ScalarExpression::column_expr(rhs_column, 42),
            ScalarExpression::column_expr(different_column, 0),
        );
        assert_case(
            &mut arena,
            ScalarExpression::Alias {
                expr: lhs_child,
                alias: AliasType::Name("lhs".to_string()),
            },
            ScalarExpression::Alias {
                expr: rhs_child,
                alias: AliasType::Name("rhs".to_string()),
            },
            ScalarExpression::Alias {
                expr: different_child,
                alias: AliasType::Name("lhs".to_string()),
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Alias {
                expr: different_child,
                alias: AliasType::Expr(lhs_child),
            },
            ScalarExpression::Alias {
                expr: different_child,
                alias: AliasType::Expr(rhs_child),
            },
            ScalarExpression::Alias {
                expr: lhs_child,
                alias: AliasType::Expr(different_child),
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::TypeCast {
                expr: lhs_child,
                ty: LogicalType::Integer,
                evaluator: None,
            },
            ScalarExpression::TypeCast {
                expr: rhs_child,
                ty: LogicalType::Integer,
                evaluator: None,
            },
            ScalarExpression::TypeCast {
                expr: rhs_child,
                ty: LogicalType::Bigint,
                evaluator: None,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::IsNull {
                negated: false,
                expr: lhs_child,
            },
            ScalarExpression::IsNull {
                negated: false,
                expr: rhs_child,
            },
            ScalarExpression::IsNull {
                negated: true,
                expr: rhs_child,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Unary {
                op: UnaryOperator::Minus,
                expr: lhs_child,
                evaluator: None,
                ty: LogicalType::Integer,
            },
            ScalarExpression::Unary {
                op: UnaryOperator::Minus,
                expr: rhs_child,
                evaluator: None,
                ty: LogicalType::Integer,
            },
            ScalarExpression::Unary {
                op: UnaryOperator::Plus,
                expr: rhs_child,
                evaluator: None,
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: lhs_child,
                right_expr: lhs_one,
                evaluator: None,
                ty: LogicalType::Integer,
            },
            ScalarExpression::Binary {
                op: BinaryOperator::Plus,
                left_expr: rhs_child,
                right_expr: rhs_one,
                evaluator: None,
                ty: LogicalType::Integer,
            },
            ScalarExpression::Binary {
                op: BinaryOperator::Minus,
                left_expr: rhs_child,
                right_expr: rhs_one,
                evaluator: None,
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::AggCall {
                distinct: true,
                kind: AggKind::Sum,
                args: vec![lhs_child],
                ty: LogicalType::Integer,
            },
            ScalarExpression::AggCall {
                distinct: true,
                kind: AggKind::Sum,
                args: vec![rhs_child],
                ty: LogicalType::Integer,
            },
            ScalarExpression::AggCall {
                distinct: false,
                kind: AggKind::Sum,
                args: vec![rhs_child],
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::In {
                negated: false,
                expr: lhs_child,
                args: vec![lhs_one],
            },
            ScalarExpression::In {
                negated: false,
                expr: rhs_child,
                args: vec![rhs_one],
            },
            ScalarExpression::In {
                negated: true,
                expr: rhs_child,
                args: vec![rhs_one],
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Between {
                negated: false,
                expr: lhs_child,
                left_expr: lhs_one,
                right_expr: lhs_three,
            },
            ScalarExpression::Between {
                negated: false,
                expr: rhs_child,
                left_expr: rhs_one,
                right_expr: rhs_three,
            },
            ScalarExpression::Between {
                negated: true,
                expr: rhs_child,
                left_expr: rhs_one,
                right_expr: rhs_three,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::SubString {
                expr: lhs_child,
                for_expr: Some(lhs_one),
                from_expr: None,
            },
            ScalarExpression::SubString {
                expr: rhs_child,
                for_expr: Some(rhs_one),
                from_expr: None,
            },
            ScalarExpression::SubString {
                expr: rhs_child,
                for_expr: Some(rhs_one),
                from_expr: Some(rhs_three),
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Position {
                expr: lhs_child,
                in_expr: lhs_one,
            },
            ScalarExpression::Position {
                expr: rhs_child,
                in_expr: rhs_one,
            },
            ScalarExpression::Position {
                expr: rhs_child,
                in_expr: different_child,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Trim {
                expr: lhs_child,
                trim_what_expr: Some(lhs_one),
                trim_where: Some(TrimWhereField::Both),
            },
            ScalarExpression::Trim {
                expr: rhs_child,
                trim_what_expr: Some(rhs_one),
                trim_where: Some(TrimWhereField::Both),
            },
            ScalarExpression::Trim {
                expr: rhs_child,
                trim_what_expr: Some(rhs_one),
                trim_where: Some(TrimWhereField::Leading),
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Empty,
            ScalarExpression::Empty,
            ScalarExpression::Constant(1.into()),
        );
        assert_case(
            &mut arena,
            ScalarExpression::Tuple(vec![lhs_child, lhs_one]),
            ScalarExpression::Tuple(vec![rhs_child, rhs_one]),
            ScalarExpression::Tuple(vec![rhs_child]),
        );
        assert_case(
            &mut arena,
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![lhs_child],
                inner: ArcScalarFunctionImpl(CurrentDate::new()),
            }),
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![rhs_child],
                inner: ArcScalarFunctionImpl(CurrentDate::new()),
            }),
            ScalarExpression::ScalaFunction(ScalarFunction {
                args: vec![different_child],
                inner: ArcScalarFunctionImpl(CurrentDate::new()),
            }),
        );
        assert_case(
            &mut arena,
            ScalarExpression::TableFunction(TableFunction {
                args: vec![lhs_child],
                catalog: TableFunctionCatalog {
                    schema: vec![],
                    inner: ArcTableFunctionImpl(Numbers::new()),
                },
            }),
            ScalarExpression::TableFunction(TableFunction {
                args: vec![rhs_child],
                catalog: TableFunctionCatalog {
                    schema: vec![],
                    inner: ArcTableFunctionImpl(Numbers::new()),
                },
            }),
            ScalarExpression::TableFunction(TableFunction {
                args: vec![different_child],
                catalog: TableFunctionCatalog {
                    schema: vec![],
                    inner: ArcTableFunctionImpl(Numbers::new()),
                },
            }),
        );
        assert_case(
            &mut arena,
            ScalarExpression::If {
                condition: lhs_child,
                left_expr: lhs_one,
                right_expr: lhs_three,
                ty: LogicalType::Integer,
            },
            ScalarExpression::If {
                condition: rhs_child,
                left_expr: rhs_one,
                right_expr: rhs_three,
                ty: LogicalType::Integer,
            },
            ScalarExpression::If {
                condition: rhs_child,
                left_expr: rhs_one,
                right_expr: different_child,
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::IfNull {
                left_expr: lhs_child,
                right_expr: lhs_one,
                ty: LogicalType::Integer,
            },
            ScalarExpression::IfNull {
                left_expr: rhs_child,
                right_expr: rhs_one,
                ty: LogicalType::Integer,
            },
            ScalarExpression::IfNull {
                left_expr: rhs_child,
                right_expr: different_child,
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::NullIf {
                left_expr: lhs_child,
                right_expr: lhs_one,
                ty: LogicalType::Integer,
            },
            ScalarExpression::NullIf {
                left_expr: rhs_child,
                right_expr: rhs_one,
                ty: LogicalType::Integer,
            },
            ScalarExpression::NullIf {
                left_expr: rhs_child,
                right_expr: rhs_one,
                ty: LogicalType::Bigint,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::Coalesce {
                exprs: vec![lhs_child, lhs_one],
                ty: LogicalType::Integer,
            },
            ScalarExpression::Coalesce {
                exprs: vec![rhs_child, rhs_one],
                ty: LogicalType::Integer,
            },
            ScalarExpression::Coalesce {
                exprs: vec![rhs_child],
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::CaseWhen {
                operand_expr: Some(lhs_child),
                expr_pairs: vec![(lhs_child, lhs_one)],
                else_expr: Some(lhs_three),
                ty: LogicalType::Integer,
            },
            ScalarExpression::CaseWhen {
                operand_expr: Some(rhs_child),
                expr_pairs: vec![(rhs_child, rhs_one)],
                else_expr: Some(rhs_three),
                ty: LogicalType::Integer,
            },
            ScalarExpression::CaseWhen {
                operand_expr: None,
                expr_pairs: vec![(rhs_child, rhs_one)],
                else_expr: Some(rhs_three),
                ty: LogicalType::Integer,
            },
        );
        assert_case(
            &mut arena,
            ScalarExpression::WindowCall(WindowCall {
                function: WindowFunction {
                    kind: WindowFunctionKind::RowNumber,
                    args: vec![lhs_child],
                    ty: LogicalType::Bigint,
                },
                spec: WindowSpec {
                    partition_by: vec![lhs_child],
                    order_by: vec![SortField::new(lhs_one, true, false)],
                },
            }),
            ScalarExpression::WindowCall(WindowCall {
                function: WindowFunction {
                    kind: WindowFunctionKind::RowNumber,
                    args: vec![rhs_child],
                    ty: LogicalType::Bigint,
                },
                spec: WindowSpec {
                    partition_by: vec![rhs_child],
                    order_by: vec![SortField::new(rhs_one, true, false)],
                },
            }),
            ScalarExpression::WindowCall(WindowCall {
                function: WindowFunction {
                    kind: WindowFunctionKind::RowNumber,
                    args: vec![rhs_child],
                    ty: LogicalType::Bigint,
                },
                spec: WindowSpec {
                    partition_by: vec![rhs_child],
                    order_by: vec![SortField::new(rhs_one, false, false)],
                },
            }),
        );

        Ok(())
    }
}
