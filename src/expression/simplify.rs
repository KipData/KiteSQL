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
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::expression::{BinaryOperator, ScalarExpression, TypeCast, UnaryOperator};
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::evaluator::{binary_create, unary_create};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::borrow::Cow;

pub struct ConstantCalculator;

impl ConstantCalculator {
    pub fn new(_arena: &(dyn MetaArena + '_)) -> Self {
        Self
    }
}

impl ExprVisitorMut for ConstantCalculator {
    fn visit_expression(
        &mut self,
        expr: &mut ScalarExpression,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<bool, DatabaseError> {
        match expr {
            ScalarExpression::Unary {
                op,
                expr: arg_expr,
                evaluator,
                ty,
            } => {
                self.visit(arg_expr, arena)?;

                if let ScalarExpression::Constant(unary_val) = arena.expression(*arg_expr) {
                    if unary_val.has_parameter() {
                        return Ok(false);
                    }
                    let value = if let Some(evaluator) = evaluator {
                        evaluator.unary_eval(unary_val)?
                    } else {
                        unary_create(Cow::Borrowed(ty), *op)?.unary_eval(unary_val)?
                    };
                    *expr = ScalarExpression::Constant(value);
                }
            }
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ..
            } => {
                let ty = LogicalType::max_logical_type(
                    &left_expr.return_type(arena),
                    &right_expr.return_type(arena),
                )?
                .into_owned();
                self.visit(left_expr, arena)?;
                self.visit(right_expr, arena)?;

                if let (
                    ScalarExpression::Constant(left_val),
                    ScalarExpression::Constant(right_val),
                ) = (arena.expression(*left_expr), arena.expression(*right_expr))
                {
                    if left_val.has_parameter() || right_val.has_parameter() {
                        return Ok(false);
                    }
                    let evaluator = binary_create(Cow::Borrowed(&ty), *op)?;
                    let left_val = left_val.clone().cast(&ty)?;
                    let right_val = right_val.clone().cast(&ty)?;
                    let value = evaluator.binary_eval(&left_val, &right_val)?;
                    *expr = ScalarExpression::Constant(value);
                }
            }
            ScalarExpression::TypeCast {
                expr: arg_expr, ty, ..
            } => {
                self.visit(arg_expr, arena)?;

                if let ScalarExpression::Constant(value) = arena.expression(*arg_expr) {
                    if value.has_parameter() {
                        return Ok(false);
                    }
                    let casted = value.clone().cast(ty)?;
                    *expr = ScalarExpression::Constant(casted);
                }
            }
            _ => return Ok(true),
        }

        Ok(false)
    }
}

#[derive(Debug, Default)]
pub struct Simplify;

impl ExprVisitorMut for Simplify {
    fn visit_expression(
        &mut self,
        expr: &mut ScalarExpression,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<bool, DatabaseError> {
        match expr {
            ScalarExpression::Unary {
                op,
                expr: arg_expr,
                evaluator,
                ty,
            } => {
                let op = *op;
                let ty = ty.clone();
                // An overflowing fold (`-MIN`) is left to fail at runtime.
                let value = if let Some(value) = arg_expr.unpack_val(arena) {
                    if let Some(evaluator) = evaluator {
                        evaluator.unary_eval(&value).ok()
                    } else {
                        unary_create(Cow::Borrowed(&ty), op)?
                            .unary_eval(&value)
                            .ok()
                    }
                } else {
                    None
                };

                if let Some(value) = value {
                    *expr = ScalarExpression::Constant(value);
                } else if matches!(op, UnaryOperator::Not) {
                    if let Some(new_expr) = Self::take_negated_range_comparison(*arg_expr, arena) {
                        *expr = new_expr;
                        return self.visit_expression(expr, arena);
                    }
                }
            }
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ..
            } => {
                self.visit(left_expr, arena)?;
                self.visit(right_expr, arena)?;

                if let Some(new_expr) =
                    Self::take_bool_normalized_range_comparison(*op, *left_expr, *right_expr, arena)
                {
                    *expr = new_expr;
                    return self.visit_expression(expr, arena);
                }

                // Move constant terms and signs off the column side, e.g.
                // `1 < -(c1 + 1)` => `c1 < -2`, so a range can be detached.
                if Self::is_rearrangeable_comparison(op) {
                    let isolated = Self::isolate_column(*left_expr, *op, *right_expr, arena)
                        .or_else(|| {
                            let flipped = Self::flip_comparison(*op);
                            Self::isolate_column(*right_expr, flipped, *left_expr, arena)
                        });
                    if let Some((column, fixed_op, value)) = isolated {
                        *op = fixed_op;
                        *left_expr = column;
                        *right_expr = arena.alloc_expression(ScalarExpression::Constant(value));
                    }
                }
            }
            ScalarExpression::TypeCast { expr: arg, ty, .. } => {
                if let Some(value) = arg.unpack_val(arena).and_then(|value| value.cast(ty).ok()) {
                    *expr = ScalarExpression::Constant(value);
                }
            }
            ScalarExpression::IsNull { negated, expr: arg } => {
                if let Some(value) = arg.unpack_val(arena) {
                    *expr =
                        ScalarExpression::Constant(DataValue::Boolean(value.is_null() != *negated));
                }
            }
            ScalarExpression::In {
                negated,
                expr: arg_expr,
                args,
            } => {
                if args.is_empty() {
                    return Ok(false);
                }

                let (op_1, op_2) = if *negated {
                    (BinaryOperator::NotEq, BinaryOperator::And)
                } else {
                    (BinaryOperator::Eq, BinaryOperator::Or)
                };
                let mut new_expr = ScalarExpression::Binary {
                    op: op_1,
                    left_expr: *arg_expr,
                    right_expr: args.remove(0),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                };

                for arg in args.drain(..) {
                    // Each comparison gets its own copy of the operand: a shared
                    // node is shifted twice by later in-place position rewrites
                    // (e.g. predicate pushdown), reading the wrong slot.
                    let arg_copy = arg_expr.clone_expression(arena)?;
                    new_expr = ScalarExpression::Binary {
                        op: op_2,
                        left_expr: arena.alloc_expression(ScalarExpression::Binary {
                            op: op_1,
                            left_expr: arg_copy,
                            right_expr: arg,
                            evaluator: None,
                            ty: LogicalType::Boolean,
                        }),
                        right_expr: arena.alloc_expression(new_expr),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    };
                }
                *expr = new_expr;
                return Ok(true);
            }
            ScalarExpression::Between {
                negated,
                expr: arg_expr,
                left_expr,
                right_expr,
            } => {
                let (op, left_op, right_op) = if *negated {
                    (BinaryOperator::Or, BinaryOperator::Lt, BinaryOperator::Gt)
                } else {
                    (
                        BinaryOperator::And,
                        BinaryOperator::GtEq,
                        BinaryOperator::LtEq,
                    )
                };
                // Same as IN: the operand must not be shared by both comparisons.
                let arg_copy = arg_expr.clone_expression(arena)?;
                *expr = ScalarExpression::Binary {
                    op,
                    left_expr: arena.alloc_expression(ScalarExpression::Binary {
                        op: left_op,
                        left_expr: *arg_expr,
                        right_expr: *left_expr,
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    }),
                    right_expr: arena.alloc_expression(ScalarExpression::Binary {
                        op: right_op,
                        left_expr: arg_copy,
                        right_expr: *right_expr,
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    }),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                };
                return Ok(true);
            }
            _ => return Ok(true),
        }
        Ok(false)
    }
}

impl Simplify {
    fn is_rearrangeable_comparison(op: &BinaryOperator) -> bool {
        matches!(
            op,
            BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
        )
    }

    fn negate_range_comparison(op: BinaryOperator) -> Option<BinaryOperator> {
        match op {
            BinaryOperator::Gt => Some(BinaryOperator::LtEq),
            BinaryOperator::GtEq => Some(BinaryOperator::Lt),
            BinaryOperator::Lt => Some(BinaryOperator::GtEq),
            BinaryOperator::LtEq => Some(BinaryOperator::Gt),
            _ => None,
        }
    }

    fn take_range_comparison(
        expr: ExprRef,
        arena: &(dyn MetaArena + '_),
    ) -> Option<ScalarExpression> {
        match arena.expression(expr) {
            expression @ ScalarExpression::Binary { op, .. }
                if Self::negate_range_comparison(*op).is_some() =>
            {
                Some(expression.clone())
            }
            _ => None,
        }
    }

    fn take_negated_range_comparison(
        expr: ExprRef,
        arena: &(dyn MetaArena + '_),
    ) -> Option<ScalarExpression> {
        let mut expression = arena.expression(expr).clone();
        match &mut expression {
            ScalarExpression::Binary { op, .. } => {
                *op = Self::negate_range_comparison(*op)?;
                Some(expression)
            }
            _ => None,
        }
    }

    fn boolean_constant(expr: ExprRef, arena: &(dyn MetaArena + '_)) -> Option<bool> {
        match arena.expression(expr) {
            ScalarExpression::Constant(DataValue::Boolean(value)) => Some(*value),
            _ => None,
        }
    }

    fn take_range_comparison_with_polarity(
        expr: ExprRef,
        positive: bool,
        arena: &(dyn MetaArena + '_),
    ) -> Option<ScalarExpression> {
        if positive {
            Self::take_range_comparison(expr, arena)
        } else {
            Self::take_negated_range_comparison(expr, arena)
        }
    }

    fn take_bool_normalized_range_comparison(
        op: BinaryOperator,
        left_expr: ExprRef,
        right_expr: ExprRef,
        arena: &(dyn MetaArena + '_),
    ) -> Option<ScalarExpression> {
        let is_eq = matches!(op, BinaryOperator::Eq);
        if !matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) {
            return None;
        }

        if let Some(value) = Self::boolean_constant(right_expr, arena) {
            return Self::take_range_comparison_with_polarity(
                left_expr,
                if is_eq { value } else { !value },
                arena,
            );
        }
        if let Some(value) = Self::boolean_constant(left_expr, arena) {
            return Self::take_range_comparison_with_polarity(
                right_expr,
                if is_eq { value } else { !value },
                arena,
            );
        }

        None
    }

    /// `a op b` <=> `b flip(op) a`.
    fn flip_comparison(op: BinaryOperator) -> BinaryOperator {
        match op {
            BinaryOperator::Gt => BinaryOperator::Lt,
            BinaryOperator::Lt => BinaryOperator::Gt,
            BinaryOperator::GtEq => BinaryOperator::LtEq,
            BinaryOperator::LtEq => BinaryOperator::GtEq,
            op => op,
        }
    }

    /// Evaluates `left op right` in `ty`; `None` on a failed cast or overflow.
    fn eval_in(
        ty: &LogicalType,
        op: BinaryOperator,
        left: DataValue,
        right: DataValue,
    ) -> Option<DataValue> {
        let left = left.cast(ty).ok()?;
        let right = right.cast(ty).ok()?;
        binary_create(Cow::Borrowed(ty), op)
            .ok()?
            .binary_eval(&left, &right)
            .ok()
    }

    /// Rewrites `expr op value` (`value` a constant) into `column op' value'`
    /// by moving constant `+`/`-` terms and unary signs to the constant side,
    /// e.g. `-(c1 + 1) > 1` => `c1 < -2`.
    ///
    /// Only exact rewrites are done: `+`/`-` terms only in integer domains
    /// (float and decimal arithmetic round), never `*`/`/` (sign-dependent and
    /// not invertible for integers), and any overflow while folding aborts.
    /// Returns `None` when nothing could be peeled.
    fn isolate_column(
        mut expr: ExprRef,
        mut op: BinaryOperator,
        value_expr: ExprRef,
        arena: &(dyn MetaArena + '_),
    ) -> Option<(ExprRef, BinaryOperator, DataValue)> {
        let mut value = value_expr.unpack_val(arena)?;
        if value.has_parameter() {
            return None;
        }
        let mut peeled = false;

        loop {
            match arena.expression(expr) {
                ScalarExpression::ColumnRef { .. } => {
                    return peeled.then_some((expr, op, value));
                }
                ScalarExpression::Alias { expr: inner, .. } => expr = *inner,
                ScalarExpression::Unary {
                    op: UnaryOperator::Plus,
                    expr: inner,
                    ..
                } => {
                    expr = *inner;
                    peeled = true;
                }
                // `-x op v` <=> `x flip(op) -v`. Negation is exact, but the
                // binder casts unsigned operands to signed first, so skip them.
                ScalarExpression::Unary {
                    op: UnaryOperator::Minus,
                    expr: inner,
                    ..
                } => {
                    if inner.return_type(arena).is_unsigned_numeric() {
                        return None;
                    }
                    let ty = value.logical_type();
                    value = Self::eval_in(&ty, BinaryOperator::Minus, DataValue::Int32(0), value)?;
                    op = Self::flip_comparison(op);
                    expr = *inner;
                    peeled = true;
                }
                ScalarExpression::Binary {
                    op: arith @ (BinaryOperator::Plus | BinaryOperator::Minus),
                    left_expr,
                    right_expr,
                    ty,
                    ..
                } => {
                    let (inner, constant, constant_left) =
                        match (left_expr.unpack_val(arena), right_expr.unpack_val(arena)) {
                            (None, Some(constant)) => (*left_expr, constant, false),
                            (Some(constant), None) => (*right_expr, constant, true),
                            _ => return None,
                        };
                    if constant.has_parameter() {
                        return None;
                    }
                    // Compare in the domain the original comparison used.
                    let domain = LogicalType::max_logical_type(ty, &value.logical_type())
                        .ok()?
                        .into_owned();
                    if !(domain.is_signed_numeric() || domain.is_unsigned_numeric()) {
                        return None;
                    }
                    value = match (*arith, constant_left) {
                        // `x + c op v`, `c + x op v` => `x op v - c`
                        (BinaryOperator::Plus, _) => {
                            Self::eval_in(&domain, BinaryOperator::Minus, value, constant)?
                        }
                        // `x - c op v` => `x op v + c`
                        (_, false) => {
                            Self::eval_in(&domain, BinaryOperator::Plus, value, constant)?
                        }
                        // `c - x op v` => `x flip(op) c - v`
                        (_, true) => {
                            op = Self::flip_comparison(op);
                            Self::eval_in(&domain, BinaryOperator::Minus, constant, value)?
                        }
                    };
                    expr = inner;
                    peeled = true;
                }
                _ => return None,
            }
        }
    }
}

impl ExprRef {
    pub(crate) fn unpack_val<A: MetaArena + ?Sized>(self, arena: &A) -> Option<DataValue> {
        match arena.expression(self) {
            ScalarExpression::Constant(val) => Some(val.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_val(arena),
            ScalarExpression::TypeCast { expr, ty, .. } => {
                expr.unpack_val(arena).and_then(|val| val.cast(ty).ok())
            }
            ScalarExpression::IsNull { negated, expr } => {
                let value = expr.unpack_val(arena)?;
                (!value.has_parameter()).then(|| DataValue::Boolean(value.is_null() != *negated))
            }
            ScalarExpression::Unary {
                expr,
                op,
                evaluator,
                ty,
            } => {
                let value = expr.unpack_val(arena)?;
                if value.has_parameter() {
                    return None;
                }
                if let Some(evaluator) = evaluator {
                    evaluator.unary_eval(&value)
                } else {
                    unary_create(Cow::Borrowed(ty), *op)
                        .ok()?
                        .unary_eval(&value)
                }
                .ok()
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
                evaluator,
            } => {
                let left = left_expr.unpack_val(arena)?.cast(ty).ok()?;
                let right = right_expr.unpack_val(arena)?.cast(ty).ok()?;
                if left.has_parameter() || right.has_parameter() {
                    return None;
                }
                if let Some(evaluator) = evaluator {
                    evaluator.binary_eval(&left, &right)
                } else {
                    binary_create(Cow::Borrowed(ty), *op)
                        .ok()?
                        .binary_eval(&left, &right)
                }
                .ok()
            }
            _ => None,
        }
    }

    pub(crate) fn unpack_bound_col<A: MetaArena + ?Sized>(
        self,
        arena: &A,
        is_deep: bool,
    ) -> Option<(ColumnRef, usize)> {
        match arena.expression(self) {
            ScalarExpression::ColumnRef { column, position } => Some((*column, *position)),
            ScalarExpression::Alias { expr, .. } => expr.unpack_bound_col(arena, is_deep),
            ScalarExpression::Unary { expr, .. } => expr.unpack_bound_col(arena, is_deep),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                if !is_deep {
                    return None;
                }

                left_expr
                    .unpack_bound_col(arena, true)
                    .or_else(|| right_expr.unpack_bound_col(arena, true))
            }
            _ => None,
        }
    }
}
