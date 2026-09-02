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
use crate::planner::{ExprRef, PlanArena};
use crate::types::evaluator::{binary_create, unary_create};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::borrow::Cow;

#[derive(Debug)]
enum Replace {
    Binary(ReplaceBinary),
    Unary(ReplaceUnary),
}

#[derive(Debug)]
struct ReplaceBinary {
    column_expr: ExprRef,
    val_expr: ExprRef,
    op: BinaryOperator,
    ty: LogicalType,
    is_column_left: bool,
}

#[derive(Debug)]
struct ReplaceUnary {
    child_expr: ExprRef,
    op: UnaryOperator,
    ty: LogicalType,
}

pub struct ConstantCalculator;

impl ConstantCalculator {
    pub fn new(_arena: &PlanArena<'_>) -> Self {
        Self
    }
}

impl ExprVisitorMut for ConstantCalculator {
    fn visit_expression(
        &mut self,
        expr: &mut ScalarExpression,
        arena: &mut PlanArena<'_>,
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
                    let value = if let Some(evaluator) = evaluator {
                        evaluator.unary_eval(unary_val)
                    } else {
                        unary_create(Cow::Borrowed(ty), *op)?.unary_eval(unary_val)
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
pub struct Simplify {
    replaces: Vec<Replace>,
}

impl ExprVisitorMut for Simplify {
    fn visit_expression(
        &mut self,
        expr: &mut ScalarExpression,
        arena: &mut PlanArena<'_>,
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
                let child_expr = *arg_expr;
                let value = if let Some(value) = arg_expr.unpack_val(arena) {
                    Some(if let Some(evaluator) = evaluator {
                        evaluator.unary_eval(&value)
                    } else {
                        unary_create(Cow::Borrowed(&ty), op)?.unary_eval(&value)
                    })
                } else {
                    None
                };

                if let Some(value) = value {
                    *expr = ScalarExpression::Constant(value);
                } else if matches!(op, UnaryOperator::Not) {
                    if let Some(new_expr) = Self::take_negated_range_comparison(*arg_expr, arena) {
                        *expr = new_expr;
                        return self.visit_expression(expr, arena);
                    } else {
                        self.replaces
                            .push(Replace::Unary(ReplaceUnary { child_expr, op, ty }));
                    }
                } else {
                    self.replaces
                        .push(Replace::Unary(ReplaceUnary { child_expr, op, ty }));
                }
            }
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ty,
                ..
            } => {
                self.fix_expr(left_expr, right_expr, op, arena)?;

                // `(c1 - 1) and (c1 + 2)` cannot fix!
                self.fix_expr(right_expr, left_expr, op, arena)?;

                if let Some(new_expr) =
                    Self::take_bool_normalized_range_comparison(*op, *left_expr, *right_expr, arena)
                {
                    *expr = new_expr;
                    return self.visit_expression(expr, arena);
                }

                if Self::is_arithmetic(op) {
                    match (
                        left_expr.unpack_bound_col(arena, false),
                        right_expr.unpack_bound_col(arena, false),
                    ) {
                        (Some((col, position)), None) => {
                            self.replaces.push(Replace::Binary(ReplaceBinary {
                                column_expr: arena
                                    .alloc_expression(ScalarExpression::column_expr(col, position)),
                                val_expr: *right_expr,
                                op: *op,
                                ty: ty.clone(),
                                is_column_left: true,
                            }));
                        }
                        (None, Some((col, position))) => {
                            self.replaces.push(Replace::Binary(ReplaceBinary {
                                column_expr: arena
                                    .alloc_expression(ScalarExpression::column_expr(col, position)),
                                val_expr: *left_expr,
                                op: *op,
                                ty: ty.clone(),
                                is_column_left: false,
                            }));
                        }
                        (None, None) => {
                            if self.replaces.is_empty() {
                                return Ok(false);
                            }

                            match (
                                left_expr.unpack_bound_col(arena, true),
                                right_expr.unpack_bound_col(arena, true),
                            ) {
                                (Some((col, position)), None) => {
                                    self.replaces.push(Replace::Binary(ReplaceBinary {
                                        column_expr: arena.alloc_expression(
                                            ScalarExpression::column_expr(col, position),
                                        ),
                                        val_expr: *right_expr,
                                        op: *op,
                                        ty: ty.clone(),
                                        is_column_left: true,
                                    }));
                                }
                                (None, Some((col, position))) => {
                                    self.replaces.push(Replace::Binary(ReplaceBinary {
                                        column_expr: arena.alloc_expression(
                                            ScalarExpression::column_expr(col, position),
                                        ),
                                        val_expr: *left_expr,
                                        op: *op,
                                        ty: ty.clone(),
                                        is_column_left: false,
                                    }));
                                }
                                _ => (),
                            }
                        }
                        _ => (),
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
                    new_expr = ScalarExpression::Binary {
                        op: op_2,
                        left_expr: arena.alloc_expression(ScalarExpression::Binary {
                            op: op_1,
                            left_expr: *arg_expr,
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
                        left_expr: *arg_expr,
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
    fn is_arithmetic(op: &mut BinaryOperator) -> bool {
        matches!(
            op,
            BinaryOperator::Plus
                | BinaryOperator::Divide
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
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

    fn take_range_comparison(expr: ExprRef, arena: &PlanArena<'_>) -> Option<ScalarExpression> {
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
        arena: &PlanArena<'_>,
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

    fn boolean_constant(expr: ExprRef, arena: &PlanArena<'_>) -> Option<bool> {
        match arena.expression(expr) {
            ScalarExpression::Constant(DataValue::Boolean(value)) => Some(*value),
            _ => None,
        }
    }

    fn take_range_comparison_with_polarity(
        expr: ExprRef,
        positive: bool,
        arena: &PlanArena<'_>,
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
        arena: &PlanArena<'_>,
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

    fn fix_expr(
        &mut self,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        op: &mut BinaryOperator,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr, arena)?;

        if Self::is_arithmetic(op) {
            return Ok(());
        }
        while let Some(replace) = self.replaces.pop() {
            match replace {
                Replace::Binary(binary) => {
                    Self::fix_binary(binary, left_expr, right_expr, op, arena)
                }
                Replace::Unary(unary) => {
                    Self::fix_unary(unary, left_expr, right_expr, op, arena);
                    self.fix_expr(left_expr, right_expr, op, arena)?;
                }
            }
        }

        Ok(())
    }

    fn fix_unary(
        replace_unary: ReplaceUnary,
        col_expr: &mut ExprRef,
        val_expr: &mut ExprRef,
        op: &mut BinaryOperator,
        arena: &mut PlanArena<'_>,
    ) {
        let ReplaceUnary {
            child_expr,
            op: fix_op,
            ty: fix_ty,
        } = replace_unary;
        *col_expr = child_expr;

        *val_expr = arena.alloc_expression(ScalarExpression::Unary {
            op: fix_op,
            expr: *val_expr,
            evaluator: None,
            ty: fix_ty,
        });
        *op = match fix_op {
            UnaryOperator::Plus => *op,
            UnaryOperator::Minus => match *op {
                BinaryOperator::Plus => BinaryOperator::Minus,
                BinaryOperator::Minus => BinaryOperator::Plus,
                BinaryOperator::Multiply => BinaryOperator::Divide,
                BinaryOperator::Divide => BinaryOperator::Multiply,
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                source_op => source_op,
            },
            UnaryOperator::Not => match *op {
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                source_op => source_op,
            },
        };
    }

    fn fix_binary(
        replace_binary: ReplaceBinary,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        op: &mut BinaryOperator,
        arena: &mut PlanArena<'_>,
    ) {
        let ReplaceBinary {
            column_expr,
            val_expr,
            op: fix_op,
            ty: fix_ty,
            is_column_left,
        } = replace_binary;
        let op_flip = |op: BinaryOperator| match op {
            BinaryOperator::Plus => BinaryOperator::Minus,
            BinaryOperator::Minus => BinaryOperator::Plus,
            BinaryOperator::Multiply => BinaryOperator::Divide,
            BinaryOperator::Divide => BinaryOperator::Multiply,
            _ => unreachable!(),
        };
        let comparison_flip = |op: BinaryOperator| match op {
            BinaryOperator::Gt => BinaryOperator::Lt,
            BinaryOperator::GtEq => BinaryOperator::LtEq,
            BinaryOperator::Lt => BinaryOperator::Gt,
            BinaryOperator::LtEq => BinaryOperator::GtEq,
            source_op => source_op,
        };
        let (fixed_op, fixed_left_expr, fixed_right_expr) = if is_column_left {
            (op_flip(fix_op), *right_expr, val_expr)
        } else {
            if matches!(fix_op, BinaryOperator::Minus | BinaryOperator::Multiply) {
                *op = comparison_flip(*op);
            }
            (fix_op, val_expr, *right_expr)
        };

        *left_expr = column_expr;
        *right_expr = arena.alloc_expression(ScalarExpression::Binary {
            op: fixed_op,
            left_expr: fixed_left_expr,
            right_expr: fixed_right_expr,
            evaluator: None,
            ty: fix_ty,
        });
    }
}

impl ExprRef {
    pub(crate) fn unpack_val(self, arena: &PlanArena<'_>) -> Option<DataValue> {
        match arena.expression(self) {
            ScalarExpression::Constant(val) => Some(val.clone()),
            ScalarExpression::Alias { expr, .. } => expr.unpack_val(arena),
            ScalarExpression::TypeCast { expr, ty, .. } => {
                expr.unpack_val(arena).and_then(|val| val.cast(ty).ok())
            }
            ScalarExpression::IsNull { negated, expr } => Some(DataValue::Boolean(
                expr.unpack_val(arena)?.is_null() != *negated,
            )),
            ScalarExpression::Unary {
                expr,
                op,
                evaluator,
                ty,
            } => Some(if let Some(evaluator) = evaluator {
                evaluator.unary_eval(&expr.unpack_val(arena)?)
            } else {
                unary_create(Cow::Borrowed(ty), *op)
                    .ok()?
                    .unary_eval(&expr.unpack_val(arena)?)
            }),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
                evaluator,
            } => {
                let left = left_expr.unpack_val(arena)?.cast(ty).ok()?;
                let right = right_expr.unpack_val(arena)?.cast(ty).ok()?;
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

    pub(crate) fn unpack_bound_col(
        self,
        arena: &PlanArena<'_>,
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
