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
use crate::expression::window::WindowCall;
use crate::expression::{
    AliasType, BinaryOperator, ScalarExpression, TrimWhereField, UnaryOperator,
};
use crate::planner::{ExprRef, PlanArena};
use crate::types::evaluator::{BinaryEvaluatorRef, CastEvaluatorRef, UnaryEvaluatorRef};
use crate::types::value::DataValue;
use crate::types::LogicalType;

pub(crate) struct ExprCloner;

impl ExprVisitorMut for ExprCloner {
    fn visit(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        *expr = arena.alloc_expression(arena.expression(*expr).clone());
        walk_mut_expr(self, expr, arena)
    }
}

pub(crate) struct PositionShift {
    pub(crate) delta: isize,
}

impl ExprVisitorMut for PositionShift {
    fn visit_column_ref(
        &mut self,
        _column: &mut ColumnRef,
        position: &mut usize,
        _arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if self.delta.is_negative() {
            *position = position.saturating_sub(self.delta.unsigned_abs());
        } else {
            *position += self.delta as usize;
        }
        Ok(())
    }
}

pub trait ExprVisitorMut: Sized {
    fn visit(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if !self.visit_expression_ref(expr, arena)? {
            return Ok(());
        }

        let mut expression =
            std::mem::replace(arena.expression_mut(*expr), ScalarExpression::Empty);
        let result = self.visit_expression(&mut expression, arena);
        *arena.expression_mut(*expr) = expression;
        if result? {
            walk_mut_expr(self, expr, arena)?;
        }
        Ok(())
    }

    fn visit_expression_ref(
        &mut self,
        _expr: &mut ExprRef,
        _arena: &mut PlanArena<'_>,
    ) -> Result<bool, DatabaseError> {
        Ok(true)
    }

    fn visit_expression(
        &mut self,
        _expr: &mut ScalarExpression,
        _arena: &mut PlanArena<'_>,
    ) -> Result<bool, DatabaseError> {
        Ok(true)
    }

    fn visit_constant(
        &mut self,
        _value: &mut DataValue,
        _arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_column_ref(
        &mut self,
        _column: &mut ColumnRef,
        _position: &mut usize,
        _arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_alias(
        &mut self,
        expr: &mut ExprRef,
        alias: &mut AliasType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if let AliasType::Expr(alias_expr) = alias {
            self.visit(alias_expr, arena)?;
        }
        self.visit(expr, arena)
    }

    fn visit_type_cast(
        &mut self,
        expr: &mut ExprRef,
        _ty: &mut LogicalType,
        _evaluator: &mut Option<CastEvaluatorRef>,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }

    fn visit_is_null(
        &mut self,
        _negated: bool,
        expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }

    fn visit_unary(
        &mut self,
        _op: &mut UnaryOperator,
        expr: &mut ExprRef,
        _evaluator: &mut Option<UnaryEvaluatorRef>,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }

    fn visit_binary(
        &mut self,
        _op: &mut BinaryOperator,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        _evaluator: &mut Option<BinaryEvaluatorRef>,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr, arena)?;
        self.visit(right_expr, arena)
    }

    fn visit_agg(
        &mut self,
        _distinct: bool,
        _kind: &mut AggKind,
        args: &mut [ExprRef],
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for arg in args {
            self.visit(arg, arena)?;
        }
        Ok(())
    }

    fn visit_window(
        &mut self,
        window: &mut WindowCall,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for expr in window
            .function
            .args
            .iter_mut()
            .chain(&mut window.spec.partition_by)
            .chain(window.spec.order_by.iter_mut().map(|field| &mut field.expr))
        {
            self.visit(expr, arena)?;
        }
        Ok(())
    }

    fn visit_in(
        &mut self,
        _negated: bool,
        expr: &mut ExprRef,
        args: &mut [ExprRef],
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        for arg in args {
            self.visit(arg, arena)?;
        }
        Ok(())
    }

    fn visit_between(
        &mut self,
        _negated: bool,
        expr: &mut ExprRef,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        self.visit(left_expr, arena)?;
        self.visit(right_expr, arena)
    }

    fn visit_substring(
        &mut self,
        expr: &mut ExprRef,
        for_expr: &mut Option<ExprRef>,
        from_expr: &mut Option<ExprRef>,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        if let Some(for_expr) = for_expr {
            self.visit(for_expr, arena)?;
        }
        if let Some(from_expr) = from_expr {
            self.visit(from_expr, arena)?;
        }
        Ok(())
    }

    fn visit_position(
        &mut self,
        expr: &mut ExprRef,
        in_expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        self.visit(in_expr, arena)
    }

    fn visit_trim(
        &mut self,
        expr: &mut ExprRef,
        trim_what_expr: &mut Option<ExprRef>,
        _trim_where: &mut Option<TrimWhereField>,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        if let Some(trim_what_expr) = trim_what_expr {
            self.visit(trim_what_expr, arena)?;
        }
        Ok(())
    }

    fn visit_empty(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_reference(
        &mut self,
        expr: &mut ExprRef,
        _pos: usize,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }

    fn visit_tuple(
        &mut self,
        exprs: &mut [ExprRef],
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for expr in exprs {
            self.visit(expr, arena)?;
        }
        Ok(())
    }

    fn visit_scala_function(
        &mut self,
        function: &mut ScalarFunction,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for arg in &mut function.args {
            self.visit(arg, arena)?;
        }
        Ok(())
    }

    fn visit_table_function(
        &mut self,
        function: &mut TableFunction,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for arg in &mut function.args {
            self.visit(arg, arena)?;
        }
        Ok(())
    }

    fn visit_if(
        &mut self,
        condition: &mut ExprRef,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(condition, arena)?;
        self.visit(left_expr, arena)?;
        self.visit(right_expr, arena)
    }

    fn visit_if_null(
        &mut self,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr, arena)?;
        self.visit(right_expr, arena)
    }

    fn visit_null_if(
        &mut self,
        left_expr: &mut ExprRef,
        right_expr: &mut ExprRef,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr, arena)?;
        self.visit(right_expr, arena)
    }

    fn visit_coalesce(
        &mut self,
        exprs: &mut [ExprRef],
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for expr in exprs {
            self.visit(expr, arena)?;
        }
        Ok(())
    }

    fn visit_case_when(
        &mut self,
        operand_expr: &mut Option<ExprRef>,
        expr_pairs: &mut [(ExprRef, ExprRef)],
        else_expr: &mut Option<ExprRef>,
        _ty: &mut LogicalType,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if let Some(expr) = operand_expr {
            self.visit(expr, arena)?;
        }
        for (when_expr, then_expr) in expr_pairs {
            self.visit(when_expr, arena)?;
            self.visit(then_expr, arena)?;
        }
        if let Some(expr) = else_expr {
            self.visit(expr, arena)?;
        }
        Ok(())
    }
}

pub fn walk_mut_expr<V: ExprVisitorMut>(
    visitor: &mut V,
    expr: &mut ExprRef,
    arena: &mut PlanArena<'_>,
) -> Result<(), DatabaseError> {
    let mut expression = std::mem::replace(arena.expression_mut(*expr), ScalarExpression::Empty);
    let result = match &mut expression {
        ScalarExpression::Constant(value) => visitor.visit_constant(value, arena),
        ScalarExpression::ColumnRef { column, position } => {
            visitor.visit_column_ref(column, position, arena)
        }
        ScalarExpression::Alias { expr, alias } => visitor.visit_alias(expr, alias, arena),
        ScalarExpression::TypeCast {
            expr,
            ty,
            evaluator,
        } => visitor.visit_type_cast(expr, ty, evaluator, arena),
        ScalarExpression::IsNull { negated, expr } => visitor.visit_is_null(*negated, expr, arena),
        ScalarExpression::Unary {
            op,
            expr,
            evaluator,
            ty,
        } => visitor.visit_unary(op, expr, evaluator, ty, arena),
        ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator,
            ty,
        } => visitor.visit_binary(op, left_expr, right_expr, evaluator, ty, arena),
        ScalarExpression::AggCall {
            distinct,
            kind,
            args,
            ty,
        } => visitor.visit_agg(*distinct, kind, args, ty, arena),
        ScalarExpression::In {
            negated,
            expr,
            args,
        } => visitor.visit_in(*negated, expr, args, arena),
        ScalarExpression::Between {
            negated,
            expr,
            left_expr,
            right_expr,
        } => visitor.visit_between(*negated, expr, left_expr, right_expr, arena),
        ScalarExpression::SubString {
            expr,
            for_expr,
            from_expr,
        } => visitor.visit_substring(expr, for_expr, from_expr, arena),
        ScalarExpression::Position { expr, in_expr } => {
            visitor.visit_position(expr, in_expr, arena)
        }
        ScalarExpression::Trim {
            expr,
            trim_what_expr,
            trim_where,
        } => visitor.visit_trim(expr, trim_what_expr, trim_where, arena),
        ScalarExpression::Empty => visitor.visit_empty(),
        ScalarExpression::Tuple(exprs) => visitor.visit_tuple(exprs, arena),
        ScalarExpression::ScalaFunction(function) => visitor.visit_scala_function(function, arena),
        ScalarExpression::TableFunction(function) => visitor.visit_table_function(function, arena),
        ScalarExpression::If {
            condition,
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_if(condition, left_expr, right_expr, ty, arena),
        ScalarExpression::IfNull {
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_if_null(left_expr, right_expr, ty, arena),
        ScalarExpression::NullIf {
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_null_if(left_expr, right_expr, ty, arena),
        ScalarExpression::Coalesce { exprs, ty } => visitor.visit_coalesce(exprs, ty, arena),
        ScalarExpression::CaseWhen {
            operand_expr,
            expr_pairs,
            else_expr,
            ty,
        } => visitor.visit_case_when(operand_expr, expr_pairs, else_expr, ty, arena),
        ScalarExpression::WindowCall(window) => visitor.visit_window(window, arena),
    };
    *arena.expression_mut(*expr) = expression;
    result
}
