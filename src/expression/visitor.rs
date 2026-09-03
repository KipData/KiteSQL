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
use crate::expression::agg::AggKind;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::function::table::TableFunction;
use crate::expression::window::WindowCall;
use crate::expression::{
    AliasType, BinaryOperator, ScalarExpression, TrimWhereField, UnaryOperator,
};
use crate::planner::{ExprRef, MetaArena};
use crate::types::evaluator::{BinaryEvaluatorRef, CastEvaluatorRef, UnaryEvaluatorRef};
use crate::types::value::DataValue;
use crate::types::LogicalType;

pub trait ExprVisitor<A: MetaArena>: Sized {
    fn visit(&mut self, expr: ExprRef, arena: &A) -> Result<(), DatabaseError> {
        if !self.visit_expression_ref(expr, arena)? {
            return Ok(());
        }
        if self.visit_expression(arena.expression(expr), arena)? {
            walk_expr(self, expr, arena)?;
        }
        Ok(())
    }

    fn visit_expression_ref(&mut self, _expr: ExprRef, _arena: &A) -> Result<bool, DatabaseError> {
        Ok(true)
    }

    fn visit_expression(
        &mut self,
        _expr: &ScalarExpression,
        _arena: &A,
    ) -> Result<bool, DatabaseError> {
        Ok(true)
    }

    fn visit_constant(&mut self, _value: &DataValue) -> Result<(), DatabaseError> {
        Ok(())
    }
    fn visit_column_ref(&mut self, _column: &ColumnRef) -> Result<(), DatabaseError> {
        Ok(())
    }
    fn visit_alias(
        &mut self,
        expr: ExprRef,
        alias: &AliasType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        if let AliasType::Expr(alias_expr) = alias {
            self.visit(*alias_expr, arena)?;
        }
        self.visit(expr, arena)
    }
    fn visit_type_cast(
        &mut self,
        expr: ExprRef,
        _ty: &LogicalType,
        _evaluator: Option<&CastEvaluatorRef>,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }
    fn visit_is_null(
        &mut self,
        _negated: bool,
        expr: ExprRef,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }
    fn visit_unary(
        &mut self,
        _op: &UnaryOperator,
        expr: ExprRef,
        _evaluator: Option<&UnaryEvaluatorRef>,
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)
    }
    fn visit_binary(
        &mut self,
        _op: &BinaryOperator,
        left: ExprRef,
        right: ExprRef,
        _evaluator: Option<&BinaryEvaluatorRef>,
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(left, arena)?;
        self.visit(right, arena)
    }
    fn visit_agg(
        &mut self,
        _distinct: bool,
        _kind: &AggKind,
        args: &[ExprRef],
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        for arg in args {
            self.visit(*arg, arena)?;
        }
        Ok(())
    }
    fn visit_window(&mut self, window: &WindowCall, arena: &A) -> Result<(), DatabaseError> {
        for expr in window
            .function
            .args
            .iter()
            .chain(&window.spec.partition_by)
            .chain(window.spec.order_by.iter().map(|field| &field.expr))
        {
            self.visit(*expr, arena)?;
        }
        Ok(())
    }
    fn visit_in(
        &mut self,
        _negated: bool,
        expr: ExprRef,
        args: &[ExprRef],
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        for arg in args {
            self.visit(*arg, arena)?;
        }
        Ok(())
    }
    fn visit_between(
        &mut self,
        _negated: bool,
        expr: ExprRef,
        left: ExprRef,
        right: ExprRef,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        self.visit(left, arena)?;
        self.visit(right, arena)
    }
    fn visit_substring(
        &mut self,
        expr: ExprRef,
        for_expr: Option<ExprRef>,
        from_expr: Option<ExprRef>,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        if let Some(expr) = for_expr {
            self.visit(expr, arena)?;
        }
        if let Some(expr) = from_expr {
            self.visit(expr, arena)?;
        }
        Ok(())
    }
    fn visit_position(
        &mut self,
        expr: ExprRef,
        in_expr: ExprRef,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        self.visit(in_expr, arena)
    }
    fn visit_trim(
        &mut self,
        expr: ExprRef,
        trim_what: Option<ExprRef>,
        _trim_where: Option<&TrimWhereField>,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(expr, arena)?;
        if let Some(expr) = trim_what {
            self.visit(expr, arena)?;
        }
        Ok(())
    }
    fn visit_empty(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }
    fn visit_tuple(&mut self, exprs: &[ExprRef], arena: &A) -> Result<(), DatabaseError> {
        for expr in exprs {
            self.visit(*expr, arena)?;
        }
        Ok(())
    }
    fn visit_scala_function(
        &mut self,
        function: &ScalarFunction,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        for arg in &function.args {
            self.visit(*arg, arena)?;
        }
        Ok(())
    }
    fn visit_table_function(
        &mut self,
        function: &TableFunction,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        for arg in &function.args {
            self.visit(*arg, arena)?;
        }
        Ok(())
    }
    fn visit_if(
        &mut self,
        condition: ExprRef,
        left: ExprRef,
        right: ExprRef,
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(condition, arena)?;
        self.visit(left, arena)?;
        self.visit(right, arena)
    }
    fn visit_if_null(
        &mut self,
        left: ExprRef,
        right: ExprRef,
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(left, arena)?;
        self.visit(right, arena)
    }
    fn visit_null_if(
        &mut self,
        left: ExprRef,
        right: ExprRef,
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        self.visit(left, arena)?;
        self.visit(right, arena)
    }
    fn visit_coalesce(
        &mut self,
        exprs: &[ExprRef],
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        for expr in exprs {
            self.visit(*expr, arena)?;
        }
        Ok(())
    }
    fn visit_case_when(
        &mut self,
        operand: Option<ExprRef>,
        pairs: &[(ExprRef, ExprRef)],
        else_expr: Option<ExprRef>,
        _ty: &LogicalType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        if let Some(expr) = operand {
            self.visit(expr, arena)?;
        }
        for (left, right) in pairs {
            self.visit(*left, arena)?;
            self.visit(*right, arena)?;
        }
        if let Some(expr) = else_expr {
            self.visit(expr, arena)?;
        }
        Ok(())
    }
}

pub fn walk_expr<A: MetaArena, V: ExprVisitor<A>>(
    visitor: &mut V,
    expr: ExprRef,
    arena: &A,
) -> Result<(), DatabaseError> {
    match arena.expression(expr) {
        ScalarExpression::Constant(value) => visitor.visit_constant(value),
        ScalarExpression::ColumnRef { column, .. } => visitor.visit_column_ref(column),
        ScalarExpression::Alias { expr, alias } => visitor.visit_alias(*expr, alias, arena),
        ScalarExpression::TypeCast {
            expr,
            ty,
            evaluator,
        } => visitor.visit_type_cast(*expr, ty, evaluator.as_ref(), arena),
        ScalarExpression::IsNull { negated, expr } => visitor.visit_is_null(*negated, *expr, arena),
        ScalarExpression::Unary {
            op,
            expr,
            evaluator,
            ty,
        } => visitor.visit_unary(op, *expr, evaluator.as_ref(), ty, arena),
        ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator,
            ty,
        } => visitor.visit_binary(op, *left_expr, *right_expr, evaluator.as_ref(), ty, arena),
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
        } => visitor.visit_in(*negated, *expr, args, arena),
        ScalarExpression::Between {
            negated,
            expr,
            left_expr,
            right_expr,
        } => visitor.visit_between(*negated, *expr, *left_expr, *right_expr, arena),
        ScalarExpression::SubString {
            expr,
            for_expr,
            from_expr,
        } => visitor.visit_substring(*expr, *for_expr, *from_expr, arena),
        ScalarExpression::Position { expr, in_expr } => {
            visitor.visit_position(*expr, *in_expr, arena)
        }
        ScalarExpression::Trim {
            expr,
            trim_what_expr,
            trim_where,
        } => visitor.visit_trim(*expr, *trim_what_expr, trim_where.as_ref(), arena),
        ScalarExpression::Empty => visitor.visit_empty(),
        ScalarExpression::Tuple(exprs) => visitor.visit_tuple(exprs, arena),
        ScalarExpression::ScalaFunction(function) => visitor.visit_scala_function(function, arena),
        ScalarExpression::TableFunction(function) => visitor.visit_table_function(function, arena),
        ScalarExpression::If {
            condition,
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_if(*condition, *left_expr, *right_expr, ty, arena),
        ScalarExpression::IfNull {
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_if_null(*left_expr, *right_expr, ty, arena),
        ScalarExpression::NullIf {
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_null_if(*left_expr, *right_expr, ty, arena),
        ScalarExpression::Coalesce { exprs, ty } => visitor.visit_coalesce(exprs, ty, arena),
        ScalarExpression::CaseWhen {
            operand_expr,
            expr_pairs,
            else_expr,
            ty,
        } => visitor.visit_case_when(*operand_expr, expr_pairs, *else_expr, ty, arena),
        ScalarExpression::WindowCall(window) => visitor.visit_window(window, arena),
    }
}
