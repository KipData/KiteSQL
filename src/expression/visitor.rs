use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::agg::AggKind;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::function::table::TableFunction;
use crate::expression::{AliasType, BinaryOperator, ScalarExpression, UnaryOperator};
use crate::types::evaluator::{BinaryEvaluatorBox, UnaryEvaluatorBox};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use sqlparser::ast::TrimWhereField;

pub trait Visitor<'a>: Sized {
    fn visit(&mut self, expr: &'a ScalarExpression) -> Result<(), DatabaseError> {
        walk_expr(self, expr)
    }

    fn visit_constant(&mut self, _value: &'a DataValue) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_column_ref(&mut self, _column: &'a ColumnRef) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_alias(
        &mut self,
        expr: &'a ScalarExpression,
        _ty: &'a AliasType,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)
    }

    fn visit_type_cast(
        &mut self,
        expr: &'a ScalarExpression,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)
    }

    fn visit_is_null(
        &mut self,
        _negated: bool,
        expr: &'a ScalarExpression,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)
    }

    fn visit_unary(
        &mut self,
        _op: &'a UnaryOperator,
        expr: &'a ScalarExpression,
        _evaluator: Option<&'a UnaryEvaluatorBox>,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)
    }

    fn visit_binary(
        &mut self,
        _op: &'a BinaryOperator,
        left_expr: &'a ScalarExpression,
        right_expr: &'a ScalarExpression,
        _evaluator: Option<&'a BinaryEvaluatorBox>,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr)?;
        self.visit(right_expr)
    }

    fn visit_agg(
        &mut self,
        _distinct: bool,
        _kind: &'a AggKind,
        args: &'a [ScalarExpression],
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        for arg in args {
            self.visit(arg)?;
        }
        Ok(())
    }

    fn visit_in(
        &mut self,
        _negated: bool,
        expr: &'a ScalarExpression,
        args: &'a [ScalarExpression],
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;
        for arg in args {
            self.visit(arg)?;
        }
        Ok(())
    }

    fn visit_between(
        &mut self,
        _negated: bool,
        expr: &'a ScalarExpression,
        left_expr: &'a ScalarExpression,
        right_expr: &'a ScalarExpression,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;
        self.visit(left_expr)?;
        self.visit(right_expr)
    }

    fn visit_substring(
        &mut self,
        expr: &'a ScalarExpression,
        for_expr: Option<&'a ScalarExpression>,
        from_expr: Option<&'a ScalarExpression>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;
        if let Some(for_expr) = for_expr {
            self.visit(for_expr)?;
        }
        if let Some(from_expr) = from_expr {
            self.visit(from_expr)?;
        }
        Ok(())
    }

    fn visit_position(
        &mut self,
        expr: &'a ScalarExpression,
        in_expr: &'a ScalarExpression,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;
        self.visit(in_expr)
    }

    fn visit_trim(
        &mut self,
        expr: &'a ScalarExpression,
        trim_what_expr: Option<&'a ScalarExpression>,
        _trim_where: Option<&'a TrimWhereField>,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)?;
        if let Some(trim_what_expr) = trim_what_expr {
            self.visit(trim_what_expr)?;
        }
        Ok(())
    }

    fn visit_empty(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_reference(
        &mut self,
        expr: &'a ScalarExpression,
        _pos: usize,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)
    }

    fn visit_tuple(&mut self, exprs: &'a [ScalarExpression]) -> Result<(), DatabaseError> {
        for expr in exprs {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_scala_function(
        &mut self,
        scalar_function: &'a ScalarFunction,
    ) -> Result<(), DatabaseError> {
        for arg in &scalar_function.args {
            self.visit(arg)?;
        }
        Ok(())
    }

    fn visit_table_function(
        &mut self,
        table_function: &'a TableFunction,
    ) -> Result<(), DatabaseError> {
        for arg in &table_function.args {
            self.visit(arg)?;
        }
        Ok(())
    }

    fn visit_if(
        &mut self,
        condition: &'a ScalarExpression,
        left_expr: &'a ScalarExpression,
        right_expr: &'a ScalarExpression,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(condition)?;
        self.visit(left_expr)?;
        self.visit(right_expr)
    }

    fn visit_if_null(
        &mut self,
        left_expr: &'a ScalarExpression,
        right_expr: &'a ScalarExpression,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr)?;
        self.visit(right_expr)
    }

    fn visit_null_if(
        &mut self,
        left_expr: &'a ScalarExpression,
        right_expr: &'a ScalarExpression,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        self.visit(left_expr)?;
        self.visit(right_expr)
    }

    fn visit_coalesce(
        &mut self,
        exprs: &'a [ScalarExpression],
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        for expr in exprs {
            self.visit(expr)?;
        }
        Ok(())
    }

    fn visit_case_when(
        &mut self,
        operand_expr: Option<&'a ScalarExpression>,
        expr_pairs: &'a [(ScalarExpression, ScalarExpression)],
        else_expr: Option<&'a ScalarExpression>,
        _ty: &'a LogicalType,
    ) -> Result<(), DatabaseError> {
        if let Some(operand_expr) = operand_expr {
            self.visit(operand_expr)?;
        }
        for (left_expr, right_expr) in expr_pairs {
            self.visit(left_expr)?;
            self.visit(right_expr)?;
        }
        if let Some(else_expr) = else_expr {
            self.visit(else_expr)?;
        }
        Ok(())
    }
}

#[recursive::recursive]
pub fn walk_expr<'a, V: Visitor<'a>>(
    visitor: &mut V,
    expr: &'a ScalarExpression,
) -> Result<(), DatabaseError> {
    match expr {
        ScalarExpression::Constant(value) => visitor.visit_constant(value),
        ScalarExpression::ColumnRef(column_ref) => visitor.visit_column_ref(column_ref),
        ScalarExpression::Alias { expr, alias } => visitor.visit_alias(expr, alias),
        ScalarExpression::TypeCast { expr, ty } => visitor.visit_type_cast(expr, ty),
        ScalarExpression::IsNull { negated, expr } => visitor.visit_is_null(*negated, expr),
        ScalarExpression::Unary {
            op,
            expr,
            evaluator,
            ty,
        } => visitor.visit_unary(op, expr, evaluator.as_ref(), ty),
        ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator,
            ty,
        } => visitor.visit_binary(op, left_expr, right_expr, evaluator.as_ref(), ty),
        ScalarExpression::AggCall {
            distinct,
            kind,
            args,
            ty,
        } => visitor.visit_agg(*distinct, kind, args, ty),
        ScalarExpression::In {
            negated,
            expr,
            args,
        } => visitor.visit_in(*negated, expr, args),
        ScalarExpression::Between {
            negated,
            expr,
            left_expr,
            right_expr,
        } => visitor.visit_between(*negated, expr, left_expr, right_expr),
        ScalarExpression::SubString {
            expr,
            for_expr,
            from_expr,
        } => visitor.visit_substring(expr, for_expr.as_deref(), from_expr.as_deref()),
        ScalarExpression::Position { expr, in_expr } => visitor.visit_position(expr, in_expr),
        ScalarExpression::Trim {
            expr,
            trim_what_expr,
            trim_where,
        } => visitor.visit_trim(expr, trim_what_expr.as_deref(), trim_where.as_ref()),
        ScalarExpression::Empty => visitor.visit_empty(),
        ScalarExpression::Reference { expr, pos } => visitor.visit_reference(expr, *pos),
        ScalarExpression::Tuple(exprs) => visitor.visit_tuple(exprs),
        ScalarExpression::ScalaFunction(scalar_function) => {
            visitor.visit_scala_function(scalar_function)
        }
        ScalarExpression::TableFunction(table_function) => {
            visitor.visit_table_function(table_function)
        }
        ScalarExpression::If {
            condition,
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_if(condition, left_expr, right_expr, ty),
        ScalarExpression::IfNull {
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_if_null(left_expr, right_expr, ty),
        ScalarExpression::NullIf {
            left_expr,
            right_expr,
            ty,
        } => visitor.visit_null_if(left_expr, right_expr, ty),
        ScalarExpression::Coalesce { exprs, ty } => visitor.visit_coalesce(exprs, ty),
        ScalarExpression::CaseWhen {
            operand_expr,
            expr_pairs,
            else_expr,
            ty,
        } => visitor.visit_case_when(
            operand_expr.as_deref(),
            expr_pairs,
            else_expr.as_deref(),
            ty,
        ),
    }
}
