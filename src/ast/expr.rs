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

use super::{Ident, ObjectName, Query, Span};
use crate::expression::{BinaryOperator, TrimWhereField, UnaryOperator};
use crate::types::LogicalType;

/// A literal as written.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Digits as written, so the binder picks the narrowest type that holds them.
    Number(String),
    String(String),
    /// The digits of `X'..'`.
    HexString(String),
    Boolean(bool),
    Null,
    /// A parameter such as `$1`.
    Placeholder(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueWithSpan {
    pub value: Value,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Identifier(Ident),
    /// `a.b`
    CompoundIdentifier(Vec<Ident>),
    Value(ValueWithSpan),
    /// `type 'text'`, such as `DATE '2024-01-01'`.
    TypedString {
        data_type: LogicalType,
        value: String,
        span: Span,
    },
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Nested(Box<Expr>),
    Like {
        negated: bool,
        expr: Box<Expr>,
        pattern: Box<Expr>,
        escape_char: Option<char>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        negated: bool,
        low: Box<Expr>,
        high: Box<Expr>,
    },
    Cast {
        expr: Box<Expr>,
        data_type: LogicalType,
    },
    Substring {
        expr: Box<Expr>,
        from: Option<Box<Expr>>,
        r#for: Option<Box<Expr>>,
    },
    Position {
        expr: Box<Expr>,
        r#in: Box<Expr>,
    },
    Trim {
        expr: Box<Expr>,
        trim_what: Option<Box<Expr>>,
        trim_where: Option<TrimWhereField>,
    },
    Function(Function),
    Case {
        operand: Option<Box<Expr>>,
        /// `WHEN condition THEN result` pairs.
        conditions: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    Tuple(Vec<Expr>),
    Exists {
        subquery: Box<Query>,
        negated: bool,
    },
    Subquery(Box<Query>),
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    /// `left op ANY (subquery)`
    AnyOp {
        left: Box<Expr>,
        compare_op: BinaryOperator,
        subquery: Box<Query>,
    },
    /// `left op ALL (subquery)`
    AllOp {
        left: Box<Expr>,
        compare_op: BinaryOperator,
        subquery: Box<Query>,
    },
}

/// A function call; a call with `over` is a window function.
#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub name: ObjectName,
    pub args: Vec<FunctionArg>,
    pub distinct: bool,
    pub over: Option<WindowSpec>,
    pub span: Span,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArg {
    Expr(Expr),
    /// `*`, as in `count(*)`.
    Wildcard,
}

/// An inline window, `OVER (PARTITION BY .. ORDER BY ..)`.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    /// `None` when neither `ASC` nor `DESC` is written.
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
}

impl Expr {
    /// The source range of this expression, from the spans of the names and literals in it.
    pub fn span(&self) -> Span {
        let union = |exprs: &mut dyn Iterator<Item = &Expr>| {
            exprs.fold(Span::empty(), |span, expr| span.union(expr.span()))
        };
        match self {
            Expr::Identifier(ident) => ident.span,
            Expr::CompoundIdentifier(idents) => idents
                .iter()
                .fold(Span::empty(), |span, ident| span.union(ident.span)),
            Expr::Value(value) => value.span,
            Expr::TypedString { span, .. } => *span,
            Expr::Function(function) => function.span,
            Expr::BinaryOp { left, right, .. } => left.span().union(right.span()),
            Expr::Like { expr, pattern, .. } => expr.span().union(pattern.span()),
            Expr::Between {
                expr, low, high, ..
            } => union(&mut [expr, low, high].into_iter().map(AsRef::as_ref)),
            Expr::InList { expr, list, .. } => expr.span().union(union(&mut list.iter())),
            Expr::Tuple(exprs) => union(&mut exprs.iter()),
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => union(
                &mut operand
                    .iter()
                    .map(AsRef::as_ref)
                    .chain(conditions.iter().flat_map(|(when, then)| [when, then]))
                    .chain(else_result.iter().map(AsRef::as_ref)),
            ),
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::Cast { expr, .. }
            | Expr::Substring { expr, .. }
            | Expr::Position { expr, .. }
            | Expr::Trim { expr, .. }
            | Expr::InSubquery { expr, .. }
            | Expr::AnyOp { left: expr, .. }
            | Expr::AllOp { left: expr, .. } => expr.span(),
            Expr::Exists { .. } | Expr::Subquery(_) => Span::empty(),
        }
    }
}
