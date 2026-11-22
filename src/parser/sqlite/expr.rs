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

use super::query::select;
use super::{ident, name, types, unquote, unsupported};
use crate::ast;
use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator as BinaryOp, TrimWhereField, UnaryOperator as UnaryOp};
use sqlite3_parser::ast::{
    Distinctness, Expr, FunctionTail, LikeOperator, Literal, NullsOrder, Operator, Over, SortOrder,
    SortedColumn, UnaryOperator,
};

fn boxed(expr: &Expr<'_>) -> Result<Box<ast::Expr>, DatabaseError> {
    self::expr(expr).map(Box::new)
}

pub(super) fn exprs(exprs: &[Expr<'_>]) -> Result<Vec<ast::Expr>, DatabaseError> {
    exprs.iter().map(expr).collect()
}

fn value(value: ast::Value) -> ast::Expr {
    ast::Expr::Value(ast::ValueWithSpan {
        value,
        span: ast::Span::empty(),
    })
}

pub(super) fn order_by(column: &SortedColumn<'_>) -> Result<ast::OrderByExpr, DatabaseError> {
    Ok(ast::OrderByExpr {
        expr: expr(&column.expr)?,
        asc: column.order.map(|order| order == SortOrder::Asc),
        nulls_first: column.nulls.map(|nulls| nulls == NullsOrder::First),
    })
}

pub(super) fn expr(expr: &Expr<'_>) -> Result<ast::Expr, DatabaseError> {
    Ok(match expr {
        Expr::Id(id) => identifier(id.0),
        Expr::Name(name) => ast::Expr::Identifier(self::name(name)),
        Expr::Qualified(table, column) => {
            ast::Expr::CompoundIdentifier(vec![name(table), name(column)])
        }
        Expr::DoublyQualified(schema, table, column) => {
            ast::Expr::CompoundIdentifier(vec![name(schema), name(table), name(column)])
        }
        Expr::Literal(literal) => self::literal(literal)?,
        Expr::Variable(variable) => value(ast::Value::Placeholder(placeholder(variable))),
        Expr::Binary(left, op, right) => binary(left, *op, right)?,
        Expr::Unary(UnaryOperator::Not, Expr::Exists(subquery)) => ast::Expr::Exists {
            subquery: select(subquery).map(Box::new)?,
            negated: true,
        },
        Expr::Unary(op, expr) => ast::Expr::UnaryOp {
            op: match op {
                UnaryOperator::Negative => UnaryOp::Minus,
                UnaryOperator::Positive => UnaryOp::Plus,
                UnaryOperator::Not => UnaryOp::Not,
                UnaryOperator::BitwiseNot => return Err(unsupported("~")),
            },
            expr: boxed(expr)?,
        },
        Expr::Parenthesized(exprs) => match exprs.as_slice() {
            [expr] => ast::Expr::Nested(boxed(expr)?),
            exprs => ast::Expr::Tuple(self::exprs(exprs)?),
        },
        Expr::Between {
            lhs,
            not,
            start,
            end,
        } => ast::Expr::Between {
            expr: boxed(lhs)?,
            negated: *not,
            low: boxed(start)?,
            high: boxed(end)?,
        },
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => ast::Expr::Case {
            operand: base.map(boxed).transpose()?,
            conditions: when_then_pairs
                .iter()
                .map(|(when, then)| Ok((self::expr(when)?, self::expr(then)?)))
                .collect::<Result<_, DatabaseError>>()?,
            else_result: else_expr.map(boxed).transpose()?,
        },
        Expr::Cast { expr, type_name } => ast::Expr::Cast {
            expr: boxed(expr)?,
            data_type: match type_name {
                Some(ty) => types::logical_type(ty)?,
                None => return Err(unsupported("CAST without a type")),
            },
        },
        Expr::Exists(subquery) => ast::Expr::Exists {
            subquery: select(subquery).map(Box::new)?,
            negated: false,
        },
        Expr::Subquery(subquery) => ast::Expr::Subquery(select(subquery).map(Box::new)?),
        Expr::InList { lhs, not, rhs } => ast::Expr::InList {
            expr: boxed(lhs)?,
            list: exprs(rhs.unwrap_or_default())?,
            negated: *not,
        },
        Expr::InSelect { lhs, not, rhs } => ast::Expr::InSubquery {
            expr: boxed(lhs)?,
            subquery: select(rhs).map(Box::new)?,
            negated: *not,
        },
        Expr::IsNull(expr) => ast::Expr::IsNull(boxed(expr)?),
        Expr::NotNull(expr) => ast::Expr::IsNotNull(boxed(expr)?),
        Expr::Like {
            lhs,
            not,
            op: LikeOperator::Like,
            rhs,
            escape,
        } => ast::Expr::Like {
            negated: *not,
            expr: boxed(lhs)?,
            pattern: boxed(rhs)?,
            escape_char: escape.map(like_escape_char).transpose()?,
        },
        Expr::FunctionCall {
            name,
            distinctness,
            args,
            order_by,
            filter_over,
        } => {
            if order_by.is_some() {
                return Err(unsupported(
                    "ORDER BY in function arguments is not supported",
                ));
            }
            function(
                name.0,
                args.unwrap_or_default(),
                matches!(distinctness, Some(Distinctness::Distinct)),
                filter_over.as_ref(),
            )?
        }
        Expr::FunctionCallStar { name, filter_over } => ast::Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ident(name.0)]),
            args: vec![ast::FunctionArg::Wildcard],
            distinct: false,
            over: over(filter_over.as_ref())?,
            span: ast::Span::empty(),
        }),
        expr => return Err(unsupported(expr.to_string())),
    })
}

/// A bare `true` or `false` is a boolean, as in SQLite.
fn identifier(id: &str) -> ast::Expr {
    match id.to_ascii_lowercase().as_str() {
        "true" => value(ast::Value::Boolean(true)),
        "false" => value(ast::Value::Boolean(false)),
        _ => ast::Expr::Identifier(ident(id)),
    }
}

fn literal(literal: &Literal<'_>) -> Result<ast::Expr, DatabaseError> {
    let current = |function: &str| {
        ast::Expr::Function(ast::Function {
            name: ast::ObjectName(vec![ident(function)]),
            args: Vec::new(),
            distinct: false,
            over: None,
            span: ast::Span::empty(),
        })
    };
    Ok(match literal {
        Literal::Numeric(number) => value(ast::Value::Number(number.to_string())),
        Literal::String(text) => value(ast::Value::String(unquote(text).0)),
        Literal::Blob(hex) => value(ast::Value::HexString(hex.to_string())),
        Literal::Null => value(ast::Value::Null),
        Literal::CurrentDate => current("current_date"),
        Literal::CurrentTime => current("current_time"),
        Literal::CurrentTimestamp => current("current_timestamp"),
        Literal::Keyword(keyword) => return Err(unsupported(keyword.to_string())),
    })
}

/// `?N` and `$N` are the positional parameter `$N`.
fn placeholder(variable: &str) -> String {
    let digits = variable.strip_prefix('$').unwrap_or(variable);
    if !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit()) {
        format!("${digits}")
    } else {
        variable.to_string()
    }
}

fn binary(left: &Expr<'_>, op: Operator, right: &Expr<'_>) -> Result<ast::Expr, DatabaseError> {
    let op = match op {
        Operator::Is | Operator::IsNot => {
            let Expr::Literal(Literal::Null) = right else {
                return Err(unsupported("IS is only supported with NULL"));
            };
            return Ok(if op == Operator::Is {
                ast::Expr::IsNull(boxed(left)?)
            } else {
                ast::Expr::IsNotNull(boxed(left)?)
            });
        }
        Operator::Add => BinaryOp::Plus,
        Operator::Subtract => BinaryOp::Minus,
        Operator::Multiply => BinaryOp::Multiply,
        Operator::Divide => BinaryOp::Divide,
        Operator::Modulus => BinaryOp::Modulo,
        Operator::Concat => BinaryOp::StringConcat,
        Operator::Equals => BinaryOp::Eq,
        Operator::NotEquals => BinaryOp::NotEq,
        Operator::Greater => BinaryOp::Gt,
        Operator::GreaterEquals => BinaryOp::GtEq,
        Operator::Less => BinaryOp::Lt,
        Operator::LessEquals => BinaryOp::LtEq,
        Operator::And => BinaryOp::And,
        Operator::Or => BinaryOp::Or,
        op => return Err(unsupported(format!("{op:?}"))),
    };
    Ok(ast::Expr::BinaryOp {
        left: boxed(left)?,
        op,
        right: boxed(right)?,
    })
}

fn like_escape_char(escape: &Expr<'_>) -> Result<char, DatabaseError> {
    let Expr::Literal(Literal::String(text)) = escape else {
        return Err(DatabaseError::InvalidValue(
            "escape character must be a quoted string".to_string(),
        ));
    };
    let text = unquote(text).0;
    let mut chars = text.chars();
    let escape = chars.next().ok_or_else(|| {
        DatabaseError::InvalidValue("escape character must not be empty".to_string())
    })?;
    if chars.next().is_some() {
        return Err(DatabaseError::InvalidValue(
            "escape character must be a single character".to_string(),
        ));
    }
    Ok(escape)
}

/// A call, with SQLite's `substr` and `trim` family read as KiteSQL's `SUBSTRING` and `TRIM`.
pub(super) fn function(
    function_name: &str,
    args: &[Expr<'_>],
    distinct: bool,
    tail: Option<&FunctionTail<'_>>,
) -> Result<ast::Expr, DatabaseError> {
    let plain = !distinct && tail.is_none();
    let trim_where = match function_name.to_ascii_lowercase().as_str() {
        "substr" | "substring" if plain && matches!(args.len(), 2 | 3) => {
            return Ok(ast::Expr::Substring {
                expr: boxed(&args[0])?,
                from: Some(boxed(&args[1])?),
                r#for: args.get(2).map(boxed).transpose()?,
            })
        }
        "trim" => Some(TrimWhereField::Both),
        "ltrim" => Some(TrimWhereField::Leading),
        "rtrim" => Some(TrimWhereField::Trailing),
        _ => None,
    };
    if let (Some(trim_where), true, 1 | 2) = (trim_where, plain, args.len()) {
        return Ok(ast::Expr::Trim {
            expr: boxed(&args[0])?,
            trim_what: args.get(1).map(boxed).transpose()?,
            trim_where: Some(trim_where),
        });
    }

    Ok(ast::Expr::Function(ast::Function {
        name: ast::ObjectName(vec![ident(function_name)]),
        args: args
            .iter()
            .map(|arg| expr(arg).map(ast::FunctionArg::Expr))
            .collect::<Result<_, _>>()?,
        distinct,
        over: over(tail)?,
        span: ast::Span::empty(),
    }))
}

fn over(tail: Option<&FunctionTail<'_>>) -> Result<Option<ast::WindowSpec>, DatabaseError> {
    let Some(tail) = tail else {
        return Ok(None);
    };
    if tail.filter_clause.is_some() {
        return Err(unsupported("FILTER is not supported"));
    }
    let window = match tail.over_clause {
        None => return Ok(None),
        Some(Over::Name(_)) => return Err(unsupported("named windows are not supported")),
        Some(Over::Window(window)) => window,
    };
    if window.base.is_some() {
        return Err(unsupported("inherited named windows are not supported"));
    }
    if window.frame_clause.is_some() {
        return Err(unsupported("explicit window frames are not supported"));
    }
    Ok(Some(ast::WindowSpec {
        partition_by: exprs(window.partition_by.unwrap_or_default())?,
        order_by: window
            .order_by
            .unwrap_or_default()
            .iter()
            .map(order_by)
            .collect::<Result<_, _>>()?,
    }))
}
