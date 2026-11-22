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

use super::{span, unsupported, Lower};
use crate::ast;
use crate::errors::DatabaseError;
use crate::expression::{
    BinaryOperator as BinaryOp, TrimWhereField as TrimWhere, UnaryOperator as UnaryOp,
};
use sqlparser::ast::{
    BinaryOperator, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, OrderByExpr, Spanned, TrimWhereField,
    UnaryOperator, Value, ValueWithSpan, WindowType,
};

impl Lower<'_> {
    pub(super) fn ident(&self, ident: Ident) -> ast::Ident {
        ast::Ident {
            value: ident.value,
            quote_style: ident.quote_style,
            span: span(ident.span),
        }
    }

    pub(super) fn idents(&self, idents: Vec<Ident>) -> Vec<ast::Ident> {
        idents.into_iter().map(|ident| self.ident(ident)).collect()
    }

    pub(super) fn object_name(&self, name: ObjectName) -> Result<ast::ObjectName, DatabaseError> {
        name.0
            .into_iter()
            .map(|part| match part {
                ObjectNamePart::Identifier(ident) => Ok(self.ident(ident)),
                part => Err(ast::attach_span_if_absent(
                    DatabaseError::invalid_table(part.to_string()),
                    span(part.span()),
                )),
            })
            .collect::<Result<_, _>>()
            .map(ast::ObjectName)
    }

    pub(super) fn boxed(&self, expr: Box<Expr>) -> Result<Box<ast::Expr>, DatabaseError> {
        self.expr(*expr).map(Box::new)
    }

    fn exprs(&self, exprs: Vec<Expr>) -> Result<Vec<ast::Expr>, DatabaseError> {
        exprs.into_iter().map(|expr| self.expr(expr)).collect()
    }

    pub(super) fn order_by(
        &self,
        order_by: OrderByExpr,
    ) -> Result<ast::OrderByExpr, DatabaseError> {
        Ok(ast::OrderByExpr {
            expr: self.expr(order_by.expr)?,
            asc: order_by.options.asc,
            nulls_first: order_by.options.nulls_first,
        })
    }

    pub(super) fn value(&self, value: ValueWithSpan) -> Result<ast::ValueWithSpan, DatabaseError> {
        let value_span = span(value.span);
        let value = match value.value {
            Value::Number(number, _) => ast::Value::Number(number),
            Value::SingleQuotedString(text) | Value::DoubleQuotedString(text) => {
                ast::Value::String(text)
            }
            Value::HexStringLiteral(hex) => ast::Value::HexString(hex),
            Value::Boolean(value) => ast::Value::Boolean(value),
            Value::Null => ast::Value::Null,
            Value::Placeholder(placeholder) => ast::Value::Placeholder(placeholder),
            value => {
                return Err(ast::attach_span_if_absent(
                    unsupported(format!("{value:?}")),
                    value_span,
                ))
            }
        };
        Ok(ast::ValueWithSpan {
            value,
            span: value_span,
        })
    }

    pub(super) fn expr(&self, expr: Expr) -> Result<ast::Expr, DatabaseError> {
        Ok(match expr {
            Expr::Identifier(ident) => ast::Expr::Identifier(self.ident(ident)),
            Expr::CompoundIdentifier(idents) => ast::Expr::CompoundIdentifier(self.idents(idents)),
            Expr::Value(value) => ast::Expr::Value(self.value(value)?),
            Expr::TypedString(typed) => {
                let value_span = span(typed.value.span);
                let data_type = self.data_type(typed.data_type)?;
                let value = typed.value.into_string().ok_or_else(|| {
                    DatabaseError::InvalidValue("typed string literal must be a string".to_string())
                })?;
                ast::Expr::TypedString {
                    data_type,
                    value,
                    span: value_span,
                }
            }
            Expr::BinaryOp { left, op, right } => ast::Expr::BinaryOp {
                left: self.boxed(left)?,
                op: binary_operator(op)?,
                right: self.boxed(right)?,
            },
            Expr::UnaryOp { op, expr } => ast::Expr::UnaryOp {
                op: unary_operator(op)?,
                expr: self.boxed(expr)?,
            },
            Expr::Nested(expr) => ast::Expr::Nested(self.boxed(expr)?),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any: _,
            } => ast::Expr::Like {
                negated,
                expr: self.boxed(expr)?,
                pattern: self.boxed(pattern)?,
                escape_char: escape_char.map(like_escape_char).transpose()?,
            },
            Expr::IsNull(expr) => ast::Expr::IsNull(self.boxed(expr)?),
            Expr::IsNotNull(expr) => ast::Expr::IsNotNull(self.boxed(expr)?),
            Expr::InList {
                expr,
                list,
                negated,
            } => ast::Expr::InList {
                expr: self.boxed(expr)?,
                list: self.exprs(list)?,
                negated,
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => ast::Expr::Between {
                expr: self.boxed(expr)?,
                negated,
                low: self.boxed(low)?,
                high: self.boxed(high)?,
            },
            Expr::Cast {
                expr, data_type, ..
            } => ast::Expr::Cast {
                expr: self.boxed(expr)?,
                data_type: self.data_type(data_type)?,
            },
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => ast::Expr::Substring {
                expr: self.boxed(expr)?,
                from: substring_from.map(|expr| self.boxed(expr)).transpose()?,
                r#for: substring_for.map(|expr| self.boxed(expr)).transpose()?,
            },
            Expr::Position { expr, r#in } => ast::Expr::Position {
                expr: self.boxed(expr)?,
                r#in: self.boxed(r#in)?,
            },
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
                ..
            } => ast::Expr::Trim {
                expr: self.boxed(expr)?,
                trim_what: trim_what.map(|expr| self.boxed(expr)).transpose()?,
                trim_where: trim_where.map(|field| match field {
                    TrimWhereField::Both => TrimWhere::Both,
                    TrimWhereField::Leading => TrimWhere::Leading,
                    TrimWhereField::Trailing => TrimWhere::Trailing,
                }),
            },
            Expr::Function(function) => ast::Expr::Function(self.function(function)?),
            Expr::Tuple(exprs) => ast::Expr::Tuple(self.exprs(exprs)?),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => ast::Expr::Case {
                operand: operand.map(|expr| self.boxed(expr)).transpose()?,
                conditions: conditions
                    .into_iter()
                    .map(|when| Ok((self.expr(when.condition)?, self.expr(when.result)?)))
                    .collect::<Result<_, DatabaseError>>()?,
                else_result: else_result.map(|expr| self.boxed(expr)).transpose()?,
            },
            Expr::Exists { subquery, negated } => ast::Expr::Exists {
                subquery: self.query(*subquery).map(Box::new)?,
                negated,
            },
            Expr::Subquery(subquery) => ast::Expr::Subquery(self.query(*subquery).map(Box::new)?),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => ast::Expr::InSubquery {
                expr: self.boxed(expr)?,
                subquery: self.query(*subquery).map(Box::new)?,
                negated,
            },
            Expr::AnyOp {
                left,
                compare_op,
                right,
                ..
            } => ast::Expr::AnyOp {
                left: self.boxed(left)?,
                compare_op: binary_operator(compare_op)?,
                subquery: self.quantified_subquery("Any", *right)?,
            },
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => ast::Expr::AllOp {
                left: self.boxed(left)?,
                compare_op: binary_operator(compare_op)?,
                subquery: self.quantified_subquery("All", *right)?,
            },
            expr => return Err(unsupported(expr.to_string())),
        })
    }

    fn quantified_subquery(
        &self,
        quantifier: &str,
        right: Expr,
    ) -> Result<Box<ast::Query>, DatabaseError> {
        let Expr::Subquery(subquery) = right else {
            return Err(unsupported(format!(
                "{quantifier} only supports subquery operands"
            )));
        };
        self.query(*subquery).map(Box::new)
    }

    pub(super) fn function(&self, function: Function) -> Result<ast::Function, DatabaseError> {
        let function_span = span(function.span());
        let Function {
            name,
            args,
            over,
            filter,
            null_treatment,
            within_group,
            ..
        } = function;
        let (args, distinct) = match args {
            FunctionArguments::List(list) => (
                list.args,
                matches!(list.duplicate_treatment, Some(DuplicateTreatment::Distinct)),
            ),
            FunctionArguments::None => (Vec::new(), false),
            FunctionArguments::Subquery(_) => {
                return Err(unsupported("subquery function args are not supported"))
            }
        };
        let args = args
            .into_iter()
            .map(|arg| match arg {
                FunctionArg::Named { arg, .. }
                | FunctionArg::ExprNamed { arg, .. }
                | FunctionArg::Unnamed(arg) => match arg {
                    FunctionArgExpr::Expr(expr) => self.expr(expr).map(ast::FunctionArg::Expr),
                    FunctionArgExpr::Wildcard => Ok(ast::FunctionArg::Wildcard),
                    expr => Err(unsupported(format!("function arg: {expr:#?}"))),
                },
            })
            .collect::<Result<_, _>>()?;
        let over = match over {
            None => None,
            Some(_) if filter.is_some() || null_treatment.is_some() || !within_group.is_empty() => {
                return Err(unsupported(
                    "FILTER, NULL treatment, and WITHIN GROUP are not supported for window functions",
                ))
            }
            Some(over) => Some(self.window(over)?),
        };
        Ok(ast::Function {
            name: self.object_name(name)?,
            args,
            distinct,
            over,
            span: function_span,
        })
    }

    fn window(&self, over: WindowType) -> Result<ast::WindowSpec, DatabaseError> {
        let WindowType::WindowSpec(spec) = over else {
            return Err(unsupported("named windows are not supported"));
        };
        if spec.window_name.is_some() {
            return Err(unsupported("inherited named windows are not supported"));
        }
        if spec.window_frame.is_some() {
            return Err(unsupported("explicit window frames are not supported"));
        }
        Ok(ast::WindowSpec {
            partition_by: self.exprs(spec.partition_by)?,
            order_by: spec
                .order_by
                .into_iter()
                .map(|order_by| self.order_by(order_by))
                .collect::<Result<_, _>>()?,
        })
    }
}

fn binary_operator(op: BinaryOperator) -> Result<BinaryOp, DatabaseError> {
    Ok(match op {
        BinaryOperator::Plus => BinaryOp::Plus,
        BinaryOperator::Minus => BinaryOp::Minus,
        BinaryOperator::Multiply => BinaryOp::Multiply,
        BinaryOperator::Divide => BinaryOp::Divide,
        BinaryOperator::Modulo => BinaryOp::Modulo,
        BinaryOperator::StringConcat => BinaryOp::StringConcat,
        BinaryOperator::Gt => BinaryOp::Gt,
        BinaryOperator::Lt => BinaryOp::Lt,
        BinaryOperator::GtEq => BinaryOp::GtEq,
        BinaryOperator::LtEq => BinaryOp::LtEq,
        BinaryOperator::Spaceship => BinaryOp::Spaceship,
        BinaryOperator::Eq => BinaryOp::Eq,
        BinaryOperator::NotEq => BinaryOp::NotEq,
        BinaryOperator::And => BinaryOp::And,
        BinaryOperator::Or => BinaryOp::Or,
        op => return Err(unsupported(op.to_string())),
    })
}

fn unary_operator(op: UnaryOperator) -> Result<UnaryOp, DatabaseError> {
    Ok(match op {
        UnaryOperator::Plus => UnaryOp::Plus,
        UnaryOperator::Minus => UnaryOp::Minus,
        UnaryOperator::Not => UnaryOp::Not,
        op => return Err(unsupported(op.to_string())),
    })
}

fn like_escape_char(value: Value) -> Result<char, DatabaseError> {
    let (Value::SingleQuotedString(text) | Value::DoubleQuotedString(text)) = value else {
        return Err(DatabaseError::InvalidValue(
            "escape character must be a quoted string".to_string(),
        ));
    };
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
