use crate::catalog::{ColumnCatalog, ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::expression;
use crate::expression::agg::AggKind;
use itertools::Itertools;
use sqlparser::ast::{
    BinaryOperator, CharLengthUnits, DataType, Expr, Function, FunctionArg, FunctionArgExpr, Ident,
    Query, UnaryOperator, Value,
};
use std::collections::HashMap;
use std::slice;
use std::sync::Arc;

use super::{lower_ident, Binder, BinderContext, QueryBindStep, SubQueryType};
use crate::expression::function::scala::{ArcScalarFunctionImpl, ScalarFunction};
use crate::expression::function::table::{ArcTableFunctionImpl, TableFunction};
use crate::expression::function::FunctionSummary;
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::{LogicalPlan, SchemaOutput};
use crate::storage::Transaction;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{ColumnId, LogicalType};

macro_rules! try_alias {
    ($context:expr, $full_name:expr) => {
        if let Some(expr) = $context.expr_aliases.get(&$full_name) {
            return Ok(ScalarExpression::Alias {
                expr: Box::new(expr.clone()),
                alias: AliasType::Name($full_name.1),
            });
        }
    };
}

macro_rules! try_default {
    ($table_name:expr, $column_name:expr) => {
        if let (None, "default") = ($table_name, $column_name.as_str()) {
            return Ok(ScalarExpression::Empty);
        }
    };
}

impl<'a, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, '_, T, A> {
    pub(crate) fn bind_expr(&mut self, expr: &Expr) -> Result<ScalarExpression, DatabaseError> {
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident), None)
            }
            Expr::CompoundIdentifier(idents) => self.bind_column_ref_from_identifiers(idents, None),
            Expr::BinaryOp { left, right, op } => self.bind_binary_op_internal(left, right, op),
            Expr::Value(v) => {
                let value = if let Value::Placeholder(name) = v {
                    self.args
                        .as_ref()
                        .iter()
                        .find_map(|(key, value)| (key == name).then(|| value.clone()))
                        .ok_or_else(|| DatabaseError::ParametersNotFound(name.to_string()))?
                } else {
                    v.try_into()?
                };
                Ok(ScalarExpression::Constant(value))
            }
            Expr::Function(func) => self.bind_function(func),
            Expr::Nested(expr) => self.bind_expr(expr),
            Expr::UnaryOp { expr, op } => self.bind_unary_op_internal(expr, op),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.bind_like(*negated, expr, pattern, escape_char),
            Expr::IsNull(expr) => self.bind_is_null(expr, false),
            Expr::IsNotNull(expr) => self.bind_is_null(expr, true),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.bind_is_in(expr, list, *negated),
            Expr::Cast {
                expr, data_type, ..
            } => self.bind_cast(expr, data_type),
            Expr::TypedString { data_type, value } => {
                let logical_type = LogicalType::try_from(data_type.clone())?;
                let value = DataValue::Utf8 {
                    value: value.to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
                .cast(&logical_type)?;

                Ok(ScalarExpression::Constant(value))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(ScalarExpression::Between {
                negated: *negated,
                expr: Box::new(self.bind_expr(expr)?),
                left_expr: Box::new(self.bind_expr(low)?),
                right_expr: Box::new(self.bind_expr(high)?),
            }),
            Expr::Substring {
                expr,
                substring_for,
                substring_from,
                ..
            } => {
                let mut for_expr = None;
                let mut from_expr = None;

                if let Some(expr) = substring_for {
                    for_expr = Some(Box::new(self.bind_expr(expr)?))
                }
                if let Some(expr) = substring_from {
                    from_expr = Some(Box::new(self.bind_expr(expr)?))
                }

                Ok(ScalarExpression::SubString {
                    expr: Box::new(self.bind_expr(expr)?),
                    for_expr,
                    from_expr,
                })
            }
            Expr::Position { expr, r#in } => Ok(ScalarExpression::Position {
                expr: Box::new(self.bind_expr(expr)?),
                in_expr: Box::new(self.bind_expr(r#in)?),
            }),
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
            } => {
                let mut trim_what_expr = None;
                if let Some(trim_what) = trim_what {
                    trim_what_expr = Some(Box::new(self.bind_expr(trim_what)?))
                }
                Ok(ScalarExpression::Trim {
                    expr: Box::new(self.bind_expr(expr)?),
                    trim_what_expr,
                    trim_where: *trim_where,
                })
            }
            Expr::Subquery(subquery) => {
                let (sub_query, column) = self.bind_subquery(subquery)?;
                let (expr, sub_query) = if !self.context.is_step(&QueryBindStep::Where) {
                    self.bind_temp_table(column, sub_query)?
                } else {
                    (ScalarExpression::ColumnRef(column), sub_query)
                };
                self.context.sub_query(SubQueryType::SubQuery(sub_query));
                Ok(expr)
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let left_expr = Box::new(self.bind_expr(expr)?);
                let (sub_query, column) = self.bind_subquery(subquery)?;

                if !self.context.is_step(&QueryBindStep::Where) {
                    return Err(DatabaseError::UnsupportedStmt(
                        "'IN (SUBQUERY)' can only appear in `WHERE`".to_string(),
                    ));
                }

                let (alias_expr, sub_query) = self.bind_temp_table(column, sub_query)?;
                self.context
                    .sub_query(SubQueryType::InSubQuery(*negated, sub_query));

                Ok(ScalarExpression::Binary {
                    op: expression::BinaryOperator::Eq,
                    left_expr,
                    right_expr: Box::new(alias_expr),
                    evaluator: None,
                    ty: LogicalType::Boolean,
                })
            }
            Expr::Tuple(exprs) => {
                let mut bond_exprs = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    bond_exprs.push(self.bind_expr(expr)?);
                }
                Ok(ScalarExpression::Tuple(bond_exprs))
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                let fn_check_ty = |ty: &mut LogicalType, result_ty| {
                    if result_ty != LogicalType::SqlNull {
                        if ty == &LogicalType::SqlNull {
                            *ty = result_ty;
                        } else if ty != &result_ty {
                            return Err(DatabaseError::Incomparable(ty.clone(), result_ty));
                        }
                    }

                    Ok(())
                };
                let mut operand_expr = None;
                let mut ty = LogicalType::SqlNull;
                if let Some(expr) = operand {
                    operand_expr = Some(Box::new(self.bind_expr(expr)?));
                }
                let mut expr_pairs = Vec::with_capacity(conditions.len());
                for i in 0..conditions.len() {
                    let result = self.bind_expr(&results[i])?;
                    let result_ty = result.return_type();

                    fn_check_ty(&mut ty, result_ty)?;
                    expr_pairs.push((self.bind_expr(&conditions[i])?, result))
                }

                let mut else_expr = None;
                if let Some(expr) = else_result {
                    let temp_expr = Box::new(self.bind_expr(expr)?);
                    let else_ty = temp_expr.return_type();

                    fn_check_ty(&mut ty, else_ty)?;
                    else_expr = Some(temp_expr);
                }

                Ok(ScalarExpression::CaseWhen {
                    operand_expr,
                    expr_pairs,
                    else_expr,
                    ty,
                })
            }
            expr => Err(DatabaseError::UnsupportedStmt(expr.to_string())),
        }
    }

    fn bind_temp_table(
        &mut self,
        column: ColumnRef,
        sub_query: LogicalPlan,
    ) -> Result<(ScalarExpression, LogicalPlan), DatabaseError> {
        let mut alias_column = ColumnCatalog::clone(&column);
        alias_column.set_ref_table(self.context.temp_table(), ColumnId::new(), true);

        let alias_expr = ScalarExpression::Alias {
            expr: Box::new(ScalarExpression::ColumnRef(column)),
            alias: AliasType::Expr(Box::new(ScalarExpression::ColumnRef(ColumnRef::from(
                alias_column,
            )))),
        };
        let alias_plan = self.bind_project(sub_query, vec![alias_expr.clone()])?;
        Ok((alias_expr, alias_plan))
    }

    fn bind_subquery(
        &mut self,
        subquery: &Query,
    ) -> Result<(LogicalPlan, ColumnRef), DatabaseError> {
        let BinderContext {
            table_cache,
            view_cache,
            transaction,
            scala_functions,
            table_functions,
            temp_table_id,
            ..
        } = &self.context;
        let mut binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                *transaction,
                scala_functions,
                table_functions,
                temp_table_id.clone(),
            ),
            self.args,
            Some(self),
        );
        let mut sub_query = binder.bind_query(subquery)?;
        let sub_query_schema = sub_query.output_schema();

        if sub_query_schema.len() != 1 {
            return Err(DatabaseError::MisMatch(
                "expects only one expression to be returned",
                "the expression returned by the subquery",
            ));
        }
        let column = sub_query_schema[0].clone();
        Ok((sub_query, column))
    }

    pub fn bind_like(
        &mut self,
        negated: bool,
        expr: &Expr,
        pattern: &Expr,
        escape_char: &Option<char>,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = Box::new(self.bind_expr(expr)?);
        let right_expr = Box::new(self.bind_expr(pattern)?);
        let op = if negated {
            expression::BinaryOperator::NotLike(*escape_char)
        } else {
            expression::BinaryOperator::Like(*escape_char)
        };
        Ok(ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator: None,
            ty: LogicalType::Boolean,
        })
    }

    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
        bind_table_name: Option<String>,
    ) -> Result<ScalarExpression, DatabaseError> {
        let full_name = match idents {
            [column] => (None, lower_ident(column)),
            [table, column] => (Some(lower_ident(table)), lower_ident(column)),
            _ => {
                return Err(DatabaseError::InvalidColumn(
                    idents
                        .iter()
                        .map(|ident| ident.value.clone())
                        .join(".")
                        .to_string(),
                ))
            }
        };
        try_alias!(self.context, full_name);
        if self.context.allow_default {
            try_default!(&full_name.0, full_name.1);
        }
        if let Some(table) = full_name.0.or(bind_table_name) {
            let source = self.context.bind_source(&table)?;
            let schema_buf = self.table_schema_buf.entry(Arc::new(table)).or_default();

            Ok(ScalarExpression::ColumnRef(
                source
                    .column(&full_name.1, schema_buf)
                    .ok_or_else(|| DatabaseError::ColumnNotFound(full_name.1.to_string()))?,
            ))
        } else {
            let op =
                |got_column: &mut Option<ScalarExpression>,
                 context: &BinderContext<'a, T>,
                 table_schema_buf: &mut HashMap<TableName, Option<SchemaOutput>>| {
                    for ((table_name, alias, _), source) in context.bind_table.iter() {
                        if got_column.is_some() {
                            break;
                        }
                        if let Some(alias) = alias {
                            *got_column = self.context.expr_aliases.iter().find_map(
                                |((alias_table, alias_column), expr)| {
                                    matches!(
                                        alias_table
                                            .as_ref()
                                            .map(|table_name| table_name == alias.as_ref()
                                                && alias_column == &full_name.1),
                                        Some(true)
                                    )
                                    .then(|| expr.clone())
                                },
                            );
                        } else if let Some(column) = {
                            let schema_buf =
                                table_schema_buf.entry(table_name.clone()).or_default();
                            source.column(&full_name.1, schema_buf)
                        } {
                            *got_column = Some(ScalarExpression::ColumnRef(column));
                        }
                    }
                };
            // handle col syntax
            let mut got_column = None;

            op(&mut got_column, &self.context, &mut self.table_schema_buf);
            if let Some(parent) = self.parent {
                op(&mut got_column, &parent.context, &mut self.table_schema_buf);
            }
            Ok(got_column.ok_or(DatabaseError::ColumnNotFound(full_name.1))?)
        }
    }

    fn bind_binary_op_internal(
        &mut self,
        left: &Expr,
        right: &Expr,
        op: &BinaryOperator,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = Box::new(self.bind_expr(left)?);
        let right_expr = Box::new(self.bind_expr(right)?);

        let ty = match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Modulo => {
                LogicalType::max_logical_type(&left_expr.return_type(), &right_expr.return_type())?
            }
            BinaryOperator::Divide => {
                if let LogicalType::Decimal(precision, scale) = LogicalType::max_logical_type(
                    &left_expr.return_type(),
                    &right_expr.return_type(),
                )? {
                    LogicalType::Decimal(precision, scale)
                } else {
                    LogicalType::Double
                }
            }
            BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Xor => LogicalType::Boolean,
            BinaryOperator::StringConcat => LogicalType::Varchar(None, CharLengthUnits::Characters),
            op => return Err(DatabaseError::UnsupportedStmt(format!("{}", op))),
        };

        Ok(ScalarExpression::Binary {
            op: (op.clone()).try_into()?,
            left_expr,
            right_expr,
            evaluator: None,
            ty,
        })
    }

    fn bind_unary_op_internal(
        &mut self,
        expr: &Expr,
        op: &UnaryOperator,
    ) -> Result<ScalarExpression, DatabaseError> {
        let expr = Box::new(self.bind_expr(expr)?);
        let ty = if let UnaryOperator::Not = op {
            LogicalType::Boolean
        } else {
            expr.return_type()
        };

        Ok(ScalarExpression::Unary {
            op: (*op).try_into()?,
            expr,
            evaluator: None,
            ty,
        })
    }

    fn bind_function(&mut self, func: &Function) -> Result<ScalarExpression, DatabaseError> {
        if !matches!(self.context.step_now(), QueryBindStep::From) {
            return Err(DatabaseError::UnsupportedStmt(
                "`TableFunction` cannot bind in non-From step".to_string(),
            ));
        }
        let mut args = Vec::with_capacity(func.args.len());

        for arg in func.args.iter() {
            let arg_expr = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg_expr {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr)?),
                FunctionArgExpr::Wildcard => args.push(Self::wildcard_expr()),
                expr => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "function arg: {:#?}",
                        expr
                    )))
                }
            }
        }
        let function_name = func.name.to_string().to_lowercase();

        match function_name.as_str() {
            "count" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of count() parameters", "1"));
                }
                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Count,
                    args,
                    ty: LogicalType::Integer,
                });
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of sum() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Sum,
                    args,
                    ty,
                });
            }
            "min" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of min() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Min,
                    args,
                    ty,
                });
            }
            "max" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of max() parameters", "1"));
                }
                let ty = args[0].return_type();

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Max,
                    args,
                    ty,
                });
            }
            "avg" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of avg() parameters", "1"));
                }

                return Ok(ScalarExpression::AggCall {
                    distinct: func.distinct,
                    kind: AggKind::Avg,
                    args,
                    ty: LogicalType::Double,
                });
            }
            "if" => {
                if args.len() != 3 {
                    return Err(DatabaseError::MisMatch("number of if() parameters", "3"));
                }
                let ty = Self::return_type(&args[1], &args[2])?;
                let right_expr = Box::new(args.pop().unwrap());
                let left_expr = Box::new(args.pop().unwrap());
                let condition = Box::new(args.pop().unwrap());

                return Ok(ScalarExpression::If {
                    condition,
                    left_expr,
                    right_expr,
                    ty,
                });
            }
            "nullif" => {
                if args.len() != 2 {
                    return Err(DatabaseError::MisMatch(
                        "number of nullif() parameters",
                        "3",
                    ));
                }
                let ty = Self::return_type(&args[0], &args[1])?;
                let right_expr = Box::new(args.pop().unwrap());
                let left_expr = Box::new(args.pop().unwrap());

                return Ok(ScalarExpression::NullIf {
                    left_expr,
                    right_expr,
                    ty,
                });
            }
            "ifnull" => {
                if args.len() != 2 {
                    return Err(DatabaseError::MisMatch(
                        "number of ifnull() parameters",
                        "3",
                    ));
                }
                let ty = Self::return_type(&args[0], &args[1])?;
                let right_expr = Box::new(args.pop().unwrap());
                let left_expr = Box::new(args.pop().unwrap());

                return Ok(ScalarExpression::IfNull {
                    left_expr,
                    right_expr,
                    ty,
                });
            }
            "coalesce" => {
                let mut ty = LogicalType::SqlNull;

                if !args.is_empty() {
                    ty = args[0].return_type();

                    for arg in args.iter_mut() {
                        let temp_ty = arg.return_type();

                        if temp_ty == LogicalType::SqlNull {
                            continue;
                        }
                        if ty == LogicalType::SqlNull && temp_ty != LogicalType::SqlNull {
                            ty = temp_ty;
                        } else if ty != temp_ty {
                            ty = LogicalType::max_logical_type(&ty, &temp_ty)?;
                        }
                    }
                }
                return Ok(ScalarExpression::Coalesce { exprs: args, ty });
            }
            _ => (),
        }
        let arg_types = args.iter().map(ScalarExpression::return_type).collect_vec();
        let summary = FunctionSummary {
            name: function_name,
            arg_types,
        };
        if let Some(function) = self.context.scala_functions.get(&summary) {
            return Ok(ScalarExpression::ScalaFunction(ScalarFunction {
                args,
                inner: ArcScalarFunctionImpl(function.clone()),
            }));
        }
        if let Some(function) = self.context.table_functions.get(&summary) {
            return Ok(ScalarExpression::TableFunction(TableFunction {
                args,
                inner: ArcTableFunctionImpl(function.clone()),
            }));
        }

        Err(DatabaseError::FunctionNotFound(summary.name))
    }

    fn return_type(
        expr_1: &ScalarExpression,
        expr_2: &ScalarExpression,
    ) -> Result<LogicalType, DatabaseError> {
        let temp_ty_1 = expr_1.return_type();
        let temp_ty_2 = expr_2.return_type();

        match (temp_ty_1, temp_ty_2) {
            (LogicalType::SqlNull, LogicalType::SqlNull) => Ok(LogicalType::SqlNull),
            (ty, LogicalType::SqlNull) | (LogicalType::SqlNull, ty) => Ok(ty),
            (ty_1, ty_2) => LogicalType::max_logical_type(&ty_1, &ty_2),
        }
    }

    fn bind_is_null(
        &mut self,
        expr: &Expr,
        negated: bool,
    ) -> Result<ScalarExpression, DatabaseError> {
        Ok(ScalarExpression::IsNull {
            negated,
            expr: Box::new(self.bind_expr(expr)?),
        })
    }

    fn bind_is_in(
        &mut self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
    ) -> Result<ScalarExpression, DatabaseError> {
        let args = list.iter().map(|expr| self.bind_expr(expr)).try_collect()?;

        Ok(ScalarExpression::In {
            negated,
            expr: Box::new(self.bind_expr(expr)?),
            args,
        })
    }

    fn bind_cast(&mut self, expr: &Expr, ty: &DataType) -> Result<ScalarExpression, DatabaseError> {
        Ok(ScalarExpression::TypeCast {
            expr: Box::new(self.bind_expr(expr)?),
            ty: LogicalType::try_from(ty.clone())?,
        })
    }

    fn wildcard_expr() -> ScalarExpression {
        ScalarExpression::Constant(DataValue::Utf8 {
            value: "*".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        })
    }
}
