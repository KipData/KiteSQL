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
use crate::expression;
use crate::expression::agg::AggKind;
use itertools::Itertools;

use super::{Binder, BinderContext, QueryBindStep, SubQueryType};
use crate::expression::function::scala::{ArcScalarFunctionImpl, ScalarFunction};
use crate::expression::function::table::TableFunction;
use crate::expression::function::FunctionSummary;
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::operator::mark_apply::MarkApplyQuantifier;
use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
use crate::planner::{LogicalPlan, PlanArena};
use crate::storage::Transaction;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{CharLengthUnits, ColumnId, LogicalType};

macro_rules! try_default {
    ($table_name:expr, $column_name:expr) => {
        if let (None, "default") = ($table_name, $column_name.as_ref()) {
            return Ok(ScalarExpression::Empty);
        }
    };
}

impl<'a, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, '_, T, A> {
    fn find_column_in_schema<'schema>(
        schema_ref: impl IntoIterator<Item = &'schema ColumnRef>,
        arena: &PlanArena,
        column_name: &str,
    ) -> Option<(usize, ColumnRef)> {
        schema_ref
            .into_iter()
            .enumerate()
            .find(|(_, column)| arena.column(**column).name() == column_name)
            .map(|(position, column)| (position, *column))
    }

    fn find_column_in_scope(
        context: &BinderContext<'a, T>,
        arena: &mut PlanArena,
        column_name: &str,
    ) -> Option<ScalarExpression> {
        let mut position_offset = 0;

        for bound_source in &context.bind_table {
            let source = &bound_source.source;

            if let Some((position, column)) =
                Self::find_column_in_schema(source.schema().iter(), arena, column_name)
            {
                return Some(ScalarExpression::column_expr(
                    column,
                    position_offset + position,
                ));
            }

            position_offset += source.schema_len();
        }

        None
    }
    pub(crate) fn bind_temp_table(
        &mut self,
        expr: ScalarExpression,
        sub_query: LogicalPlan,
        arena: &mut PlanArena,
    ) -> Result<(ScalarExpression, LogicalPlan), DatabaseError> {
        let (exprs, is_tuple) = match expr {
            ScalarExpression::Tuple(exprs) => (exprs, true),
            expr => (vec![expr], false),
        };
        let mut alias_exprs = Vec::with_capacity(exprs.len());
        let mut alias_refs = Vec::with_capacity(exprs.len());

        for (position, expr) in exprs.into_iter().enumerate() {
            let (alias_expr, alias_ref) = self.bind_temp_table_alias(expr, position, arena);
            if !is_tuple {
                let alias_plan = Self::build_project_plan(sub_query, vec![alias_expr.clone()]);
                return Ok((alias_expr, alias_plan));
            }
            alias_exprs.push(alias_expr);
            alias_refs.push(alias_ref);
        }

        let alias_plan = Self::build_project_plan(sub_query, alias_exprs);
        Ok((ScalarExpression::Tuple(alias_refs), alias_plan))
    }

    pub(crate) fn bind_temp_table_alias(
        &mut self,
        expr: ScalarExpression,
        position: usize,
        arena: &mut PlanArena,
    ) -> (ScalarExpression, ScalarExpression) {
        let output_column = expr.output_column_ref(arena);
        let mut alias_column = arena.clone_column(output_column);
        alias_column.set_ref_table(arena.temp_table(), ColumnId::new(), true);

        let alias_column = arena.alloc_column(alias_column);
        let alias_ref = ScalarExpression::column_expr(alias_column, position);
        (
            ScalarExpression::Alias {
                expr: Box::new(expr),
                alias: AliasType::Expr(Box::new(alias_ref.clone())),
            },
            alias_ref,
        )
    }

    pub(crate) fn bind_subquery_plan<'arena, F>(
        &mut self,
        arena: &mut PlanArena<'arena>,
        build: F,
    ) -> Result<(LogicalPlan, bool), DatabaseError>
    where
        F: FnOnce(
            &mut Binder<'a, '_, T, A>,
            &mut PlanArena<'arena>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let BinderContext {
            table_cache,
            view_cache,
            transaction,
            scala_functions,
            table_functions,
            ..
        } = &self.context;
        let mut binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                *transaction,
                scala_functions,
                table_functions,
            ),
            self.args,
            Some(&self.context),
        );
        let sub_query = build(&mut binder, arena)?;
        let correlated = binder.context.has_outer_refs();
        Ok((sub_query, correlated))
    }

    pub(crate) fn bind_subquery_plan_with_output<'arena, F>(
        &mut self,
        value_ty: Option<&LogicalType>,
        arena: &mut PlanArena<'arena>,
        build: F,
    ) -> Result<(LogicalPlan, ScalarExpression, bool), DatabaseError>
    where
        F: FnOnce(
            &mut Binder<'a, '_, T, A>,
            &mut PlanArena<'arena>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let (mut sub_query, correlated) = self.bind_subquery_plan(arena, build)?;
        let sub_query_schema = sub_query.output_schema(arena);

        let fn_check = |len: usize| {
            if sub_query_schema.len() != len {
                return Err(DatabaseError::MisMatch(
                    "expects only one expression to be returned",
                    "the expression returned by the subquery",
                ));
            }
            Ok(())
        };

        let expr = if let Some(LogicalType::Tuple(tys)) = value_ty {
            fn_check(tys.len())?;

            let columns = sub_query_schema
                .iter()
                .enumerate()
                .map(|(position, column)| ScalarExpression::column_expr(*column, position))
                .collect::<Vec<_>>();
            ScalarExpression::Tuple(columns)
        } else {
            fn_check(1)?;

            ScalarExpression::column_expr(sub_query_schema[0], 0)
        };
        Ok((sub_query, expr, correlated))
    }

    pub(crate) fn bind_scalar_subquery_plan<'arena, F>(
        &mut self,
        arena: &mut PlanArena<'arena>,
        build: F,
    ) -> Result<ScalarExpression, DatabaseError>
    where
        F: FnOnce(
            &mut Binder<'a, '_, T, A>,
            &mut PlanArena<'arena>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let (sub_query, column, correlated) =
            self.bind_subquery_plan_with_output(None, arena, build)?;
        let sub_query = ScalarSubqueryOperator::build(sub_query);
        let (expr, sub_query) = if !self.context.is_step(&QueryBindStep::Where) {
            self.bind_temp_table(column, sub_query, arena)?
        } else {
            (column, sub_query)
        };
        self.context.sub_query(SubQueryType::SubQuery {
            plan: sub_query,
            correlated,
        });
        Ok(expr)
    }

    pub(crate) fn bind_exists_subquery_plan<'arena, F>(
        &mut self,
        negated: bool,
        arena: &mut PlanArena<'arena>,
        build: F,
    ) -> Result<ScalarExpression, DatabaseError>
    where
        F: FnOnce(
            &mut Binder<'a, '_, T, A>,
            &mut PlanArena<'arena>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let (sub_query, correlated) = self.bind_subquery_plan(arena, build)?;
        let (_, marker_ref) = self.bind_temp_table_alias(
            ScalarExpression::Constant(DataValue::Boolean(true)),
            0,
            arena,
        );
        let output_column = marker_ref.output_column_ref(arena);
        self.context.sub_query(SubQueryType::ExistsSubQuery {
            plan: sub_query,
            correlated,
            output_column,
        });
        if negated {
            Ok(ScalarExpression::Unary {
                op: expression::UnaryOperator::Not,
                expr: Box::new(marker_ref),
                evaluator: None,
                ty: LogicalType::Boolean,
            })
        } else {
            Ok(marker_ref)
        }
    }

    pub(crate) fn bind_quantified_subquery_plan<'arena, F>(
        &mut self,
        quantifier: MarkApplyQuantifier,
        negated: bool,
        left_expr: ScalarExpression,
        compare_op: expression::BinaryOperator,
        arena: &mut PlanArena<'arena>,
        build: F,
    ) -> Result<ScalarExpression, DatabaseError>
    where
        F: FnOnce(
            &mut Binder<'a, '_, T, A>,
            &mut PlanArena<'arena>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let left_ty = left_expr.return_type(arena).into_owned();
        let (sub_query, column, correlated) =
            self.bind_subquery_plan_with_output(Some(&left_ty), arena, build)?;

        if !self.context.is_step(&QueryBindStep::Where) {
            return Err(DatabaseError::UnsupportedStmt(
                "quantified subqueries can only appear in `WHERE`".to_string(),
            ));
        }

        let (alias_expr, sub_query) = self.bind_temp_table(column, sub_query, arena)?;
        let predicate = ScalarExpression::Binary {
            op: compare_op,
            left_expr: Box::new(left_expr),
            right_expr: Box::new(alias_expr),
            evaluator: None,
            ty: LogicalType::Boolean,
        };
        let (_, marker_ref) = self.bind_temp_table_alias(
            ScalarExpression::Constant(DataValue::Boolean(true)),
            0,
            arena,
        );
        let output_column = marker_ref.output_column_ref(arena);
        self.context.sub_query(SubQueryType::QuantifiedSubQuery {
            quantifier,
            negated,
            plan: sub_query,
            correlated,
            output_column,
            predicate,
        });

        if negated {
            Ok(ScalarExpression::Unary {
                op: expression::UnaryOperator::Not,
                expr: Box::new(marker_ref),
                evaluator: None,
                ty: LogicalType::Boolean,
            })
        } else {
            Ok(marker_ref)
        }
    }

    pub(crate) fn bind_column_ref_by_name(
        &mut self,
        table_name: Option<&str>,
        column_name: &str,
        bind_table_name: Option<&str>,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        if table_name.is_none() {
            if let Some((_, expr)) = self
                .context
                .expr_aliases
                .iter()
                .find(|((table, column), _)| table.is_none() && column == column_name)
            {
                return Ok(ScalarExpression::Alias {
                    expr: Box::new(expr.clone()),
                    alias: AliasType::Name(column_name.to_string()),
                });
            }
        }
        if self.context.allow_default {
            try_default!(&table_name, column_name);
        }
        if let Some(table) = table_name.or(bind_table_name) {
            let (source, position_offset) =
                match Self::resolve_source_columns_in_scope(&self.context, &table) {
                    Ok(source) => source,
                    Err(err) => {
                        if let Some(parent) = self.parent {
                            self.context.mark_outer_ref();
                            Self::resolve_source_columns_in_scope(parent, &table)
                                .map_err(|_| err)?
                        } else {
                            return Err(err);
                        }
                    }
                };
            let (position, column) =
                Self::find_column_in_schema(source.schema().iter(), arena, column_name)
                    .ok_or_else(|| DatabaseError::column_not_found(column_name.to_string()))?;

            Ok(ScalarExpression::column_expr(
                column,
                position_offset + position,
            ))
        } else {
            // handle col syntax
            let mut find_visible_column =
                |context: &BinderContext<'a, T>| -> Result<Option<ScalarExpression>, DatabaseError> {
                    Ok(context
                        .using
                        .get(column_name)
                        .map(|using_column| using_column.visible_expr(arena))
                        .transpose()?
                        .or_else(|| {
                            Self::find_column_in_scope(context, arena, column_name)
                        }))
                };
            let mut got_column = find_visible_column(&self.context)?;
            if got_column.is_none() {
                if let Some(parent) = self.parent {
                    self.context.mark_outer_ref();
                    got_column = find_visible_column(parent)?;
                }
            }
            match got_column {
                Some(column) => Ok(column),
                None => Err(DatabaseError::column_not_found(column_name.to_string())),
            }
        }
    }

    pub(crate) fn bind_binary_op_expr(
        &mut self,
        left_expr: ScalarExpression,
        right_expr: ScalarExpression,
        op: expression::BinaryOperator,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = Box::new(left_expr);
        let right_expr = Box::new(right_expr);
        let left_ty = left_expr.return_type(arena);
        let right_ty = right_expr.return_type(arena);
        let ty = match &op {
            expression::BinaryOperator::Plus
            | expression::BinaryOperator::Minus
            | expression::BinaryOperator::Multiply
            | expression::BinaryOperator::Modulo => {
                LogicalType::max_logical_type(&left_ty, &right_ty)?.into_owned()
            }
            expression::BinaryOperator::Divide => {
                if let LogicalType::Decimal(precision, scale) =
                    LogicalType::max_logical_type(&left_ty, &right_ty)?.into_owned()
                {
                    LogicalType::Decimal(precision, scale)
                } else {
                    LogicalType::Double
                }
            }
            expression::BinaryOperator::Gt
            | expression::BinaryOperator::Lt
            | expression::BinaryOperator::GtEq
            | expression::BinaryOperator::LtEq
            | expression::BinaryOperator::Eq
            | expression::BinaryOperator::NotEq
            | expression::BinaryOperator::Like(_)
            | expression::BinaryOperator::NotLike(_)
            | expression::BinaryOperator::And
            | expression::BinaryOperator::Or => LogicalType::Boolean,
            expression::BinaryOperator::StringConcat => {
                LogicalType::Varchar(None, CharLengthUnits::Characters)
            }
            op => return Err(DatabaseError::UnsupportedStmt(format!("{op}"))),
        };

        Ok(ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator: None,
            ty,
        })
    }

    pub(crate) fn bind_unary_op_expr(
        &mut self,
        expr: ScalarExpression,
        op: expression::UnaryOperator,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let expr = Box::new(expr);
        let ty = if let expression::UnaryOperator::Not = op {
            LogicalType::Boolean
        } else {
            expr.return_type(arena).into_owned()
        };

        Ok(ScalarExpression::Unary {
            op,
            expr,
            evaluator: None,
            ty,
        })
    }

    pub(crate) fn bind_function_call(
        &mut self,
        function_name: String,
        mut args: Vec<ScalarExpression>,
        is_distinct: bool,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        match function_name.as_str() {
            "count" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of count() parameters", "1"));
                }
                return Ok(ScalarExpression::AggCall {
                    distinct: is_distinct,
                    kind: AggKind::Count,
                    args,
                    ty: LogicalType::Integer,
                });
            }
            "sum" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of sum() parameters", "1"));
                }
                let ty = args[0].return_type(arena).into_owned();

                return Ok(ScalarExpression::AggCall {
                    distinct: is_distinct,
                    kind: AggKind::Sum,
                    args,
                    ty,
                });
            }
            "min" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of min() parameters", "1"));
                }
                let ty = args[0].return_type(arena).into_owned();

                return Ok(ScalarExpression::AggCall {
                    distinct: is_distinct,
                    kind: AggKind::Min,
                    args,
                    ty,
                });
            }
            "max" => {
                if args.len() != 1 {
                    return Err(DatabaseError::MisMatch("number of max() parameters", "1"));
                }
                let ty = args[0].return_type(arena).into_owned();

                return Ok(ScalarExpression::AggCall {
                    distinct: is_distinct,
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
                    distinct: is_distinct,
                    kind: AggKind::Avg,
                    args,
                    ty: LogicalType::Double,
                });
            }
            "if" => {
                if args.len() != 3 {
                    return Err(DatabaseError::MisMatch("number of if() parameters", "3"));
                }
                let ty = Self::return_type(&args[1], &args[2], arena)?;
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
                let ty = Self::return_type(&args[0], &args[1], arena)?;
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
                let ty = Self::return_type(&args[0], &args[1], arena)?;
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
                    ty = args[0].return_type(arena).into_owned();

                    for arg in args.iter_mut() {
                        let temp_ty = arg.return_type(arena).into_owned();

                        if temp_ty == LogicalType::SqlNull {
                            continue;
                        }
                        if ty == LogicalType::SqlNull && temp_ty != LogicalType::SqlNull {
                            ty = temp_ty;
                        } else if ty != temp_ty {
                            ty = LogicalType::max_logical_type(&ty, &temp_ty)?.into_owned();
                        }
                    }
                }
                return Ok(ScalarExpression::Coalesce { exprs: args, ty });
            }
            _ => (),
        }
        let arg_types = args
            .iter()
            .map(|arg| arg.return_type(arena).into_owned())
            .collect_vec();
        let summary = FunctionSummary {
            name: function_name.into(),
            arg_types,
        };
        if let Some(function) = self.context.scala_functions.get(&summary) {
            return Ok(ScalarExpression::ScalaFunction(ScalarFunction {
                args,
                inner: ArcScalarFunctionImpl(function.clone()),
            }));
        }
        if let Some(function) = self.context.table_functions.get(&summary) {
            if !matches!(self.context.step_now(), QueryBindStep::From) {
                return Err(DatabaseError::UnsupportedStmt(
                    "`TableFunction` cannot bind in non-From step".to_string(),
                ));
            }
            return Ok(ScalarExpression::TableFunction(TableFunction {
                args,
                catalog: function.clone(),
            }));
        }

        Err(DatabaseError::function_not_found(summary.name.to_string()))
    }

    pub(crate) fn return_type(
        expr_1: &ScalarExpression,
        expr_2: &ScalarExpression,
        arena: &PlanArena,
    ) -> Result<LogicalType, DatabaseError> {
        let temp_ty_1 = expr_1.return_type(arena);
        let temp_ty_2 = expr_2.return_type(arena);

        match (temp_ty_1.as_ref(), temp_ty_2.as_ref()) {
            (LogicalType::SqlNull, LogicalType::SqlNull) => Ok(LogicalType::SqlNull),
            (ty, LogicalType::SqlNull) | (LogicalType::SqlNull, ty) => Ok(ty.clone()),
            (ty_1, ty_2) => Ok(LogicalType::max_logical_type(ty_1, ty_2)?.into_owned()),
        }
    }

    pub(crate) fn wildcard_expr() -> ScalarExpression {
        ScalarExpression::Constant(DataValue::Utf8 {
            value: "*".to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        })
    }
}
