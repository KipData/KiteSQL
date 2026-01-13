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

use ahash::RandomState;
use itertools::Itertools;
use sqlparser::ast::{Expr, OrderByExpr};
use std::collections::HashSet;

use super::{Binder, QueryBindStep};
use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::{
    expression::ScalarExpression,
    planner::operator::{aggregate::AggregateOperator, sort::SortField},
};

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub fn bind_aggregate(
        &mut self,
        children: LogicalPlan,
        agg_calls: Vec<ScalarExpression>,
        groupby_exprs: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        self.context.step(QueryBindStep::Agg);

        AggregateOperator::build(children, agg_calls, groupby_exprs, false)
    }

    pub fn extract_select_aggregate(
        &mut self,
        select_items: &mut [ScalarExpression],
    ) -> Result<(), DatabaseError> {
        for column in select_items {
            self.visit_column_agg_expr(column)?;
        }
        Ok(())
    }

    pub fn extract_group_by_aggregate(
        &mut self,
        select_list: &mut [ScalarExpression],
        groupby: &[Expr],
    ) -> Result<(), DatabaseError> {
        let mut group_by_exprs = Vec::with_capacity(groupby.len());
        for expr in groupby.iter() {
            group_by_exprs.push(self.bind_expr(expr)?);
        }

        self.validate_groupby_illegal_column(select_list, &group_by_exprs)?;

        for expr in group_by_exprs.iter_mut() {
            self.visit_group_by_expr(select_list, expr);
        }
        Ok(())
    }

    pub fn extract_having_orderby_aggregate(
        &mut self,
        having: &Option<Expr>,
        orderbys: &[OrderByExpr],
    ) -> Result<(Option<ScalarExpression>, Option<Vec<SortField>>), DatabaseError> {
        // Extract having expression.
        let return_having = if let Some(having) = having {
            let mut having = self.bind_expr(having)?;
            self.visit_column_agg_expr(&mut having)?;

            Some(having)
        } else {
            None
        };

        // Extract orderby expression.
        let return_orderby = if !orderbys.is_empty() {
            let mut return_orderby = vec![];
            for orderby in orderbys {
                let OrderByExpr {
                    expr,
                    asc,
                    nulls_first,
                } = orderby;
                let mut expr = self.bind_expr(expr)?;
                self.visit_column_agg_expr(&mut expr)?;

                return_orderby.push(SortField::new(
                    expr,
                    asc.is_none_or(|asc| asc),
                    nulls_first.unwrap_or(true),
                ));
            }
            Some(return_orderby)
        } else {
            None
        };
        Ok((return_having, return_orderby))
    }

    fn visit_column_agg_expr(&mut self, expr: &mut ScalarExpression) -> Result<(), DatabaseError> {
        match expr {
            ScalarExpression::AggCall { .. } => {
                self.context.agg_calls.push(expr.clone());
            }
            ScalarExpression::TypeCast { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::IsNull { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::Unary { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::Alias { expr, .. } => self.visit_column_agg_expr(expr)?,
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(left_expr)?;
                self.visit_column_agg_expr(right_expr)?;
            }
            ScalarExpression::In { expr, args, .. } => {
                self.visit_column_agg_expr(expr)?;
                for arg in args {
                    self.visit_column_agg_expr(arg)?;
                }
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(expr)?;
                self.visit_column_agg_expr(left_expr)?;
                self.visit_column_agg_expr(right_expr)?;
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                self.visit_column_agg_expr(expr)?;
                if let Some(expr) = for_expr {
                    self.visit_column_agg_expr(expr)?;
                }
                if let Some(expr) = from_expr {
                    self.visit_column_agg_expr(expr)?;
                }
            }
            ScalarExpression::Position { expr, in_expr } => {
                self.visit_column_agg_expr(expr)?;
                self.visit_column_agg_expr(in_expr)?;
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                self.visit_column_agg_expr(expr)?;
                if let Some(trim_what_expr) = trim_what_expr {
                    self.visit_column_agg_expr(trim_what_expr)?;
                }
            }
            ScalarExpression::Constant(_) | ScalarExpression::ColumnRef { .. } => (),
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Tuple(args)
            | ScalarExpression::ScalaFunction(ScalarFunction { args, .. })
            | ScalarExpression::Coalesce { exprs: args, .. } => {
                for expr in args {
                    self.visit_column_agg_expr(expr)?;
                }
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(condition)?;
                self.visit_column_agg_expr(left_expr)?;
                self.visit_column_agg_expr(right_expr)?;
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                self.visit_column_agg_expr(left_expr)?;
                self.visit_column_agg_expr(right_expr)?;
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                if let Some(expr) = operand_expr {
                    self.visit_column_agg_expr(expr)?;
                }
                for (expr_1, expr_2) in expr_pairs {
                    self.visit_column_agg_expr(expr_1)?;
                    self.visit_column_agg_expr(expr_2)?;
                }
                if let Some(expr) = else_expr {
                    self.visit_column_agg_expr(expr)?;
                }
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
        }

        Ok(())
    }

    /// Validate select exprs must appear in the GROUP BY clause or be used in
    /// an aggregate function.
    /// e.g. SELECT a,count(b) FROM t GROUP BY a. it's ok.
    ///      SELECT a,b FROM t GROUP BY a.        it's error.
    ///      SELECT a,count(b) FROM t GROUP BY b. it's error.
    fn validate_groupby_illegal_column(
        &mut self,
        select_items: &[ScalarExpression],
        groupby: &[ScalarExpression],
    ) -> Result<(), DatabaseError> {
        let mut group_raw_exprs = vec![];
        for expr in groupby {
            if let ScalarExpression::Alias { alias, .. } = expr {
                let alias_expr = select_items.iter().find(|column| {
                    if let ScalarExpression::Alias {
                        alias: inner_alias, ..
                    } = &column
                    {
                        alias == inner_alias
                    } else {
                        false
                    }
                });

                if let Some(inner_expr) = alias_expr {
                    group_raw_exprs.push(inner_expr);
                }
            } else {
                group_raw_exprs.push(expr);
            }
        }
        let mut group_raw_set: HashSet<&ScalarExpression, RandomState> =
            HashSet::from_iter(group_raw_exprs.iter().copied());

        for expr in select_items {
            if expr.has_agg_call() {
                continue;
            }
            group_raw_set.remove(expr);

            if !group_raw_exprs.iter().contains(&expr) {
                return Err(DatabaseError::AggMiss(format!(
                    "`{expr}` must appear in the GROUP BY clause or be used in an aggregate function"
                )));
            }
        }

        if !group_raw_set.is_empty() {
            return Err(DatabaseError::AggMiss(
                "in the GROUP BY clause the field must be in the select clause".to_string(),
            ));
        }

        Ok(())
    }

    fn visit_group_by_expr(
        &mut self,
        select_list: &mut [ScalarExpression],
        expr: &mut ScalarExpression,
    ) {
        if let ScalarExpression::Alias { alias, .. } = expr {
            if let Some(i) = select_list.iter().position(|inner_expr| {
                if let ScalarExpression::Alias {
                    alias: inner_alias, ..
                } = &inner_expr
                {
                    alias == inner_alias
                } else {
                    false
                }
            }) {
                self.context.group_by_exprs.push(select_list[i].clone());
                return;
            }
        }

        if let Some(i) = select_list.iter().position(|column| column == expr) {
            self.context.group_by_exprs.push(select_list[i].clone())
        }
    }

    /// Validate having or orderby clause is valid, if SQL has group by clause.
    pub fn validate_having_orderby(&self, expr: &ScalarExpression) -> Result<(), DatabaseError> {
        if self.context.group_by_exprs.is_empty() {
            return Ok(());
        }

        match expr {
            ScalarExpression::AggCall { .. } => {
                if self.context.group_by_exprs.contains(expr)
                    || self.context.agg_calls.contains(expr)
                {
                    return Ok(());
                }

                Err(DatabaseError::AggMiss(format!(
                    "expression '{expr}' must appear in the GROUP BY clause or be used in an aggregate function"
                )))
            }
            ScalarExpression::ColumnRef { .. } | ScalarExpression::Alias { .. } => {
                if self.context.group_by_exprs.contains(expr) {
                    return Ok(());
                }
                if matches!(expr, ScalarExpression::Alias { .. }) {
                    return self.validate_having_orderby(expr.unpack_alias_ref());
                }

                Err(DatabaseError::AggMiss(format!(
                    "expression '{expr}' must appear in the GROUP BY clause or be used in an aggregate function"
                )))
            }

            ScalarExpression::TypeCast { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::IsNull { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::Unary { expr, .. } => self.validate_having_orderby(expr),
            ScalarExpression::In { expr, args, .. } => {
                self.validate_having_orderby(expr)?;
                for arg in args {
                    self.validate_having_orderby(arg)?;
                }
                Ok(())
            }
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;
                Ok(())
            }
            ScalarExpression::Between {
                expr,
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(expr)?;
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;
                Ok(())
            }
            ScalarExpression::SubString {
                expr,
                for_expr,
                from_expr,
            } => {
                self.validate_having_orderby(expr)?;
                if let Some(expr) = for_expr {
                    self.validate_having_orderby(expr)?;
                }
                if let Some(expr) = from_expr {
                    self.validate_having_orderby(expr)?;
                }
                Ok(())
            }
            ScalarExpression::Position { expr, in_expr } => {
                self.validate_having_orderby(expr)?;
                self.validate_having_orderby(in_expr)?;
                Ok(())
            }
            ScalarExpression::Trim {
                expr,
                trim_what_expr,
                ..
            } => {
                self.validate_having_orderby(expr)?;
                if let Some(trim_what_expr) = trim_what_expr {
                    self.validate_having_orderby(trim_what_expr)?;
                }
                Ok(())
            }
            ScalarExpression::Constant(_) => Ok(()),
            ScalarExpression::Empty => unreachable!(),
            ScalarExpression::Tuple(args)
            | ScalarExpression::ScalaFunction(ScalarFunction { args, .. })
            | ScalarExpression::Coalesce { exprs: args, .. } => {
                for expr in args {
                    self.validate_having_orderby(expr)?;
                }
                Ok(())
            }
            ScalarExpression::If {
                condition,
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(condition)?;
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;

                Ok(())
            }
            ScalarExpression::IfNull {
                left_expr,
                right_expr,
                ..
            }
            | ScalarExpression::NullIf {
                left_expr,
                right_expr,
                ..
            } => {
                self.validate_having_orderby(left_expr)?;
                self.validate_having_orderby(right_expr)?;

                Ok(())
            }
            ScalarExpression::CaseWhen {
                operand_expr,
                expr_pairs,
                else_expr,
                ..
            } => {
                if let Some(expr) = operand_expr {
                    self.validate_having_orderby(expr)?;
                }
                for (expr_1, expr_2) in expr_pairs {
                    self.validate_having_orderby(expr_1)?;
                    self.validate_having_orderby(expr_2)?;
                }
                if let Some(expr) = else_expr {
                    self.validate_having_orderby(expr)?;
                }

                Ok(())
            }
            ScalarExpression::TableFunction(_) => unreachable!(),
        }
    }
}
