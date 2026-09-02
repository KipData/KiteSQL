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

use super::{Binder, QueryBindStep};
use crate::errors::DatabaseError;
use crate::expression::visitor::{walk_expr, ExprVisitor};
use crate::expression::visitor_mut::{walk_mut_expr, ExprVisitorMut};
use crate::planner::{ExprRef, LogicalPlan, PlanArena};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::{
    expression::ScalarExpression,
    planner::operator::{aggregate::AggregateOperator, sort::SortField},
};

struct AggregateCallCollector<'a> {
    agg_calls: &'a mut Vec<ExprRef>,
}

impl ExprVisitor<PlanArena<'_>> for AggregateCallCollector<'_> {
    fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
        match arena.expression(expr) {
            ScalarExpression::AggCall { .. } => self.agg_calls.push(expr),
            ScalarExpression::Alias { expr, .. } => self.visit(*expr, arena)?,
            ScalarExpression::Empty | ScalarExpression::TableFunction(_) => unreachable!(),
            _ => walk_expr(self, expr, arena)?,
        }
        Ok(())
    }
}

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub fn bind_aggregate(
        &mut self,
        children: LogicalPlan,
        agg_calls: Vec<ExprRef>,
        groupby_exprs: Vec<ExprRef>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Agg);
        Ok(AggregateOperator::build(
            children,
            agg_calls,
            groupby_exprs,
            false,
            self.force_spill,
        ))
    }

    pub fn extract_select_aggregate(
        &mut self,
        select_items: &mut [ExprRef],
        arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        for column in select_items {
            self.collect_aggregate_calls(*column, arena)?;
        }
        Ok(())
    }

    pub fn extract_group_by_aggregate_exprs(
        &mut self,
        select_list: &mut [ExprRef],
        mut group_by_exprs: Vec<ExprRef>,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        self.validate_groupby_illegal_column(select_list, &group_by_exprs, arena)?;

        for expr in group_by_exprs.iter_mut() {
            self.visit_group_by_expr(select_list, *expr, arena)?;
        }
        Ok(())
    }

    pub fn extract_having_orderby_aggregate_exprs<'arena, I, F>(
        &mut self,
        mut having: Option<ExprRef>,
        orderby: Option<I>,
        mut bind_sort_field: F,
        arena: &mut PlanArena<'arena>,
    ) -> Result<(Option<ExprRef>, Option<Vec<SortField>>), DatabaseError>
    where
        I: IntoIterator,
        F: FnMut(&mut Self, I::Item, &mut PlanArena<'arena>) -> Result<SortField, DatabaseError>,
    {
        if let Some(having) = having.as_mut() {
            self.collect_aggregate_calls(*having, arena)?;
        }
        let mut return_orderby = None;
        if let Some(orderby) = orderby {
            let mut fields = Vec::new();
            for orderby in orderby {
                let field = bind_sort_field(self, orderby, arena)?;
                self.collect_aggregate_calls(field.expr, arena)?;
                fields.push(field);
            }
            return_orderby = Some(fields);
        }
        Ok((having, return_orderby))
    }

    pub fn bind_aggregate_output_exprs<'c>(
        &mut self,
        exprs: impl IntoIterator<Item = &'c mut ExprRef>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        self.bind_aggregate_output_exprs_with_outputs(
            &self.context.agg_calls,
            &self.context.group_by_exprs,
            exprs,
            arena,
        )
    }

    pub(crate) fn bind_aggregate_output_exprs_with_outputs<'c>(
        &self,
        agg_calls: &[ExprRef],
        group_by_exprs: &[ExprRef],
        exprs: impl IntoIterator<Item = &'c mut ExprRef>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut binder = AggregateOutputBinder::new(agg_calls, group_by_exprs);
        for expr in exprs {
            binder.visit(expr, arena)?;
        }
        Ok(())
    }

    pub(crate) fn collect_aggregate_calls(
        &mut self,
        expr: ExprRef,
        arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        AggregateCallCollector {
            agg_calls: &mut self.context.agg_calls,
        }
        .visit(expr, arena)
    }

    /// Validate select exprs must appear in the GROUP BY clause or be used in
    /// an aggregate function.
    /// e.g. SELECT a,count(b) FROM t GROUP BY a. it's ok.
    ///      SELECT a,b FROM t GROUP BY a.        it's error.
    ///      SELECT a,count(b) FROM t GROUP BY b. it's error.
    fn validate_groupby_illegal_column(
        &mut self,
        select_items: &[ExprRef],
        groupby: &[ExprRef],
        arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        let mut unmatched_group_exprs = Vec::with_capacity(groupby.len());
        for expr in groupby {
            if let ScalarExpression::Alias { alias, .. } = arena.expression(*expr) {
                let alias_expr = select_items.iter().find(|column| {
                    if let ScalarExpression::Alias {
                        alias: inner_alias, ..
                    } = arena.expression(**column)
                    {
                        alias == inner_alias
                    } else {
                        false
                    }
                });

                if let Some(inner_expr) = alias_expr {
                    unmatched_group_exprs.push(*inner_expr);
                }
            } else {
                unmatched_group_exprs.push(*expr);
            }
        }

        for expr in select_items {
            if expr.has_window_call(arena)? {
                HavingOrderByValidator::new(groupby, &self.context.agg_calls)
                    .visit(*expr, arena)?;
                continue;
            }
            if expr.has_agg_call(arena)? {
                continue;
            }
            let Some(position) = unmatched_group_exprs
                .iter()
                .position(|group_expr| expr.eq_ignore_colref_pos(*group_expr, arena))
            else {
                return Err(DatabaseError::AggMiss(format!(
                    "`{}` must appear in the GROUP BY clause or be used in an aggregate function",
                    expr.output_name(arena)
                )));
            };
            unmatched_group_exprs.remove(position);
        }

        if !unmatched_group_exprs.is_empty() {
            return Err(DatabaseError::AggMiss(
                "in the GROUP BY clause the field must be in the select clause".to_string(),
            ));
        }

        Ok(())
    }

    fn visit_group_by_expr(
        &mut self,
        select_list: &mut [ExprRef],
        expr: ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if let ScalarExpression::Alias { alias, .. } = arena.expression(expr) {
            if let Some(i) = select_list.iter().position(|inner_expr| {
                if let ScalarExpression::Alias {
                    alias: inner_alias, ..
                } = arena.expression(*inner_expr)
                {
                    alias == inner_alias
                } else {
                    false
                }
            }) {
                // GROUP BY evaluates against the aggregate input, while the select
                // expression is later rewritten against aggregate output.
                self.context
                    .group_by_exprs
                    .push(select_list[i].clone_expression(arena)?);
                return Ok(());
            }
        }

        if let Some(i) = select_list
            .iter()
            .position(|column| column.eq_ignore_colref_pos(expr, arena))
        {
            self.context
                .group_by_exprs
                .push(select_list[i].clone_expression(arena)?);
        }
        Ok(())
    }

    /// Validate having or orderby clause is valid, if SQL has group by clause.
    pub fn validate_having_orderby(
        &self,
        expr: ExprRef,
        arena: &PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if self.context.group_by_exprs.is_empty() {
            return Ok(());
        }

        HavingOrderByValidator::new(&self.context.group_by_exprs, &self.context.agg_calls)
            .visit(expr, arena)
    }
}

struct HavingOrderByValidator<'a> {
    group_by_exprs: &'a [ExprRef],
    agg_calls: &'a [ExprRef],
}

impl<'a> HavingOrderByValidator<'a> {
    fn new(group_by_exprs: &'a [ExprRef], agg_calls: &'a [ExprRef]) -> Self {
        Self {
            group_by_exprs,
            agg_calls,
        }
    }

    fn agg_miss(expr: ExprRef, arena: &PlanArena<'_>) -> DatabaseError {
        DatabaseError::AggMiss(format!(
            "expression '{}' must appear in the GROUP BY clause or be used in an aggregate function",
            expr.output_name(arena)
        ))
    }
}

impl ExprVisitor<PlanArena<'_>> for HavingOrderByValidator<'_> {
    fn visit(&mut self, expr: ExprRef, arena: &PlanArena<'_>) -> Result<(), DatabaseError> {
        let contains = |expressions: &[ExprRef]| {
            expressions
                .iter()
                .any(|candidate| candidate.eq_ignore_colref_pos(expr, arena))
        };
        match arena.expression(expr) {
            ScalarExpression::AggCall { .. } => {
                if contains(self.group_by_exprs) || contains(self.agg_calls) {
                    Ok(())
                } else {
                    Err(Self::agg_miss(expr, arena))
                }
            }
            ScalarExpression::ColumnRef { .. } => {
                if contains(self.group_by_exprs) {
                    Ok(())
                } else {
                    Err(Self::agg_miss(expr, arena))
                }
            }
            ScalarExpression::Alias { .. } => {
                if contains(self.group_by_exprs) {
                    Ok(())
                } else {
                    self.visit(expr.unpack_alias(arena), arena)
                }
            }
            ScalarExpression::Empty | ScalarExpression::TableFunction(_) => unreachable!(),
            _ => walk_expr(self, expr, arena),
        }
    }
}

struct AggregateOutputBinder<'a> {
    agg_calls: &'a [ExprRef],
    group_by_exprs: &'a [ExprRef],
}

impl<'a> AggregateOutputBinder<'a> {
    fn new(agg_calls: &'a [ExprRef], group_by_exprs: &'a [ExprRef]) -> Self {
        Self {
            agg_calls,
            group_by_exprs,
        }
    }

    fn output_ref(
        &mut self,
        expr: ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<Option<ScalarExpression>, DatabaseError> {
        let output_count = self.agg_calls.len() + self.group_by_exprs.len();
        self.agg_calls
            .iter()
            .chain(self.group_by_exprs.iter())
            .position(|candidate| {
                candidate.eq_ignore_colref_pos(expr, arena)
                    || candidate
                        .unpack_alias(arena)
                        .eq_ignore_colref_pos(expr.unpack_alias(arena), arena)
            })
            .map(|position| {
                let output_expr = self
                    .agg_calls
                    .iter()
                    .chain(self.group_by_exprs.iter())
                    .nth(position)
                    .ok_or_else(|| {
                        DatabaseError::InvalidValue(format!(
                            "aggregate output position {position} is out of bounds for {output_count} output expressions"
                        ))
                    })?;
                Ok(ScalarExpression::column_expr(
                    output_expr.output_column_ref(arena),
                    position,
                ))
            })
            .transpose()
    }
}

impl ExprVisitorMut for AggregateOutputBinder<'_> {
    fn visit(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if let ScalarExpression::Alias {
            alias: crate::expression::AliasType::Name(_),
            ..
        } = arena.expression(*expr)
        {
            return walk_mut_expr(self, expr, arena);
        }

        if let Some(output) = self.output_ref(*expr, arena)? {
            *expr = arena.alloc_expression(output);
            return Ok(());
        }

        walk_mut_expr(self, expr, arena)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::AggregateOutputBinder;
    use crate::binder::test::build_t1_table;
    use crate::binder::{Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::expression::agg::AggKind;
    use crate::expression::visitor_mut::ExprVisitorMut;
    use crate::expression::{AliasType, BinaryOperator, ScalarExpression};
    use crate::planner::{ExprRef, PlanArena};
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn test_column(arena: &mut PlanArena, name: &str, ty: LogicalType) -> ColumnRef {
        arena.alloc_column(ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(ty, None, false, None).unwrap(),
        ))
    }

    fn test_count(arena: &mut PlanArena, expr: ExprRef) -> ExprRef {
        arena.alloc_expression(ScalarExpression::AggCall {
            distinct: false,
            kind: AggKind::Count,
            args: vec![expr],
            ty: LogicalType::Bigint,
        })
    }

    #[test]
    fn test_aggregate_output_binder_rewrites_agg_and_group_slots() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let group_column = test_column(&mut arena, "c1", LogicalType::Integer);
        let agg_column = test_column(&mut arena, "c2", LogicalType::Integer);

        let group_expr = arena.alloc_expression(ScalarExpression::column_expr(group_column, 0));
        let agg_arg = arena.alloc_expression(ScalarExpression::column_expr(agg_column, 1));
        let agg_expr = test_count(&mut arena, agg_arg);

        let agg_output = arena.alloc_expression(ScalarExpression::Alias {
            expr: agg_expr,
            alias: AliasType::Name("cnt".to_string()),
        });
        let group_output = arena.alloc_expression(ScalarExpression::Alias {
            expr: group_expr,
            alias: AliasType::Name("g".to_string()),
        });

        let mut order_by_agg = arena.alloc_expression(ScalarExpression::Alias {
            expr: agg_expr,
            alias: AliasType::Name("cnt".to_string()),
        });
        let mut order_by_group = group_expr;
        {
            let mut binder = AggregateOutputBinder::new(
                std::slice::from_ref(&agg_output),
                std::slice::from_ref(&group_output),
            );
            binder.visit(&mut order_by_agg, &mut arena)?;
            binder.visit(&mut order_by_group, &mut arena)?;
        }
        let agg_column = agg_output.output_column_ref(&mut arena);
        let expected_agg_inner =
            arena.alloc_expression(ScalarExpression::column_expr(agg_column, 0));
        let expected_agg = arena.alloc_expression(ScalarExpression::Alias {
            expr: expected_agg_inner,
            alias: AliasType::Name("cnt".to_string()),
        });
        assert!(order_by_agg.eq_ignore_colref_pos(expected_agg, &arena));

        let group_column = group_output.output_column_ref(&mut arena);
        let expected_group = arena.alloc_expression(ScalarExpression::column_expr(group_column, 1));
        assert!(order_by_group.eq_ignore_colref_pos(expected_group, &arena));

        Ok(())
    }

    #[test]
    fn test_aggregate_output_binder_matches_alias_expr_reference() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let group_column = test_column(&mut arena, "c1", LogicalType::Integer);
        let group_expr = arena.alloc_expression(ScalarExpression::column_expr(group_column, 0));
        let group_output = arena.alloc_expression(ScalarExpression::Alias {
            expr: group_expr,
            alias: AliasType::Name("g".to_string()),
        });

        let constant = arena.alloc_expression(ScalarExpression::Constant(1_i32.into()));
        let mut target = arena.alloc_expression(ScalarExpression::Alias {
            expr: constant,
            alias: AliasType::Expr(group_expr),
        });

        {
            let mut binder = AggregateOutputBinder::new(&[], std::slice::from_ref(&group_output));
            binder.visit(&mut target, &mut arena)?;
        }
        let output_column = group_output.output_column_ref(&mut arena);
        let expected = arena.alloc_expression(ScalarExpression::column_expr(output_column, 0));
        assert!(target.eq_ignore_colref_pos(expected, &arena));

        Ok(())
    }

    #[test]
    fn test_validate_having_orderby_rejects_missing_group_expr() -> Result<(), DatabaseError> {
        let tables = build_t1_table()?;
        let scala_functions = Default::default();
        let table_functions = Default::default();
        let transaction = tables.storage.transaction()?;
        let args: [(&'static str, DataValue); 0] = [];
        let mut binder = Binder::new(
            BinderContext::new(
                &tables.table_cache,
                &tables.view_cache,
                &transaction,
                &scala_functions,
                &table_functions,
            ),
            &args,
            None,
        );
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let group_column = test_column(&mut arena, "c1", LogicalType::Integer);
        let missing_column = test_column(&mut arena, "c2", LogicalType::Integer);
        let group_expr = arena.alloc_expression(ScalarExpression::column_expr(group_column, 0));
        let missing_expr = arena.alloc_expression(ScalarExpression::column_expr(missing_column, 1));
        binder.context.group_by_exprs.push(group_expr);

        binder.validate_having_orderby(group_expr, &arena)?;
        let group_alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: group_expr,
            alias: AliasType::Name("group_alias".to_string()),
        });
        binder.validate_having_orderby(group_alias, &arena)?;

        assert!(matches!(
            binder.validate_having_orderby(missing_expr, &arena),
            Err(DatabaseError::AggMiss(_))
        ));

        let registered_agg = test_count(&mut arena, missing_expr);
        binder.context.agg_calls.push(registered_agg);
        binder.validate_having_orderby(registered_agg, &arena)?;
        let constant = arena.alloc_expression(ScalarExpression::Constant(1_i32.into()));
        let unregistered_agg = test_count(&mut arena, constant);
        assert!(matches!(
            binder.validate_having_orderby(unregistered_agg, &arena),
            Err(DatabaseError::AggMiss(_))
        ));

        let invalid_binary = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: group_expr,
            right_expr: missing_expr,
            evaluator: None,
            ty: LogicalType::Boolean,
        });
        assert!(matches!(
            binder.validate_having_orderby(invalid_binary, &arena),
            Err(DatabaseError::AggMiss(_))
        ));

        Ok(())
    }
}
