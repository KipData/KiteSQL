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

use crate::binder::{Binder, QueryBindStep};
use crate::errors::DatabaseError;
use crate::expression::visitor_mut::{walk_mut_expr, ExprVisitorMut};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::{ExprRef, LogicalPlan, PlanArena};
use crate::storage::Transaction;
use crate::types::value::DataValue;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub fn bind_distinct(
        &mut self,
        children: LogicalPlan,
        select_list: Vec<ExprRef>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Distinct);

        Ok(AggregateOperator::build(
            children,
            vec![],
            select_list,
            true,
            self.force_spill,
        ))
    }

    pub fn bind_distinct_output_exprs<'c>(
        &mut self,
        select_list: &[ExprRef],
        exprs: impl IntoIterator<Item = &'c mut ExprRef>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut binder = DistinctOutputBinder::new(select_list);
        for expr in exprs {
            binder.visit(expr, arena)?;
        }
        Ok(())
    }

    pub fn bind_distinct_orderby_exprs(
        &mut self,
        select_list: &[ExprRef],
        orderby: &mut [SortField],
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut binder = DistinctOutputBinder::new(select_list);

        for field in orderby {
            let output = binder.output_ref(field.expr, arena).ok_or_else(|| {
                DatabaseError::InvalidValue(format!(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list: '{}'",
                    field.expr.output_name(arena)
                ))
            })?;
            field.expr = arena.alloc_expression(output);
        }

        Ok(())
    }
}

struct DistinctOutputBinder<'a> {
    select_list: &'a [ExprRef],
}

impl<'a> DistinctOutputBinder<'a> {
    fn new(select_list: &'a [ExprRef]) -> Self {
        Self { select_list }
    }

    fn output_ref(&mut self, expr: ExprRef, arena: &mut PlanArena<'_>) -> Option<ScalarExpression> {
        self.select_list
            .iter()
            .position(|candidate| {
                candidate.eq_ignore_colref_pos(expr, arena)
                    || candidate
                        .unpack_alias(arena)
                        .eq_ignore_colref_pos(expr.unpack_alias(arena), arena)
            })
            .map(|position| {
                let output_expr = self.select_list[position];
                ScalarExpression::column_expr(output_expr.output_column_ref(arena), position)
            })
    }
}

impl ExprVisitorMut for DistinctOutputBinder<'_> {
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

        if let Some(output_ref) = self.output_ref(*expr, arena) {
            *expr = arena.alloc_expression(output_ref);
            return Ok(());
        }
        walk_mut_expr(self, expr, arena)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::DistinctOutputBinder;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::expression::visitor_mut::ExprVisitorMut;
    use crate::expression::{AliasType, ScalarExpression};
    use crate::planner::PlanArena;
    use crate::types::LogicalType;

    fn test_column(arena: &mut PlanArena, name: &str, ty: LogicalType) -> ColumnRef {
        arena.alloc_column(ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(ty, None, false, None).unwrap(),
        ))
    }

    #[test]
    fn test_distinct_output_binder_rewrites_output_slot() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left_column = test_column(&mut arena, "c1", LogicalType::Integer);
        let right_column = test_column(&mut arena, "c2", LogicalType::Integer);

        let left_expr = arena.alloc_expression(ScalarExpression::column_expr(left_column, 0));
        let right_expr = arena.alloc_expression(ScalarExpression::column_expr(right_column, 1));
        let second_output = right_expr;
        let select_output = arena.alloc_expression(ScalarExpression::Alias {
            expr: left_expr,
            alias: AliasType::Name("v".to_string()),
        });
        let select_list = [select_output, right_expr];

        let mut order_by_alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: left_expr,
            alias: AliasType::Name("v".to_string()),
        });
        let mut order_by_second = right_expr;
        {
            let mut binder = DistinctOutputBinder::new(&select_list);
            binder.visit(&mut order_by_alias, &mut arena)?;
            binder.visit(&mut order_by_second, &mut arena)?;
        }
        let select_column = select_output.output_column_ref(&mut arena);
        let expected_inner =
            arena.alloc_expression(ScalarExpression::column_expr(select_column, 0));
        let expected_alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: expected_inner,
            alias: AliasType::Name("v".to_string()),
        });
        assert!(order_by_alias.eq_ignore_colref_pos(expected_alias, &arena));

        let second_column = second_output.output_column_ref(&mut arena);
        let expected_second =
            arena.alloc_expression(ScalarExpression::column_expr(second_column, 1));
        assert!(order_by_second.eq_ignore_colref_pos(expected_second, &arena));

        Ok(())
    }

    #[test]
    fn test_distinct_output_binder_matches_alias_expr_reference() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let column = test_column(&mut arena, "c1", LogicalType::Integer);
        let expr = arena.alloc_expression(ScalarExpression::column_expr(column, 0));
        let select_output = arena.alloc_expression(ScalarExpression::Alias {
            expr,
            alias: AliasType::Name("v".to_string()),
        });

        let constant = arena.alloc_expression(ScalarExpression::Constant(1_i32.into()));
        let mut target = arena.alloc_expression(ScalarExpression::Alias {
            expr: constant,
            alias: AliasType::Expr(expr),
        });

        {
            let mut binder = DistinctOutputBinder::new(std::slice::from_ref(&select_output));
            binder.visit(&mut target, &mut arena)?;
        }
        let output_column = select_output.output_column_ref(&mut arena);
        let expected = arena.alloc_expression(ScalarExpression::column_expr(output_column, 0));
        assert!(target.eq_ignore_colref_pos(expected, &arena));

        Ok(())
    }
}
