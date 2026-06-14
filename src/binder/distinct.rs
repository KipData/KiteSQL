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
use crate::expression::visitor_mut::{walk_mut_expr, VisitorMut};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub fn bind_distinct(
        &mut self,
        children: LogicalPlan,
        select_list: Vec<ScalarExpression>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Distinct);

        Ok(AggregateOperator::build(
            children,
            vec![],
            select_list,
            true,
        ))
    }

    pub fn bind_distinct_output_exprs<'c>(
        &mut self,
        select_list: &[ScalarExpression],
        exprs: impl IntoIterator<Item = &'c mut ScalarExpression>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut binder = DistinctOutputBinder::new(select_list, arena);
        for expr in exprs {
            binder.visit(expr)?;
        }
        Ok(())
    }

    pub fn bind_distinct_orderby_exprs(
        &mut self,
        select_list: &[ScalarExpression],
        orderby: &mut [SortField],
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut binder = DistinctOutputBinder::new(select_list, arena);

        for field in orderby {
            field.expr = binder.output_ref(&field.expr).ok_or_else(|| {
                DatabaseError::InvalidValue(format!(
                    "for SELECT DISTINCT, ORDER BY expressions must appear in select list: '{}'",
                    field.expr
                ))
            })?;
        }

        Ok(())
    }
}

struct DistinctOutputBinder<'a, 'p> {
    select_list: &'a [ScalarExpression],
    arena: &'a mut crate::planner::PlanArena<'p>,
}

impl<'a, 'p> DistinctOutputBinder<'a, 'p> {
    fn new(
        select_list: &'a [ScalarExpression],
        arena: &'a mut crate::planner::PlanArena<'p>,
    ) -> Self {
        Self { select_list, arena }
    }

    fn output_ref(&mut self, expr: &ScalarExpression) -> Option<ScalarExpression> {
        self.select_list
            .iter()
            .position(|candidate| {
                candidate.eq_ignore_colref_pos(expr, self.arena)
                    || candidate
                        .unpack_alias_ref()
                        .eq_ignore_colref_pos(expr.unpack_alias_ref(), self.arena)
            })
            .map(|position| {
                let output_expr = &self.select_list[position];
                ScalarExpression::column_expr(output_expr.output_column_ref(self.arena), position)
            })
    }
}

impl<'a> VisitorMut<'a> for DistinctOutputBinder<'_, '_> {
    fn visit(&mut self, expr: &'a mut ScalarExpression) -> Result<(), DatabaseError> {
        if let ScalarExpression::Alias {
            expr: inner_expr,
            alias: crate::expression::AliasType::Name(_),
        } = expr
        {
            return self.visit(inner_expr);
        }

        if let Some(output_ref) = self.output_ref(expr) {
            *expr = output_ref;
            return Ok(());
        }
        walk_mut_expr(self, expr)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::DistinctOutputBinder;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::expression::visitor_mut::VisitorMut;
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

        let left_expr = ScalarExpression::column_expr(left_column, 0);
        let right_expr = ScalarExpression::column_expr(right_column, 1);
        let second_output = right_expr.clone();
        let select_output = ScalarExpression::Alias {
            expr: Box::new(left_expr.clone()),
            alias: AliasType::Name("v".to_string()),
        };
        let select_list = [select_output.clone(), right_expr.clone()];

        let mut order_by_alias = ScalarExpression::Alias {
            expr: Box::new(left_expr),
            alias: AliasType::Name("v".to_string()),
        };
        let mut order_by_second = right_expr;
        {
            let mut binder = DistinctOutputBinder::new(&select_list, &mut arena);
            binder.visit(&mut order_by_alias)?;
            binder.visit(&mut order_by_second)?;
        }
        let expected_alias = ScalarExpression::Alias {
            expr: Box::new(ScalarExpression::column_expr(
                select_output.output_column_ref(&mut arena),
                0,
            )),
            alias: AliasType::Name("v".to_string()),
        };
        assert!(order_by_alias.eq_ignore_colref_pos(&expected_alias, &arena));

        let expected_second =
            ScalarExpression::column_expr(second_output.output_column_ref(&mut arena), 1);
        assert!(order_by_second.eq_ignore_colref_pos(&expected_second, &arena));

        Ok(())
    }

    #[test]
    fn test_distinct_output_binder_matches_alias_expr_reference() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let column = test_column(&mut arena, "c1", LogicalType::Integer);
        let expr = ScalarExpression::column_expr(column, 0);
        let select_output = ScalarExpression::Alias {
            expr: Box::new(expr.clone()),
            alias: AliasType::Name("v".to_string()),
        };

        let mut target = ScalarExpression::Alias {
            expr: Box::new(ScalarExpression::Constant(1_i32.into())),
            alias: AliasType::Expr(Box::new(expr)),
        };

        {
            let mut binder =
                DistinctOutputBinder::new(std::slice::from_ref(&select_output), &mut arena);
            binder.visit(&mut target)?;
        }
        let expected =
            ScalarExpression::column_expr(select_output.output_column_ref(&mut arena), 0);
        assert!(target.eq_ignore_colref_pos(&expected, &arena));

        Ok(())
    }
}
