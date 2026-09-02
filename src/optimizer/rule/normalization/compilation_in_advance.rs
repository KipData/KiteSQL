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

use crate::errors::DatabaseError;
use crate::expression::BindEvaluator;
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::visitor_mut::{OperatorExprVisitorMut, OperatorVisitorMut};
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan, PlanArena};

#[derive(Clone)]
pub struct EvaluatorBind;

pub(crate) fn evaluator_bind_current(
    plan: &mut LogicalPlan,
    arena: &mut PlanArena,
) -> Result<(), DatabaseError> {
    let mut evaluator = BindEvaluator;
    OperatorExprVisitorMut::new(&mut evaluator, arena).visit_operator(&mut plan.operator)
}

impl EvaluatorBind {
    fn _apply(plan: &mut LogicalPlan, arena: &mut PlanArena) -> Result<(), DatabaseError> {
        match plan.childrens.as_mut() {
            Childrens::Only(child) => Self::_apply(child, arena)?,
            Childrens::Twins { left, right } => {
                Self::_apply(left, arena)?;
                let bind_right = matches!(
                    plan.operator,
                    Operator::ScalarApply(_)
                        | Operator::MarkApply(_)
                        | Operator::Join(_)
                        | Operator::Union(_)
                        | Operator::SetMembership(_)
                );
                let bind_right = bind_right || matches!(plan.operator, Operator::RecursiveCte(_));
                if bind_right {
                    Self::_apply(right, arena)?;
                }
            }
            Childrens::None => {}
        }

        evaluator_bind_current(plan, arena)
    }
}

impl NormalizationRule for EvaluatorBind {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        Self::_apply(plan, arena)?;
        Ok(true)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::function::table::{
        ArcTableFunctionImpl, TableFunction, TableFunctionCatalog, TableFunctionImpl,
    };
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::function::numbers::Numbers;
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::function_scan::FunctionScanOperator;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::mark_apply::MarkApplyOperator;
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::top_k::TopKOperator;
    use crate::planner::operator::union::UnionOperator;
    use crate::planner::operator::update::UpdateOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{ExprRef, TableArenaCell};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn unbound_binary(arena: &mut PlanArena, column: crate::catalog::ColumnRef) -> ExprRef {
        let left_expr = arena.alloc_expression(ScalarExpression::column_expr(column, 0));
        let right_expr = arena.alloc_expression(DataValue::Int32(1).into());
        arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Plus,
            left_expr,
            right_expr,
            evaluator: None,
            ty: LogicalType::Integer,
        })
    }

    fn is_bound(expr: ExprRef, arena: &PlanArena) -> bool {
        matches!(
            arena.expression(expr),
            ScalarExpression::Binary {
                evaluator: Some(_),
                ..
            }
        )
    }

    #[test]
    fn binds_each_expression_container() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let numbers = Numbers::new();
        let mut schema = Vec::new();
        numbers.output_schema_into(table_arena.borrow_mut(), &mut schema);
        let mut arena = PlanArena::new(&table_arena);
        let column = arena.alloc_column(ColumnCatalog::new(
            "value".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let filter_expr = unbound_binary(&mut arena, column);
        let sort_expr = unbound_binary(&mut arena, column);
        let top_k_expr = unbound_binary(&mut arena, column);
        let mark_expr = unbound_binary(&mut arena, column);
        let update_expr = unbound_binary(&mut arena, column);
        let function_expr = unbound_binary(&mut arena, column);

        let mut operators = vec![
            Operator::Filter(FilterOperator {
                predicate: filter_expr,
                is_optimized: false,
                having: false,
            }),
            Operator::Sort(SortOperator {
                sort_fields: vec![SortField::from(sort_expr)],
            }),
            Operator::TopK(TopKOperator {
                sort_fields: vec![SortField::from(top_k_expr)],
                limit: 1,
                offset: None,
            }),
            Operator::MarkApply(MarkApplyOperator::new_exists(column, vec![mark_expr])),
            Operator::Update(UpdateOperator {
                table_name: "t1".into(),
                value_exprs: vec![(column, update_expr)],
            }),
        ];

        operators.push(Operator::FunctionScan(FunctionScanOperator {
            table_function: TableFunction {
                args: vec![function_expr],
                catalog: TableFunctionCatalog {
                    schema,
                    inner: ArcTableFunctionImpl(numbers),
                },
            },
        }));

        for operator in &mut operators {
            let mut plan = LogicalPlan::new(operator.clone(), Childrens::None);
            evaluator_bind_current(&mut plan, &mut arena)?;
            match &plan.operator {
                Operator::Filter(op) => assert!(is_bound(op.predicate, &arena)),
                Operator::Sort(op) => assert!(is_bound(op.sort_fields[0].expr, &arena)),
                Operator::TopK(op) => assert!(is_bound(op.sort_fields[0].expr, &arena)),
                Operator::MarkApply(op) => assert!(is_bound(op.predicates()[0], &arena)),
                Operator::Update(op) => assert!(is_bound(op.value_exprs[0].1, &arena)),
                Operator::FunctionScan(op) => assert!(is_bound(op.table_function.args[0], &arena)),
                _ => unreachable!(),
            }
        }

        Ok(())
    }

    #[test]
    fn binds_join_conditions_and_selected_twin_subtrees() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let column = arena.alloc_column(ColumnCatalog::new(
            "value".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        fn filter(arena: &mut PlanArena, column: crate::catalog::ColumnRef) -> LogicalPlan {
            let predicate = unbound_binary(arena, column);
            LogicalPlan::new(
                Operator::Filter(FilterOperator {
                    predicate,
                    is_optimized: false,
                    having: false,
                }),
                Childrens::None,
            )
        }

        let join_left = unbound_binary(&mut arena, column);
        let join_right = unbound_binary(&mut arena, column);
        let join_filter = unbound_binary(&mut arena, column);
        let left_filter = filter(&mut arena, column);
        let right_filter = filter(&mut arena, column);

        let mut join = LogicalPlan::new(
            Operator::Join(JoinOperator {
                join_type: JoinType::Inner,
                force_nested_loop: false,
                on: JoinCondition::On {
                    on: vec![(join_left, join_right)],
                    filter: Some(join_filter),
                },
            }),
            Childrens::Twins {
                left: Box::new(left_filter),
                right: Box::new(right_filter),
            },
        );
        assert!(EvaluatorBind.apply(&mut join, &mut arena)?);
        let Operator::Join(op) = &join.operator else {
            unreachable!()
        };
        let JoinCondition::On {
            on,
            filter: join_filter,
        } = &op.on
        else {
            unreachable!()
        };
        assert!(is_bound(on[0].0, &arena));
        assert!(is_bound(on[0].1, &arena));
        assert!(is_bound(*join_filter.as_ref().unwrap(), &arena));
        assert!(join.childrens.iter().all(|child| {
            matches!(&child.operator, Operator::Filter(op) if is_bound(op.predicate, &arena))
        }));

        let union_left = filter(&mut arena, column);
        let union_right = filter(&mut arena, column);
        let mut union = UnionOperator::build(vec![column], vec![column], union_left, union_right);
        EvaluatorBind.apply(&mut union, &mut arena)?;
        assert!(union.childrens.iter().all(|child| {
            matches!(&child.operator, Operator::Filter(op) if is_bound(op.predicate, &arena))
        }));

        Ok(())
    }
}
