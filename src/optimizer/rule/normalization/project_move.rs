// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::DatabaseError;
use crate::expression::function::scala::ScalarFunction;
use crate::expression::visitor::ExprVisitor;
use crate::expression::{AliasType, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::bitset::BitSet;

pub struct ProjectMove;

struct InputUses {
    counts: Vec<u8>,
    invalid: bool,
}

impl<A: MetaArena + ?Sized> ExprVisitor<A> for InputUses {
    fn visit_expression_ref(&mut self, expr: ExprRef, arena: &A) -> Result<bool, DatabaseError> {
        if let ScalarExpression::ColumnRef { position, .. } = arena.expression(expr) {
            if let Some(count) = self.counts.get_mut(*position) {
                *count = count.saturating_add(1).min(2);
            } else {
                self.invalid = true;
            }
            return Ok(false);
        }
        Ok(true)
    }

    fn visit_alias(
        &mut self,
        expr: ExprRef,
        alias: &AliasType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        let expr = match alias {
            AliasType::Expr(expr) => *expr,
            AliasType::Name(_) => expr,
        };
        self.visit(expr, arena)
    }

    fn visit_scala_function(
        &mut self,
        _function: &ScalarFunction,
        _arena: &A,
    ) -> Result<(), DatabaseError> {
        // Functions can read Input directly, beyond their declared arguments.
        self.invalid = true;
        Ok(())
    }
}

impl NormalizationRule for ProjectMove {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let (Operator::Project(op), Childrens::Only(input)) =
            (&mut plan.operator, plan.childrens.as_mut())
        else {
            return Ok(false);
        };
        let width = input.output_schema(arena).len();
        let mut uses = InputUses {
            counts: vec![0; width],
            invalid: false,
        };
        for expr in &op.exprs {
            uses.visit(*expr, arena)?;
        }
        if uses.invalid || !uses.counts.contains(&1) {
            return Ok(op.moveable.take().is_some());
        }
        let changed = op.moveable.as_ref().is_none_or(|mask| {
            uses.counts
                .iter()
                .enumerate()
                .any(|(position, count)| mask.contains(position) != (*count == 1))
        });
        let moveable = op
            .moveable
            .get_or_insert_with(|| BitSet::with_capacity(width));
        moveable.clear();
        for (position, count) in uses.counts.iter().enumerate() {
            if *count == 1 {
                moveable.insert(position);
            }
        }
        Ok(changed)
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::function::scala::ArcScalarFunctionImpl;
    use crate::expression::BinaryOperator;
    use crate::function::upper::Upper;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::TableArenaCell;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn input(arena: &mut PlanArena) -> Result<(LogicalPlan, [ExprRef; 3]), DatabaseError> {
        let mut columns = Vec::new();
        let mut exprs = Vec::new();
        for position in 0..3 {
            let column = arena.alloc_column(ColumnCatalog::new(
                format!("c{position}"),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
            ));
            columns.push(column);
            exprs.push(arena.alloc_expression(ScalarExpression::column_expr(column, position)));
        }
        Ok((
            LogicalPlan::new(
                Operator::Values(ValuesOperator::new(Vec::new(), 0, columns)),
                Childrens::None,
            ),
            exprs.try_into().unwrap(),
        ))
    }

    fn project(plan: &LogicalPlan) -> &ProjectOperator {
        let Operator::Project(op) = &plan.operator else {
            unreachable!()
        };
        op
    }

    #[test]
    fn counts_nested_and_repeated_positions_and_reuses_bitmap() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (source, [a, b, c]) = input(&mut arena)?;
        let one = arena.alloc_expression(DataValue::Int32(1).into());
        let sum = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Plus,
            left_expr: b,
            right_expr: one,
            evaluator: None,
            ty: LogicalType::Integer,
        });
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![c, a, a, sum])),
            Childrens::Only(Box::new(source)),
        );
        assert!(project(&plan).moveable.is_none());
        assert!(ProjectMove.apply(&mut plan, &mut arena, None)?);
        assert!(!project(&plan).moveable.as_ref().unwrap().contains(0));
        assert!(project(&plan).moveable.as_ref().unwrap().contains(1));
        assert!(project(&plan).moveable.as_ref().unwrap().contains(2));
        assert!(!ProjectMove.apply(&mut plan, &mut arena, None)?);

        let Operator::Project(op) = &mut plan.operator else {
            unreachable!()
        };
        op.exprs = vec![b, b];
        assert!(ProjectMove.apply(&mut plan, &mut arena, None)?);
        assert!(project(&plan).moveable.is_none());
        assert!(!ProjectMove.apply(&mut plan, &mut arena, None)?);
        Ok(())
    }

    #[test]
    fn counts_only_the_evaluated_alias_branch() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (source, [a, _, c]) = input(&mut arena)?;
        let named = arena.alloc_expression(ScalarExpression::Alias {
            expr: a,
            alias: AliasType::Name("named".into()),
        });
        let mapped = arena.alloc_expression(ScalarExpression::Alias {
            expr: a,
            alias: AliasType::Expr(c),
        });
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![named, mapped])),
            Childrens::Only(Box::new(source)),
        );
        assert!(ProjectMove.apply(&mut plan, &mut arena, None)?);
        assert!(project(&plan).moveable.as_ref().unwrap().contains(0));
        assert!(!project(&plan).moveable.as_ref().unwrap().contains(1));
        assert!(project(&plan).moveable.as_ref().unwrap().contains(2));
        let Operator::Project(op) = &mut plan.operator else {
            unreachable!()
        };
        op.exprs.push(c);
        assert!(ProjectMove.apply(&mut plan, &mut arena, None)?);
        assert!(project(&plan).moveable.as_ref().unwrap().contains(0));
        assert!(!project(&plan).moveable.as_ref().unwrap().contains(2));
        Ok(())
    }

    #[test]
    fn unknown_function_reads_and_invalid_positions_clear_previous_marks(
    ) -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (source, [a, b, c]) = input(&mut arena)?;
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![a, b, c])),
            Childrens::Only(Box::new(source)),
        );
        assert!(ProjectMove.apply(&mut plan, &mut arena, None)?);
        let function = arena.alloc_expression(ScalarExpression::ScalaFunction(ScalarFunction {
            args: vec![a],
            inner: ArcScalarFunctionImpl(Upper::new()),
        }));
        let Operator::Project(op) = &mut plan.operator else {
            unreachable!()
        };
        op.exprs = vec![function, b];
        assert!(ProjectMove.apply(&mut plan, &mut arena, None)?);
        assert!(project(&plan).moveable.is_none());
        assert!(!ProjectMove.apply(&mut plan, &mut arena, None)?);

        let ScalarExpression::ColumnRef { column, .. } = arena.expression(a) else {
            unreachable!()
        };
        let invalid = arena.alloc_expression(ScalarExpression::column_expr(*column, 3));
        let Operator::Project(op) = &mut plan.operator else {
            unreachable!()
        };
        op.exprs = vec![invalid, b];
        assert!(!ProjectMove.apply(&mut plan, &mut arena, None)?);
        assert!(project(&plan).moveable.is_none());
        Ok(())
    }
}
// GRCOV_EXCL_STOP
