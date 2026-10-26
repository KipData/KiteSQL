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

use super::NormalizationRule;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator};
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan, PlanArena};

pub(crate) struct MaterializeMarkApply;

impl MaterializeMarkApply {
    fn can_materialize(op: &MarkApplyOperator, arena: &PlanArena) -> bool {
        // The first predicate compares values; additional predicates bind outer rows.
        // The binder builds that comparison as `outer_operand op subquery_output`,
        // so the RHS keys can be cached by evaluating its right side alone.
        matches!(op.kind, MarkApplyKind::Quantified(_))
            && op.parameterized_probe().is_none()
            && matches!(
                op.predicates(),
                [predicate] if matches!(arena.expression(*predicate), ScalarExpression::Binary { .. })
            )
    }
}

impl NormalizationRule for MaterializeMarkApply {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let (Operator::MarkApply(op), Childrens::Twins { left, right }) =
            (&mut plan.operator, plan.childrens.as_ref())
        else {
            return Ok(false);
        };
        if op.materialize || !Self::can_materialize(op, arena) {
            return Ok(false);
        }
        // Streaming rebuilds the RHS once per left row, which nested subqueries multiply,
        // so materialize by default. Stream only when estimates show the cache would not
        // pay off: no repeated left input, or fewer rebuilds than retained RHS rows.
        let stream = match (left.estimated_rows(), right.estimated_rows()) {
            (Some(left_rows), _) if left_rows <= 1 => true,
            (Some(left_rows), Some(right_rows)) => left_rows < right_rows,
            _ => false,
        };
        if stream {
            return Ok(false);
        }
        op.materialize = true;
        Ok(true)
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::BinaryOperator;
    use crate::planner::operator::mark_apply::MarkApplyQuantifier;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::{PhysicalOption, PlanImpl, RowCountOption, SortOption};
    use crate::planner::TableArenaCell;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn selects_materialization_without_changing_probe_or_correlated_paths(
    ) -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let key = arena.alloc_column(ColumnCatalog::new(
            "key".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let marker = arena.alloc_column(ColumnCatalog::new(
            "marker".to_string(),
            true,
            ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
        ));
        let right_key = arena.alloc_column(ColumnCatalog::new(
            "right_key".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let value = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(2)));
        let left = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(vec![value; 8], 8, vec![key])),
            Childrens::None,
        );
        let right = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(vec![value; 2], 2, vec![right_key])),
            Childrens::None,
        );
        let left_ref = arena.alloc_expression(ScalarExpression::column_expr(key, 0));
        let right_ref = arena.alloc_expression(ScalarExpression::column_expr(right_key, 1));
        let compare = |arena: &mut PlanArena, left_expr, right_expr| {
            arena.alloc_expression(ScalarExpression::Binary {
                op: BinaryOperator::Eq,
                left_expr,
                right_expr,
                evaluator: None,
                ty: LogicalType::Boolean,
            })
        };
        let predicate = compare(&mut arena, left_ref, right_ref);
        let constant = arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let op =
            MarkApplyOperator::new_quantified(MarkApplyQuantifier::Any, marker, vec![predicate]);
        let mut plan = LogicalPlan::new(
            Operator::MarkApply(op),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        for (left_rows, right_rows, materialize) in [
            (Some(0), Some(2), false),
            (Some(1), Some(0), false),
            (Some(1), None, false),
            (Some(2), Some(8), false),
            (Some(2), Some(2), true),
            (Some(8), Some(2), true),
            (None, None, true),
            (Some(8), None, true),
        ] {
            let Operator::MarkApply(op) = &mut plan.operator else {
                unreachable!()
            };
            op.materialize = false;
            let Childrens::Twins { left, right } = plan.childrens.as_mut() else {
                unreachable!()
            };
            let estimated = |rows: Option<usize>| {
                rows.map(|rows| {
                    PhysicalOption::new(
                        PlanImpl::Values,
                        SortOption::None,
                        RowCountOption::Estimated { rows, ndv: None },
                    )
                })
            };
            left.physical_option = estimated(left_rows);
            right.physical_option = estimated(right_rows);
            assert_eq!(
                MaterializeMarkApply.apply(&mut plan, &mut arena, None)?,
                materialize
            );
        }
        assert!(!MaterializeMarkApply.apply(&mut plan, &mut arena, None)?);
        let Operator::MarkApply(op) = &mut plan.operator else {
            unreachable!()
        };
        assert!(op.materialize);
        op.materialize = false;
        op.set_parameterized_probe(Some(predicate));
        assert!(!MaterializeMarkApply.apply(&mut plan, &mut arena, None)?);
        let Operator::MarkApply(op) = &mut plan.operator else {
            unreachable!()
        };
        op.set_parameterized_probe(None);
        op.predicates.push(predicate);
        assert!(!MaterializeMarkApply.apply(&mut plan, &mut arena, None)?);
        let Operator::MarkApply(op) = &mut plan.operator else {
            unreachable!()
        };
        op.predicates.pop();
        // Only a binary comparison exposes the RHS side whose keys are cached.
        let Operator::MarkApply(op) = &mut plan.operator else {
            unreachable!()
        };
        op.predicates[0] = constant;
        assert!(!MaterializeMarkApply.apply(&mut plan, &mut arena, None)?);
        let Operator::MarkApply(op) = &mut plan.operator else {
            unreachable!()
        };
        op.predicates[0] = predicate;
        Ok(())
    }
}
// GRCOV_EXCL_STOP
