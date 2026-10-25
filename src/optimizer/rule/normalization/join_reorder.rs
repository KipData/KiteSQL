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
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::expression::ScalarExpression;
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::tuple::Schema;

pub(crate) struct GreedyJoinReorder;

fn estimate_join_rows(left: usize, right: usize) -> usize {
    // TODO: For INNER equijoins, use left_rows * right_rows / max(left_ndv, right_ndv)
    // and cap the estimate with join-key uniqueness. Uniqueness bounds matches;
    // it does not guarantee every input row finds a match.
    left.saturating_mul(right)
}

fn reorderable(plan: &LogicalPlan) -> bool {
    matches!(&plan.operator, Operator::Join(JoinOperator {
        join_type: JoinType::Inner,
        on: JoinCondition::On { on, filter: None },
        force_nested_loop: false,
        limit_pushed: false,
    }) if !on.is_empty())
}

fn inspect<'plan>(
    plan: &'plan mut LogicalPlan,
    schemas: &mut Vec<&'plan Schema>,
    keys: &mut Vec<(ExprRef, ExprRef)>,
    arena: &mut PlanArena,
) -> Option<usize> {
    if reorderable(plan) {
        let (
            Operator::Join(JoinOperator {
                on: JoinCondition::On { on, .. },
                ..
            }),
            Childrens::Twins { left, right },
        ) = (&plan.operator, plan.childrens.as_mut())
        else {
            unreachable!()
        };
        keys.extend_from_slice(on);
        let left_rows = inspect(left, schemas, keys, arena)?;
        let right_rows = inspect(right, schemas, keys, arena)?;
        let rows = estimate_join_rows(left_rows, right_rows);
        if let Some(option) = &mut plan.physical_option {
            option.row_count = RowCountOption::Estimated(rows);
        }
        Some(rows)
    } else {
        let rows = plan.estimated_rows()?;
        schemas.push(plan.output_schema(arena));
        Some(rows)
    }
}

fn flatten(plan: LogicalPlan, inputs: &mut Vec<(LogicalPlan, usize)>) {
    if reorderable(&plan) {
        let (left, right) = plan.childrens.pop_twins();
        flatten(left, inputs);
        flatten(right, inputs);
    } else {
        // SAFETY: inspect has already verified every input's row estimate before detaching the region.
        let rows = plan.estimated_rows().unwrap();
        inputs.push((plan, rows));
    }
}

fn key_in_schema(expr: ExprRef, schema: &Schema, arena: &PlanArena) -> Result<bool, DatabaseError> {
    expr.all_referenced_columns(arena, |arena, column| {
        schema
            .iter()
            .any(|candidate| arena.same_column(*candidate, *column))
    })
}

struct BindPosition<'a>(&'a Schema);

impl ExprVisitorMut for BindPosition<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        *position = self
            .0
            .iter()
            .position(|candidate| arena.same_column(*candidate, *column))
            .ok_or(DatabaseError::SourceNotFound)?;
        Ok(())
    }
}

impl NormalizationRule for GreedyJoinReorder {
    fn apply(&self, plan: &mut LogicalPlan, arena: &mut PlanArena) -> Result<bool, DatabaseError> {
        if !reorderable(plan) {
            return Ok(false);
        }
        let original_schema = plan.output_schema(arena).clone();
        let mut schemas = Vec::new();
        let mut keys = Vec::new();
        if inspect(plan, &mut schemas, &mut keys, arena).is_none() || schemas.len() < 3 {
            return Ok(false);
        }
        // Ambiguous column identities cannot be rebound to an individual input.
        for (i, left) in schemas.iter().enumerate() {
            for right in &schemas[i + 1..] {
                if left
                    .iter()
                    .any(|l| right.iter().any(|r| arena.same_column(*l, *r)))
                {
                    return Ok(false);
                }
            }
        }
        // Constants and keys spanning several inputs need separate predicate handling.
        for &(left, right) in &keys {
            let mut endpoints = [None, None];
            for (endpoint, expr) in endpoints.iter_mut().zip([left, right]) {
                if !expr.any_referenced_column(arena, |_, _| true)? {
                    return Ok(false);
                }
                for (index, schema) in schemas.iter().enumerate() {
                    if key_in_schema(expr, schema, arena)? {
                        *endpoint = Some(index);
                        break;
                    }
                }
                if endpoint.is_none() {
                    return Ok(false);
                }
            }
            if endpoints[0] == endpoints[1] {
                return Ok(false);
            }
        }
        let mut inputs = Vec::with_capacity(schemas.len());
        flatten(plan.take(), &mut inputs);
        while inputs.len() > 1 {
            let mut best = None;
            for (i, (_, left)) in inputs.iter().enumerate() {
                for (j, (_, right)) in inputs.iter().enumerate().skip(i + 1) {
                    let candidate = (estimate_join_rows(*left, *right), i, j);
                    if best.is_none_or(|previous| candidate < previous) {
                        best = Some(candidate);
                    }
                }
            }
            let Some((product, i, j)) = best else {
                return Ok(false);
            };
            let (mut right, _) = inputs.remove(j);
            let (mut left, _) = inputs.remove(i);
            let left_schema = left.output_schema(arena);
            let right_schema = right.output_schema(arena);
            let mut on = Vec::new();
            let mut index = 0;
            while index < keys.len() {
                let (l, r) = keys[index];
                let key = if key_in_schema(l, left_schema, arena)?
                    && key_in_schema(r, right_schema, arena)?
                {
                    Some((l, r))
                } else if key_in_schema(r, left_schema, arena)?
                    && key_in_schema(l, right_schema, arena)?
                {
                    Some((r, l))
                } else {
                    None
                };
                if let Some(key) = key {
                    keys.remove(index);
                    on.push(key);
                } else {
                    index += 1;
                }
            }
            for (l, r) in &mut on {
                *l = l.clone_expression(arena)?;
                *r = r.clone_expression(arena)?;
                BindPosition(left_schema).visit(l, arena)?;
                BindPosition(right_schema).visit(r, arena)?;
            }
            let (condition, join_type, implementation) = if on.is_empty() {
                (JoinCondition::None, JoinType::Cross, PlanImpl::NestLoopJoin)
            } else {
                (
                    JoinCondition::On { on, filter: None },
                    JoinType::Inner,
                    PlanImpl::HashJoin,
                )
            };
            let mut join = JoinOperator::build(left, right, condition, join_type, false);
            join.physical_option = Some(PhysicalOption::new(
                implementation,
                SortOption::None,
                RowCountOption::Estimated(product),
            ));
            inputs.insert(i, (join, product));
        }
        // SAFETY: At this point, there should be exactly one plan left in the inputs vector, which is the final join result.
        let (mut join, _) = inputs.pop().unwrap();
        let schema = join.output_schema(arena);
        if &original_schema == schema {
            *plan = join;
            return Ok(true);
        }
        let exprs = original_schema
            .iter()
            .map(|column| {
                let position = schema
                    .iter()
                    .position(|candidate| arena.same_column(*candidate, *column))
                    .unwrap();
                arena.alloc_expression(ScalarExpression::column_expr(*column, position))
            })
            .collect();
        *plan = LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs }),
            Childrens::Only(Box::new(join)),
        );
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        Ok(true)
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::TableArenaCell;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn input(name: &str, rows: usize, arena: &mut PlanArena) -> (LogicalPlan, ExprRef) {
        let column = arena.alloc_column(ColumnCatalog::new(
            name.into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let key = arena.alloc_expression(ScalarExpression::column_expr(column, 0));
        let mut plan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: name.into(),
                columns: vec![column],
                limit: (None, None),
                index_infos: Vec::new(),
                with_pk: false,
            }),
            Childrens::None,
        );
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated(rows),
        ));
        (plan, key)
    }

    fn three_inputs(arena: &mut PlanArena) -> LogicalPlan {
        let (a, a_key) = input("a", 1000, arena);
        let (b, b_key) = input("b", 10, arena);
        let (c, c_key) = input("c", 20, arena);
        let ab = JoinOperator::build(
            a,
            b,
            JoinCondition::On {
                on: vec![(a_key, b_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let b_column = b_key.output_column_ref(arena);
        let b_key = arena.alloc_expression(ScalarExpression::column_expr(b_column, 1));
        JoinOperator::build(
            ab,
            c,
            JoinCondition::On {
                on: vec![(b_key, c_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        )
    }

    #[test]
    fn smallest_connected_product_preserves_schema_and_key_positions() -> Result<(), DatabaseError>
    {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut plan = three_inputs(&mut arena);
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.estimated_rows(), Some(200000));
        let (left, right) = plan.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["a".into()]);
        assert_eq!(right.referenced_table(), vec!["b".into(), "c".into()]);
        assert_eq!(right.estimated_rows(), Some(200));
        let Operator::Join(op) = &right.operator else {
            unreachable!()
        };
        let JoinCondition::On { on, .. } = &op.on else {
            unreachable!()
        };
        for expr in [on[0].0, on[0].1] {
            assert!(matches!(
                arena.expression(expr),
                ScalarExpression::ColumnRef { position: 0, .. }
            ));
        }
        Ok(())
    }

    #[test]
    fn nonadjacent_pair_restores_output_order_and_keeps_all_keys() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (a, a_key) = input("a", 10, &mut arena);
        let (b, b_key) = input("b", 1000, &mut arena);
        let (c, c_key) = input("c", 20, &mut arena);
        let ab = JoinOperator::build(
            a,
            b,
            JoinCondition::On {
                on: vec![(a_key, b_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let b_column = b_key.output_column_ref(&mut arena);
        let b_key = arena.alloc_expression(ScalarExpression::column_expr(b_column, 1));
        let mut plan = JoinOperator::build(
            ab,
            c,
            JoinCondition::On {
                on: vec![(a_key, c_key), (b_key, c_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        let Operator::Project(project) = &plan.operator else {
            unreachable!()
        };
        for (expr, expected) in project.exprs.iter().zip([0, 2, 1]) {
            assert!(
                matches!(arena.expression(*expr), ScalarExpression::ColumnRef { position, .. } if *position == expected)
            );
        }
        let join = plan.childrens.only();
        let (left, right) = join.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["a".into(), "c".into()]);
        assert_eq!(right.referenced_table(), vec!["b".into()]);
        let Operator::Join(op) = &join.operator else {
            unreachable!()
        };
        let JoinCondition::On { on, .. } = &op.on else {
            unreachable!()
        };
        assert_eq!(on.len(), 2);
        let Operator::Join(op) = &left.operator else {
            unreachable!()
        };
        let JoinCondition::On { on, .. } = &op.on else {
            unreachable!()
        };
        assert_eq!(on.len(), 1);
        Ok(())
    }

    #[test]
    fn smallest_pair_without_a_key_defers_both_conditions() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut plan = three_inputs(&mut arena);
        if let Childrens::Twins { left, .. } = plan.childrens.as_mut() {
            if let Childrens::Twins { left, right } = left.childrens.as_mut() {
                left.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated(10);
                right.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated(1000);
            }
        }
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        let join = plan.childrens.only();
        let (left, right) = join.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["a".into(), "c".into()]);
        assert_eq!(right.referenced_table(), vec!["b".into()]);
        assert!(matches!(
            &left.operator,
            Operator::Join(JoinOperator {
                join_type: JoinType::Cross,
                on: JoinCondition::None,
                ..
            })
        ));
        assert_eq!(
            left.physical_option.as_ref().unwrap().plan,
            PlanImpl::NestLoopJoin
        );
        let Operator::Join(op) = &join.operator else {
            unreachable!()
        };
        let JoinCondition::On { on, .. } = &op.on else {
            unreachable!()
        };
        assert_eq!(on.len(), 2);
        Ok(())
    }

    #[test]
    fn rebuilding_join_region_preserves_parent_and_input_subtrees() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut join = three_inputs(&mut arena);
        let Childrens::Twins { right, .. } = join.childrens.as_mut() else {
            unreachable!()
        };
        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let mut filter = FilterOperator::build(predicate, right.take(), false);
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Estimated(20),
        ));
        **right = filter;
        let schema = join.output_schema(&mut arena).clone();
        let exprs = schema
            .iter()
            .enumerate()
            .map(|(position, column)| {
                arena.alloc_expression(ScalarExpression::column_expr(*column, position))
            })
            .collect();
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs }),
            Childrens::Only(Box::new(join)),
        );
        let original_parent = plan.operator.clone();
        let Childrens::Only(join) = plan.childrens.as_mut() else {
            unreachable!()
        };
        assert!(GreedyJoinReorder.apply(join, &mut arena)?);
        assert_eq!(plan.operator, original_parent);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        let (_, right) = plan.childrens.only().childrens.twins();
        let (_, filter) = right.childrens.twins();
        assert!(matches!(filter.operator, Operator::Filter(_)));
        assert_eq!(filter.childrens.only().referenced_table(), vec!["c".into()]);
        assert_eq!(filter.estimated_rows(), Some(20));
        Ok(())
    }

    #[test]
    fn unknown_inputs_and_outer_joins_keep_original_order() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut plan = three_inputs(&mut arena);
        if let Childrens::Twins { right, .. } = plan.childrens.as_mut() {
            right.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
        }
        let original = plan.clone();
        assert!(!GreedyJoinReorder.apply(&mut plan, &mut arena)?);
        assert_eq!(plan, original);
        if let Operator::Join(op) = &mut plan.operator {
            op.join_type = JoinType::LeftOuter;
        }
        let original = plan.clone();
        assert!(!GreedyJoinReorder.apply(&mut plan, &mut arena)?);
        assert_eq!(plan, original);
        let mut plan = three_inputs(&mut arena);
        if let Childrens::Twins { left, right } = plan.childrens.as_mut() {
            right.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated(10);
            if let Childrens::Twins { left, .. } = left.childrens.as_mut() {
                left.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated(10);
            }
        }
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        let (left, right) = plan.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["a".into(), "b".into()]);
        assert_eq!(right.referenced_table(), vec!["c".into()]);
        assert_eq!(estimate_join_rows(usize::MAX, 2), usize::MAX);
        assert_eq!(estimate_join_rows(0, 20), 0);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
