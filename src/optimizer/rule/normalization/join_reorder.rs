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
use std::mem::swap;

pub(crate) struct GreedyJoinReorder;
pub(crate) struct OrientNestedLoopJoin;

impl NormalizationRule for OrientNestedLoopJoin {
    fn apply(&self, plan: &mut LogicalPlan, arena: &mut PlanArena) -> Result<bool, DatabaseError> {
        let (
            Operator::Join(JoinOperator {
                join_type: JoinType::Cross,
                on: JoinCondition::None,
                limit_pushed: false,
                ..
            }),
            Childrens::Twins { left, right },
            Some(PhysicalOption {
                plan: PlanImpl::NestLoopJoin,
                ..
            }),
        ) = (
            &plan.operator,
            plan.childrens.as_ref(),
            plan.physical_option.as_ref(),
        )
        else {
            return Ok(false);
        };
        if left.has_materialization() || !right.has_materialization() {
            return Ok(false);
        }
        let original_schema = plan.output_schema(arena).clone();
        for (index, column) in original_schema.iter().enumerate() {
            if original_schema[index + 1..]
                .iter()
                .any(|other| arena.same_column(*column, *other))
            {
                return Ok(false);
            }
        }
        let mut join = plan.take();
        let Childrens::Twins { left, right } = join.childrens.as_mut() else {
            unreachable!()
        };
        swap(left, right);
        join.reset_output_schema_cache();
        *plan = restore_join_output(join, original_schema, arena);
        Ok(true)
    }
}

fn restore_join_output(
    mut join: LogicalPlan,
    original_schema: Schema,
    arena: &mut PlanArena,
) -> LogicalPlan {
    let schema = join.output_schema(arena);
    if &original_schema == schema {
        return join;
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
    let mut plan = LogicalPlan::new(
        Operator::Project(ProjectOperator { exprs }),
        Childrens::Only(Box::new(join)),
    );
    plan.physical_option = Some(PhysicalOption::new(
        PlanImpl::Project,
        SortOption::Follow,
        RowCountOption::Follow,
    ));
    plan
}

// Prefer equijoins, then fewer rows, then the original input order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct JoinCandidate {
    cross_join: bool,
    rows: usize,
    left: usize,
    right: usize,
}

fn estimate_join_rows(left: usize, right: usize) -> usize {
    // TODO: For INNER equijoins, use left_rows * right_rows / max(left_ndv, right_ndv)
    // and cap the estimate with join-key uniqueness. Uniqueness bounds matches;
    // it does not guarantee every input row finds a match.
    left.saturating_mul(right)
}

fn can_expand(plan: &LogicalPlan) -> bool {
    let Operator::Join(JoinOperator {
        join_type,
        on,
        force_nested_loop: false,
        limit_pushed: false,
    }) = &plan.operator
    else {
        return false;
    };
    matches!((join_type, on), (JoinType::Cross, JoinCondition::None))
        || matches!((join_type, on),
            (JoinType::Inner, JoinCondition::On { on, filter: None }) if !on.is_empty())
}

fn inspect<'plan>(
    plan: &'plan mut LogicalPlan,
    schemas: &mut Vec<&'plan Schema>,
    keys: &mut Vec<(ExprRef, ExprRef)>,
    arena: &mut PlanArena,
) -> Option<usize> {
    if can_expand(plan) {
        let (Operator::Join(join), Childrens::Twins { left, right }) =
            (&plan.operator, plan.childrens.as_mut())
        else {
            unreachable!()
        };
        if let JoinCondition::On { on, .. } = &join.on {
            keys.extend_from_slice(on);
        }
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
    if can_expand(&plan) {
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

fn key_between(
    (left, right): (ExprRef, ExprRef),
    left_schema: &Schema,
    right_schema: &Schema,
    arena: &PlanArena,
) -> Result<Option<(ExprRef, ExprRef)>, DatabaseError> {
    if key_in_schema(left, left_schema, arena)? && key_in_schema(right, right_schema, arena)? {
        Ok(Some((left, right)))
    } else if key_in_schema(right, left_schema, arena)? && key_in_schema(left, right_schema, arena)?
    {
        Ok(Some((right, left)))
    } else {
        Ok(None)
    }
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
        if !matches!(&plan.operator, Operator::Join(join) if join.join_type == JoinType::Inner)
            || !can_expand(plan)
        {
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
            let mut best: Option<JoinCandidate> = None;
            for i in 0..inputs.len() {
                let (prefix, suffix) = inputs.split_at_mut(i + 1);
                let (left, left_rows) = &mut prefix[i];
                let left_schema = left.output_schema(arena);
                for (offset, (right, right_rows)) in suffix.iter_mut().enumerate() {
                    let j = i + 1 + offset;
                    let product = estimate_join_rows(*left_rows, *right_rows);
                    if let Some(previous) = best {
                        // Equal row estimates retain the earlier pair in enumeration order.
                        if !previous.cross_join && product >= previous.rows {
                            continue;
                        }
                    }
                    let right_schema = right.output_schema(arena);
                    let mut connected = false;
                    for &key in &keys {
                        if key_between(key, left_schema, right_schema, arena)?.is_some() {
                            connected = true;
                            break;
                        }
                    }
                    // Apply available join predicates before forming Cartesian intermediates.
                    let candidate = JoinCandidate {
                        cross_join: !connected,
                        rows: product,
                        left: i,
                        right: j,
                    };
                    if best.is_none_or(|previous| candidate < previous) {
                        best = Some(candidate);
                    }
                }
            }
            let Some(JoinCandidate {
                rows: product,
                left: i,
                right: j,
                ..
            }) = best
            else {
                return Ok(false);
            };
            let (mut right, _) = inputs.remove(j);
            let (mut left, _) = inputs.remove(i);
            let left_schema = left.output_schema(arena);
            let right_schema = right.output_schema(arena);
            let mut on = Vec::new();
            let mut index = 0;
            while index < keys.len() {
                if let Some(key) = key_between(keys[index], left_schema, right_schema, arena)? {
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
        let (join, _) = inputs.pop().unwrap();
        *plan = restore_join_output(join, original_schema, arena);
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

    fn materialized_right(arena: &mut PlanArena) -> LogicalPlan {
        let (b, b_key) = input("b", 10, arena);
        let (c, c_key) = input("c", 20, arena);
        let mut join = JoinOperator::build(
            b,
            c,
            JoinCondition::On {
                on: vec![(b_key, c_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        join.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let schema = join.output_schema(arena);
        let exprs = schema
            .iter()
            .enumerate()
            .map(|(position, column)| {
                arena.alloc_expression(ScalarExpression::column_expr(*column, position))
            })
            .collect();
        let mut project = LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs }),
            Childrens::Only(Box::new(join)),
        );
        project.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        project
    }

    #[test]
    fn nested_loop_moves_materialization_left_and_restores_positions() -> Result<(), DatabaseError>
    {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (left, _) = input("a", 100, &mut arena);
        let right = materialized_right(&mut arena);
        assert!(!left.has_materialization());
        assert!(right.has_materialization());
        let mut plan =
            JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Estimated(20000),
        ));
        let schema = plan.output_schema(&mut arena).clone();
        assert!(OrientNestedLoopJoin.apply(&mut plan, &mut arena)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.estimated_rows(), Some(20000));
        let Operator::Project(project) = &plan.operator else {
            unreachable!()
        };
        for (expr, position) in project.exprs.iter().zip([2, 0, 1]) {
            assert!(matches!(
                arena.expression(*expr),
                ScalarExpression::ColumnRef { position: actual, .. } if *actual == position
            ));
        }
        let join = plan.childrens.only();
        let (left, right) = join.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["b".into(), "c".into()]);
        assert_eq!(right.referenced_table(), vec!["a".into()]);
        let mut join = join.clone();
        assert!(!OrientNestedLoopJoin.apply(&mut join, &mut arena)?);
        Ok(())
    }

    #[test]
    fn nested_loop_orientation_keeps_equal_work_and_semantic_boundaries(
    ) -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        for (left_materialized, right_materialized, join_type, limit_pushed) in [
            (false, false, JoinType::Cross, false),
            (true, false, JoinType::Cross, false),
            (true, true, JoinType::Cross, false),
            (false, true, JoinType::LeftOuter, false),
            (false, true, JoinType::Cross, true),
        ] {
            let left = if left_materialized {
                materialized_right(&mut arena)
            } else {
                input("a", 100, &mut arena).0
            };
            let right = if right_materialized {
                materialized_right(&mut arena)
            } else {
                input("d", 100, &mut arena).0
            };
            let mut plan = JoinOperator::build(left, right, JoinCondition::None, join_type, false);
            if let Operator::Join(join) = &mut plan.operator {
                join.limit_pushed = limit_pushed;
            }
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::NestLoopJoin,
                SortOption::None,
                RowCountOption::Unknown,
            ));
            let original = plan.clone();
            assert!(!OrientNestedLoopJoin.apply(&mut plan, &mut arena)?);
            assert_eq!(plan, original);
        }
        Ok(())
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
    fn connected_pair_precedes_a_smaller_cartesian_product() -> Result<(), DatabaseError> {
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
        let (left, right) = plan.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["a".into(), "b".into()]);
        assert_eq!(right.referenced_table(), vec!["c".into()]);
        assert_eq!(left.estimated_rows(), Some(10000));
        for join in [&plan, left] {
            let Operator::Join(op) = &join.operator else {
                unreachable!()
            };
            let JoinCondition::On { on, .. } = &op.on else {
                unreachable!()
            };
            assert_eq!(op.join_type, JoinType::Inner);
            assert_eq!(on.len(), 1);
        }
        Ok(())
    }

    #[test]
    fn inner_join_expands_cross_join_but_cross_join_is_not_an_entry() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (a, a_key) = input("a", 1000, &mut arena);
        let (b, b_key) = input("b", 10, &mut arena);
        let (c, c_key) = input("c", 20, &mut arena);
        let mut cross = JoinOperator::build(a, b, JoinCondition::None, JoinType::Cross, false);
        let original = cross.clone();
        assert!(!GreedyJoinReorder.apply(&mut cross, &mut arena)?);
        assert_eq!(cross, original);
        let b_column = b_key.output_column_ref(&mut arena);
        let b_key = arena.alloc_expression(ScalarExpression::column_expr(b_column, 1));
        let mut plan = JoinOperator::build(
            cross,
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
        let (left, right) = plan.childrens.twins();
        assert_eq!(left.referenced_table(), vec!["a".into()]);
        assert_eq!(right.referenced_table(), vec!["b".into(), "c".into()]);
        assert_eq!(right.estimated_rows(), Some(200));
        for join in [&plan, right] {
            let Operator::Join(op) = &join.operator else {
                unreachable!()
            };
            let JoinCondition::On { on, .. } = &op.on else {
                unreachable!()
            };
            assert_eq!(op.join_type, JoinType::Inner);
            assert_eq!(on.len(), 1);
        }
        Ok(())
    }

    #[test]
    fn disconnected_input_keeps_a_final_cross_join() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (a, a_key) = input("a", 1000, &mut arena);
        let (b, b_key) = input("b", 10, &mut arena);
        let (c, c_key) = input("c", 20, &mut arena);
        let (d, _) = input("d", 1, &mut arena);
        let ab = JoinOperator::build(a, b, JoinCondition::None, JoinType::Cross, false);
        let abd = JoinOperator::build(ab, d, JoinCondition::None, JoinType::Cross, false);
        let b_column = b_key.output_column_ref(&mut arena);
        let b_key = arena.alloc_expression(ScalarExpression::column_expr(b_column, 1));
        let mut plan = JoinOperator::build(
            abd,
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
        let join = plan.childrens.only();
        assert!(
            matches!(&join.operator, Operator::Join(join) if join.join_type == JoinType::Cross)
        );
        let (left, right) = join.childrens.twins();
        assert_eq!(
            left.referenced_table(),
            vec!["a".into(), "b".into(), "c".into()]
        );
        assert_eq!(right.referenced_table(), vec!["d".into()]);
        let (_, bc) = left.childrens.twins();
        assert_eq!(bc.referenced_table(), vec!["b".into(), "c".into()]);
        assert_eq!(bc.estimated_rows(), Some(200));
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
            RowCountOption::Follow,
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
