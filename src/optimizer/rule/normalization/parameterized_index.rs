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
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyQuantifier};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl};
use crate::planner::{Childrens, ExprRef, LogicalPlan};
use crate::types::index::{IndexLookup, IndexType};
use crate::types::tuple::Schema;

pub(crate) struct ParameterizeMarkApply;

impl NormalizationRule for ParameterizeMarkApply {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let (op, new_probe) = match (&mut plan.operator, plan.childrens.as_mut()) {
            (Operator::MarkApply(op), Childrens::Twins { left, right }) => {
                let probe = find_parameterized_probe(
                    op.kind,
                    op.predicates(),
                    left.output_schema(arena),
                    right.output_schema(arena),
                    arena,
                )?;
                let new_probe = probe.and_then(|(right_column, left_expr)| {
                    parameterize_right_subtree(right, &right_column, arena).then_some(left_expr)
                });
                (op, new_probe)
            }
            _ => return Ok(false),
        };

        let changed = op.parameterized_probe().copied() != new_probe;
        op.set_parameterized_probe(new_probe);
        Ok(changed)
    }
}

fn find_parameterized_probe(
    kind: MarkApplyKind,
    predicates: &[ExprRef],
    left_schema: &Schema,
    right_schema: &Schema,
    arena: &crate::planner::PlanArena,
) -> Result<Option<(ColumnRef, ExprRef)>, DatabaseError> {
    match kind {
        MarkApplyKind::Exists => {
            for predicate in predicates {
                if let Some(probe) =
                    extract_parameterized_probe(*predicate, left_schema, right_schema, arena)?
                {
                    return Ok(Some(probe));
                }
            }
            Ok(None)
        }
        MarkApplyKind::Quantified(MarkApplyQuantifier::Any) => {
            if let Some(predicate) = predicates.first() {
                extract_parameterized_probe(*predicate, left_schema, right_schema, arena)
            } else {
                Ok(None)
            }
        }
        MarkApplyKind::Quantified(MarkApplyQuantifier::All) => Ok(None),
    }
}

fn extract_parameterized_probe(
    predicate: ExprRef,
    left_schema: &Schema,
    right_schema: &Schema,
    arena: &crate::planner::PlanArena,
) -> Result<Option<(ColumnRef, ExprRef)>, DatabaseError> {
    match predicate.unpack_alias_ref(arena) {
        ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr,
            right_expr,
            ..
        } => {
            if let Some(probe) = extract_parameterized_probe_side(
                *left_expr,
                *right_expr,
                left_schema,
                right_schema,
                arena,
            )? {
                return Ok(Some(probe));
            }
            extract_parameterized_probe_side(
                *right_expr,
                *left_expr,
                left_schema,
                right_schema,
                arena,
            )
        }
        _ => Ok(None),
    }
}

fn extract_parameterized_probe_side(
    right_expr: ExprRef,
    left_expr: ExprRef,
    left_schema: &Schema,
    right_schema: &Schema,
    arena: &crate::planner::PlanArena,
) -> Result<Option<(ColumnRef, ExprRef)>, DatabaseError> {
    let Some((right_column, _)) = right_expr
        .unpack_alias(arena)
        .unpack_bound_col(arena, false)
    else {
        return Ok(None);
    };

    if !schema_contains_column(right_schema, &right_column, arena) {
        return Ok(None);
    }
    if !left_expr.all_referenced_columns(arena, |arena, candidate| {
        schema_contains_column(left_schema, candidate, arena)
    })? {
        return Ok(None);
    }
    if left_expr.any_referenced_column(arena, |arena, candidate| {
        schema_contains_column(right_schema, candidate, arena)
    })? {
        return Ok(None);
    }

    Ok(Some((right_column, left_expr)))
}

fn parameterize_right_subtree(
    plan: &mut LogicalPlan,
    right_column: &ColumnRef,
    arena: &crate::planner::PlanArena,
) -> bool {
    if matches!(plan.operator, Operator::TableScan(_)) {
        let index_info = {
            let Operator::TableScan(scan_op) = &mut plan.operator else {
                unreachable!();
            };
            let Some(target_index) =
                pick_parameterized_index_position(scan_op, right_column, arena)
            else {
                return false;
            };
            scan_op.index_infos[target_index].lookup = Some(IndexLookup::Probe);
            scan_op.index_infos[target_index].clone()
        };
        let sort_option = index_info.sort_option.clone();
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(index_info)),
            sort_option,
        ));
        return true;
    }

    let passthrough = matches!(
        plan.operator,
        Operator::Filter(_)
            | Operator::Project(_)
            | Operator::Limit(_)
            | Operator::Sort(_)
            | Operator::TopK(_)
    );

    if !passthrough {
        return false;
    }

    match plan.childrens.as_mut() {
        Childrens::Only(child) => parameterize_right_subtree(child, right_column, arena),
        _ => false,
    }
}

fn pick_parameterized_index_position(
    scan_op: &TableScanOperator,
    right_column: &ColumnRef,
    arena: &crate::planner::PlanArena,
) -> Option<usize> {
    let right_column = arena.column(*right_column);
    let column_id = right_column.id()?;
    let table_name = right_column.table_name()?;

    if &scan_op.table_name != table_name {
        return None;
    }

    scan_op
        .index_infos
        .iter()
        .enumerate()
        .filter(|(_, index_info)| {
            let index_meta = arena.index(index_info.meta);
            index_meta.table_name == *table_name
                && index_meta.column_ids.first().copied() == Some(column_id)
        })
        .min_by_key(|(_, index_info)| index_priority(arena.index(index_info.meta).ty))
        .map(|(position, _)| position)
}

fn index_priority(index_type: IndexType) -> usize {
    match index_type {
        IndexType::PrimaryKey { .. } => 0,
        IndexType::Unique => 1,
        IndexType::Composite => 2,
        IndexType::Normal => 3,
    }
}

fn schema_contains_column(
    schema: &Schema,
    column: &ColumnRef,
    arena: &crate::planner::PlanArena,
) -> bool {
    schema
        .iter()
        .any(|candidate| arena.same_column(*candidate, *column))
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::types::LogicalType;

    fn column(arena: &mut PlanArena, name: &str) -> ColumnRef {
        arena.alloc_column(ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ))
    }

    fn expr(arena: &mut PlanArena, column: ColumnRef) -> ExprRef {
        arena.alloc_expression(ScalarExpression::column_expr(column, 0))
    }

    fn eq(arena: &mut PlanArena, left: ExprRef, right: ExprRef) -> ExprRef {
        arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: left,
            right_expr: right,
            evaluator: None,
            ty: LogicalType::Boolean,
        })
    }

    #[test]
    fn probe_detection_covers_quantifiers_and_rejected_sides() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = column(&mut arena, "left");
        let right = column(&mut arena, "right");
        let outside = column(&mut arena, "outside");
        let left_schema = vec![left];
        let right_schema = vec![right];
        let overlapping_schema = vec![left, right];

        let all_right = expr(&mut arena, right);
        let all_left = expr(&mut arena, left);
        let all_predicate = eq(&mut arena, all_right, all_left);
        assert!(find_parameterized_probe(
            MarkApplyKind::Quantified(MarkApplyQuantifier::Any),
            &[],
            &left_schema,
            &right_schema,
            &arena,
        )?
        .is_none());
        assert!(find_parameterized_probe(
            MarkApplyKind::Quantified(MarkApplyQuantifier::All),
            &[all_predicate],
            &left_schema,
            &right_schema,
            &arena,
        )?
        .is_none());

        let exists_right = expr(&mut arena, right);
        let exists_left = expr(&mut arena, left);
        let exists_predicate = eq(&mut arena, exists_right, exists_left);
        let predicates = vec![
            arena.alloc_expression(ScalarExpression::from(true)),
            exists_predicate,
        ];
        let probe = find_parameterized_probe(
            MarkApplyKind::Exists,
            &predicates,
            &left_schema,
            &right_schema,
            &arena,
        )?
        .expect("right = left should be parameterizable");
        assert_eq!(probe.0, right);

        let outside_right = expr(&mut arena, right);
        let outside_expr = expr(&mut arena, outside);
        let outside_predicate = eq(&mut arena, outside_right, outside_expr);
        assert!(extract_parameterized_probe(
            outside_predicate,
            &left_schema,
            &right_schema,
            &arena,
        )?
        .is_none());
        let overlap_left = expr(&mut arena, right);
        let overlap_right = expr(&mut arena, right);
        let overlap_predicate = eq(&mut arena, overlap_left, overlap_right);
        assert!(extract_parameterized_probe(
            overlap_predicate,
            &overlapping_schema,
            &right_schema,
            &arena,
        )?
        .is_none());
        assert!(extract_parameterized_probe(
            arena.alloc_expression(ScalarExpression::from(false)),
            &left_schema,
            &right_schema,
            &arena,
        )?
        .is_none());

        Ok(())
    }

    #[test]
    fn parameterization_rejects_unsupported_operator_and_child_shapes() -> Result<(), DatabaseError>
    {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let column = column(&mut arena, "value");
        let mut plan = LogicalPlan::new(Operator::Dummy, Childrens::None);

        assert!(!ParameterizeMarkApply.apply(&mut plan, &mut arena)?);
        assert!(!parameterize_right_subtree(&mut plan, &column, &arena));

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: arena.alloc_expression(ScalarExpression::from(true)),
                is_optimized: false,
                having: false,
            }),
            Childrens::None,
        );
        assert!(!parameterize_right_subtree(&mut filter, &column, &arena));

        assert_eq!(
            index_priority(IndexType::PrimaryKey { is_multiple: false }),
            0
        );
        assert_eq!(index_priority(IndexType::Unique), 1);
        assert_eq!(index_priority(IndexType::Composite), 2);
        assert_eq!(index_priority(IndexType::Normal), 3);

        Ok(())
    }
}
// GRCOV_EXCL_STOP
