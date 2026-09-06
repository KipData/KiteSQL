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
use crate::expression::visitor_mut::{ExprVisitorMut, PositionShift};
use crate::expression::{BinaryOperator, ScalarExpression, TypeCast};
use crate::optimizer::core::rule::NormalizationRule;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::{JoinCondition, JoinType};
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::types::index::{IndexLookup, IndexType};
use crate::types::tuple::Schema;
use crate::types::LogicalType;

pub(crate) struct ParameterizeMarkApply;

impl NormalizationRule for ParameterizeMarkApply {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let (op, new_probe) = match (&mut plan.operator, plan.childrens.as_mut()) {
            (Operator::MarkApply(op), Childrens::Twins { left, right })
                if !matches!(op.kind, MarkApplyKind::InnerJoin) =>
            {
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
        MarkApplyKind::InnerJoin | MarkApplyKind::Quantified(MarkApplyQuantifier::All) => Ok(None),
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

pub(crate) struct ParameterizeInnerJoin;

// Collect constant equalities without consuming the filter: it must still check
// all conditions after a static index range is replaced by a runtime probe.
fn constant_keys(expr: ExprRef, keys: &mut Vec<(ExprRef, ExprRef)>, arena: &PlanArena<'_>) {
    if let ScalarExpression::Binary {
        op,
        left_expr,
        right_expr,
        ..
    } = arena.expression(expr)
    {
        match op {
            BinaryOperator::And => {
                constant_keys(*left_expr, keys, arena);
                constant_keys(*right_expr, keys, arena);
            }
            BinaryOperator::Eq => {
                if matches!(arena.expression(*right_expr), ScalarExpression::Constant(_)) {
                    keys.push((*right_expr, *left_expr));
                } else if matches!(arena.expression(*left_expr), ScalarExpression::Constant(_)) {
                    keys.push((*left_expr, *right_expr));
                }
            }
            _ => {}
        }
    }
}

fn parameterize(
    plan: &mut LogicalPlan,
    mut keys: Vec<(ExprRef, ExprRef)>,
    arena: &PlanArena<'_>,
) -> Option<Vec<ExprRef>> {
    match (&mut plan.operator, plan.childrens.as_mut()) {
        (Operator::Filter(filter), Childrens::Only(child)) => {
            constant_keys(filter.predicate, &mut keys, arena);
            parameterize(child, keys, arena)
        }
        (Operator::TableScan(scan), _) if scan.limit == (None, None) => {
            'indexes: for info in &mut scan.index_infos {
                let meta = arena.index(info.meta);
                let mut probe = Vec::with_capacity(meta.column_ids.len());
                'columns: for id in &meta.column_ids {
                    let Some(candidate) = scan
                        .columns
                        .iter()
                        .find(|column| arena.column(**column).id() == Some(*id))
                    else {
                        continue 'indexes;
                    };
                    for &(value, column) in &keys {
                        let ScalarExpression::ColumnRef { column, .. } =
                            arena.expression(column.unpack_alias(arena))
                        else {
                            continue;
                        };
                        if arena.same_column(*candidate, *column)
                            && value.return_type(arena).as_ref()
                                == arena.column(*candidate).datatype()
                        {
                            probe.push(value);
                            continue 'columns;
                        }
                    }
                    continue 'indexes;
                }
                info.lookup = Some(IndexLookup::Probe);
                info.residual_predicate = None;
                plan.physical_option = Some(PhysicalOption::new(
                    PlanImpl::IndexScan(Box::new(info.clone())),
                    info.sort_option.clone(),
                ));
                return Some(probe);
            }
            None
        }
        // LIMIT/aggregation/projection are not row-local filters. Moving a probe
        // below them could change which rows the original inner input produces.
        _ => None,
    }
}

impl NormalizationRule for ParameterizeInnerJoin {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena<'_>,
    ) -> Result<bool, DatabaseError> {
        let (Operator::Join(join), Childrens::Twins { left, right }) =
            (&mut plan.operator, plan.childrens.as_mut())
        else {
            return Ok(false);
        };
        if join.join_type != JoinType::Inner || join.force_nested_loop {
            return Ok(false);
        }
        let JoinCondition::On { on, filter } = &mut join.on else {
            return Ok(false);
        };
        if on.is_empty() {
            return Ok(false);
        }
        let mut keys = on.clone();
        let (project, probe_keys) = if let Some(probe) = parameterize(right, keys.clone(), arena) {
            (None, probe)
        } else {
            for (l, r) in &mut keys {
                std::mem::swap(l, r);
            }
            let Some(probe) = parameterize(left, keys.clone(), arena) else {
                return Ok(false);
            };
            let left_schema = left.output_schema(arena);
            let right_schema = right.output_schema(arena);
            let old_left_len = left_schema.len();
            let left_len = right_schema.len();
            // Preserve the original output slots after changing the driving side.
            let exprs = left_schema
                .iter()
                .chain(right_schema.iter())
                .copied()
                .enumerate()
                .map(|(i, column)| {
                    let position = if i < old_left_len {
                        left_len + i
                    } else {
                        i - old_left_len
                    };
                    arena.alloc_expression(ScalarExpression::column_expr(column, position))
                })
                .collect();
            std::mem::swap(left, right);
            let mut project = LogicalPlan::new(
                Operator::Project(ProjectOperator { exprs }),
                Childrens::None,
            );
            project.physical_option =
                Some(PhysicalOption::new(PlanImpl::Project, SortOption::Follow));
            (Some(project), probe)
        };

        let filter = filter.take();
        let (mut left, right) = plan.take().childrens.pop_twins();
        let left_len = left.output_schema(arena).len();
        let probe = if probe_keys.len() == 1 {
            probe_keys[0]
        } else {
            arena.alloc_expression(ScalarExpression::Tuple(probe_keys))
        };
        let mut predicates = Vec::with_capacity(keys.len());
        for (left_expr, mut right_expr) in keys {
            PositionShift {
                delta: left_len as isize,
            }
            .visit(&mut right_expr, arena)?;
            predicates.push(arena.alloc_expression(ScalarExpression::Binary {
                op: BinaryOperator::Eq,
                left_expr,
                right_expr,
                evaluator: None,
                ty: LogicalType::Boolean,
            }));
        }
        let apply = LogicalPlan::new(
            Operator::MarkApply(MarkApplyOperator::new_inner_join(predicates, probe)),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        *plan = if let Some(mut project) = project {
            project.childrens = Box::new(Childrens::Only(Box::new(apply)));
            project
        } else {
            apply
        };
        if let Some(filter) = filter {
            // The original join filter uses the original left/right output slots.
            *plan = FilterOperator::build(filter, plan.take(), false);
            plan.physical_option = Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow));
        }
        Ok(true)
    }
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

#[cfg(test)]
mod inner_join_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::range_detacher::Range;
    use crate::planner::operator::join::JoinOperator;
    use crate::planner::operator::mark_apply::MarkApplyKind;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::TableArenaCell;
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::value::DataValue;

    fn scan(
        arena: &mut PlanArena<'_>,
        name: &str,
        types: &[LogicalType],
        indexes: &[&[usize]],
    ) -> LogicalPlan {
        let columns: Vec<_> = types
            .iter()
            .enumerate()
            .map(|(id, ty)| {
                let mut column = ColumnCatalog::new(
                    format!("c{id}"),
                    true,
                    ColumnDesc::new(ty.clone(), None, false, None).unwrap(),
                );
                column.set_ref_table(name.into(), id as _, true);
                arena.alloc_column(column)
            })
            .collect();
        let index_infos = indexes
            .iter()
            .enumerate()
            .map(|(id, keys)| IndexInfo {
                meta: arena.alloc_index(IndexMeta {
                    id: id as _,
                    column_ids: keys.iter().map(|id| *id as _).collect(),
                    table_name: name.into(),
                    pk_ty: LogicalType::Integer,
                    value_ty: if keys.len() == 1 {
                        types[keys[0]].clone()
                    } else {
                        LogicalType::Tuple(keys.iter().map(|id| types[*id].clone()).collect())
                    },
                    name: format!("idx{id}"),
                    ty: if keys.len() == 1 {
                        IndexType::Normal
                    } else {
                        IndexType::Composite
                    },
                }),
                lookup: None,
                residual_predicate: None,
                sort_option: SortOption::None,
                covered_deserializers: None,
                cover_mapping: None,
                sort_elimination_hint: None,
                stream_aggregate_hint: None,
            })
            .collect();
        LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: name.into(),
                columns,
                limit: (None, None),
                index_infos,
                with_pk: false,
            }),
            Childrens::None,
        )
    }

    fn column(plan: &LogicalPlan, position: usize, arena: &mut PlanArena<'_>) -> ExprRef {
        let Operator::TableScan(scan) = &plan.operator else {
            panic!("expected scan")
        };
        arena.alloc_expression(ScalarExpression::column_expr(
            scan.columns[position],
            position,
        ))
    }

    fn binary(
        arena: &mut PlanArena<'_>,
        op: BinaryOperator,
        left_expr: ExprRef,
        right_expr: ExprRef,
    ) -> ExprRef {
        arena.alloc_expression(ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator: None,
            ty: LogicalType::Boolean,
        })
    }

    fn position(expr: ExprRef, arena: &PlanArena<'_>) -> usize {
        let ScalarExpression::ColumnRef { position, .. } = arena.expression(expr) else {
            panic!("expected column")
        };
        *position
    }

    fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        on: Vec<(ExprRef, ExprRef)>,
        filter: Option<ExprRef>,
    ) -> LogicalPlan {
        JoinOperator::build(
            left,
            right,
            JoinCondition::On { on, filter },
            JoinType::Inner,
            false,
        )
    }

    #[test]
    fn composite_probe_uses_index_order_and_preserves_static_filter() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let left = scan(
            &mut arena,
            "outer",
            &[const { LogicalType::Integer }; 2],
            &[],
        );
        // First index cannot be probed. Second requires constant + two join keys.
        let mut right = scan(
            &mut arena,
            "inner",
            &[const { LogicalType::Integer }; 4],
            &[&[3], &[0, 2, 1]],
        );
        let l0 = column(&left, 0, &mut arena);
        let l1 = column(&left, 1, &mut arena);
        let r0 = column(&right, 0, &mut arena);
        let r1 = column(&right, 1, &mut arena);
        let r2 = column(&right, 2, &mut arena);
        let r3 = column(&right, 3, &mut arena);
        let constant = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(7)));
        let equality = binary(&mut arena, BinaryOperator::Eq, constant, r0);
        let residual = binary(&mut arena, BinaryOperator::Gt, r3, constant);
        let predicate = binary(&mut arena, BinaryOperator::And, equality, residual);
        let Operator::TableScan(scan) = &mut right.operator else {
            unreachable!()
        };
        let info = &mut scan.index_infos[1];
        info.lookup = Some(IndexLookup::Static(Range::Eq(DataValue::Int32(7))));
        info.residual_predicate = Some(residual);
        let index = info.meta;
        right.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(info.clone())),
            SortOption::None,
        ));
        let right = FilterOperator::build(predicate, right, false);
        let mut plan = join(left, right, vec![(l0, r1), (l1, r2)], None);

        assert!(ParameterizeInnerJoin.apply(&mut plan, &mut arena)?);
        let Operator::MarkApply(apply) = &plan.operator else {
            panic!("expected apply without projection")
        };
        assert_eq!(apply.kind, MarkApplyKind::InnerJoin);
        assert_eq!(
            arena.expression(*apply.parameterized_probe().unwrap()),
            &ScalarExpression::Tuple(vec![constant, l1, l0])
        );
        assert_eq!((position(l0, &arena), position(l1, &arena)), (0, 1));
        assert_eq!((position(r1, &arena), position(r2, &arena)), (3, 4));
        let Childrens::Twins { right, .. } = plan.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::Filter(filter) = &right.operator else {
            panic!("full filter must survive")
        };
        assert_eq!(filter.predicate, predicate);
        let Childrens::Only(scan) = right.childrens.as_ref() else {
            unreachable!()
        };
        let PlanImpl::IndexScan(info) = &scan.physical_option.as_ref().unwrap().plan else {
            panic!("expected index scan")
        };
        assert_eq!(info.meta, index);
        assert_eq!(info.lookup, Some(IndexLookup::Probe));
        assert_eq!(info.residual_predicate, None);
        assert_eq!(position(r0, &arena), 0);
        assert_eq!(position(r3, &arena), 3);
        Ok(())
    }

    #[test]
    fn swapped_join_restores_unequal_widths_and_filter_positions() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut left = scan(
            &mut arena,
            "left",
            &[const { LogicalType::Integer }; 3],
            &[&[1]],
        );
        let mut right = scan(&mut arena, "right", &[LogicalType::Integer], &[]);
        let mut schema = left.output_schema(&mut arena).clone();
        schema.extend_from_slice(right.output_schema(&mut arena));
        let l = column(&left, 1, &mut arena);
        let r = column(&right, 0, &mut arena);
        let filter_left = arena.alloc_expression(ScalarExpression::column_expr(schema[2], 2));
        let filter_right = arena.alloc_expression(ScalarExpression::column_expr(schema[3], 3));
        let predicate = binary(&mut arena, BinaryOperator::Gt, filter_left, filter_right);
        let mut plan = join(left, right, vec![(l, r)], Some(predicate));

        assert!(ParameterizeInnerJoin.apply(&mut plan, &mut arena)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        let Operator::Filter(filter) = &plan.operator else {
            panic!("join filter must remain above projection")
        };
        assert_eq!(filter.predicate, predicate);
        assert_eq!(
            (
                position(filter_left, &arena),
                position(filter_right, &arena)
            ),
            (2, 3)
        );
        let Childrens::Only(project) = plan.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::Project(project_op) = &project.operator else {
            panic!("expected reorder projection")
        };
        assert_eq!(
            project_op
                .exprs
                .iter()
                .map(|expr| position(*expr, &arena))
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 0]
        );
        let Childrens::Only(apply_plan) = project.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::MarkApply(apply) = &apply_plan.operator else {
            panic!("expected apply")
        };
        assert_eq!(apply.parameterized_probe(), Some(&r));
        assert_eq!((position(r, &arena), position(l, &arena)), (0, 2));
        let ScalarExpression::Binary {
            left_expr,
            right_expr,
            ..
        } = arena.expression(apply.predicates()[0])
        else {
            unreachable!()
        };
        assert_eq!((*left_expr, *right_expr), (r, l));
        let Childrens::Twins { left, right } = apply_plan.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::TableScan(outer) = &left.operator else {
            unreachable!()
        };
        let Operator::TableScan(inner) = &right.operator else {
            unreachable!()
        };
        assert_eq!(outer.table_name.as_ref(), "right");
        assert_eq!(inner.table_name.as_ref(), "left");
        Ok(())
    }

    #[test]
    fn rejected_probe_leaves_join_and_expression_positions_unchanged() -> Result<(), DatabaseError>
    {
        for case in ["missing_key", "type_mismatch", "inner_limit", "outer_join"] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let left = scan(&mut arena, "left", &[LogicalType::Integer], &[]);
            let ty = if case == "type_mismatch" {
                LogicalType::Bigint
            } else {
                LogicalType::Integer
            };
            let index: &[usize] = if case == "missing_key" { &[0, 1] } else { &[0] };
            let mut right = scan(&mut arena, "right", &[ty, LogicalType::Integer], &[index]);
            if case == "inner_limit" {
                let Operator::TableScan(scan) = &mut right.operator else {
                    unreachable!()
                };
                scan.limit = (None, Some(1));
            }
            let l = column(&left, 0, &mut arena);
            let r = column(&right, 0, &mut arena);
            let mut plan = join(left, right, vec![(l, r)], None);
            if case == "outer_join" {
                let Operator::Join(join) = &mut plan.operator else {
                    unreachable!()
                };
                join.join_type = JoinType::LeftOuter;
            }
            let before = plan.clone();
            assert!(
                !ParameterizeInnerJoin.apply(&mut plan, &mut arena)?,
                "{case}"
            );
            assert_eq!(plan, before, "{case}");
            assert_eq!((position(l, &arena), position(r, &arena)), (0, 0), "{case}");
        }
        Ok(())
    }
}
