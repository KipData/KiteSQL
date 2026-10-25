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
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::rule::normalization::pushdown_predicates::{index_covering, is_index_covers};
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::{JoinCondition, JoinType};
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{
    rows_work, Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption, CPU_OPERATOR_WORK,
    DEFAULT_TABLE_ROWS, PROBE_RESET_WORK,
};
use crate::planner::MetaArena;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::types::index::{IndexLookup, IndexMetaRef, IndexType};
use crate::types::tuple::Schema;
use crate::types::LogicalType;
use std::borrow::Cow;
use std::mem;

pub(crate) struct ParameterizeMarkApply;

impl NormalizationRule for ParameterizeMarkApply {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
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
                let new_probe = if let Some((right_column, left_expr)) = probe {
                    parameterize_right_subtree(right, &right_column, left_expr, arena)?
                } else {
                    None
                };
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
    arena: &dyn MetaArena,
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
    arena: &dyn MetaArena,
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
    arena: &dyn MetaArena,
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
    if !left_expr.all_referenced_columns(arena, |arena, candidate, _| {
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
    mut left_expr: ExprRef,
    arena: &mut PlanArena<'_>,
) -> Result<Option<ExprRef>, DatabaseError> {
    match (&mut plan.operator, plan.childrens.as_mut()) {
        (Operator::Project(project), Childrens::Only(child)) => {
            let Some(mut expr) = project.exprs.iter().copied().find(|expr| {
                let column = expr.output_column_ref(arena);
                arena.same_column(column, *right_column)
            }) else {
                return Ok(None);
            };
            // Follow the source, not the projection's output relation/name.
            // Computed and cast keys are not index probes.
            while let ScalarExpression::Alias { expr: source, .. } = arena.expression(expr) {
                expr = *source;
            }
            let ScalarExpression::ColumnRef { column, .. } = *arena.expression(expr) else {
                return Ok(None);
            };
            parameterize_right_subtree(child, &column, left_expr, arena)
        }
        (Operator::TableScan(scan), _) => {
            if scan.limit != (None, None) {
                return Ok(None);
            }
            let Some(position) = pick_parameterized_index_position(scan, right_column, arena)
            else {
                return Ok(None);
            };
            let key_type = &arena.index(scan.index_infos[position].meta).value_ty;
            let left_type = left_expr.return_type(arena);
            if left_type.as_ref() != key_type {
                let common = LogicalType::max_logical_type(&left_type, key_type)?;
                if common.as_ref() != key_type {
                    return Ok(None);
                }
                left_expr = left_expr.type_cast(Cow::Owned(common.into_owned()), arena)?;
            }
            scan.index_infos[position].lookup = Some(IndexLookup::Probe);
            let index_info = scan.index_infos[position].clone();
            let sort_option = index_info.sort_option.clone();
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::IndexScan(Box::new(index_info)),
                sort_option,
                RowCountOption::Unknown,
            ));
            Ok(Some(left_expr))
        }
        (Operator::Filter(_) | Operator::Sort(_), Childrens::Only(child)) => {
            parameterize_right_subtree(child, right_column, left_expr, arena)
        }
        _ => Ok(None),
    }
}

fn pick_parameterized_index_position(
    scan_op: &TableScanOperator,
    right_column: &ColumnRef,
    arena: &dyn MetaArena,
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
                && index_meta.column_ids.len() == 1
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

fn schema_contains_column(schema: &Schema, column: &ColumnRef, arena: &dyn MetaArena) -> bool {
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

/// A PK/Unique index of a `Filter* -> TableScan` input that join keys can probe.
pub(crate) struct ProbeIndex {
    position: usize,
    pub(crate) probe: Vec<ExprRef>,
    pub(crate) lookup_work: usize,
}

pub(crate) fn probe_index(
    plan: &LogicalPlan,
    keys: &mut Vec<(ExprRef, ExprRef)>,
    join_key_count: usize,
    arena: &PlanArena<'_>,
    loader: Option<&StatisticMetaLoader<'_>>,
) -> Result<Option<ProbeIndex>, DatabaseError> {
    match (&plan.operator, plan.childrens.as_ref()) {
        (Operator::Filter(filter), Childrens::Only(child)) => {
            let key_count = keys.len();
            constant_keys(filter.predicate, keys, arena);
            let probe = probe_index(child, keys, join_key_count, arena, loader);
            // Keep filter keys local to this traversal, including failed attempts.
            keys.truncate(key_count);
            probe
        }
        (Operator::TableScan(scan), _) if scan.limit == (None, None) => {
            let mut best: Option<ProbeIndex> = None;
            let mut probe = Vec::new();
            'indexes: for (position, info) in scan.index_infos.iter().enumerate() {
                let meta = arena.index(info.meta);
                if !matches!(meta.ty, IndexType::PrimaryKey { .. } | IndexType::Unique) {
                    continue;
                }
                probe.clear();
                let mut uses_join_key = false;
                'columns: for id in &meta.column_ids {
                    let Some(candidate) = scan
                        .columns
                        .iter()
                        .find(|column| arena.column(**column).id() == Some(*id))
                    else {
                        continue 'indexes;
                    };
                    for (position, &(value, column)) in keys.iter().enumerate() {
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
                            uses_join_key |= position < join_key_count;
                            continue 'columns;
                        }
                    }
                    continue 'indexes;
                }
                // Constants appended by filters may complete a composite key,
                // but must not trigger a per-outer-row probe on their own.
                if !uses_join_key {
                    continue;
                }
                let lookup_work = lookup_work(scan, info.meta, arena, loader)?;
                // Equal work keeps the index order.
                if best
                    .as_ref()
                    .is_none_or(|best| lookup_work < best.lookup_work)
                {
                    best = Some(ProbeIndex {
                        position,
                        probe: mem::take(&mut probe),
                        lookup_work,
                    });
                }
            }
            Ok(best)
        }
        _ => Ok(None),
    }
}

/// RFC 0001 §4.3: reset the probed input, seek the index (twice when it must read the
/// table row too), and read one row. Without statistics the index has `DEFAULT_TABLE_ROWS`.
fn lookup_work(
    scan: &TableScanOperator,
    index: IndexMetaRef,
    arena: &PlanArena<'_>,
    loader: Option<&StatisticMetaLoader<'_>>,
) -> Result<usize, DatabaseError> {
    let meta = arena.index(index);
    let rows = match loader {
        Some(loader) => loader
            .load(&scan.table_name, meta.id)?
            .map(|statistics| statistics.histogram().values_len()),
        None => None,
    }
    .unwrap_or(DEFAULT_TABLE_ROWS);
    let seeks = if matches!(meta.ty, IndexType::PrimaryKey { .. })
        || is_index_covers(&scan.columns, index, arena)
    {
        1
    } else {
        2
    };
    let depth = (usize::BITS - rows.saturating_sub(1).leading_zeros()) as usize;
    Ok(PROBE_RESET_WORK
        .saturating_add(seeks * depth * CPU_OPERATOR_WORK)
        .saturating_add(rows_work(1, scan.columns.len())))
}

fn use_probe_index(plan: &mut LogicalPlan, position: usize, arena: &PlanArena<'_>) {
    match (&mut plan.operator, plan.childrens.as_mut()) {
        (Operator::Filter(_), Childrens::Only(child)) => use_probe_index(child, position, arena),
        (Operator::TableScan(scan), _) => {
            let info = &mut scan.index_infos[position];
            info.lookup = Some(IndexLookup::Probe);
            info.residual_predicate = None;
            (info.covered_deserializers, info.cover_mapping) =
                match index_covering(&scan.columns, info.meta, arena) {
                    Some((deserializers, mapping)) => (Some(deserializers), mapping),
                    None => (None, None),
                };
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::IndexScan(Box::new(info.clone())),
                info.sort_option.clone(),
                RowCountOption::Unknown,
            ));
        }
        // SAFETY: `probe_index` only accepts filters over a table scan.
        _ => unreachable!(),
    }
}

/// Builds the inner `MarkApply` probing `right` with `index` for every `left` row. `on` holds
/// the join keys bound to each side; all of them are still checked after the probe.
pub(crate) fn probe_join(
    mut left: LogicalPlan,
    mut right: LogicalPlan,
    on: Vec<(ExprRef, ExprRef)>,
    index: ProbeIndex,
    row_count: RowCountOption,
    arena: &mut PlanArena<'_>,
) -> Result<LogicalPlan, DatabaseError> {
    use_probe_index(&mut right, index.position, arena);
    let left_len = left.output_schema(arena).len();
    let probe = if index.probe.len() == 1 {
        index.probe[0]
    } else {
        arena.alloc_expression(ScalarExpression::Tuple(index.probe))
    };
    let mut predicates = Vec::with_capacity(on.len());
    for (left_expr, mut right_expr) in on {
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
    let mut apply = LogicalPlan::new(
        Operator::MarkApply(MarkApplyOperator::new_inner_join(predicates, probe)),
        Childrens::Twins {
            left: Box::new(left),
            right: Box::new(right),
        },
    );
    let mut option = PhysicalOption::new(PlanImpl::MarkApply, SortOption::Follow, row_count);
    option.lookup_work = Some(index.lookup_work);
    apply.physical_option = Some(option);
    Ok(apply)
}

impl NormalizationRule for ParameterizeInnerJoin {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena<'_>,
        loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        if plan
            .physical_option
            .as_ref()
            .is_some_and(|option| option.join_decided)
        {
            return Ok(false);
        }
        let rows = plan.estimated_rows();
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
        // Filter traversal only appends constants after these outer-dependent keys.
        let join_key_count = keys.len();
        let right_probe = probe_index(right, &mut keys, join_key_count, arena, loader)?;
        let (project, index) = if let Some(index) = right_probe {
            (None, index)
        } else {
            for (l, r) in &mut keys {
                std::mem::swap(l, r);
            }
            let Some(index) = probe_index(left, &mut keys, join_key_count, arena, loader)? else {
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
                Operator::Project(ProjectOperator::new(exprs)),
                Childrens::None,
            );
            project.physical_option = Some(PhysicalOption::new(
                PlanImpl::Project,
                SortOption::Follow,
                RowCountOption::Follow,
            ));
            (Some(project), index)
        };

        let filter = filter.take();
        let (left, right) = plan.take().childrens.pop_twins();
        // This is the same join output, not the per-key cardinality of the probe.
        let row_count = rows.map_or(RowCountOption::Unknown, |rows| RowCountOption::Estimated {
            rows,
            ndv: None,
        });
        let apply = probe_join(left, right, keys, index, row_count, arena)?;
        *plan = if let Some(mut project) = project {
            project.childrens = Box::new(Childrens::Only(Box::new(apply)));
            project
        } else {
            apply
        };
        if let Some(filter) = filter {
            // The original join filter uses the original left/right output slots.
            *plan = FilterOperator::build(filter, plan.take(), false);
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::Filter,
                SortOption::Follow,
                RowCountOption::Follow,
            ));
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
        let probe = arena.alloc_expression(ScalarExpression::from(2));
        let mut plan = LogicalPlan::new(Operator::Dummy, Childrens::None);

        assert!(!ParameterizeMarkApply.apply(&mut plan, &mut arena, None)?);
        assert!(parameterize_right_subtree(&mut plan, &column, probe, &mut arena)?.is_none());

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: arena.alloc_expression(ScalarExpression::from(true)),
                is_optimized: false,
                having: false,
            }),
            Childrens::None,
        );
        assert!(parameterize_right_subtree(&mut filter, &column, probe, &mut arena)?.is_none());

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

// GRCOV_EXCL_START
#[cfg(test)]
mod inner_join_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::range_detacher::Range;
    use crate::expression::{AliasType, UnaryOperator};
    use crate::optimizer::rule::normalization::join_reorder::GreedyJoinReorder;
    use crate::planner::operator::join::JoinOperator;
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::mark_apply::MarkApplyKind;
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::operator::WorkCost;
    use crate::planner::TableArenaCell;
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
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

    fn unique_scan(
        arena: &mut PlanArena<'_>,
        name: &str,
        types: &[LogicalType],
        indexes: &[&[usize]],
    ) -> LogicalPlan {
        let mut plan = scan(arena, name, types, indexes);
        let Operator::TableScan(scan) = &mut plan.operator else {
            unreachable!()
        };
        for info in &mut scan.index_infos {
            let mut meta = arena.index(info.meta).clone();
            meta.ty = IndexType::Unique;
            info.meta = arena.alloc_index(meta);
        }
        plan
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
    fn scalar_probe_rejects_composite_unique_keys() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut right = scan(
            &mut arena,
            "right",
            &[LogicalType::Integer, LogicalType::Integer],
            &[&[0, 1], &[0]],
        );
        let (column, single) = {
            let Operator::TableScan(scan) = &mut right.operator else {
                unreachable!()
            };
            let mut composite = arena.index(scan.index_infos[0].meta).clone();
            composite.ty = IndexType::Unique;
            scan.index_infos[0].meta = arena.alloc_index(composite);
            (scan.columns[0], scan.index_infos.pop().unwrap())
        };
        let probe = arena.alloc_expression(ScalarExpression::from(2));
        assert!(parameterize_right_subtree(&mut right, &column, probe, &mut arena)?.is_none());
        assert!(right.physical_option.is_none());
        let Operator::TableScan(scan) = &mut right.operator else {
            unreachable!()
        };
        assert!(scan.index_infos[0].lookup.is_none());
        scan.index_infos.push(single);
        // Prefer a usable scalar index even though UNIQUE normally ranks higher.
        assert_eq!(
            pick_parameterized_index_position(scan, &column, &arena),
            Some(1)
        );
        assert_eq!(
            parameterize_right_subtree(&mut right, &column, probe, &mut arena)?,
            Some(probe)
        );
        let PlanImpl::IndexScan(index) = &right.physical_option.as_ref().unwrap().plan else {
            unreachable!()
        };
        assert_eq!(arena.index(index.meta).column_ids, vec![0]);
        assert!(matches!(index.lookup, Some(IndexLookup::Probe)));
        Ok(())
    }

    #[test]
    fn scalar_probe_normalizes_only_safe_key_types() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut right = scan(&mut arena, "right", &[LogicalType::Bigint], &[&[0]]);
        let Operator::TableScan(op) = &right.operator else {
            unreachable!()
        };
        let column = op.columns[0];
        let original = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(2)));
        let probe = parameterize_right_subtree(&mut right, &column, original, &mut arena)?.unwrap();
        assert_eq!(probe.return_type(&arena).as_ref(), &LogicalType::Bigint);
        assert_eq!(
            *arena.expression(probe).eval(&arena, None::<&Tuple>)?,
            DataValue::Int64(2)
        );
        assert_eq!(
            *arena.expression(original).eval(&arena, None::<&Tuple>)?,
            DataValue::Int32(2)
        );

        for value in [
            DataValue::Float64(2.5.into()),
            DataValue::from("hello".to_string()),
        ] {
            let mut right = scan(&mut arena, "right", &[LogicalType::Bigint], &[&[0]]);
            let Operator::TableScan(op) = &right.operator else {
                unreachable!()
            };
            let column = op.columns[0];
            let original = arena.alloc_expression(ScalarExpression::Constant(value));
            assert!(
                parameterize_right_subtree(&mut right, &column, original, &mut arena)?.is_none()
            );
            assert!(right.physical_option.is_none());
            let Operator::TableScan(op) = &right.operator else {
                unreachable!()
            };
            assert!(op.index_infos[0].lookup.is_none());
        }

        let mut right = scan(&mut arena, "narrow", &[LogicalType::Integer], &[&[0]]);
        let Operator::TableScan(op) = &right.operator else {
            unreachable!()
        };
        let column = op.columns[0];
        let large = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int64(1 << 40)));
        assert!(parameterize_right_subtree(&mut right, &column, large, &mut arena)?.is_none());
        assert!(right.physical_option.is_none());
        Ok(())
    }

    #[test]
    fn projected_probe_resolves_output_alias_but_rejects_computed_keys_and_limits(
    ) -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let base = scan(&mut arena, "right", &[LogicalType::Integer], &[&[0]]);
        let source = column(&base, 0, &mut arena);
        let output = arena.alloc_column(ColumnCatalog::new(
            "renamed".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let alias_column = arena.alloc_expression(ScalarExpression::column_expr(output, 0));
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: source,
            alias: AliasType::Expr(alias_column),
        });
        let project = |input| {
            LogicalPlan::new(
                Operator::Project(ProjectOperator::new(vec![alias])),
                Childrens::Only(Box::new(input)),
            )
        };
        let probe = arena.alloc_expression(ScalarExpression::from(2));
        let mut projected = project(base.clone());
        assert_eq!(
            parameterize_right_subtree(&mut projected, &output, probe, &mut arena)?,
            Some(probe)
        );
        let input = projected.childrens.only();
        let Operator::TableScan(input) = &input.operator else {
            unreachable!()
        };
        assert_eq!(input.index_infos[0].lookup, Some(IndexLookup::Probe));
        assert!(matches!(
            projected
                .childrens
                .only()
                .physical_option
                .as_ref()
                .unwrap()
                .plan,
            PlanImpl::IndexScan(_)
        ));

        let mut limited = project(LimitOperator::build(Some(1), Some(1), base.clone()));
        assert!(parameterize_right_subtree(&mut limited, &output, probe, &mut arena)?.is_none());
        let mut pushed = base.clone();
        let Operator::TableScan(op) = &mut pushed.operator else {
            unreachable!()
        };
        op.limit = (None, Some(1));
        let mut pushed = project(pushed);
        assert!(parameterize_right_subtree(&mut pushed, &output, probe, &mut arena)?.is_none());

        let computed = arena.alloc_expression(ScalarExpression::Unary {
            op: UnaryOperator::Minus,
            expr: source,
            evaluator: None,
            ty: LogicalType::Integer,
        });
        let computed = arena.alloc_expression(ScalarExpression::Alias {
            expr: computed,
            alias: AliasType::Expr(alias_column),
        });
        let mut projected = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![computed])),
            Childrens::Only(Box::new(base)),
        );
        assert!(parameterize_right_subtree(&mut projected, &output, probe, &mut arena)?.is_none());
        Ok(())
    }

    #[test]
    fn parameterized_join_preserves_rows_without_guessing_probe_cardinality(
    ) -> Result<(), DatabaseError> {
        for rows in [None, Some(0), Some(120)] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let mut left = scan(&mut arena, "outer", &[LogicalType::Integer], &[]);
            let mut right = unique_scan(&mut arena, "inner", &[LogicalType::Integer], &[&[0]]);
            let l = column(&left, 0, &mut arena);
            let r = column(&right, 0, &mut arena);
            left.physical_option = Some(PhysicalOption::new(
                PlanImpl::SeqScan,
                SortOption::None,
                RowCountOption::Estimated {
                    rows: 129,
                    ndv: None,
                },
            ));
            right.physical_option = Some(PhysicalOption::new(
                PlanImpl::SeqScan,
                SortOption::None,
                RowCountOption::Estimated {
                    rows: 120,
                    ndv: None,
                },
            ));
            let mut plan = join(left, right, vec![(l, r)], None);
            if let Some(rows) = rows {
                plan.physical_option = Some(PhysicalOption::new(
                    PlanImpl::HashJoin,
                    SortOption::None,
                    RowCountOption::Estimated { rows, ndv: None },
                ));
                plan.physical_option.as_mut().unwrap().cost = Some(WorkCost {
                    width: 1,
                    startup: Some(9999),
                    total: Some(9999),
                });
            }
            assert!(ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?);
            let option = plan.physical_option.as_ref().unwrap();
            assert_eq!(option.plan, PlanImpl::MarkApply);
            assert_eq!(option.cost, None);
            assert_eq!(plan.estimated_rows(), rows);
            let Childrens::Twins { right, .. } = plan.childrens.as_ref() else {
                unreachable!()
            };
            assert_eq!(right.estimated_rows(), None);
            assert_eq!(
                right.physical_option.as_ref().unwrap().row_count,
                RowCountOption::Unknown
            );
            assert_eq!(plan.estimated_startup_work(&mut arena)?, Some(0));
        }
        Ok(())
    }

    #[test]
    fn greedy_decided_join_is_not_parameterized() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let left = scan(&mut arena, "outer", &[LogicalType::Integer], &[]);
        let right = unique_scan(&mut arena, "inner", &[LogicalType::Integer], &[&[0]]);
        let l = column(&left, 0, &mut arena);
        let r = column(&right, 0, &mut arena);
        let mut plan = join(left, right, vec![(l, r)], None);
        let mut option = PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Unknown,
        );
        option.join_decided = true;
        plan.physical_option = Some(option);
        let original = plan.clone();
        assert!(!ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan, original);
        Ok(())
    }

    #[test]
    fn parameterized_join_allows_parent_nested_loop_orientation() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut left = scan(&mut arena, "outer", &[LogicalType::Integer], &[]);
        let mut right = unique_scan(&mut arena, "inner", &[LogicalType::Integer], &[&[0]]);
        let l = column(&left, 0, &mut arena);
        let r = column(&right, 0, &mut arena);
        left.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 129,
                ndv: None,
            },
        ));
        right.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 120,
                ndv: None,
            },
        ));
        let mut apply = join(left, right, vec![(l, r)], None);
        apply.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 10,
                ndv: None,
            },
        ));
        assert!(ParameterizeInnerJoin.apply(&mut apply, &mut arena, None)?);
        let mut other = scan(&mut arena, "other", &[LogicalType::Integer], &[]);
        other.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 200,
                ndv: None,
            },
        ));
        let mut plan =
            JoinOperator::build(other, apply, JoinCondition::None, JoinType::Cross, false);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.estimated_rows(), Some(2000));
        let Childrens::Only(join) = plan.childrens.as_ref() else {
            panic!("expected output-restoring projection")
        };
        let Childrens::Twins { left, .. } = join.childrens.as_ref() else {
            unreachable!()
        };
        assert!(matches!(left.operator, Operator::MarkApply(_)));
        Ok(())
    }

    #[test]
    fn constant_only_probe_leaves_static_scan_and_join_unchanged() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let left = scan(&mut arena, "outer", &[LogicalType::Integer], &[]);
        let mut right = unique_scan(
            &mut arena,
            "inner",
            &[LogicalType::Integer, LogicalType::Integer],
            &[&[1]],
        );
        let l = column(&left, 0, &mut arena);
        let r = column(&right, 0, &mut arena);
        let static_key = column(&right, 1, &mut arena);
        let constant = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(782)));
        let predicate = binary(&mut arena, BinaryOperator::Eq, static_key, constant);
        let Operator::TableScan(scan) = &mut right.operator else {
            unreachable!()
        };
        let info = &mut scan.index_infos[0];
        info.lookup = Some(IndexLookup::Static(Range::Eq(DataValue::Int32(782))));
        info.residual_predicate = Some(predicate);
        right.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(info.clone())),
            SortOption::None,
            RowCountOption::Unknown,
        ));
        // Keep the join-key boundary intact across multiple filter layers.
        let passthrough = arena.alloc_expression(ScalarExpression::from(true));
        let right = FilterOperator::build(passthrough, right, false);
        let right = FilterOperator::build(predicate, right, false);
        let mut plan = join(left, right, vec![(l, r)], None);
        let before = plan.clone();

        assert!(!ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan, before);
        assert_eq!((position(l, &arena), position(r, &arena)), (0, 0));
        Ok(())
    }

    #[test]
    fn constant_only_index_does_not_block_outer_key_probe() -> Result<(), DatabaseError> {
        for swap in [false, true] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let left_indexes: &[&[usize]] = if swap { &[&[0]] } else { &[] };
            let right_indexes: &[&[usize]] = if swap { &[&[1]] } else { &[&[1], &[0]] };
            let left = unique_scan(&mut arena, "outer", &[LogicalType::Integer], left_indexes);
            let right = unique_scan(
                &mut arena,
                "inner",
                &[LogicalType::Integer, LogicalType::Integer],
                right_indexes,
            );
            let l = column(&left, 0, &mut arena);
            let r = column(&right, 0, &mut arena);
            let static_key = column(&right, 1, &mut arena);
            let constant =
                arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(782)));
            let predicate = binary(&mut arena, BinaryOperator::Eq, constant, static_key);
            let right = FilterOperator::build(predicate, right, false);
            let original_right = right.clone();
            let mut plan = join(left, right, vec![(l, r)], None);
            let schema = plan.output_schema(&mut arena).clone();

            assert!(ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?);
            assert_eq!(plan.output_schema(&mut arena), &schema);
            let apply_plan = if swap {
                let Childrens::Only(child) = plan.childrens.as_ref() else {
                    panic!("expected output-restoring projection")
                };
                child.as_ref()
            } else {
                &plan
            };
            let Operator::MarkApply(apply) = &apply_plan.operator else {
                panic!("expected inner join apply")
            };
            assert_eq!(apply.kind, MarkApplyKind::InnerJoin);
            assert_eq!(apply.predicates().len(), 1);
            assert_eq!(
                apply.parameterized_probe().copied(),
                Some(if swap { r } else { l })
            );
            let Childrens::Twins { left, right } = apply_plan.childrens.as_ref() else {
                unreachable!()
            };
            let scan = if swap {
                assert_eq!(left.as_ref(), &original_right);
                right.as_ref()
            } else {
                let Operator::Filter(filter) = &right.operator else {
                    panic!("static filter must survive")
                };
                assert_eq!(filter.predicate, predicate);
                let Childrens::Only(child) = right.childrens.as_ref() else {
                    unreachable!()
                };
                child.as_ref()
            };
            let PlanImpl::IndexScan(info) = &scan.physical_option.as_ref().unwrap().plan else {
                panic!("expected index scan")
            };
            assert_eq!(arena.index(info.meta).column_ids, vec![0]);
            assert_eq!(info.lookup, Some(IndexLookup::Probe));
        }
        Ok(())
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
        // Skip the constant-only and unmatched indexes; the last uses both key sources.
        let mut right = unique_scan(
            &mut arena,
            "inner",
            &[const { LogicalType::Integer }; 4],
            &[&[0], &[3], &[0, 2, 1]],
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
        let info = &mut scan.index_infos[2];
        let mut meta = arena.index(info.meta).clone();
        meta.ty = IndexType::PrimaryKey { is_multiple: true };
        info.meta = arena.alloc_index(meta);
        info.lookup = Some(IndexLookup::Static(Range::Eq(DataValue::Int32(7))));
        info.residual_predicate = Some(residual);
        let index = info.meta;
        right.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(info.clone())),
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let right = FilterOperator::build(predicate, right, false);
        let mut plan = join(left, right, vec![(l0, r1), (l1, r2)], None);

        assert!(ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?);
        let Operator::MarkApply(apply) = &plan.operator else {
            panic!("expected apply without projection")
        };
        assert_eq!(apply.kind, MarkApplyKind::InnerJoin);
        assert_eq!(apply.predicates().len(), 2);
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
        let mut left = unique_scan(
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
        left.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 100,
                ndv: None,
            },
        ));
        right.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 10,
                ndv: None,
            },
        ));
        let mut plan = join(left, right, vec![(l, r)], Some(predicate));
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 30,
                ndv: None,
            },
        ));

        assert!(ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.estimated_rows(), Some(30));
        assert_eq!(
            plan.physical_option.as_ref().unwrap().row_count,
            RowCountOption::Follow
        );
        assert_eq!(plan.estimated_startup_work(&mut arena)?, Some(0));
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
            project.physical_option.as_ref().unwrap().row_count,
            RowCountOption::Follow
        );
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
        assert_eq!(apply_plan.estimated_rows(), Some(30));
        assert_eq!(
            apply_plan.physical_option.as_ref().unwrap().plan,
            PlanImpl::MarkApply
        );
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
        for case in [
            "missing_key",
            "type_mismatch",
            "inner_limit",
            "outer_join",
            "normal_index",
            "composite_index",
        ] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let left = scan(&mut arena, "left", &[LogicalType::Integer], &[]);
            let ty = if case == "type_mismatch" {
                LogicalType::Bigint
            } else {
                LogicalType::Integer
            };
            let index: &[usize] = if matches!(case, "missing_key" | "composite_index") {
                &[0, 1]
            } else {
                &[0]
            };
            let mut right = if matches!(case, "normal_index" | "composite_index") {
                scan(&mut arena, "right", &[ty, LogicalType::Integer], &[index])
            } else {
                unique_scan(&mut arena, "right", &[ty, LogicalType::Integer], &[index])
            };
            if case == "inner_limit" {
                let Operator::TableScan(scan) = &mut right.operator else {
                    unreachable!()
                };
                scan.limit = (None, Some(1));
            }
            let l = column(&left, 0, &mut arena);
            let r = column(&right, 0, &mut arena);
            let mut keys = vec![(l, r)];
            if case == "composite_index" {
                keys.push((l, column(&right, 1, &mut arena)));
            }
            let mut plan = join(left, right, keys, None);
            if case == "outer_join" {
                let Operator::Join(join) = &mut plan.operator else {
                    unreachable!()
                };
                join.join_type = JoinType::LeftOuter;
            }
            let before = plan.clone();
            assert!(
                !ParameterizeInnerJoin.apply(&mut plan, &mut arena, None)?,
                "{case}"
            );
            assert_eq!(plan, before, "{case}");
            assert_eq!((position(l, &arena), position(r, &arena)), (0, 0), "{case}");
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
