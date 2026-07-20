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
use crate::expression::ScalarExpression;
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child, wrap_child_with};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::planner::{Childrens, LogicalPlan};
use crate::types::index::{IndexLookup, IndexOrderHint};

pub struct EliminateRedundantSort;

impl NormalizationRule for EliminateRedundantSort {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let (sort_fields, topk_limit) = match &plan.operator {
            Operator::Sort(sort_op) => (sort_op.sort_fields.clone(), None),
            Operator::TopK(topk_op) => (
                topk_op.sort_fields.clone(),
                Some((topk_op.limit, topk_op.offset)),
            ),
            _ => return Ok(false),
        };

        let child = match only_child_mut(plan) {
            Some(child) => child,
            None => return Ok(false),
        };
        mark_sort_preserving_indexes(child, &sort_fields, arena)?;
        let can_remove = ensure_order(child, &sort_fields, arena);

        if !can_remove {
            return Ok(false);
        }

        if let Some((limit, offset)) = topk_limit {
            plan.operator = Operator::Limit(LimitOperator {
                offset,
                limit: Some(limit),
            });
            plan.physical_option = Some(PhysicalOption::new(PlanImpl::Limit, SortOption::Follow));
            return Ok(true);
        }

        Ok(replace_with_only_child(plan))
    }
}

pub struct EliminateIndexFilter;

impl NormalizationRule for EliminateIndexFilter {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        _: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        if !matches!(plan.operator, Operator::Filter(_)) {
            return Ok(false);
        }

        let residual = {
            let Some(child) = only_child_mut(plan) else {
                return Ok(false);
            };
            let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(index_info),
                ..
            }) = child.physical_option.as_ref()
            else {
                return Ok(false);
            };
            if !matches!(index_info.lookup, Some(IndexLookup::Static(_))) {
                return Ok(false);
            }
            index_info.residual_predicate.clone()
        };

        if let Some(residual) = residual {
            let Operator::Filter(filter_op) = &mut plan.operator else {
                unreachable!("filter operator checked before residual rewrite");
            };
            filter_op.predicate = residual;
            return Ok(true);
        }

        Ok(replace_with_only_child(plan))
    }
}

fn mark_sort_preserving_indexes(
    plan: &mut LogicalPlan,
    required: &[SortField],
    arena: &crate::planner::PlanArena,
) -> Result<(), DatabaseError> {
    mark_order_hint(plan, required, OrderHintKind::SortElimination, arena)
}

#[derive(Copy, Clone)]
pub(crate) enum OrderHintKind {
    SortElimination,
    StreamAggregate,
}

#[derive(Copy, Clone)]
pub(crate) enum ScanOrderHint<'a> {
    SortFields(&'a [SortField]),
    GroupBy(&'a [ScalarExpression]),
}

impl<'a> ScanOrderHint<'a> {
    pub(crate) fn sort_fields(fields: &'a [SortField]) -> Self {
        Self::SortFields(fields)
    }

    pub(crate) fn groupby(groupby_exprs: &'a [ScalarExpression]) -> Self {
        Self::GroupBy(groupby_exprs)
    }
}

fn mark_order_hint(
    plan: &mut LogicalPlan,
    required: &[SortField],
    hint: OrderHintKind,
    arena: &crate::planner::PlanArena,
) -> Result<(), DatabaseError> {
    if required.is_empty() {
        return Ok(());
    }

    match &mut plan.operator {
        Operator::Filter(_)
        | Operator::Project(_)
        | Operator::Limit(_)
        | Operator::TopK(_)
        | Operator::Sort(_) => {
            if let Childrens::Only(child) = plan.childrens.as_mut() {
                mark_order_hint(child, required, hint, arena)?;
            }
        }
        Operator::TableScan(scan_op) => {
            apply_scan_order_hint(scan_op, ScanOrderHint::sort_fields(required), hint, arena)?;
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn apply_scan_order_hint(
    scan_op: &mut TableScanOperator,
    required: ScanOrderHint<'_>,
    hint: OrderHintKind,
    arena: &crate::planner::PlanArena,
) -> Result<(), DatabaseError> {
    let mut required_from_table = true;
    for index in 0..hint_len(required) {
        let expr = match required {
            ScanOrderHint::SortFields(fields) => &fields[index].expr,
            ScanOrderHint::GroupBy(groupby_exprs) => &groupby_exprs[index],
        };
        if !expr.all_referenced_columns(arena, |arena, column| {
            scan_op
                .columns
                .iter()
                .any(|scan_column| arena.same_column(*scan_column, *column))
        })? {
            required_from_table = false;
            break;
        }
    }
    if !required_from_table {
        return Ok(());
    }
    for index_info in scan_op.index_infos.iter_mut() {
        if hint_covers(required, &index_info.sort_option, arena) {
            let covered = hint_len(required);
            match hint {
                OrderHintKind::SortElimination => {
                    if let Some(hint) = &mut index_info.sort_elimination_hint {
                        hint.merge_cover_num(covered);
                    } else {
                        index_info.sort_elimination_hint = Some(IndexOrderHint::new(covered));
                    }
                }
                OrderHintKind::StreamAggregate => {
                    if let Some(hint) = &mut index_info.stream_aggregate_hint {
                        hint.merge_cover_num(covered);
                    } else {
                        index_info.stream_aggregate_hint = Some(IndexOrderHint::new(covered));
                    }
                }
            }
        }
    }
    Ok(())
}

fn hint_len(required: ScanOrderHint<'_>) -> usize {
    match required {
        ScanOrderHint::SortFields(fields) => fields.len(),
        ScanOrderHint::GroupBy(groupby_exprs) => groupby_exprs.len(),
    }
}

fn hint_covers(
    required: ScanOrderHint<'_>,
    provided: &SortOption,
    arena: &crate::planner::PlanArena,
) -> bool {
    match required {
        ScanOrderHint::SortFields(fields) => covers(fields, provided, |required, provided| {
            sort_field_matches(required, provided, arena)
        }),
        ScanOrderHint::GroupBy(groupby_exprs) => covers(groupby_exprs, provided, |expr, field| {
            field.asc && !field.nulls_first && expr.eq_ignore_colref_pos(&field.expr, arena)
        }),
    }
}

pub(crate) fn groupby_sort_fields(groupby_exprs: &[ScalarExpression]) -> Vec<SortField> {
    groupby_exprs
        .iter()
        .cloned()
        .map(|expr| SortField::new(expr, true, false))
        .collect()
}

pub struct UseStreamAggregate;

impl NormalizationRule for UseStreamAggregate {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let Operator::Aggregate(op) = &plan.operator else {
            return Ok(false);
        };
        if op.groupby_exprs.is_empty() {
            return Ok(false);
        }
        if !matches!(
            &plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::HashAggregate,
                ..
            })
        ) {
            return Ok(false);
        }

        let implementation = if op.is_distinct && op.agg_calls.is_empty() {
            PlanImpl::StreamDistinct
        } else {
            PlanImpl::StreamAggregate
        };
        let required = groupby_sort_fields(&op.groupby_exprs);
        let child = match only_child_mut(plan) {
            Some(child) => child,
            None => return Ok(false),
        };
        if !ensure_order(child, &required, arena) {
            return Ok(false);
        }

        plan.physical_option = Some(PhysicalOption::new(implementation, SortOption::Follow));
        Ok(true)
    }
}

pub struct ForceSpillAggregate;

impl NormalizationRule for ForceSpillAggregate {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        _: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let (implementation, sort_fields) = match (&plan.operator, &plan.physical_option) {
            (
                Operator::Aggregate(op),
                Some(PhysicalOption {
                    plan: PlanImpl::HashAggregate,
                    ..
                }),
            ) if op.force_spill && !op.groupby_exprs.is_empty() => {
                let implementation = if op.is_distinct && op.agg_calls.is_empty() {
                    PlanImpl::StreamDistinct
                } else {
                    PlanImpl::StreamAggregate
                };
                (implementation, groupby_sort_fields(&op.groupby_exprs))
            }
            _ => return Ok(false),
        };

        if !cfg!(feature = "spill") {
            return Err(DatabaseError::UnsupportedStmt(
                "FORCE_AGG_SPILL requires the `spill` feature".to_string(),
            ));
        }

        let sort_option = SortOption::OrderBy {
            fields: sort_fields.clone(),
            ignore_prefix_len: 0,
        };
        if !wrap_child_with(plan, 0, Operator::Sort(SortOperator { sort_fields })) {
            return Ok(false);
        }
        let sort = only_child_mut(plan).expect("aggregate child was wrapped with sort");
        sort.physical_option = Some(PhysicalOption::new(PlanImpl::Sort, sort_option));
        plan.physical_option = Some(PhysicalOption::new(implementation, SortOption::Follow));
        Ok(true)
    }
}

pub(crate) fn apply_annotated_post_rules(
    plan: &mut LogicalPlan,
    arena: &mut crate::planner::PlanArena,
) -> Result<bool, DatabaseError> {
    let mut changed = false;

    if EliminateRedundantSort.apply(plan, arena)? {
        changed = true;
    }
    if EliminateIndexFilter.apply(plan, arena)? {
        changed = true;
    }
    if UseStreamAggregate.apply(plan, arena)? {
        changed = true;
    }
    // Run last so an existing ordered child is reused before a forced spill adds a Sort.
    if ForceSpillAggregate.apply(plan, arena)? {
        changed = true;
    }

    Ok(changed)
}

fn ensure_order(
    plan: &mut LogicalPlan,
    required: &[SortField],
    arena: &crate::planner::PlanArena,
) -> bool {
    if let Some(PhysicalOption {
        plan: PlanImpl::IndexScan(index_info),
        ..
    }) = plan.physical_option.as_ref()
    {
        if covers(required, &index_info.sort_option, |required, provided| {
            sort_field_matches(required, provided, arena)
        }) {
            return true;
        }
    }

    if let Some(physical_option) = plan.physical_option.as_ref() {
        match physical_option.sort_option() {
            SortOption::OrderBy { .. } => {
                return covers(
                    required,
                    physical_option.sort_option(),
                    |required, provided| sort_field_matches(required, provided, arena),
                );
            }
            SortOption::Follow => {
                if let Childrens::Only(child) = plan.childrens.as_mut() {
                    if ensure_order(child, required, arena) {
                        return true;
                    }
                }
            }
            SortOption::None => {}
        }
    }

    false
}

fn sort_field_matches(
    required: &SortField,
    provided: &SortField,
    arena: &crate::planner::PlanArena,
) -> bool {
    required.asc == provided.asc
        && required.nulls_first == provided.nulls_first
        && required.expr.eq_ignore_colref_pos(&provided.expr, arena)
}

pub(crate) fn covers<T>(
    required: &[T],
    provided: &SortOption,
    mut matches: impl FnMut(&T, &SortField) -> bool,
) -> bool {
    if required.is_empty() {
        return true;
    }

    match provided {
        SortOption::OrderBy {
            fields,
            ignore_prefix_len,
        } => {
            if fields.is_empty() {
                return false;
            }
            let max_skip = (*ignore_prefix_len).min(fields.len());

            for skip in 0..=max_skip {
                if fields.len() < skip + required.len() {
                    continue;
                }
                if required
                    .iter()
                    .zip(fields.iter().skip(skip))
                    .all(|(lhs, rhs)| matches(lhs, rhs))
                {
                    return true;
                }
            }
            false
        }
        SortOption::Follow | SortOption::None => false,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{
        EliminateIndexFilter, EliminateRedundantSort, ForceSpillAggregate, UseStreamAggregate,
    };
    use crate::catalog::{ColumnCatalog, TableName};
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::expression::ScalarExpression;
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::operator::top_k::TopKOperator;
    use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
    use crate::planner::{Childrens, LogicalPlan};
    use crate::types::index::{IndexInfo, IndexLookup, IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::ColumnId;
    use crate::types::LogicalType;
    use std::ops::Bound;
    fn make_sort_field(arena: &mut crate::planner::PlanArena, name: &str) -> SortField {
        make_sort_field_with_position(arena, name, 0)
    }

    fn make_sort_field_with_position(
        arena: &mut crate::planner::PlanArena,
        name: &str,
        position: usize,
    ) -> SortField {
        let column = arena.alloc_column(ColumnCatalog::new_dummy(name.to_string()));
        SortField::new(ScalarExpression::column_expr(column, position), true, false)
    }

    fn build_plan(
        arena: &mut crate::planner::PlanArena,
        required_fields: Vec<SortField>,
        index_fields: Vec<SortField>,
        ignore_prefix_len: usize,
    ) -> LogicalPlan {
        let (index_info, index_sort_option) =
            build_index_info(arena, index_fields, ignore_prefix_len);

        let mut leaf = LogicalPlan::new(Operator::Dummy, Childrens::None);
        leaf.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(index_info)),
            index_sort_option,
        ));

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: ScalarExpression::Constant(DataValue::Boolean(true)),
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(leaf)),
        );
        filter.physical_option = Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow));

        LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: required_fields,
            }),
            Childrens::Only(Box::new(filter)),
        )
    }

    fn build_index_info(
        arena: &mut crate::planner::PlanArena,
        index_fields: Vec<SortField>,
        ignore_prefix_len: usize,
    ) -> (IndexInfo, SortOption) {
        let len = index_fields.len();
        let sort_option = SortOption::OrderBy {
            fields: index_fields,
            ignore_prefix_len,
        };
        let table_name: TableName = ::std::sync::Arc::from("t1");
        let meta = arena.alloc_index(IndexMeta {
            id: 1,
            column_ids: (1..=len as ColumnId).collect(),
            table_name,
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "idx".to_string(),
            ty: IndexType::PrimaryKey {
                is_multiple: len > 1,
            },
        });
        let index_info = IndexInfo {
            meta,
            sort_option: sort_option.clone(),
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        };
        (index_info, sort_option)
    }

    fn build_filter_with_selected_index(
        arena: &mut crate::planner::PlanArena,
        predicate: ScalarExpression,
        residual: Option<ScalarExpression>,
    ) -> LogicalPlan {
        let column = arena.alloc_column(ColumnCatalog::new_dummy("c1".to_string()));
        let sort_field = SortField::new(ScalarExpression::column_expr(column, 0), true, false);
        let (mut index_info, sort_option) = build_index_info(arena, vec![sort_field], 0);
        index_info.lookup = Some(IndexLookup::Static(Range::Scope {
            min: Bound::Unbounded,
            max: Bound::Unbounded,
        }));
        index_info.residual_predicate = residual;

        let mut scan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: ::std::sync::Arc::from("t1"),
                columns: vec![column],
                limit: (None, None),
                index_infos: vec![index_info.clone()],
                with_pk: false,
            }),
            Childrens::None,
        );
        scan.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(index_info)),
            sort_option,
        ));

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(scan)),
        );
        filter.physical_option = Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow));
        filter
    }

    fn build_distinct_scan_plan(
        arena: &mut crate::planner::PlanArena,
    ) -> (LogicalPlan, SortOption) {
        let table_name: TableName = ::std::sync::Arc::from("t1");
        let c1 = arena.alloc_column(ColumnCatalog::new_dummy("c1".to_string()));
        let c1_id = 1;
        let columns = vec![c1];

        let sort_fields = vec![SortField::new(
            ScalarExpression::column_expr(c1, 0),
            true,
            false,
        )];
        let sort_option = SortOption::OrderBy {
            fields: sort_fields.clone(),
            ignore_prefix_len: 0,
        };
        let index_info = IndexInfo {
            meta: arena.alloc_index(IndexMeta {
                id: 1,
                column_ids: vec![c1_id],
                table_name: table_name.clone(),
                pk_ty: LogicalType::Integer,
                value_ty: LogicalType::Integer,
                name: "idx".to_string(),
                ty: IndexType::PrimaryKey { is_multiple: false },
            }),
            sort_option: sort_option.clone(),
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        };

        let scan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name,
                columns,
                limit: (None, None),
                index_infos: vec![index_info],
                with_pk: false,
            }),
            Childrens::None,
        );

        let plan = LogicalPlan::new(
            Operator::Aggregate(AggregateOperator {
                groupby_exprs: vec![ScalarExpression::column_expr(c1, 0)],
                agg_calls: vec![],
                is_distinct: true,
                force_spill: false,
            }),
            Childrens::Only(Box::new(scan)),
        );

        (plan, sort_option)
    }

    #[test]
    fn exact_index_filter_is_removed_after_physical_selection() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let predicate = ScalarExpression::Constant(DataValue::Boolean(true));
        let mut plan = build_filter_with_selected_index(&mut arena, predicate, None);

        let rule = EliminateIndexFilter;
        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(plan.operator, Operator::TableScan(_)));
        Ok(())
    }

    #[test]
    fn partial_index_filter_keeps_residual_after_physical_selection() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let predicate = ScalarExpression::Constant(DataValue::Boolean(true));
        let residual = ScalarExpression::Constant(DataValue::Boolean(false));
        let mut plan =
            build_filter_with_selected_index(&mut arena, predicate, Some(residual.clone()));

        let rule = EliminateIndexFilter;
        assert!(rule.apply(&mut plan, &mut arena)?);
        let Operator::Filter(filter_op) = &plan.operator else {
            unreachable!("residual predicate should keep filter");
        };
        assert_eq!(filter_op.predicate, residual);
        Ok(())
    }

    #[test]
    fn probe_index_filter_is_not_removed_after_physical_selection() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let predicate = ScalarExpression::Constant(DataValue::Boolean(true));
        let mut plan = build_filter_with_selected_index(&mut arena, predicate, None);
        let Childrens::Only(child) = plan.childrens.as_mut() else {
            unreachable!("filter should have a scan child");
        };
        let Operator::TableScan(scan_op) = &mut child.operator else {
            unreachable!("filter child should be a table scan");
        };
        scan_op.index_infos[0].lookup = Some(IndexLookup::Probe);
        let Some(physical_option) = child.physical_option.as_mut() else {
            unreachable!("scan should have selected physical option");
        };
        let PlanImpl::IndexScan(index_info) = &mut physical_option.plan else {
            unreachable!("scan should have selected index scan");
        };
        index_info.lookup = Some(IndexLookup::Probe);

        let rule = EliminateIndexFilter;
        assert!(!rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(plan.operator, Operator::Filter(_)));
        Ok(())
    }

    #[test]
    fn remove_sort_when_index_matches_order() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let sort_field = make_sort_field(&mut arena, "c1");
        let mut plan = build_plan(&mut arena, vec![sort_field.clone()], vec![sort_field], 0);
        let rule = EliminateRedundantSort;

        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(plan.operator, Operator::Filter(_)));
        Ok(())
    }

    #[test]
    fn remove_topk_when_index_matches_order() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let sort_field = make_sort_field(&mut arena, "c1");
        let mut plan = build_plan(
            &mut arena,
            vec![sort_field.clone()],
            vec![sort_field.clone()],
            0,
        );
        plan.operator = Operator::TopK(TopKOperator {
            sort_fields: vec![sort_field],
            limit: 10,
            offset: Some(5),
        });
        let rule = EliminateRedundantSort;

        assert!(rule.apply(&mut plan, &mut arena)?);
        match plan.operator {
            Operator::Limit(limit_op) => {
                assert_eq!(limit_op.limit, Some(10));
                assert_eq!(limit_op.offset, Some(5));
            }
            _ => unreachable!("expected limit operator after removing topk"),
        }
        Ok(())
    }

    #[test]
    fn remove_sort_when_prefix_can_be_ignored() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let c1 = make_sort_field(&mut arena, "c1");
        let c2 = make_sort_field(&mut arena, "c2");
        let mut plan = build_plan(&mut arena, vec![c2.clone()], vec![c1, c2.clone()], 1);
        super::mark_sort_preserving_indexes(&mut plan, &[c2], &arena)?;
        let rule = EliminateRedundantSort;

        assert!(rule.apply(&mut plan, &mut arena)?);
        Ok(())
    }

    #[test]
    fn remove_topk_when_index_matches_same_column_with_different_positions(
    ) -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let required = make_sort_field_with_position(&mut arena, "no_o_id", 0);
        let provided_prefix_1 = make_sort_field_with_position(&mut arena, "no_w_id", 0);
        let provided_prefix_2 = make_sort_field_with_position(&mut arena, "no_d_id", 1);
        let provided_target = make_sort_field_with_position(&mut arena, "no_o_id", 2);

        let mut plan = build_plan(
            &mut arena,
            vec![required.clone()],
            vec![provided_prefix_1, provided_prefix_2, provided_target],
            2,
        );
        plan.operator = Operator::TopK(TopKOperator {
            sort_fields: vec![required],
            limit: 1,
            offset: None,
        });

        let rule = EliminateRedundantSort;
        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(plan.operator, Operator::Limit(_)));
        Ok(())
    }

    #[test]
    fn annotate_sets_sort_hint_on_table_scan() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let column = arena.alloc_column(ColumnCatalog::new_dummy("c1".to_string()));
        let sort_field = SortField::new(ScalarExpression::column_expr(column, 0), true, false);
        let (index_info, _) = build_index_info(&mut arena, vec![sort_field.clone()], 0);

        let columns = vec![column];
        let table_name: TableName = ::std::sync::Arc::from("t");
        let table_scan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: table_name.clone(),
                columns,
                limit: (None, None),
                index_infos: vec![index_info],
                with_pk: false,
            }),
            Childrens::None,
        );

        let mut plan = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![sort_field],
            }),
            Childrens::Only(Box::new(table_scan)),
        );

        let sort_fields = match &plan.operator {
            Operator::Sort(sort_op) => sort_op.sort_fields.clone(),
            _ => unreachable!("expected sort operator"),
        };
        super::mark_sort_preserving_indexes(&mut plan, &sort_fields, &arena)?;

        let table_plan = plan.childrens.pop_only();
        match table_plan.operator {
            Operator::TableScan(scan_op) => assert!(
                scan_op
                    .index_infos
                    .iter()
                    .any(|info| info.sort_elimination_hint.is_some()),
                "expected sort elimination hint on at least one index"
            ),
            _ => unreachable!("expected table scan under sort"),
        }
        Ok(())
    }

    #[test]
    fn annotate_sets_stream_aggregate_hint_on_table_scan() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, _) = build_distinct_scan_plan(&mut arena);
        let required = match &plan.operator {
            Operator::Aggregate(op) => super::groupby_sort_fields(&op.groupby_exprs),
            _ => unreachable!("expected aggregate operator"),
        };
        if let Childrens::Only(child) = plan.childrens.as_mut() {
            super::mark_order_hint(
                child,
                &required,
                super::OrderHintKind::StreamAggregate,
                &arena,
            )?;
        }

        let child = plan.childrens.pop_only();
        let Operator::TableScan(scan_op) = child.operator else {
            unreachable!()
        };

        assert_eq!(scan_op.index_infos.len(), 1);
        assert_eq!(
            scan_op.index_infos[0]
                .stream_aggregate_hint
                .map(|hint| hint.cover_num()),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn use_stream_distinct_when_order_satisfied() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, sort_option) = build_distinct_scan_plan(&mut arena);
        if let Childrens::Only(child) = plan.childrens.as_mut() {
            if let Operator::TableScan(scan_op) = &child.operator {
                let index_info = scan_op.index_infos[0].clone();
                child.physical_option = Some(PhysicalOption::new(
                    PlanImpl::IndexScan(Box::new(index_info)),
                    sort_option.clone(),
                ));
            }
        }
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
        ));

        let rule = UseStreamAggregate;
        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(
            plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::StreamDistinct,
                ..
            })
        ));
        Ok(())
    }

    #[test]
    fn use_stream_aggregate_only_when_order_satisfied() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, sort_option) = build_distinct_scan_plan(&mut arena);
        let Operator::Aggregate(op) = &mut plan.operator else {
            unreachable!()
        };
        op.is_distinct = false;
        op.force_spill = true;
        let Childrens::Only(child) = plan.childrens.as_mut() else {
            unreachable!()
        };
        let Operator::TableScan(scan_op) = &child.operator else {
            unreachable!()
        };
        child.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(scan_op.index_infos[0].clone())),
            sort_option,
        ));
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
        ));

        let rule = UseStreamAggregate;
        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(
            plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::StreamAggregate,
                ..
            })
        ));
        let force_spill = ForceSpillAggregate;
        assert!(!force_spill.apply(&mut plan, &mut arena)?);
        assert!(!matches!(
            plan.childrens.as_ref(),
            Childrens::Only(child) if matches!(child.operator, Operator::Sort(_))
        ));

        let (mut unordered, _) = build_distinct_scan_plan(&mut arena);
        let Operator::Aggregate(op) = &mut unordered.operator else {
            unreachable!()
        };
        op.is_distinct = false;
        unordered.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
        ));
        assert!(!rule.apply(&mut unordered, &mut arena)?);
        assert!(matches!(
            unordered.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::HashAggregate,
                ..
            })
        ));
        Ok(())
    }

    #[cfg(feature = "spill")]
    #[test]
    fn force_spill_distinct_sorts_unordered_input() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, _) = build_distinct_scan_plan(&mut arena);
        let Operator::Aggregate(op) = &mut plan.operator else {
            unreachable!()
        };
        op.force_spill = true;
        let expected_fields = super::groupby_sort_fields(&op.groupby_exprs);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
        ));

        let rule = ForceSpillAggregate;
        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(
            plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::StreamDistinct,
                ..
            })
        ));
        let Childrens::Only(sort) = plan.childrens.as_ref() else {
            unreachable!()
        };
        assert!(matches!(
            &sort.operator,
            Operator::Sort(SortOperator { sort_fields }) if sort_fields == &expected_fields
        ));
        assert!(matches!(
            sort.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::Sort,
                ..
            })
        ));
        assert!(matches!(
            sort.childrens.as_ref(),
            Childrens::Only(child) if matches!(child.operator, Operator::TableScan(_))
        ));
        Ok(())
    }

    #[test]
    fn keep_sort_when_order_not_covered() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let c1 = make_sort_field(&mut arena, "c1");
        let c2 = make_sort_field(&mut arena, "c2");
        let mut plan = build_plan(
            &mut arena,
            vec![c2.clone()],
            vec![c1.clone(), c2.clone()],
            0,
        );
        super::mark_sort_preserving_indexes(&mut plan, &[c2], &arena)?;
        let rule = EliminateRedundantSort;

        assert!(!rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(plan.operator, Operator::Sort(_)));
        Ok(())
    }

    #[test]
    fn promote_index_to_remove_sort() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let column = arena.alloc_column(ColumnCatalog::new_dummy("c_first".to_string()));
        let sort_field = SortField::new(ScalarExpression::column_expr(column, 0), true, false);
        let (mut index_info, _) = build_index_info(&mut arena, vec![sort_field.clone()], 0);
        index_info.lookup = Some(IndexLookup::Static(Range::Scope {
            min: Bound::Unbounded,
            max: Bound::Unbounded,
        }));

        let columns = vec![column];

        let mut scan_plan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: ::std::sync::Arc::from("t"),
                columns,
                limit: (None, None),
                index_infos: vec![index_info],
                with_pk: false,
            }),
            Childrens::None,
        );
        if let Operator::TableScan(scan_op) = &scan_plan.operator {
            let index_info = scan_op.index_infos[0].clone();
            scan_plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::IndexScan(Box::new(index_info.clone())),
                index_info.sort_option.clone(),
            ));
        }

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: ScalarExpression::Constant(DataValue::Boolean(true)),
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(scan_plan)),
        );
        filter.physical_option = Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow));

        let mut plan = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![sort_field],
            }),
            Childrens::Only(Box::new(filter)),
        );

        let sort_fields = match &plan.operator {
            Operator::Sort(sort_op) => sort_op.sort_fields.clone(),
            _ => unreachable!("expected sort operator"),
        };
        super::mark_sort_preserving_indexes(&mut plan, &sort_fields, &arena)?;
        let rule = EliminateRedundantSort;
        assert!(rule.apply(&mut plan, &mut arena)?);
        assert!(matches!(plan.operator, Operator::Filter(_)));

        let table_plan = plan.childrens.pop_only();
        assert!(matches!(
            table_plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::IndexScan(_),
                ..
            })
        ));
        Ok(())
    }
}
