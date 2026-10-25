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
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::expression::{AliasType, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::plan_utils::{only_child_mut, replace_with_only_child, wrap_child_with};
use crate::planner::operator::join::{JoinCondition, JoinType};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::visitor_mut::{OperatorExprVisitorMut, OperatorVisitorMut};
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::index::{IndexLookup, IndexOrderHint};
use std::collections::HashSet;

pub struct EliminateProject;

#[derive(Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Preserve,
    Inline,
    ColumnsOnly,
}

#[derive(Default)]
struct Substitute {
    exprs: Option<Vec<ExprRef>>,
    visited: HashSet<ExprRef>,
    offset: usize,
    opaque: bool,
}

impl<A: MetaArena + ?Sized> ExprVisitorMut<A> for Substitute {
    fn visit_expression_ref(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut A,
    ) -> Result<bool, DatabaseError> {
        if matches!(
            arena.expression(*expr),
            ScalarExpression::ScalaFunction(_) | ScalarExpression::TableFunction(_)
        ) {
            self.opaque = true;
            return Ok(false);
        }
        if let ScalarExpression::ColumnRef { column, position } = arena.expression(*expr) {
            if let Some(exprs) = &self.exprs {
                *expr = exprs[*position];
            } else if self.offset != 0 {
                *expr = arena.alloc_expression(ScalarExpression::column_expr(
                    *column,
                    self.offset + *position,
                ));
            }
            return Ok(false); // Do not revisit the replacement's input layout.
        }
        Ok((self.exprs.is_none() && self.offset == 0) || self.visited.insert(*expr))
    }

    fn visit_alias(
        &mut self,
        expr: &mut ExprRef,
        alias: &mut AliasType,
        arena: &mut A,
    ) -> Result<(), DatabaseError> {
        ExprVisitorMut::visit(
            self,
            match alias {
                AliasType::Expr(expr) => expr,
                _ => expr,
            },
            arena,
        )
    }
}

struct EliminateVisitor<'a, 'b> {
    arena: &'a mut PlanArena<'b>,
    mapping: Substitute,
    output: OutputMode,
    changed: bool,
}

impl EliminateVisitor<'_, '_> {
    fn visit_input(
        &mut self,
        input: &mut LogicalPlan,
        output: OutputMode,
        apply: impl FnOnce(&mut Self, &mut LogicalPlan) -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError> {
        self.output = output;
        self.visit_plan(input)?;
        apply(self, input)
    }
}

impl<'p> OperatorVisitorMut<'p> for EliminateVisitor<'_, '_> {
    fn visit_plan(&mut self, plan: &'p mut LogicalPlan) -> Result<(), DatabaseError> {
        let output = self.output;
        let schema = plan.output_schema(self.arena).clone();

        self.mapping.exprs = None;
        self.mapping.visited.clear();
        self.mapping.opaque = false;
        OperatorExprVisitorMut::new(&mut self.mapping, self.arena)
            .visit_operator(&mut plan.operator, None)?;
        let opaque = self.mapping.opaque;
        match plan.childrens.as_mut() {
            Childrens::Only(child) => {
                let input_schema = child.output_schema(self.arena);
                let forwards = schema.starts_with(input_schema);
                let produces =
                    matches!(plan.operator, Operator::Project(_) | Operator::Aggregate(_));
                // A scalar subquery must materialize its value before generating
                // the NULL row for an empty input. Other new layouts are also
                // boundaries unless expressions explicitly produce their slots.
                let input_mode = if opaque
                    || matches!(plan.operator, Operator::ScalarSubquery(_))
                    || !(forwards || produces)
                {
                    OutputMode::Preserve
                } else {
                    match (output, produces) {
                        (OutputMode::ColumnsOnly, false) => OutputMode::ColumnsOnly,
                        _ => OutputMode::Inline,
                    }
                };
                self.visit_input(child, input_mode, |visitor, child| {
                    let Some(_) = &visitor.mapping.exprs else {
                        return Ok(());
                    };
                    OperatorExprVisitorMut::new(&mut visitor.mapping, visitor.arena)
                        .visit_operator(&mut plan.operator, None)?;
                    if !produces {
                        let width = child.output_schema(visitor.arena).len();
                        if let Some(exprs) = &mut visitor.mapping.exprs {
                            for (index, column) in schema[exprs.len()..].iter().enumerate() {
                                exprs.push(visitor.arena.alloc_expression(
                                    ScalarExpression::column_expr(*column, width + index),
                                ));
                            }
                        }
                    }
                    Ok(())
                })?;
            }
            // TODO: Handle other join types and conditions appropriately.
            Childrens::Twins { left, right }
                if matches!(&plan.operator, Operator::Join(op)
                    if op.join_type == JoinType::Cross && op.on == JoinCondition::None) =>
            {
                let old_left_width = left.output_schema(self.arena).len();
                let left_mode = match output {
                    OutputMode::ColumnsOnly => OutputMode::ColumnsOnly,
                    _ => OutputMode::Inline,
                };
                let mut left_mapping = None;
                self.visit_input(left, left_mode, |visitor, _| {
                    left_mapping = visitor.mapping.exprs.take();
                    Ok(())
                })?;
                let offset = left.output_schema(self.arena).len();
                self.visit_input(right, OutputMode::ColumnsOnly, |visitor, _| {
                    let mut right_mapping = visitor.mapping.exprs.take();
                    if let Some(exprs) = right_mapping.as_mut().filter(|_| offset != 0) {
                        visitor.mapping.offset = offset;
                        visitor.mapping.visited.clear();
                        for expr in exprs {
                            // Keep aliases shared with retained branches untouched.
                            *expr = expr.unpack_alias(visitor.arena);
                            ExprVisitorMut::visit(&mut visitor.mapping, expr, visitor.arena)?;
                        }
                        visitor.mapping.offset = 0;
                    }
                    let mut append_identity = |exprs: &mut Vec<_>, columns: &[_], start| {
                        exprs.extend(columns.iter().enumerate().map(|(index, column)| {
                            visitor
                                .arena
                                .alloc_expression(ScalarExpression::column_expr(
                                    *column,
                                    start + index,
                                ))
                        }));
                    };
                    match (left_mapping.as_mut(), right_mapping.as_mut()) {
                        (Some(left), Some(right)) => left.append(right),
                        (Some(left), None) => {
                            append_identity(left, &schema[old_left_width..], offset)
                        }
                        (None, Some(right)) => {
                            append_identity(right, &schema[..old_left_width], 0);
                            right.rotate_right(old_left_width);
                        }
                        (None, None) => (),
                    }
                    visitor.mapping.exprs = left_mapping.or(right_mapping);
                    visitor.mapping.visited.clear();
                    Ok(())
                })?;
            }
            Childrens::Twins { left, right } => {
                // Other binary operators retain their input layouts.
                self.visit_input(left, OutputMode::Preserve, |_, _| Ok(()))?;
                self.visit_input(right, OutputMode::Preserve, |_, _| Ok(()))?;
            }
            Childrens::None => (),
        }
        match &mut plan.operator {
            Operator::Project(op) => {
                self.mapping.exprs = None; // Project defines a new output slot space.
                if !opaque
                    && match output {
                        OutputMode::Preserve => false,
                        OutputMode::Inline => true,
                        OutputMode::ColumnsOnly => op.exprs.iter().all(|expr| {
                            matches!(
                                expr.unpack_alias_ref(self.arena),
                                ScalarExpression::ColumnRef { .. }
                            )
                        }),
                    }
                {
                    let exprs = std::mem::take(&mut op.exprs);
                    replace_with_only_child(plan);
                    self.mapping.exprs = Some(exprs);
                    self.mapping.visited.clear();
                    self.changed = true;
                    return Ok(());
                }
                for (expr, column) in op.exprs.iter_mut().zip(&schema) {
                    let output = expr.output_column_ref(self.arena);
                    if self.arena.column(output).name() != self.arena.column(*column).name() {
                        let alias = AliasType::Name(self.arena.column(*column).name().to_string());
                        *expr = self
                            .arena
                            .alloc_expression(ScalarExpression::Alias { expr: *expr, alias });
                    }
                }
                op.moveable = None;
            }
            Operator::Aggregate(_) => self.mapping.exprs = None,
            _ => (),
        }
        plan.reset_output_schema_cache();
        if self.changed {
            plan.clear_startup_work();
        }
        self.mapping.exprs = match (output, self.mapping.exprs.take()) {
            (OutputMode::Preserve, Some(exprs)) => {
                let input = plan.take();
                *plan = LogicalPlan::new(
                    Operator::Project(ProjectOperator::new(exprs)),
                    Childrens::Only(Box::new(input)),
                );
                None
            }
            (_, exprs) => exprs,
        };
        Ok(())
    }
}

impl NormalizationRule for EliminateProject {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        let mut visitor = EliminateVisitor {
            arena,
            mapping: Substitute::default(),
            output: OutputMode::Preserve,
            changed: false,
        };
        visitor.visit_plan(plan)?;
        Ok(visitor.changed)
    }
}

pub struct EliminateRedundantSort;

impl NormalizationRule for EliminateRedundantSort {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
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
        let can_remove = ensure_order(child, &sort_fields, true, arena);

        if !can_remove {
            return Ok(false);
        }

        if let Some((limit, offset)) = topk_limit {
            plan.operator = Operator::Limit(LimitOperator {
                offset,
                limit: Some(limit),
            });
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::Limit,
                SortOption::Follow,
                RowCountOption::Unknown,
            ));
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
        _: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
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
            index_info.residual_predicate
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
    GroupBy(&'a [ExprRef]),
}

impl<'a> ScanOrderHint<'a> {
    pub(crate) fn sort_fields(fields: &'a [SortField]) -> Self {
        Self::SortFields(fields)
    }

    pub(crate) fn groupby(groupby_exprs: &'a [ExprRef]) -> Self {
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
            ScanOrderHint::SortFields(fields) => fields[index].expr,
            ScanOrderHint::GroupBy(groupby_exprs) => groupby_exprs[index],
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
        if hint_covers(required, &index_info.sort_option, arena)
            || (matches!(hint, OrderHintKind::SortElimination)
                && hint_covers_reversed(required, &index_info.sort_option, arena))
        {
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
            field.asc && !field.nulls_first && expr.eq_ignore_colref_pos(field.expr, arena)
        }),
    }
}

fn hint_covers_reversed(
    required: ScanOrderHint<'_>,
    provided: &SortOption,
    arena: &crate::planner::PlanArena,
) -> bool {
    match required {
        ScanOrderHint::SortFields(fields) => covers(fields, provided, |required, provided| {
            sort_field_matches_reversed(required, provided, arena)
        }),
        ScanOrderHint::GroupBy(_) => false,
    }
}

pub(crate) fn groupby_sort_fields(groupby_exprs: &[ExprRef]) -> Vec<SortField> {
    groupby_exprs
        .iter()
        .copied()
        .map(|expr| SortField::new(expr, true, false))
        .collect()
}

pub struct UseStreamAggregate;

impl NormalizationRule for UseStreamAggregate {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
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
        if !ensure_order(child, &required, false, arena) {
            return Ok(false);
        }

        plan.physical_option = Some(PhysicalOption::new(
            implementation,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));
        Ok(true)
    }
}

pub struct ForceSpillAggregate;

impl NormalizationRule for ForceSpillAggregate {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        _: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
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
        sort.physical_option = Some(PhysicalOption::new(
            PlanImpl::Sort,
            sort_option,
            RowCountOption::Follow,
        ));
        plan.physical_option = Some(PhysicalOption::new(
            implementation,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));
        Ok(true)
    }
}

pub(crate) fn apply_annotated_post_rules(
    plan: &mut LogicalPlan,
    arena: &mut crate::planner::PlanArena,
) -> Result<bool, DatabaseError> {
    let mut changed = false;

    if EliminateRedundantSort.apply(plan, arena, None)? {
        changed = true;
    }
    if UseStreamAggregate.apply(plan, arena, None)? {
        changed = true;
    }
    // Run last so an existing ordered child is reused before a forced spill adds a Sort.
    if ForceSpillAggregate.apply(plan, arena, None)? {
        changed = true;
    }

    Ok(changed)
}

fn ensure_order(
    plan: &mut LogicalPlan,
    required: &[SortField],
    allow_reverse: bool,
    arena: &crate::planner::PlanArena,
) -> bool {
    if let Some(PhysicalOption {
        plan: PlanImpl::IndexScan(index_info),
        sort_option,
        ..
    }) = plan.physical_option.as_mut()
    {
        if covers(required, &index_info.sort_option, |required, provided| {
            sort_field_matches(required, provided, arena)
        }) {
            return true;
        }
        let scan_limited = matches!(
            &plan.operator,
            Operator::TableScan(scan) if scan.limit != (None, None)
        );
        if allow_reverse
            && !scan_limited
            && !index_info.is_reverse()
            && covers(required, &index_info.sort_option, |required, provided| {
                sort_field_matches_reversed(required, provided, arena)
            })
        {
            index_info.reverse_order();
            *sort_option = index_info.sort_option.clone();
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
                // Limit keeps the first N rows in scan order. Reversing the scan below it
                // would keep the last N rows instead, changing the result set.
                let allow_reverse = allow_reverse && !matches!(plan.operator, Operator::Limit(_));
                if let Childrens::Only(child) = plan.childrens.as_mut() {
                    if ensure_order(child, required, allow_reverse, arena) {
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
        && required.expr.eq_ignore_colref_pos(provided.expr, arena)
}

fn sort_field_matches_reversed(
    required: &SortField,
    provided: &SortField,
    arena: &crate::planner::PlanArena,
) -> bool {
    required.asc != provided.asc
        && (required.nulls_first != provided.nulls_first || !expr_nullable(required.expr, arena))
        && required.expr.eq_ignore_colref_pos(provided.expr, arena)
}

fn expr_nullable(expr: ExprRef, arena: &crate::planner::PlanArena) -> bool {
    match arena.expression(expr) {
        ScalarExpression::ColumnRef { column, .. } => arena.column(*column).nullable(),
        _ => true,
    }
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

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{
        EliminateIndexFilter, EliminateProject, EliminateRedundantSort, EliminateVisitor,
        ForceSpillAggregate, OutputMode, Substitute, UseStreamAggregate,
    };
    use crate::binder::test::build_t1_table;
    use crate::catalog::{ColumnCatalog, TableName};
    use crate::errors::DatabaseError;
    use crate::expression::agg::AggKind;
    use crate::expression::range_detacher::Range;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::operator::top_k::TopKOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::{
        Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption,
    };
    use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
    use crate::types::index::{IndexInfo, IndexLookup, IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::ColumnId;
    use crate::types::LogicalType;
    use std::ops::Bound;
    fn column(arena: &mut PlanArena, source: ExprRef, position: usize) -> ExprRef {
        let column = source.output_column_ref(arena);
        arena.alloc_expression(ScalarExpression::column_expr(column, position))
    }

    fn column_expr(arena: &mut PlanArena, name: &str, position: usize) -> ExprRef {
        let column = arena.alloc_column(ColumnCatalog::new_dummy(name.to_string()));
        arena.alloc_expression(ScalarExpression::column_expr(column, position))
    }

    fn binary(arena: &mut PlanArena, left: ExprRef, op: BinaryOperator, value: i32) -> ExprRef {
        let right = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(value)));
        arena.alloc_expression(ScalarExpression::Binary {
            op,
            left_expr: left,
            right_expr: right,
            evaluator: None,
            ty: if matches!(op, BinaryOperator::Gt) {
                LogicalType::Boolean
            } else {
                LogicalType::Integer
            },
        })
    }

    fn source(arena: &mut PlanArena, mut plan: LogicalPlan) -> (LogicalPlan, ExprRef) {
        let Operator::Project(op) = plan.operator else {
            unreachable!()
        };
        let expr = binary(arena, op.exprs[1], BinaryOperator::Plus, 1);
        let input = plan.childrens.pop_only();
        plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![expr, op.exprs[0]])),
            Childrens::Only(Box::new(input)),
        );
        (plan, expr)
    }

    #[test]
    fn substitutes_through_filter_and_sort_without_cloning_sources() -> Result<(), DatabaseError> {
        let table = build_t1_table()?;
        let mut arena = PlanArena::new(&table.table_arena);
        let scan = table.plan_with_arena("select c1, c2 from t1", &mut arena)?;
        let (input, replacement) = source(&mut arena, scan);
        let predicate_column = column(&mut arena, replacement, 0);
        let predicate = binary(&mut arena, predicate_column, BinaryOperator::Gt, 10);
        let filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                having: false,
                is_optimized: false,
            }),
            Childrens::Only(Box::new(input)),
        );
        let sort_column = column(&mut arena, replacement, 0);
        let sort = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![SortField {
                    expr: sort_column,
                    asc: true,
                    nulls_first: false,
                }],
            }),
            Childrens::Only(Box::new(filter)),
        );
        let result_column = column(&mut arena, replacement, 0);
        let result = binary(&mut arena, result_column, BinaryOperator::Multiply, 2);
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![result])),
            Childrens::Only(Box::new(sort)),
        );

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
        let Operator::Project(op) = &plan.operator else {
            unreachable!()
        };
        let ScalarExpression::Binary { left_expr, .. } = op.exprs[0].unpack_alias_ref(&arena)
        else {
            unreachable!()
        };
        assert_eq!(*left_expr, replacement);
        let Childrens::Only(sort) = plan.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::Sort(op) = &sort.operator else {
            unreachable!()
        };
        assert_eq!(op.sort_fields[0].expr, replacement);
        let Childrens::Only(filter) = sort.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::Filter(op) = &filter.operator else {
            unreachable!()
        };
        let ScalarExpression::Binary { left_expr, .. } = arena.expression(op.predicate) else {
            unreachable!()
        };
        assert_eq!(*left_expr, replacement);
        let Childrens::Only(scan) = filter.childrens.as_ref() else {
            unreachable!()
        };
        assert!(matches!(scan.operator, Operator::TableScan(_)));
        assert!(!EliminateProject.apply(&mut plan, &mut arena, None)?);
        Ok(())
    }

    #[test]
    fn aggregate_consumes_mapping_but_its_output_starts_a_new_slot_space(
    ) -> Result<(), DatabaseError> {
        let table = build_t1_table()?;
        let mut arena = PlanArena::new(&table.table_arena);
        let scan = table.plan_with_arena("select c1, c2 from t1", &mut arena)?;
        let (input, replacement) = source(&mut arena, scan);
        let arg = column(&mut arena, replacement, 0);
        let sum = arena.alloc_expression(ScalarExpression::AggCall {
            distinct: false,
            kind: AggKind::Sum,
            args: vec![arg],
            ty: LogicalType::Integer,
        });
        let aggregate = LogicalPlan::new(
            Operator::Aggregate(AggregateOperator {
                agg_calls: vec![sum],
                groupby_exprs: vec![],
                is_distinct: false,
                force_spill: false,
            }),
            Childrens::Only(Box::new(input)),
        );
        let output = column(&mut arena, sum, 0);
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![output])),
            Childrens::Only(Box::new(aggregate)),
        );

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
        let Operator::Project(op) = &plan.operator else {
            unreachable!()
        };
        assert_eq!(op.exprs[0], output);
        let Childrens::Only(aggregate) = plan.childrens.as_ref() else {
            unreachable!()
        };
        let Operator::Aggregate(op) = &aggregate.operator else {
            unreachable!()
        };
        let ScalarExpression::AggCall { args, .. } = arena.expression(op.agg_calls[0]) else {
            unreachable!()
        };
        assert_eq!(args, &[replacement]);
        let Childrens::Only(scan) = aggregate.childrens.as_ref() else {
            unreachable!()
        };
        assert!(matches!(scan.operator, Operator::TableScan(_)));
        Ok(())
    }

    #[test]
    fn repeated_refs_are_rewritten_once_and_replacements_are_not_revisited(
    ) -> Result<(), DatabaseError> {
        let table = build_t1_table()?;
        let mut arena = PlanArena::new(&table.table_arena);
        let scan = table.plan_with_arena("select c1, c2 from t1", &mut arena)?;
        let (input, replacement) = source(&mut arena, scan);
        let reference = column(&mut arena, replacement, 0);
        let expr = binary(&mut arena, reference, BinaryOperator::Plus, 2);
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![expr, expr])),
            Childrens::Only(Box::new(input)),
        );

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
        let ScalarExpression::Binary { left_expr, .. } = arena.expression(expr) else {
            unreachable!()
        };
        assert_eq!(*left_expr, replacement);
        let ScalarExpression::Binary { left_expr, .. } = arena.expression(replacement) else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef { position, .. } = left_expr.unpack_alias_ref(&arena)
        else {
            unreachable!()
        };
        assert_eq!(*position, 1);
        Ok(())
    }

    #[test]
    fn mapping_transfer_keeps_shared_expressions_visited_across_plan_nodes(
    ) -> Result<(), DatabaseError> {
        let table = build_t1_table()?;
        let mut arena = PlanArena::new(&table.table_arena);
        let scan = table.plan_with_arena("select c1, c2 from t1", &mut arena)?;
        let (input, replacement) = source(&mut arena, scan);
        let reference = column(&mut arena, replacement, 0);
        let shared = binary(&mut arena, reference, BinaryOperator::Gt, 10);
        let filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: shared,
                having: false,
                is_optimized: false,
            }),
            Childrens::Only(Box::new(input)),
        );
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![shared])),
            Childrens::Only(Box::new(filter)),
        );

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
        let ScalarExpression::Binary { left_expr, .. } = arena.expression(shared) else {
            unreachable!()
        };
        assert_eq!(*left_expr, replacement);
        let ScalarExpression::Binary { left_expr, .. } = arena.expression(replacement) else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef { position, .. } = left_expr.unpack_alias_ref(&arena)
        else {
            unreachable!()
        };
        assert_eq!(*position, 1);
        assert!(!EliminateProject.apply(&mut plan, &mut arena, None)?);
        Ok(())
    }

    fn values(arena: &mut PlanArena, names: &[&str]) -> LogicalPlan {
        let schema = names
            .iter()
            .map(|name| arena.alloc_column(ColumnCatalog::new_dummy(name.to_string())))
            .collect();
        LogicalPlan::new(
            Operator::Values(ValuesOperator::new(vec![], 0, schema)),
            Childrens::None,
        )
    }

    fn permutation(arena: &mut PlanArena, mut input: LogicalPlan, slots: &[usize]) -> LogicalPlan {
        let schema = input.output_schema(arena).clone();
        let exprs = slots
            .iter()
            .map(|position| {
                arena.alloc_expression(ScalarExpression::column_expr(schema[*position], *position))
            })
            .collect();
        LogicalPlan::new(
            Operator::Project(ProjectOperator::new(exprs)),
            Childrens::Only(Box::new(input)),
        )
    }

    #[test]
    fn cross_join_composes_p4_and_p9_into_final_computations() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let outer_left = values(&mut arena, &["d2", "e2", "c5"]);
        let t3 = values(&mut arena, &["a3", "e3", "x3"]);
        let t4 = values(&mut arena, &["b4", "d4"]);
        let inner = JoinOperator::build(t3, t4, JoinCondition::None, JoinType::Inner, false);
        let t9 = values(&mut arena, &["b9", "d9"]);
        let t1 = values(&mut arena, &["a1", "e1"]);
        let cross4 = JoinOperator::build(t9, t1, JoinCondition::None, JoinType::Cross, false);
        let p4 = permutation(&mut arena, cross4, &[2, 3, 0, 1]);
        let cross9 = JoinOperator::build(inner, p4, JoinCondition::None, JoinType::Cross, false);
        let p9 = permutation(&mut arena, cross9, &[5, 6, 7, 8, 0, 1, 2, 3, 4]);
        let outer =
            JoinOperator::build(outer_left, p9, JoinCondition::None, JoinType::Cross, false);
        let mut plan = permutation(&mut arena, outer, &(0..12).collect::<Vec<_>>());
        let Operator::Project(op) = &mut plan.operator else {
            unreachable!()
        };
        for expr in &mut op.exprs {
            *expr = binary(&mut arena, *expr, BinaryOperator::Multiply, 2);
        }
        let schema = plan.output_schema(&mut arena).clone();

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.explain(&mut arena, 0).matches("Projection").count(), 1);
        let Operator::Project(op) = &plan.operator else {
            unreachable!()
        };
        for (expr, expected) in op.exprs.iter().zip([0, 1, 2, 10, 11, 8, 9, 3, 4, 5, 6, 7]) {
            let ScalarExpression::Binary { left_expr, .. } = expr.unpack_alias_ref(&arena) else {
                unreachable!()
            };
            let ScalarExpression::ColumnRef { position, .. } = left_expr.unpack_alias_ref(&arena)
            else {
                unreachable!()
            };
            assert_eq!(*position, expected);
        }
        let Childrens::Only(input) = plan.childrens.as_mut() else {
            unreachable!()
        };
        let names: Vec<_> = input
            .output_schema(&mut arena)
            .iter()
            .map(|column| arena.column(*column).name().to_string())
            .collect();
        assert_eq!(
            names,
            ["d2", "e2", "c5", "a3", "e3", "x3", "b4", "d4", "b9", "d9", "a1", "e1"]
        );
        assert!(!EliminateProject.apply(&mut plan, &mut arena, None)?);
        Ok(())
    }

    #[test]
    fn cross_join_uses_new_left_width_for_mapped_and_unchanged_right_inputs(
    ) -> Result<(), DatabaseError> {
        for (right_slots, expected) in [(Some(&[1, 0][..]), [2, 4, 3]), (None, [2, 3, 4])] {
            let table_arena = crate::planner::TableArenaCell::default();
            let mut arena = PlanArena::new(&table_arena);
            let left = values(&mut arena, &["l0", "l1", "l2"]);
            let left = permutation(&mut arena, left, &[2]);
            let right = values(&mut arena, &["r0", "r1"]);
            let right = match right_slots {
                Some(slots) => permutation(&mut arena, right, slots),
                None => right,
            };
            let cross =
                JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
            let mut plan = permutation(&mut arena, cross, &[0, 1, 2]);

            assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
            assert_eq!(plan.explain(&mut arena, 0).matches("Projection").count(), 1);
            let Operator::Project(op) = &plan.operator else {
                unreachable!()
            };
            for (expr, expected) in op.exprs.iter().zip(expected) {
                let ScalarExpression::ColumnRef { position, .. } = expr.unpack_alias_ref(&arena)
                else {
                    unreachable!()
                };
                assert_eq!(*position, expected);
            }
            assert!(!EliminateProject.apply(&mut plan, &mut arena, None)?);
        }
        Ok(())
    }

    #[test]
    fn cross_join_callback_reuses_an_existing_mapping_buffer() -> Result<(), DatabaseError> {
        for (map_left, map_right, expected) in [
            (true, true, [1, 0, 3, 2]),
            (true, false, [1, 0, 2, 3]),
            (false, true, [0, 1, 3, 2]),
        ] {
            let table_arena = crate::planner::TableArenaCell::default();
            let mut arena = PlanArena::new(&table_arena);
            let mut left = values(&mut arena, &["l0", "l1"]);
            let mut right = values(&mut arena, &["r0", "r1"]);
            if map_left {
                left = permutation(&mut arena, left, &[1, 0]);
            }
            if map_right {
                right = permutation(&mut arena, right, &[1, 0]);
            }
            let source = if map_left { &mut left } else { &mut right };
            let Operator::Project(op) = &mut source.operator else {
                unreachable!()
            };
            op.exprs.reserve(8);
            let buffer = op.exprs.as_ptr();
            let mut plan =
                JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
            let mut visitor = EliminateVisitor {
                arena: &mut arena,
                mapping: Substitute::default(),
                output: OutputMode::Inline,
                changed: false,
            };
            visitor.visit_input(&mut plan, OutputMode::Inline, |visitor, _| {
                let Some(exprs) = &visitor.mapping.exprs else {
                    panic!("expected a mapping")
                };
                assert_eq!(exprs.as_ptr(), buffer);
                assert_eq!(exprs.len(), expected.len());
                for (expr, expected) in exprs.iter().zip(expected) {
                    let ScalarExpression::ColumnRef { position, .. } =
                        expr.unpack_alias_ref(visitor.arena)
                    else {
                        unreachable!()
                    };
                    assert_eq!(*position, expected);
                }
                Ok(())
            })?;
        }
        Ok(())
    }

    #[test]
    fn cross_join_distinguishes_empty_mapping_from_identity() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = values(&mut arena, &["l0", "l1", "l2"]);
        let left = permutation(&mut arena, left, &[]);
        let right = values(&mut arena, &["r0", "r1"]);
        let cross = JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
        let mut plan = permutation(&mut arena, cross, &[0, 1]);

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.explain(&mut arena, 0).matches("Projection").count(), 1);
        let Operator::Project(op) = &plan.operator else {
            unreachable!()
        };
        for (expr, expected) in op.exprs.iter().zip([3, 4]) {
            let ScalarExpression::ColumnRef { position, .. } = expr.unpack_alias_ref(&arena) else {
                unreachable!()
            };
            assert_eq!(*position, expected);
        }
        assert!(!EliminateProject.apply(&mut plan, &mut arena, None)?);
        Ok(())
    }

    #[test]
    fn cross_join_preserves_computed_right_output_without_copying_it() -> Result<(), DatabaseError>
    {
        let table = build_t1_table()?;
        let mut arena = PlanArena::new(&table.table_arena);
        let scan = table.plan_with_arena("select c1, c2 from t1", &mut arena)?;
        let (right, _) = source(&mut arena, scan);
        let left = values(&mut arena, &["l0"]);
        let cross = JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
        let mut plan = permutation(&mut arena, cross, &[0, 1, 2]);

        assert!(!EliminateProject.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.explain(&mut arena, 0).matches("Projection").count(), 2);
        Ok(())
    }

    #[test]
    fn test_collapse_project() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan =
            table_state.plan_with_arena("select c1 from (select c1, c2 from t1) t", &mut arena)?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_collapse_project",
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::EliminateProject],
            )
            .build();
        let best_plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;

        if let Operator::Project(op) = &best_plan.operator {
            assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator")
        }

        let scan_op = best_plan.childrens.pop_only();
        assert!(matches!(scan_op.operator, Operator::TableScan(_)));
        assert!(matches!(scan_op.childrens.as_ref(), Childrens::None));

        Ok(())
    }

    #[test]
    fn test_collapse_project_with_alias() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state
            .plan_with_arena("select t.x from (select c1 as x from t1) t", &mut arena)?;
        let original = plan.clone();
        let original_child = original.childrens.pop_only();
        assert!(matches!(original_child.operator, Operator::Project(_)));
        let original_grandchild = original_child.childrens.pop_only();
        assert!(matches!(original_grandchild.operator, Operator::Project(_)));

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_collapse_project_with_alias",
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::EliminateProject],
            )
            .build();
        let best_plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;
        if let Operator::Project(op) = &best_plan.operator {
            assert_eq!(op.exprs.len(), 1);
        } else {
            unreachable!("Should be a project operator");
        }

        let scan_op = best_plan.childrens.pop_only();
        assert!(matches!(scan_op.operator, Operator::TableScan(_)));

        Ok(())
    }

    #[test]
    fn test_collapse_project_remaps_reordered_passthrough_positions() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let child = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![
                column_expr(&mut arena, "c2", 1),
                column_expr(&mut arena, "c1", 0),
            ])),
            Childrens::Only(Box::new(LogicalPlan::new(Operator::Dummy, Childrens::None))),
        );
        let mut plan = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![column_expr(&mut arena, "c2", 0)])),
            Childrens::Only(Box::new(child)),
        );

        assert!(EliminateProject.apply(&mut plan, &mut arena, None)?);

        let Operator::Project(op) = &plan.operator else {
            unreachable!("expected project");
        };
        let ScalarExpression::ColumnRef { position, .. } = op.exprs[0].unpack_alias_ref(&arena)
        else {
            unreachable!("expected column ref");
        };
        assert_eq!(*position, 1);
        assert!(matches!(
            plan.childrens.pop_only().operator,
            Operator::Dummy
        ));
        Ok(())
    }

    fn make_sort_field(arena: &mut crate::planner::PlanArena, name: &str) -> SortField {
        make_sort_field_with_position(arena, name, 0)
    }

    fn make_sort_field_with_position(
        arena: &mut crate::planner::PlanArena,
        name: &str,
        position: usize,
    ) -> SortField {
        let column = arena.alloc_column(ColumnCatalog::new_dummy(name.to_string()));
        SortField::new(
            arena.alloc_expression(ScalarExpression::column_expr(column, position)),
            true,
            false,
        )
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
            RowCountOption::Unknown,
        ));

        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(leaf)),
        );
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));

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
        predicate: ExprRef,
        residual: Option<ExprRef>,
    ) -> LogicalPlan {
        let column = arena.alloc_column(ColumnCatalog::new_dummy("c1".to_string()));
        let sort_field = SortField::new(
            arena.alloc_expression(ScalarExpression::column_expr(column, 0)),
            true,
            false,
        );
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
            RowCountOption::Unknown,
        ));

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(scan)),
        );
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));
        filter
    }

    fn build_distinct_scan_plan(
        arena: &mut crate::planner::PlanArena,
    ) -> (LogicalPlan, SortOption) {
        let table_name: TableName = ::std::sync::Arc::from("t1");
        let c1 = arena.alloc_column(ColumnCatalog::new_dummy("c1".to_string()));
        let c1_id = 1;
        let columns = vec![c1];

        let group_expr = arena.alloc_expression(ScalarExpression::column_expr(c1, 0));
        let sort_fields = vec![SortField::new(group_expr, true, false)];
        let sort_option = SortOption::OrderBy {
            fields: sort_fields,
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
                groupby_exprs: vec![group_expr],
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
        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let mut plan = build_filter_with_selected_index(&mut arena, predicate, None);

        let rule = EliminateIndexFilter;
        assert!(rule.apply(&mut plan, &mut arena, None)?);
        assert!(matches!(plan.operator, Operator::TableScan(_)));
        Ok(())
    }

    #[test]
    fn partial_index_filter_keeps_residual_after_physical_selection() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let residual =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(false)));
        let mut plan = build_filter_with_selected_index(&mut arena, predicate, Some(residual));

        let rule = EliminateIndexFilter;
        assert!(rule.apply(&mut plan, &mut arena, None)?);
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
        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let stale_residual =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(false)));
        let mut plan =
            build_filter_with_selected_index(&mut arena, predicate, Some(stale_residual));
        // Physical annotation must leave the full predicate available for a later
        // parameterization rule, even when a static index initially won.
        super::apply_annotated_post_rules(&mut plan, &mut arena)?;
        let Operator::Filter(filter) = &plan.operator else {
            panic!("filter removed before parameterization")
        };
        assert_eq!(filter.predicate, predicate);
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
        assert!(!rule.apply(&mut plan, &mut arena, None)?);
        let Operator::Filter(filter) = &plan.operator else {
            panic!("probe cannot discharge the static filter")
        };
        assert_eq!(filter.predicate, predicate);
        Ok(())
    }

    #[test]
    fn remove_sort_when_index_matches_order() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let sort_field = make_sort_field(&mut arena, "c1");
        let mut plan = build_plan(&mut arena, vec![sort_field.clone()], vec![sort_field], 0);
        let rule = EliminateRedundantSort;

        assert!(rule.apply(&mut plan, &mut arena, None)?);
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

        assert!(rule.apply(&mut plan, &mut arena, None)?);
        match plan.operator {
            Operator::Limit(limit_op) => {
                assert_eq!(limit_op.limit, Some(10));
                assert_eq!(limit_op.offset, Some(5));
            }
            _ => unreachable!("expected limit operator after removing topk"),
        }
        Ok(())
    }

    fn desc_topk_plan(
        arena: &mut crate::planner::PlanArena,
        nullable: bool,
    ) -> (LogicalPlan, SortField) {
        let mut column = ColumnCatalog::new_dummy("c1".to_string());
        column.set_nullable(nullable);
        let column = arena.alloc_column(column);
        let index_field = SortField::new(
            arena.alloc_expression(ScalarExpression::column_expr(column, 0)),
            true,
            false,
        );
        let required = SortField::new(index_field.expr, false, false);
        let mut plan = build_plan(arena, vec![required.clone()], vec![index_field], 0);
        plan.operator = Operator::TopK(TopKOperator {
            sort_fields: vec![required.clone()],
            limit: 1,
            offset: None,
        });
        (plan, required)
    }

    fn scan_is_reverse(plan: &LogicalPlan) -> bool {
        let mut plan = plan;
        loop {
            if let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(info),
                ..
            }) = &plan.physical_option
            {
                return info.is_reverse();
            }
            match plan.childrens.as_ref() {
                Childrens::Only(child) => plan = child,
                _ => return false,
            }
        }
    }

    #[test]
    fn remove_desc_topk_by_reversing_non_null_index() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, _) = desc_topk_plan(&mut arena, false);

        assert!(EliminateRedundantSort.apply(&mut plan, &mut arena, None)?);
        assert!(matches!(plan.operator, Operator::Limit(_)));
        assert!(scan_is_reverse(&plan));
        Ok(())
    }

    #[test]
    fn keep_desc_topk_on_nullable_index() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, _) = desc_topk_plan(&mut arena, true);

        assert!(!EliminateRedundantSort.apply(&mut plan, &mut arena, None)?);
        assert!(matches!(plan.operator, Operator::TopK(_)));
        assert!(!scan_is_reverse(&plan));
        Ok(())
    }

    #[test]
    fn keep_desc_topk_when_limit_is_below() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let (mut plan, _) = desc_topk_plan(&mut arena, false);
        let Childrens::Only(filter) = plan.childrens.as_mut() else {
            unreachable!()
        };
        filter.operator = Operator::Limit(LimitOperator {
            offset: None,
            limit: Some(3),
        });
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Limit,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));

        assert!(!EliminateRedundantSort.apply(&mut plan, &mut arena, None)?);
        assert!(!scan_is_reverse(&plan));
        Ok(())
    }

    #[test]
    fn remove_sort_when_prefix_can_be_ignored() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let c1 = make_sort_field(&mut arena, "c1");
        let c2 = make_sort_field(&mut arena, "c2");
        for lookup in [
            None,
            Some(IndexLookup::Static(Range::Eq(DataValue::Parameter {
                id: 1,
                ty: LogicalType::Integer,
            }))),
        ] {
            let mut plan = build_plan(
                &mut arena,
                vec![c2.clone()],
                vec![c1.clone(), c2.clone()],
                1,
            );
            let Childrens::Only(filter) = plan.childrens.as_mut() else {
                panic!("expected filter")
            };
            let Childrens::Only(leaf) = filter.childrens.as_mut() else {
                panic!("expected index scan")
            };
            if let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(info),
                ..
            }) = &mut leaf.physical_option
            {
                info.lookup = lookup;
            }
            super::mark_sort_preserving_indexes(&mut plan, std::slice::from_ref(&c2), &arena)?;
            assert!(EliminateRedundantSort.apply(&mut plan, &mut arena, None)?);
        }
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
        assert!(rule.apply(&mut plan, &mut arena, None)?);
        assert!(matches!(plan.operator, Operator::Limit(_)));
        Ok(())
    }

    #[test]
    fn annotate_sets_sort_hint_on_table_scan() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let column = arena.alloc_column(ColumnCatalog::new_dummy("c1".to_string()));
        let sort_field = SortField::new(
            arena.alloc_expression(ScalarExpression::column_expr(column, 0)),
            true,
            false,
        );
        let (index_info, _) = build_index_info(&mut arena, vec![sort_field.clone()], 0);

        let columns = vec![column];
        let table_name: TableName = ::std::sync::Arc::from("t");
        let table_scan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name,
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
                    sort_option,
                    RowCountOption::Unknown,
                ));
            }
        }
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
            RowCountOption::Unknown,
        ));

        let rule = UseStreamAggregate;
        assert!(rule.apply(&mut plan, &mut arena, None)?);
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
            RowCountOption::Unknown,
        ));
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
            RowCountOption::Unknown,
        ));

        let rule = UseStreamAggregate;
        assert!(rule.apply(&mut plan, &mut arena, None)?);
        assert!(matches!(
            plan.physical_option,
            Some(PhysicalOption {
                plan: PlanImpl::StreamAggregate,
                ..
            })
        ));
        let force_spill = ForceSpillAggregate;
        assert!(!force_spill.apply(&mut plan, &mut arena, None)?);
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
            RowCountOption::Unknown,
        ));
        assert!(!rule.apply(&mut unordered, &mut arena, None)?);
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
            RowCountOption::Unknown,
        ));

        let rule = ForceSpillAggregate;
        assert!(rule.apply(&mut plan, &mut arena, None)?);
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
        let mut plan = build_plan(&mut arena, vec![c2.clone()], vec![c1, c2.clone()], 0);
        super::mark_sort_preserving_indexes(&mut plan, &[c2], &arena)?;
        let rule = EliminateRedundantSort;

        assert!(!rule.apply(&mut plan, &mut arena, None)?);
        assert!(matches!(plan.operator, Operator::Sort(_)));
        Ok(())
    }

    #[test]
    fn promote_index_to_remove_sort() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let column = arena.alloc_column(ColumnCatalog::new_dummy("c_first".to_string()));
        let sort_field = SortField::new(
            arena.alloc_expression(ScalarExpression::column_expr(column, 0)),
            true,
            false,
        );
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
                index_info.sort_option,
                RowCountOption::Unknown,
            ));
        }

        let mut filter = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate: arena
                    .alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true))),
                is_optimized: false,
                having: false,
            }),
            Childrens::Only(Box::new(scan_plan)),
        );
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));

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
        assert!(rule.apply(&mut plan, &mut arena, None)?);
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
// GRCOV_EXCL_STOP
