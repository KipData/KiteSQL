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
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::plan_utils::scan_column;
use crate::optimizer::rule::normalization::parameterized_index::{
    probe_index, probe_join, ProbeIndex,
};
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::{
    rows_work, InputCost, JoinImpl, Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption,
    DEFAULT_TABLE_ROWS,
};
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::index::{IndexMeta, IndexMetaRef, IndexType};
use crate::types::tuple::Schema;
use std::borrow::Borrow;
use std::mem::swap;
use std::ops::Range;

pub(crate) struct GreedyJoinReorder;

/// Whether a cross nested loop should drive from its right input.
fn nest_loop_swap(left: &InputCost, right: &InputCost) -> bool {
    if left.rows == 0 {
        false
    } else if right.rows == 0 {
        true
    } else {
        // Both directions emit the same output, so it is left out of the comparison.
        let nest_loop = JoinImpl::NestLoop { full: false };
        nest_loop.work(right, left, 0, 0) < nest_loop.work(left, right, 0, 0)
    }
}

fn project_join_output(
    join: LogicalPlan,
    original_schema: Schema,
    positions: impl Iterator<Item = usize> + Clone,
    arena: &mut PlanArena,
) -> LogicalPlan {
    if positions.clone().eq(0..original_schema.len()) {
        return join;
    }
    let exprs = original_schema
        .into_iter()
        .zip(positions)
        .map(|(column, position)| {
            arena.alloc_expression(ScalarExpression::column_expr(column, position))
        })
        .collect();
    let mut plan = LogicalPlan::new(
        Operator::Project(ProjectOperator::new(exprs)),
        Childrens::Only(Box::new(join)),
    );
    plan.physical_option = Some(PhysicalOption::new(
        PlanImpl::Project,
        SortOption::Follow,
        RowCountOption::Follow,
    ));
    plan
}

// Prefer equijoins, then less work in this step, then the original input order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct JoinCandidate {
    cross_join: bool,
    work: i128,
    rows: usize,
    leaf: usize,
    swapped: bool,
    probe: bool,
}

fn column_ndv<K: Borrow<(ExprRef, ExprRef)>>(
    left: &LogicalPlan,
    right: &LogicalPlan,
    keys: impl IntoIterator<Item = K>,
    loader: Option<&StatisticMetaLoader<'_>>,
    arena: &PlanArena,
) -> Result<Option<(usize, usize)>, DatabaseError> {
    // scan_column cannot trace columns through a join.
    if matches!(&left.operator, Operator::Join(_)) || matches!(&right.operator, Operator::Join(_)) {
        return Ok(None);
    }
    let Some(loader) = loader else {
        return Ok(None);
    };
    let (Some(left_rows), Some(right_rows)) = (left.estimated_rows(), right.estimated_rows())
    else {
        return Ok(None);
    };
    let mut result = None;
    let mut divisor = 0;
    for key in keys {
        let &(left_key, right_key) = key.borrow();
        let (
            ScalarExpression::ColumnRef { column: l, .. },
            ScalarExpression::ColumnRef { column: r, .. },
        ) = (arena.expression(left_key), arena.expression(right_key))
        else {
            continue;
        };
        for (l, r) in [(*l, *r), (*r, *l)] {
            let Some((left_table, left_column)) = scan_column(left, l, arena)? else {
                continue;
            };
            let Some((right_table, right_column)) = scan_column(right, r, arena)? else {
                continue;
            };
            let Some(left_meta) = loader.load_column(left_table, left_column) else {
                continue;
            };
            let Some(right_meta) = loader.load_column(right_table, right_column) else {
                continue;
            };
            let left_ndv = left_meta.distinct_values_len().min(left_rows);
            let right_ndv = right_meta.distinct_values_len().min(right_rows);
            if left_ndv == 0 || right_ndv == 0 {
                return Ok(Some((left_ndv, right_ndv)));
            }
            // Do not multiply selection rates for potentially correlated keys.
            let candidate = left_ndv.max(right_ndv);
            if candidate > divisor {
                divisor = candidate;
                result = Some((left_ndv, right_ndv));
            }
        }
    }
    Ok(result)
}

fn index_statistics(plan: &LogicalPlan) -> Option<(IndexMetaRef, Option<usize>)> {
    let option = plan.row_count_option()?;
    let (PlanImpl::IndexScan(index), RowCountOption::Estimated { ndv, .. }) =
        (&option.plan, &option.row_count)
    else {
        return None;
    };
    Some((index.meta, *ndv))
}

struct JoinInputStatistics<'a> {
    rows: usize,
    schema: &'a [ColumnRef],
    index: Option<(&'a IndexMeta, Option<usize>)>,
}

fn index_column(column: ColumnRef, index: &IndexMeta, arena: &PlanArena) -> bool {
    let column = arena.column(column);
    column.table_name() == Some(&index.table_name)
        && column.id().is_some_and(|id| index.column_ids.contains(&id))
}

fn covers_index<I: Iterator<Item = (ExprRef, ExprRef)>>(
    index: &IndexMeta,
    other: &JoinInputStatistics,
    keys: impl Fn() -> I,
    require_other_index: bool,
    arena: &PlanArena,
) -> bool {
    index.column_ids.iter().all(|id| {
        keys().any(|(left, right)| {
            let (
                ScalarExpression::ColumnRef { column: left, .. },
                ScalarExpression::ColumnRef { column: right, .. },
            ) = (arena.expression(left), arena.expression(right))
            else {
                return false;
            };
            [(*left, *right), (*right, *left)]
                .iter()
                .any(|&(own, opposite)| {
                    index_column(own, index, arena)
                        && arena.column(own).id() == Some(*id)
                        && other
                            .schema
                            .iter()
                            .any(|column| arena.same_column(*column, opposite))
                        && (!require_other_index
                            || other
                                .index
                                .is_some_and(|(index, _)| index_column(opposite, index, arena)))
                })
        })
    })
}

#[derive(Clone, Copy)]
struct JoinInputEstimate {
    rows: usize,
    ndv: Option<usize>,
    unique: bool,
}

fn estimate_join_rows(left: JoinInputEstimate, right: JoinInputEstimate) -> usize {
    // J = min(ceil(L * R / D), left_unique ? R : inf, right_unique ? L : inf).
    // L/R are rows, N_L/N_R are NDVs; D = max(min(N_L, L), min(N_R, R), 1).
    // Missing NDV means D = 1; either known NDV of zero means J = 0.
    let mut rows = left.rows.saturating_mul(right.rows);
    if left.ndv == Some(0) || right.ndv == Some(0) {
        return 0;
    }
    if let (Some(left_ndv), Some(right_ndv)) = (left.ndv, right.ndv) {
        let ndv = left_ndv
            .min(left.rows)
            .max(right_ndv.min(right.rows))
            .max(1);
        rows = ((left.rows as u128 * right.rows as u128).div_ceil(ndv as u128))
            .min(usize::MAX as u128) as usize;
    }
    if left.unique {
        rows = rows.min(right.rows);
    }
    if right.unique {
        rows = rows.min(left.rows);
    }
    rows
}

fn join_rows<I: Iterator<Item = (ExprRef, ExprRef)>>(
    left: &JoinInputStatistics,
    right: &JoinInputStatistics,
    keys: impl Fn() -> I,
    ndvs: Option<(usize, usize)>,
    arena: &PlanArena,
) -> usize {
    let mut left_estimate = JoinInputEstimate {
        rows: left.rows,
        ndv: None,
        unique: false,
    };
    let mut right_estimate = JoinInputEstimate {
        rows: right.rows,
        ndv: None,
        unique: false,
    };
    if let (Some((left_index, Some(left_ndv))), Some((right_index, Some(right_ndv)))) =
        (left.index, right.index)
    {
        if covers_index(left_index, right, &keys, true, arena)
            && covers_index(right_index, left, &keys, true, arena)
        {
            left_estimate.ndv = Some(left_ndv);
            right_estimate.ndv = Some(right_ndv);
        }
    }
    if left_estimate.ndv.is_none() || right_estimate.ndv.is_none() {
        if let Some((left_ndv, right_ndv)) = ndvs {
            left_estimate.ndv = Some(left_ndv);
            right_estimate.ndv = Some(right_ndv);
        }
    }
    for (own, other, estimate) in [
        (left, right, &mut left_estimate),
        (right, left, &mut right_estimate),
    ] {
        if let Some((index, _)) = own.index {
            if matches!(index.ty, IndexType::PrimaryKey { .. } | IndexType::Unique)
                && covers_index(index, other, &keys, false, arena)
            {
                estimate.unique = true;
            }
        }
    }
    // TODO: Estimate expression keys and intermediate Join key distributions;
    // do not inherit uniqueness through joins that can duplicate an input row.
    estimate_join_rows(left_estimate, right_estimate)
}

// A region only spans joins sharing the `FORCE_NEST_LOOP_JOIN` setting of its root.
fn can_expand(plan: &LogicalPlan, forced: bool) -> bool {
    let Operator::Join(JoinOperator {
        join_type,
        on,
        force_nested_loop,
        limit_pushed: false,
    }) = &plan.operator
    else {
        return false;
    };
    *force_nested_loop == forced
        && (matches!((join_type, on), (JoinType::Cross, JoinCondition::None))
            || matches!((join_type, on),
                (JoinType::Inner, JoinCondition::On { on, filter: None }) if !on.is_empty()))
}

// Counts a region's inputs.
fn region_inputs(plan: &LogicalPlan, forced: bool) -> usize {
    if !can_expand(plan, forced) {
        return 1;
    }
    let (left, right) = plan.childrens.twins();
    region_inputs(left, forced) + region_inputs(right, forced)
}

// Collects input schemas and join keys; returns whether every input has a row estimate.
// Joins inside the region are rebuilt by the reorder, so their own rows are not estimated here.
fn inspect<'plan>(
    plan: &'plan mut LogicalPlan,
    schemas: &mut Vec<&'plan Schema>,
    keys: &mut JoinKeys,
    arena: &mut PlanArena,
    forced: bool,
) -> Result<bool, DatabaseError> {
    if !can_expand(plan, forced) {
        let estimated = plan.estimated_rows().is_some();
        schemas.push(plan.output_schema(arena));
        return Ok(estimated);
    }
    let (Operator::Join(join), Childrens::Twins { left, right }) =
        (&plan.operator, plan.childrens.as_mut())
    else {
        unreachable!()
    };
    let on = match &join.on {
        JoinCondition::On { on, .. } => on.as_slice(),
        JoinCondition::None => &[],
    };
    let start = schemas.len();
    let left_estimated = inspect(left, schemas, keys, arena, forced)?;
    let middle = schemas.len();
    let right_estimated = inspect(right, schemas, keys, arena, forced)?;

    for &(left, right) in on {
        let mut endpoints = [None, None];
        for (endpoint, (expr, range)) in endpoints
            .iter_mut()
            .zip([(left, start..middle), (right, middle..schemas.len())])
        {
            for index in range {
                if key_in_schema(expr, schemas[index], arena)? {
                    *endpoint = Some(index);
                    break;
                }
            }
            if endpoint.is_none() {
                return Err(DatabaseError::InvalidPlan(format!(
                    "join key `{}` is not produced by any join input",
                    expr.output_name(arena)
                )));
            }
        }
        if endpoints[0] == endpoints[1] {
            return Err(DatabaseError::InvalidPlan(format!(
                "join key `{} = {}` compares columns of the same join input",
                left.output_name(arena),
                right.output_name(arena)
            )));
        }
        // SAFETY: The loop above returns early if either endpoint is None.
        let [l, r] = endpoints.map(Option::unwrap);
        keys.insert(1u64 << l, 1u64 << r, (left, right));
    }
    Ok(left_estimated && right_estimated)
}

/// An input's cost, read once at its (default) rows when its estimate is incomplete.
fn input_cost_or_default(
    input: &mut LogicalPlan,
    rows: usize,
    arena: &mut PlanArena,
) -> Result<InputCost, DatabaseError> {
    Ok(match input.price(arena).and_then(|cost| cost.input()) {
        Some(cost) => cost,
        None => {
            let width = input.output_schema(arena).len();
            InputCost {
                rows,
                width,
                work: rows_work(rows, width),
            }
        }
    })
}

/// Collects a region's inputs with their costs; a leaf's cost never changes. `start` tracks
/// the smallest input (cheapest to read on ties, earliest on full ties).
fn flatten(
    mut plan: LogicalPlan,
    leaves: &mut Vec<(LogicalPlan, InputCost, u64)>,
    start: &mut usize,
    forced: bool,
    arena: &mut PlanArena,
) -> Result<(), DatabaseError> {
    if can_expand(&plan, forced) {
        let (left, right) = plan.childrens.pop_twins();
        flatten(left, leaves, start, forced, arena)?;
        flatten(right, leaves, start, forced, arena)?;
    } else {
        // Without statistics an input is assumed to have the default rows (RFC 0001 §3.9).
        let rows = plan.estimated_rows().unwrap_or(DEFAULT_TABLE_ROWS);
        let cost = input_cost_or_default(&mut plan, rows, arena)?;
        if leaves
            .get(*start)
            .is_some_and(|(_, best, _)| (cost.rows, cost.work) < (best.rows, best.work))
        {
            *start = leaves.len();
        }
        let mask = 1u64 << leaves.len();
        leaves.push((plan, cost, mask));
    }
    Ok(())
}

fn key_in_schema(expr: ExprRef, schema: &Schema, arena: &PlanArena) -> Result<bool, DatabaseError> {
    expr.all_referenced_columns(arena, |arena, column, _| {
        schema
            .iter()
            .any(|candidate| arena.same_column(*candidate, *column))
    })
}

/// Join keys between original inputs, each edge stored once per endpoint as
/// `(endpoint bit, other bit, endpoint key, other key)`. Sorted by endpoint bit, so an input's
/// edges are contiguous; equal endpoints keep insertion order.
#[derive(Default)]
struct JoinKeys(Vec<(u64, u64, ExprRef, ExprRef)>);

impl JoinKeys {
    fn insert(&mut self, left: u64, right: u64, (l, r): (ExprRef, ExprRef)) {
        for entry in [(left, right, l, r), (right, left, r, l)] {
            let at = self.0.partition_point(|e| e.0 <= entry.0);
            self.0.insert(at, entry);
        }
    }

    fn range(&self, leaf: u64) -> Range<usize> {
        let start = self.0.partition_point(|e| e.0 < leaf);
        let len = self.0[start..].iter().take_while(|e| e.0 == leaf).count();
        start..start + len
    }

    fn get(&self, current: u64, leaf: u64) -> impl Iterator<Item = (ExprRef, ExprRef)> + '_ {
        self.0[self.range(leaf)]
            .iter()
            .filter(move |e| e.1 & current != 0)
            .map(|&(_, _, leaf_key, other_key)| (other_key, leaf_key))
    }

    fn take(&mut self, current: u64, leaf: u64) -> Vec<(ExprRef, ExprRef)> {
        let range = self.range(leaf);
        self.0
            .drain(range)
            .filter(|e| e.1 & current != 0)
            .map(|(_, _, leaf_key, other_key)| (other_key, leaf_key))
            .collect()
    }
}

struct BindPosition<'a>(&'a Schema);

impl<A: MetaArena + ?Sized> ExprVisitorMut<A> for BindPosition<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        _: &mut A,
    ) -> Result<(), DatabaseError> {
        // Join keys keep the refs their inputs output.
        *position = self
            .0
            .iter()
            .position(|candidate| candidate == column)
            .ok_or(DatabaseError::SourceNotFound)?;
        Ok(())
    }
}

impl NormalizationRule for GreedyJoinReorder {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        if plan
            .physical_option
            .as_ref()
            .is_some_and(|option| option.join_decided)
        {
            return Ok(false);
        }
        // `FORCE_NEST_LOOP_JOIN` regions are reordered too, but only with nested loops.
        let forced = matches!(
            &plan.operator,
            Operator::Join(JoinOperator {
                force_nested_loop: true,
                ..
            })
        );
        if !can_expand(plan, forced) {
            return Ok(false);
        }
        let input_count = region_inputs(plan, forced);
        if input_count > u64::BITS as usize {
            return Ok(false);
        }
        let original_schema = plan.output_schema(arena).clone();
        let mut schemas = Vec::new();
        let mut keys = JoinKeys::default();
        // Inputs without statistics are priced with default rows, but joins built from them keep
        // an unknown row count for later rules.
        let estimated = inspect(plan, &mut schemas, &mut keys, arena, forced)?;
        // Each original column keeps its input bit and position within its current join.
        let mut positions = Vec::with_capacity(original_schema.len());
        for (input, schema) in schemas.iter().enumerate() {
            positions.extend((0..schema.len()).map(|position| (1u64 << input, position)));
        }
        let mut leaves = Vec::with_capacity(schemas.len());
        // Left-deep: start from the smallest input, then join one remaining leaf per step, so
        // every leaf can still be probed (RFC 0001 §3.2).
        let mut start = 0;
        flatten(plan.take(), &mut leaves, &mut start, forced, arena)?;
        let mut probe_keys = Vec::new();
        let (mut current, mut current_cost, mut current_mask) = leaves.remove(start);
        while !leaves.is_empty() {
            let mut best: Option<(JoinCandidate, Option<ProbeIndex>)> = None;
            // A result of at most one row may take a Cartesian product before an equijoin.
            let single = current_cost.rows <= 1;
            for (k, (leaf, leaf_cost, leaf_mask)) in leaves.iter_mut().enumerate() {
                let leaf_cost = *leaf_cost;
                // Keys run from `current` to `leaf`, matching the unswapped join sides.
                let candidate_keys = || keys.get(current_mask, *leaf_mask);
                let product = {
                    let ndvs = column_ndv(&current, leaf, candidate_keys(), loader, arena)?;
                    let current_stats = index_statistics(&current);
                    let leaf_stats = index_statistics(leaf);
                    let current_schema = current.output_schema(arena);
                    let leaf_schema = leaf.output_schema(arena);
                    join_rows(
                        &JoinInputStatistics {
                            rows: current_cost.rows,
                            schema: current_schema,
                            index: current_stats.map(|(index, ndv)| (arena.index(index), ndv)),
                        },
                        &JoinInputStatistics {
                            rows: leaf_cost.rows,
                            schema: leaf_schema,
                            index: leaf_stats.map(|(index, ndv)| (arena.index(index), ndv)),
                        },
                        candidate_keys,
                        ndvs,
                        arena,
                    )
                };
                let connected = candidate_keys().next().is_some();
                let hash = connected && !forced;
                // Every input is read once in any order, so only this step's work is compared.
                // A probe never reads its inner input, so its step can be negative.
                let step =
                    |implementation: JoinImpl, outer: &InputCost, inner: &InputCost, rows| {
                        implementation.work(outer, inner, rows, outer.width + inner.width) as i128
                            - outer.work as i128
                            - inner.work as i128
                    };
                // (work, rows, swapped, probe index). Unswapped, `current` is the hash build side
                // and the nested loop / probe outer side.
                let (work, rows, swapped, probe) = if hash {
                    let keep = step(JoinImpl::Hash, &current_cost, &leaf_cost, product);
                    let swap = step(JoinImpl::Hash, &leaf_cost, &current_cost, product);
                    let mut shape = (keep.min(swap), product, swap < keep, None);
                    // Only a leaf can be probed: `leaf` always, `current` before its first join.
                    for swapped in [false, true] {
                        let (inner, outer_cost, inner_cost) = if swapped {
                            (&current, &leaf_cost, &current_cost)
                        } else {
                            (&*leaf, &current_cost, &leaf_cost)
                        };
                        // Probe keys run from the outer value to the probed column.
                        probe_keys.clear();
                        probe_keys.extend(candidate_keys().map(|(c, l)| {
                            if swapped {
                                (l, c)
                            } else {
                                (c, l)
                            }
                        }));
                        let key_count = probe_keys.len();
                        let Some(index) =
                            probe_index(inner, &mut probe_keys, key_count, arena, loader)?
                        else {
                            continue;
                        };
                        // A unique probe emits at most one row per outer row.
                        let rows = product.min(outer_cost.rows);
                        let lookup_work = index.lookup_work;
                        let work = step(
                            JoinImpl::IndexProbe { lookup_work },
                            outer_cost,
                            inner_cost,
                            rows,
                        );
                        if work <= shape.0 {
                            shape = (work, rows, swapped, Some(index));
                        }
                    }
                    shape
                } else {
                    let swapped = nest_loop_swap(&current_cost, &leaf_cost);
                    let (outer, inner) = if swapped {
                        (&leaf_cost, &current_cost)
                    } else {
                        (&current_cost, &leaf_cost)
                    };
                    let work = step(JoinImpl::NestLoop { full: false }, outer, inner, product);
                    (work, product, swapped, None)
                };
                let candidate = JoinCandidate {
                    cross_join: !connected && !single,
                    work,
                    rows,
                    leaf: k,
                    swapped,
                    probe: probe.is_some(),
                };
                if best
                    .as_ref()
                    .is_none_or(|(previous, _)| candidate < *previous)
                {
                    best = Some((candidate, probe));
                }
            }
            // SAFETY: `leaves` is not empty, so the loop above chose a candidate.
            let (
                JoinCandidate {
                    rows: product,
                    leaf: k,
                    swapped,
                    ..
                },
                probe,
            ) = best.unwrap();
            let (leaf, _, leaf_mask) = leaves.remove(k);
            let mut on = keys.take(current_mask, leaf_mask);
            let (mut left, mut right, right_mask) = if swapped {
                for (c, l) in &mut on {
                    swap(c, l);
                }
                (leaf, current, current_mask)
            } else {
                (current, leaf, leaf_mask)
            };
            let left_schema = left.output_schema(arena);
            let right_schema = right.output_schema(arena);
            for (input, position) in &mut positions {
                if *input & right_mask != 0 {
                    *position += left_schema.len();
                }
            }
            for (l, r) in &mut on {
                *l = l.clone_expression(arena)?;
                *r = r.clone_expression(arena)?;
                BindPosition(left_schema).visit(l, arena)?;
                BindPosition(right_schema).visit(r, arena)?;
            }
            let row_count = if estimated {
                RowCountOption::Estimated {
                    rows: product,
                    ndv: None,
                }
            } else {
                RowCountOption::Unknown
            };
            current = if let Some(mut index) = probe {
                // Probe values were chosen before binding; they come from the outer (left) input.
                for value in &mut index.probe {
                    *value = value.clone_expression(arena)?;
                    BindPosition(left_schema).visit(value, arena)?;
                }
                probe_join(left, right, on, index, row_count, arena)?
            } else {
                let (condition, join_type) = if on.is_empty() {
                    (JoinCondition::None, JoinType::Cross)
                } else {
                    (JoinCondition::On { on, filter: None }, JoinType::Inner)
                };
                let mut join = JoinOperator::build(left, right, condition, join_type, forced);
                let Operator::Join(op) = &join.operator else {
                    unreachable!()
                };
                let implementation = op.plan_impl();
                let mut option = PhysicalOption::new(implementation, SortOption::None, row_count);
                option.join_decided = true;
                join.physical_option = Some(option);
                join
            };
            current_cost = input_cost_or_default(&mut current, product, arena)?;
            current_mask |= leaf_mask;
        }
        *plan = project_join_output(
            current,
            original_schema,
            positions.iter().map(|(_, position)| *position),
            arena,
        );
        Ok(true)
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::expression::BinaryOperator;
    use crate::optimizer::core::histogram::{HistogramMetaBuilder, DEFAULT_HLL_PRECISION};
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::TableArenaCell;
    use crate::storage::StatisticsMetaCache;
    use crate::types::index::IndexInfo;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn input(name: &str, rows: usize, arena: &mut PlanArena) -> (LogicalPlan, ExprRef) {
        let mut column = ColumnCatalog::new(
            name.into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        );
        column.set_ref_table(name.into(), 0, false);
        let column = arena.alloc_column(column);
        let key = arena.alloc_expression(ScalarExpression::column_expr(column, 0));
        let mut plan = LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: name.into(),
                deserializers: crate::storage::deserializers_for_test(vec![column]),
                limit: (None, None),
                index_infos: Vec::new(),
                with_pk: false,
            }),
            Childrens::None,
        );
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated { rows, ndv: None },
        ));
        (plan, key)
    }

    fn indexed_input(
        name: &str,
        rows: usize,
        ndv: Option<usize>,
        arena: &mut PlanArena,
    ) -> (LogicalPlan, ExprRef, IndexMetaRef) {
        let (mut plan, key) = input(name, rows, arena);
        let meta = arena.alloc_index(IndexMeta {
            id: 1,
            table_name: name.into(),
            name: format!("idx_{name}"),
            column_ids: vec![0],
            ty: IndexType::Normal,
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
        });
        let info = IndexInfo {
            meta,
            sort_option: SortOption::None,
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        };
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::IndexScan(Box::new(info)),
            SortOption::None,
            RowCountOption::Estimated { rows, ndv },
        ));
        (plan, key, meta)
    }

    #[test]
    fn malformed_regions_are_planning_errors() {
        // (outer join key, expected message). Each side of a key is only searched in its own
        // input range, so a left column used on the right is reported as not produced.
        type OuterKey = fn(ExprRef, ExprRef, &mut PlanArena) -> (ExprRef, ExprRef);
        let cases: [(OuterKey, &str); 2] = [
            (|a, _, _| (a, a), "is not produced by any join input"),
            (
                |_, c, arena| (input("z", 1, arena).1, c),
                "is not produced by any join input",
            ),
        ];
        for (outer_key, message) in cases {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let (a, ak) = input("a", 1, &mut arena);
            let (b, bk) = input("b", 1, &mut arena);
            let (c, ck) = input("c", 1, &mut arena);
            let ab = JoinOperator::build(
                a,
                b,
                JoinCondition::On {
                    on: vec![(ak, bk)],
                    filter: None,
                },
                JoinType::Inner,
                false,
            );
            let mut plan = JoinOperator::build(
                ab,
                c,
                JoinCondition::On {
                    on: vec![outer_key(ak, ck, &mut arena)],
                    filter: None,
                },
                JoinType::Inner,
                false,
            );
            let Err(DatabaseError::InvalidPlan(error)) =
                GreedyJoinReorder.apply(&mut plan, &mut arena, None)
            else {
                panic!("expected invalid plan: {message}");
            };
            assert!(error.contains(message), "{error}");
        }
    }

    #[test]
    fn input_masks_support_64_inputs_and_skip_larger_regions() -> Result<(), DatabaseError> {
        for count in [64, 65] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let (mut plan, mut previous) = input("t0", 1, &mut arena);
            for index in 1..count {
                let (next, key) = input(&format!("t{index}"), 1, &mut arena);
                plan = JoinOperator::build(
                    plan,
                    next,
                    JoinCondition::On {
                        on: vec![(previous, key)],
                        filter: None,
                    },
                    JoinType::Inner,
                    false,
                );
                previous = key;
            }
            let schema = plan.output_schema(&mut arena).clone();
            let original = plan.clone();
            assert_eq!(
                GreedyJoinReorder.apply(&mut plan, &mut arena, None)?,
                count == 64
            );
            assert_eq!(plan.output_schema(&mut arena), &schema);
            if count == 65 {
                assert_eq!(plan, original);
            }
        }
        Ok(())
    }

    #[test]
    fn column_ndv_is_read_from_scan_without_an_index() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut left, l) = input("l", 1000, &mut arena);
        let (right, r) = input("r", 200, &mut arena);
        let mut cache = StatisticsMetaCache::default();
        for (table, rows) in [("l", 1000), ("r", 200)] {
            let mut builder = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION)?;
            for value in 0..rows {
                builder.append(&DataValue::Int32(value % 100));
            }
            cache.columns.insert((table.into(), 0), builder.build());
        }
        let loader = StatisticMetaLoader::new(&cache);
        assert_eq!(
            column_ndv(&left, &right, [(l, r)], Some(&loader), &arena)?,
            Some((100, 100))
        );
        assert_eq!(column_ndv(&left, &right, [(l, r)], None, &arena)?, None);
        assert_eq!(
            column_ndv(
                &left,
                &right,
                [] as [(ExprRef, ExprRef); 0],
                Some(&loader),
                &arena
            )?,
            None
        );
        left.physical_option.as_mut().unwrap().row_count =
            RowCountOption::Estimated { rows: 8, ndv: None };
        assert_eq!(
            column_ndv(&left, &right, [(l, r)], Some(&loader), &arena)?,
            Some((8, 100))
        );
        left.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
            rows: 1000,
            ndv: None,
        };
        let mut nulls = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION)?;
        nulls.append(&DataValue::Null);
        cache.columns.insert(("r".into(), 0), nulls.build());
        assert_eq!(
            column_ndv(
                &left,
                &right,
                [(l, r)],
                Some(&StatisticMetaLoader::new(&cache)),
                &arena
            )?,
            Some((100, 0))
        );
        cache.columns.remove(&("r".into(), 0));
        assert_eq!(
            column_ndv(
                &left,
                &right,
                [(l, r)],
                Some(&StatisticMetaLoader::new(&cache)),
                &arena
            )?,
            None
        );
        Ok(())
    }

    #[test]
    fn complete_index_keys_reduce_rows_and_bound_uniqueness() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut left, l, li) = indexed_input("l", 1000, Some(100), &mut arena);
        let (mut right, r, ri) = indexed_input("r", 200, Some(100), &mut arena);
        let one = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let expr_key = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Plus,
            left_expr: l,
            right_expr: one,
            evaluator: None,
            ty: LogicalType::Integer,
        });
        let left_schema = left.output_schema(&mut arena);
        let right_schema = right.output_schema(&mut arena);
        let l_input = JoinInputStatistics {
            rows: 1000,
            schema: left_schema,
            index: Some((arena.index(li), Some(100))),
        };
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: right_schema,
            index: Some((arena.index(ri), Some(100))),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            2000
        );
        assert_eq!(
            join_rows(&l_input, &r_input, std::iter::empty, None, &arena),
            200000
        );
        let mut unique_right = arena.index(ri).clone();
        unique_right.ty = IndexType::Unique;
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: right_schema,
            index: Some((&unique_right, None)),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            1000
        );
        let mut unique_left = arena.index(li).clone();
        unique_left.ty = IndexType::PrimaryKey { is_multiple: false };
        let l_input = JoinInputStatistics {
            rows: 1000,
            schema: left_schema,
            index: Some((&unique_left, None)),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            200
        );
        unique_right.column_ids.push(1);
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: right_schema,
            index: Some((&unique_right, Some(100))),
        };
        let l_input = JoinInputStatistics {
            rows: 1000,
            schema: left_schema,
            index: Some((arena.index(li), Some(100))),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            200000
        );
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: right_schema,
            index: Some((arena.index(ri), Some(100))),
        };
        assert_eq!(
            join_rows(
                &l_input,
                &r_input,
                || std::iter::once((expr_key, r)),
                None,
                &arena
            ),
            200000
        );
        Ok(())
    }

    #[test]
    fn composite_ndv_requires_the_whole_key() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (_left, l, li) = indexed_input("l", 1000, Some(100), &mut arena);
        let (_right, r, ri) = indexed_input("r", 200, Some(100), &mut arena);
        let mut lc = ColumnCatalog::new(
            "l_second".into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        lc.set_ref_table("l".into(), 1, false);
        let lc = arena.alloc_column(lc);
        let mut rc = ColumnCatalog::new(
            "r_second".into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        rc.set_ref_table("r".into(), 1, false);
        let rc = arena.alloc_column(rc);
        let l2 = arena.alloc_expression(ScalarExpression::column_expr(lc, 1));
        let r2 = arena.alloc_expression(ScalarExpression::column_expr(rc, 1));
        let mut l_index = arena.index(li).clone();
        let mut r_index = arena.index(ri).clone();
        for index in [&mut l_index, &mut r_index] {
            index.column_ids.push(1);
            index.ty = IndexType::Composite;
            index.value_ty = LogicalType::Tuple(vec![LogicalType::Integer, LogicalType::Integer]);
        }
        let ScalarExpression::ColumnRef { column: l_col, .. } = arena.expression(l) else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef { column: r_col, .. } = arena.expression(r) else {
            unreachable!()
        };
        let l_schema = [*l_col, lc];
        let r_schema = [*r_col, rc];
        let l_input = JoinInputStatistics {
            rows: 1000,
            schema: &l_schema,
            index: Some((&l_index, Some(100))),
        };
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: &r_schema,
            index: Some((&r_index, Some(100))),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            200000
        );
        assert_eq!(
            join_rows(
                &l_input,
                &r_input,
                || [(l, r), (l2, r2)].into_iter(),
                None,
                &arena
            ),
            2000
        );
        Ok(())
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
            RowCountOption::Estimated {
                rows: 200,
                ndv: None,
            },
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
            Operator::Project(ProjectOperator::new(exprs)),
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
    fn unknown_startup_does_not_become_zero() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut scan, key) = input("source", 16, &mut arena);
        scan.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
        let mut sort = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![SortField {
                    expr: key,
                    asc: true,
                    nulls_first: false,
                }],
            }),
            Childrens::Only(Box::new(scan)),
        );
        sort.physical_option = Some(PhysicalOption::new(
            PlanImpl::Sort,
            SortOption::None,
            RowCountOption::Follow,
        ));
        assert_eq!(sort.price(&mut arena).and_then(|cost| cost.startup), None);
        assert_eq!(
            sort.price(&mut arena)
                .and_then(|cost| cost.input())
                .map(|cost| cost.work),
            None
        );

        Ok(())
    }

    #[test]
    fn nested_loop_moves_materialization_left_and_restores_positions() -> Result<(), DatabaseError>
    {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (mut left, _) = input("a", 100, &mut arena);
        let mut right = materialized_right(&mut arena);
        assert_eq!(
            left.price(&mut arena).and_then(|cost| cost.startup),
            Some(0)
        );
        // The 10-row build side is read (50), stored whole (50) and hashed.
        assert_eq!(
            right.price(&mut arena).and_then(|cost| cost.startup),
            Some(100 + 10 * crate::planner::operator::HASH_BUILD_WORK)
        );
        let mut plan =
            JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 20000,
                ndv: None,
            },
        ));
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
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
        assert!(!GreedyJoinReorder.apply(&mut join, &mut arena, None)?);
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
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.estimated_rows(), Some(200000));
        // BC (200 rows) is merged first and builds against A (1000 rows).
        let (bc, a) = plan.childrens.only().childrens.twins();
        assert_eq!(a.referenced_table(), vec!["a".into()]);
        assert_eq!(bc.referenced_table(), vec!["b".into(), "c".into()]);
        assert_eq!(bc.estimated_rows(), Some(200));
        let Operator::Join(op) = &bc.operator else {
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
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
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
    fn unestimated_region_defers_cartesian_products_by_input_order() -> Result<(), DatabaseError> {
        // FROM a, c, d, b WHERE a = b AND c = b: FROM order crosses a, c and d
        // before any join key applies. Without estimates the region is priced with
        // default rows, and only d stays a cross join.
        for known_b in [false, true] {
            let table_arena = TableArenaCell::default();
            let mut arena = PlanArena::new(&table_arena);
            let unknown = |name: &str, arena: &mut PlanArena| {
                let (mut plan, key) = input(name, 10, arena);
                if !(known_b && name == "b") {
                    plan.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
                }
                (plan, key)
            };
            let (a, a_key) = unknown("a", &mut arena);
            let (c, c_key) = unknown("c", &mut arena);
            let (d, _) = unknown("d", &mut arena);
            let (b, b_key) = unknown("b", &mut arena);
            let ac = JoinOperator::build(a, c, JoinCondition::None, JoinType::Cross, false);
            let acd = JoinOperator::build(ac, d, JoinCondition::None, JoinType::Cross, false);
            let c_column = c_key.output_column_ref(&mut arena);
            let c_key = arena.alloc_expression(ScalarExpression::column_expr(c_column, 1));
            let mut plan = JoinOperator::build(
                acd,
                b,
                JoinCondition::On {
                    on: vec![(a_key, b_key), (c_key, b_key)],
                    filter: None,
                },
                JoinType::Inner,
                false,
            );
            let schema = plan.output_schema(&mut arena).clone();
            assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
            assert_eq!(plan.output_schema(&mut arena), &schema);

            let cross = plan.childrens.only();
            assert!(matches!(
                &cross.operator,
                Operator::Join(join) if join.join_type == JoinType::Cross
            ));
            let (abc, d) = cross.childrens.twins();
            assert_eq!(d.referenced_table(), vec!["d".into()]);
            // Without estimates, the narrower C becomes the build side of AB.
            let (c, ab) = abc.childrens.twins();
            assert_eq!(c.referenced_table(), vec!["c".into()]);
            let mut tables = ab.referenced_table();
            tables.sort();
            assert_eq!(tables, vec!["a".into(), "b".into()]);
            for join in [cross, abc, ab] {
                assert_eq!(join.row_count_option(), None);
            }
            for join in [abc, ab] {
                let Operator::Join(JoinOperator {
                    join_type: JoinType::Inner,
                    on: JoinCondition::On { on, .. },
                    ..
                }) = &join.operator
                else {
                    unreachable!()
                };
                assert_eq!(on.len(), 1);
            }
        }
        Ok(())
    }

    #[test]
    fn outer_joins_keep_original_order() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut plan = three_inputs(&mut arena);
        if let Operator::Join(op) = &mut plan.operator {
            op.join_type = JoinType::LeftOuter;
        }
        let original = plan.clone();
        assert!(!GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan, original);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
