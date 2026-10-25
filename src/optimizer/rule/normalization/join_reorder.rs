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
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::index::{IndexMeta, IndexMetaRef, IndexType};
use crate::types::tuple::Schema;
use std::borrow::Borrow;
use std::collections::{btree_map::Entry, BTreeMap, HashMap};
use std::mem::swap;

pub(crate) struct GreedyJoinReorder;
pub(crate) struct OrientNestedLoopJoin;

impl NormalizationRule for OrientNestedLoopJoin {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        _loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
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
            plan.childrens.as_mut(),
            plan.physical_option.as_ref(),
        )
        else {
            return Ok(false);
        };
        let (Some(left_startup), Some(right_startup)) = (
            left.estimated_startup_work()?,
            right.estimated_startup_work()?,
        ) else {
            return Ok(false);
        };
        let should_swap = match (left.estimated_rows(), right.estimated_rows()) {
            (Some(left_rows), Some(right_rows)) => {
                if left_rows == 0 {
                    false
                } else if right_rows == 0 {
                    true
                } else {
                    // Repeat the complete inner work for every outer row,
                    // including rows discarded by filters. Shared output work
                    // is omitted from this local direction comparison.
                    let (Some(left_work), Some(right_work)) =
                        (left.estimated_work()?, right.estimated_work()?)
                    else {
                        return Ok(false);
                    };
                    let keep = left_work.saturating_add(left_rows.saturating_mul(right_work));
                    let swap = right_work.saturating_add(right_rows.saturating_mul(left_work));
                    swap < keep
                }
            }
            (Some(0 | 1), None) => false,
            _ => right_startup > left_startup,
        };
        if !should_swap {
            return Ok(false);
        }
        let left_len = left.output_schema(arena).len();
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
        join.clear_startup_work();
        let right_len = original_schema.len() - left_len;
        let positions = (right_len..original_schema.len()).chain(0..right_len);
        *plan = project_join_output(join, original_schema, positions, arena);
        Ok(true)
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

// Prefer equijoins, then less intermediate/build data, then the original input order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct JoinCandidate {
    cross_join: bool,
    work: u128,
    rows: usize,
    left: usize,
    right: usize,
    swapped: bool,
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

// Counts a region's inputs and reports whether it already contains a Cartesian product.
fn region_shape(plan: &LogicalPlan) -> (usize, bool) {
    if !can_expand(plan) {
        return (1, false);
    }
    let (left, right) = plan.childrens.twins();
    let (left_inputs, left_cross) = region_shape(left);
    let (right_inputs, right_cross) = region_shape(right);
    let cross = matches!(
        &plan.operator,
        Operator::Join(JoinOperator {
            join_type: JoinType::Cross,
            ..
        })
    );
    (
        left_inputs + right_inputs,
        cross || left_cross || right_cross,
    )
}

// Collects input schemas and join keys
// returns `None` when any input lacks a row estimate.
fn inspect<'plan>(
    plan: &'plan mut LogicalPlan,
    schemas: &mut Vec<&'plan Schema>,
    keys: &mut JoinKeys,
    arena: &mut PlanArena,
    loader: Option<&StatisticMetaLoader<'_>>,
) -> Result<Option<usize>, DatabaseError> {
    if can_expand(plan) {
        let (Operator::Join(join), Childrens::Twins { left, right }) =
            (&plan.operator, plan.childrens.as_mut())
        else {
            unreachable!()
        };
        let left_stats = index_statistics(left);
        let right_stats = index_statistics(right);
        let on = match &join.on {
            JoinCondition::On { on, .. } => on.as_slice(),
            JoinCondition::None => &[],
        };
        let ndvs = column_ndv(left, right, on.iter().copied(), loader, arena)?;
        let start = schemas.len();
        let left_rows = inspect(left, schemas, keys, arena, loader)?;
        let middle = schemas.len();
        let right_rows = inspect(right, schemas, keys, arena, loader)?;

        for &(left, right) in on {
            let mut endpoints = [None, None];
            for (endpoint, expr) in endpoints.iter_mut().zip([left, right]) {
                for (index, schema) in schemas.iter().enumerate() {
                    if key_in_schema(expr, schema, arena)? {
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
        let (Some(left_rows), Some(right_rows)) = (left_rows, right_rows) else {
            return Ok(None);
        };
        let rows = join_rows(
            &JoinInputStatistics {
                rows: left_rows,
                schema: if middle == start + 1 {
                    schemas[start]
                } else {
                    &[]
                },
                index: left_stats.map(|(index, ndv)| (arena.index(index), ndv)),
            },
            &JoinInputStatistics {
                rows: right_rows,
                schema: if schemas.len() == middle + 1 {
                    schemas[middle]
                } else {
                    &[]
                },
                index: right_stats.map(|(index, ndv)| (arena.index(index), ndv)),
            },
            || on.iter().copied(),
            ndvs,
            arena,
        );
        if let Some(option) = &mut plan.physical_option {
            if option.row_count != (RowCountOption::Estimated { rows, ndv: None }) {
                option.row_count = RowCountOption::Estimated {
                    rows: rows,
                    ndv: None,
                };
                option.startup_work = None;
            }
        }
        Ok(Some(rows))
    } else {
        let rows = plan.estimated_rows();
        schemas.push(plan.output_schema(arena));
        Ok(rows)
    }
}

fn flatten(plan: LogicalPlan, inputs: &mut Vec<(LogicalPlan, usize, u64)>) {
    if can_expand(&plan) {
        let (left, right) = plan.childrens.pop_twins();
        flatten(left, inputs);
        flatten(right, inputs);
    } else {
        // Unused when the region is not fully estimated.
        let rows = plan.estimated_rows().unwrap_or(0);
        let mask = 1u64 << inputs.len();
        inputs.push((plan, rows, mask));
    }
}

fn key_in_schema(expr: ExprRef, schema: &Schema, arena: &PlanArena) -> Result<bool, DatabaseError> {
    expr.all_referenced_columns(arena, |arena, column| {
        schema
            .iter()
            .any(|candidate| arena.same_column(*candidate, *column))
    })
}

#[derive(Default)]
struct JoinKeys {
    links: BTreeMap<(u64, u64), usize>,
    // Link IDs are indices here; consumed lists leave empty slots.
    on_keys: Vec<Vec<(ExprRef, ExprRef)>>,
}

impl JoinKeys {
    fn insert(&mut self, left: u64, right: u64, mut exprs: (ExprRef, ExprRef)) {
        if left > right {
            swap(&mut exprs.0, &mut exprs.1);
        }
        match self.links.entry((left, right)) {
            Entry::Occupied(entry) => self.on_keys[*entry.get()].push(exprs),
            Entry::Vacant(entry) => {
                let id = self.on_keys.len();
                self.on_keys.push(vec![exprs]);
                entry.insert(id);
                self.links.insert((right, left), id);
            }
        }
    }

    // Both directions share one canonically oriented list; take aligns the selected ON.
    fn get(&self, left: u64, right: u64) -> impl Iterator<Item = (ExprRef, ExprRef)> + '_ {
        self.links
            .get(&(left, right))
            .into_iter()
            .flat_map(|&id| self.on_keys[id].iter().copied())
    }

    fn pairs(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.links
            .keys()
            .copied()
            .filter(|(left, right)| left < right)
    }

    fn take(&mut self, left: u64, right: u64) -> Vec<(ExprRef, ExprRef)> {
        let Some(id) = self.links.remove(&(left, right)) else {
            return Vec::new();
        };
        self.links.remove(&(right, left));
        let mut on = std::mem::take(&mut self.on_keys[id]);
        if left > right {
            for (l, r) in &mut on {
                swap(l, r);
            }
        }
        on
    }

    fn rewrite(&mut self, left: u64, right: u64) {
        // take has already consumed the internal edge; only external links remain.
        debug_assert!(!self.links.contains_key(&(left, right)));
        let merged = left | right;
        for old in [left, right] {
            loop {
                let link = self
                    .links
                    .extract_if((old, 0)..=(old, u64::MAX), |_, _| true)
                    .next();
                let Some(((_, other), id)) = link else { break };
                self.links.remove(&(other, old));
                if (old > other) != (merged > other) {
                    for (l, r) in &mut self.on_keys[id] {
                        swap(l, r);
                    }
                }
                if let Some(&existing) = self.links.get(&(merged, other)) {
                    let mut extra = std::mem::take(&mut self.on_keys[id]);
                    self.on_keys[existing].append(&mut extra);
                } else {
                    self.links.insert((merged, other), id);
                    self.links.insert((other, merged), id);
                }
            }
        }
    }
}

struct BindPosition<'a>(&'a Schema);

impl<A: MetaArena + ?Sized> ExprVisitorMut<A> for BindPosition<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        arena: &mut A,
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
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        if !matches!(&plan.operator, Operator::Join(join) if join.join_type == JoinType::Inner)
            || !can_expand(plan)
        {
            return Ok(false);
        }
        let (input_count, _) = region_shape(plan);
        if input_count > u64::BITS as usize {
            return Ok(false);
        }
        let original_schema = plan.output_schema(arena).clone();
        let mut schemas = Vec::new();
        let mut keys = JoinKeys::default();
        let estimated = inspect(plan, &mut schemas, &mut keys, arena, loader)?.is_some();
        // Unknown row counts remain unknown; only column widths break their ties.
        // Two-input regions also need to choose the smaller hash build side.
        // Each original column keeps its input bit and position within its current join.
        let mut positions = Vec::with_capacity(original_schema.len());
        for (input, schema) in schemas.iter().enumerate() {
            positions.extend((0..schema.len()).map(|position| (1u64 << input, position)));
        }
        let mut inputs = Vec::with_capacity(schemas.len());
        flatten(plan.take(), &mut inputs);
        let mut row_cache = HashMap::new();
        while inputs.len() > 1 {
            let mut best: Option<JoinCandidate> = None;
            let connected = !keys.links.is_empty();
            let mut evaluate = |inputs: &mut [(LogicalPlan, usize, u64)],
                                i: usize,
                                j: usize|
             -> Result<(), DatabaseError> {
                let (prefix, suffix) = inputs.split_at_mut(j);
                let (left, left_rows, left_mask) = &mut prefix[i];
                let (right, right_rows, right_mask) = &mut suffix[0];
                // This pair's inputs and keys stay fixed until either input is merged.
                let cache_key = (*left_mask, *right_mask);
                let candidate_keys = || keys.get(*left_mask, *right_mask);
                let product = if !estimated {
                    0
                } else if let Some(rows) = row_cache.get(&cache_key) {
                    *rows
                } else {
                    let ndvs = column_ndv(left, right, candidate_keys(), loader, arena)?;
                    let left_stats = index_statistics(left);
                    let right_stats = index_statistics(right);
                    let left_schema = left.output_schema(arena);
                    let right_schema = right.output_schema(arena);
                    let rows = join_rows(
                        &JoinInputStatistics {
                            rows: *left_rows,
                            schema: left_schema,
                            index: left_stats.map(|(index, ndv)| (arena.index(index), ndv)),
                        },
                        &JoinInputStatistics {
                            rows: *right_rows,
                            schema: right_schema,
                            index: right_stats.map(|(index, ndv)| (arena.index(index), ndv)),
                        },
                        &candidate_keys,
                        ndvs,
                        arena,
                    );
                    row_cache.insert(cache_key, rows);
                    rows
                };
                let left_width = left.output_schema(arena).len() as u128;
                let right_width = right.output_schema(arena).len() as u128;
                let left_size = if estimated { *left_rows as u128 } else { 1 } * left_width;
                let right_size = if estimated { *right_rows as u128 } else { 1 } * right_width;
                // HashJoin builds on the left. Count intermediate values and saved build values,
                // not just output rows; use widths alone when cardinalities are unavailable.
                let work = (if estimated { product as u128 } else { 1 })
                    .saturating_mul(left_width + right_width)
                    .saturating_add(if connected {
                        left_size.min(right_size)
                    } else {
                        0
                    });
                let candidate = JoinCandidate {
                    cross_join: !connected,
                    work,
                    rows: product,
                    left: i,
                    right: j,
                    swapped: connected && right_size < left_size,
                };
                if best.is_none_or(|previous| candidate < previous) {
                    best = Some(candidate);
                }
                Ok(())
            };
            if connected {
                for (left, right) in keys.pairs() {
                    // SAFETY: keys only contains active input masks; merge rewrites removed
                    // endpoints. Disjoint masks have unique lowest bits, and inserting each
                    // merged input at the earlier position preserves their sorted order.
                    let i = inputs
                        .binary_search_by_key(&(left.trailing_zeros(), left), |(_, _, mask)| {
                            (mask.trailing_zeros(), *mask)
                        })
                        .unwrap();
                    let j = inputs
                        .binary_search_by_key(&(right.trailing_zeros(), right), |(_, _, mask)| {
                            (mask.trailing_zeros(), *mask)
                        })
                        .unwrap();
                    evaluate(&mut inputs, i.min(j), i.max(j))?;
                }
            } else {
                for i in 0..inputs.len() {
                    for j in i + 1..inputs.len() {
                        evaluate(&mut inputs, i, j)?;
                    }
                }
            }
            let Some(JoinCandidate {
                rows: product,
                left: i,
                right: j,
                swapped,
                ..
            }) = best
            else {
                return Ok(false);
            };
            let mut right = inputs.remove(j);
            let mut left = inputs.remove(i);
            if swapped {
                swap(&mut left, &mut right);
            }
            let (mut left, _, left_mask) = left;
            let (mut right, _, right_mask) = right;
            let left_schema = left.output_schema(arena);
            let right_schema = right.output_schema(arena);
            for (input, position) in &mut positions {
                if *input & right_mask != 0 {
                    *position += left_schema.len();
                }
            }
            let mut on = keys.take(left_mask, right_mask);
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
                if estimated {
                    RowCountOption::Estimated {
                        rows: product,
                        ndv: None,
                    }
                } else {
                    RowCountOption::Unknown
                },
            ));
            keys.rewrite(left_mask, right_mask);
            inputs.insert(i, (join, product, left_mask | right_mask));
        }
        // SAFETY: At this point, there should be exactly one plan left in the inputs vector, which is the final join result.
        let (join, _, _) = inputs.pop().unwrap();
        *plan = project_join_output(
            join,
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
    use crate::expression::{AliasType, BinaryOperator};
    use crate::optimizer::core::histogram::{HistogramMetaBuilder, DEFAULT_HLL_PRECISION};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::mark_apply::MarkApplyOperator;
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::TableArenaCell;
    use crate::storage::StatisticsMetaCache;
    use crate::types::index::{IndexInfo, IndexLookup};
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
            RowCountOption::Estimated {
                rows: rows,
                ndv: None,
            },
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

    fn parameterized_input(
        input_rows: usize,
        output_rows: usize,
        arena: &mut PlanArena,
    ) -> LogicalPlan {
        let (left, probe) = input("probe_input", input_rows, arena);
        let (mut right, _, _) = indexed_input("lookup", 0, None, arena);
        let option = right.physical_option.as_mut().unwrap();
        option.row_count = RowCountOption::Unknown;
        let PlanImpl::IndexScan(info) = &mut option.plan else {
            unreachable!()
        };
        info.lookup = Some(IndexLookup::Probe);
        let mut plan = LogicalPlan::new(
            Operator::MarkApply(MarkApplyOperator::new_inner_join(Vec::new(), probe)),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::MarkApply,
            SortOption::Follow,
            RowCountOption::Estimated {
                rows: output_rows,
                ndv: None,
            },
        ));
        plan
    }

    #[test]
    fn greedy_tracks_positions_across_unequal_width_inputs() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut a, a_key) = input("a", 100, &mut arena);
        let (b, _) = input("b", 1, &mut arena);
        let (mut c, c_key) = input("c", 10, &mut arena);
        for (plan, key, width, table) in [(&mut a, a_key, 2, "a"), (&mut c, c_key, 3, "c")] {
            let column = key.output_column_ref(&mut arena);
            let column = arena.column(column).clone();
            let Operator::TableScan(scan) = &mut plan.operator else {
                panic!("expected a scan");
            };
            for index in 1..width {
                let mut extra = column.clone();
                extra.summary_mut().name = format!("{table}{index}");
                extra.set_ref_table(table.into(), index, false);
                scan.columns.push(arena.alloc_column(extra));
            }
        }
        let left = JoinOperator::build(a, b, JoinCondition::None, JoinType::Cross, false);
        let mut plan = JoinOperator::build(
            left,
            c,
            JoinCondition::On {
                on: vec![(a_key, c_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let original_schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &original_schema);
        let Operator::Project(project) = &plan.operator else {
            panic!("expected a projection restoring A, B, C output order");
        };
        assert_eq!(project.exprs.len(), 6);
        for (expr, position) in project.exprs.iter().zip([0, 1, 5, 2, 3, 4]) {
            assert!(matches!(
                arena.expression(*expr),
                ScalarExpression::ColumnRef { position: actual, .. } if *actual == position
            ));
        }
        Ok(())
    }

    #[test]
    fn parameterized_join_work_changes_nested_loop_direction() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (left, _) = input("cheap", 12, &mut arena);
        let right = parameterized_input(12, 13, &mut arena);
        let mut plan =
            JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let schema = plan.output_schema(&mut arena).clone();
        assert!(OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.estimated_rows(), Some(156));
        let Operator::Project(project) = &plan.operator else {
            panic!("expected a projection restoring the two swapped inputs");
        };
        assert_eq!(project.exprs.len(), 3);
        for (expr, position) in project.exprs.iter().zip([2, 0, 1]) {
            assert!(matches!(
                arena.expression(*expr),
                ScalarExpression::ColumnRef { position: actual, .. } if *actual == position
            ));
        }
        let Childrens::Only(join) = plan.childrens.as_ref() else {
            unreachable!()
        };
        let Childrens::Twins { left, right } = join.childrens.as_ref() else {
            unreachable!()
        };
        assert!(matches!(left.operator, Operator::MarkApply(_)));
        assert_eq!(left.estimated_rows(), Some(13));
        assert_eq!(right.estimated_rows(), Some(12));
        Ok(())
    }

    #[test]
    fn filtered_input_work_changes_nested_loop_direction() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (left, _) = input("small", 7, &mut arena);
        let (right, key) = input("filtered", 98, &mut arena);
        let value = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let predicate = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: key,
            right_expr: value,
            ty: LogicalType::Boolean,
            evaluator: None,
        });
        let mut right = FilterOperator::build(predicate, right, false);
        right.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Estimated { rows: 8, ndv: None },
        ));
        assert_eq!(right.estimated_startup_work()?, Some(0));
        assert_eq!(right.estimated_work()?, Some(392));
        let column = key.unpack_bound_col(&arena, false).unwrap().0;
        assert_eq!(
            scan_column(&right, column, &arena)?,
            Some((&"filtered".into(), 0))
        );
        let mut plan =
            JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let schema = plan.output_schema(&mut arena).clone();
        assert!(OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        assert_eq!(plan.estimated_rows(), Some(56));
        let Childrens::Only(join) = plan.childrens.as_ref() else {
            unreachable!()
        };
        let Childrens::Twins { left, .. } = join.childrens.as_ref() else {
            unreachable!()
        };
        assert!(matches!(left.operator, Operator::Filter(_)));
        Ok(())
    }

    #[test]
    fn join_keys_preserve_conditions_across_merges() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (_, a) = input("a", 10, &mut arena);
        let (_, b) = input("b", 10, &mut arena);
        let (_, c) = input("c", 10, &mut arena);
        let (_, d) = input("d", 10, &mut arena);
        let alias = a.clone_expression(&mut arena)?;
        let mut keys = JoinKeys::default();
        keys.insert(1, 4, (a, c));
        keys.insert(1, 4, (alias, c));
        keys.insert(4, 1, (c, a));
        keys.insert(2, 4, (b, c));
        keys.insert(1, 2, (a, b));
        keys.insert(2, 8, (b, d));
        keys.insert(4, 8, (c, d));
        assert_eq!(
            keys.pairs().collect::<Vec<_>>(),
            vec![(1, 2), (1, 4), (2, 4), (2, 8), (4, 8)]
        );
        assert_eq!(
            keys.get(4, 1).collect::<Vec<_>>(),
            vec![(a, c), (alias, c), (a, c)]
        );
        assert!(keys.get(1, 8).next().is_none());
        assert_eq!(keys.take(1, 2), vec![(a, b)]);
        assert!(keys.links.contains_key(&(1, 4)));
        keys.rewrite(1, 2);
        assert_eq!(
            keys.get(3, 4).collect::<Vec<_>>(),
            vec![(a, c), (alias, c), (a, c), (b, c)]
        );
        assert_eq!(keys.get(3, 8).collect::<Vec<_>>(), vec![(b, d)]);
        assert!(keys.get(1, 4).next().is_none());
        assert!(keys.get(1, 2).next().is_none());
        assert_eq!(keys.take(4, 3), vec![(c, a), (c, alias), (c, a), (c, b)]);
        keys.rewrite(4, 3);
        assert_eq!(keys.take(8, 7), vec![(d, c), (d, b)]);
        keys.rewrite(8, 7);
        assert!(keys.links.is_empty());
        Ok(())
    }

    #[test]
    fn join_keys_preserve_condition_direction_after_merge() {
        let (a, b, c, d, e) = (
            ExprRef::new(0),
            ExprRef::new(1),
            ExprRef::new(2),
            ExprRef::new(3),
            ExprRef::new(4),
        );
        let mut keys = JoinKeys::default();
        keys.insert(2, 4, (b, c));
        keys.insert(2, 4, (d, a));
        keys.insert(8, 4, (e, c));
        assert!(keys.take(2, 8).is_empty());
        keys.rewrite(2, 8);
        assert_eq!(
            keys.get(10, 4).collect::<Vec<_>>(),
            vec![(c, b), (a, d), (c, e)]
        );
        assert_eq!(keys.take(10, 4), vec![(b, c), (d, a), (e, c)]);
        assert!(keys.links.is_empty());
    }

    #[test]
    fn malformed_regions_are_planning_errors() {
        // (outer join key, expected message)
        let cases: [(
            fn(ExprRef, ExprRef, &mut PlanArena) -> (ExprRef, ExprRef),
            &str,
        ); 2] = [
            (|a, _, _| (a, a), "compares columns of the same join input"),
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
    fn merged_inputs_do_not_reuse_leaf_pair_row_estimates() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (a, ak) = input("a", 100, &mut arena);
        let (b, bk) = input("b", 100, &mut arena);
        let (c, ck) = input("c", 100, &mut arena);
        let (d, dk) = input("d", 100, &mut arena);
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
        let abc = JoinOperator::build(
            ab,
            c,
            JoinCondition::On {
                on: vec![(bk, ck)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let mut plan = JoinOperator::build(
            abc,
            d,
            JoinCondition::On {
                on: vec![(ck, dk)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let schema = plan.output_schema(&mut arena).clone();
        let mut cache = StatisticsMetaCache::default();
        for (table, ndv) in [("a", 100), ("b", 100), ("c", 10), ("d", 10)] {
            let mut builder = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION)?;
            for value in 0..ndv {
                builder.append(&DataValue::Int32(value));
            }
            cache.columns.insert((table.into(), 0), builder.build());
        }
        assert!(GreedyJoinReorder.apply(
            &mut plan,
            &mut arena,
            Some(&StatisticMetaLoader::new(&cache))
        )?);
        // A-B estimates 100, C-D estimates 1,000. Their merged inputs must not
        // reuse any leaf-pair result when Vec positions are removed and reused.
        assert_eq!(plan.estimated_rows(), Some(100 * 1_000));
        assert_eq!(plan.output_schema(&mut arena), &schema);
        Ok(())
    }

    #[test]
    fn column_ndv_is_read_from_scan_without_an_index() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut left, l) = input("l", 1000, &mut arena);
        let (mut right, r) = input("r", 200, &mut arena);
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
            column_ndv(&left, &right, &[(l, r)], Some(&loader), &arena)?,
            Some((100, 100))
        );
        assert_eq!(column_ndv(&left, &right, &[(l, r)], None, &arena)?, None);
        assert_eq!(column_ndv(&left, &right, &[], Some(&loader), &arena)?, None);
        left.physical_option.as_mut().unwrap().row_count =
            RowCountOption::Estimated { rows: 8, ndv: None };
        assert_eq!(
            column_ndv(&left, &right, &[(l, r)], Some(&loader), &arena)?,
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
                &[(l, r)],
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
                &[(l, r)],
                Some(&StatisticMetaLoader::new(&cache)),
                &arena
            )?,
            None
        );
        let mut builder = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION)?;
        for value in 0..200 {
            builder.append(&DataValue::Int32(value % 100));
        }
        cache.columns.insert(("r".into(), 0), builder.build());
        let loader = StatisticMetaLoader::new(&cache);
        let left_schema = left.output_schema(&mut arena);
        let right_schema = right.output_schema(&mut arena);
        assert_eq!(
            join_rows(
                &JoinInputStatistics {
                    rows: 1000,
                    schema: left_schema,
                    index: None
                },
                &JoinInputStatistics {
                    rows: 200,
                    schema: right_schema,
                    index: None
                },
                || std::iter::once((l, r)),
                Some((100, 100)),
                &arena
            ),
            2000
        );
        let mut join = JoinOperator::build(
            left,
            right,
            JoinCondition::On {
                on: vec![(l, r)],
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
        let pipeline = HepOptimizerPipeline::builder()
            .after_batch(
                "column statistics",
                HepBatchStrategy::fix_point_topdown(1),
                vec![NormalizationRuleImpl::GreedyJoinReorder],
            )
            .build();
        let plan = pipeline
            .instantiate(join)
            .find_best(Some(&loader), &mut arena)?;
        assert_eq!(plan.estimated_rows(), Some(2000));
        Ok(())
    }

    #[test]
    fn column_origin_follows_direct_projections_but_stops_at_computation(
    ) -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (left, key) = input("l", 10, &mut arena);
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: key,
            alias: AliasType::Name("alias_l".into()),
        });
        let output = alias.output_column_ref(&mut arena);
        let mut projected = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![alias])),
            Childrens::Only(Box::new(left)),
        );
        projected.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        assert_eq!(
            scan_column(&projected, output, &arena)?,
            Some((&"l".into(), 0))
        );
        let alias_target = arena.alloc_expression(ScalarExpression::column_expr(output, 0));
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: key,
            alias: AliasType::Expr(alias_target),
        });
        let Operator::Project(project) = &mut projected.operator else {
            unreachable!()
        };
        project.exprs[0] = alias;
        assert_eq!(
            scan_column(&projected, output, &arena)?,
            Some((&"l".into(), 0))
        );
        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let mut filtered = LogicalPlan::new(
            Operator::Filter(FilterOperator {
                predicate,
                having: false,
                is_optimized: false,
            }),
            Childrens::Only(Box::new(projected)),
        );
        filtered.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        assert_eq!(
            scan_column(&filtered, output, &arena)?,
            Some((&"l".into(), 0))
        );
        let (other, other_key) = input("other", 10, &mut arena);
        assert_eq!(
            scan_column(
                &filtered,
                other_key.unpack_bound_col(&arena, false).unwrap().0,
                &arena
            )?,
            None
        );
        let mut join = JoinOperator::build(
            filtered,
            other,
            JoinCondition::On {
                on: vec![(key, other_key)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        join.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 100,
                ndv: None,
            },
        ));
        assert_eq!(scan_column(&join, output, &arena)?, None);
        let one = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let computed = arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Plus,
            left_expr: key,
            right_expr: one,
            ty: LogicalType::Integer,
            evaluator: None,
        });
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: computed,
            alias: AliasType::Name("computed".into()),
        });
        let output = alias.output_column_ref(&mut arena);
        let (source, _) = input("l", 10, &mut arena);
        let mut project = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![alias])),
            Childrens::Only(Box::new(source)),
        );
        project.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        assert_eq!(scan_column(&project, output, &arena)?, None);
        let (mut scan, key) = input("limited", 10, &mut arena);
        let column = key.unpack_bound_col(&arena, false).unwrap().0;
        let Operator::TableScan(op) = &mut scan.operator else {
            unreachable!()
        };
        op.limit = (Some(1), Some(3));
        assert_eq!(scan_column(&scan, column, &arena)?, None);
        let limited = LogicalPlan::new(
            Operator::Limit(LimitOperator {
                offset: Some(1),
                limit: Some(3),
            }),
            Childrens::Only(Box::new(scan)),
        );
        assert_eq!(scan_column(&limited, column, &arena)?, None);
        let aggregate = LogicalPlan::new(
            Operator::Aggregate(AggregateOperator {
                agg_calls: vec![],
                groupby_exprs: vec![key],
                is_distinct: false,
                force_spill: false,
            }),
            *limited.childrens,
        );
        assert_eq!(scan_column(&aggregate, column, &arena)?, None);
        Ok(())
    }

    #[test]
    fn column_origin_ignores_nested_alias_targets_and_rejects_casts() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (scan, key) = input("l", 10, &mut arena);
        let (_, unrelated) = input("other", 10, &mut arena);
        let inner_alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: key,
            alias: AliasType::Expr(unrelated),
        });
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: inner_alias,
            alias: AliasType::Name("nested".into()),
        });
        let output = alias.output_column_ref(&mut arena);
        let mut project = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![alias])),
            Childrens::Only(Box::new(scan)),
        );
        assert_eq!(
            scan_column(&project, output, &arena)?,
            Some((&"l".into(), 0))
        );
        let cast = arena.alloc_expression(ScalarExpression::TypeCast {
            expr: key,
            ty: LogicalType::Bigint,
            evaluator: None,
        });
        let alias = arena.alloc_expression(ScalarExpression::Alias {
            expr: cast,
            alias: AliasType::Name("casted".into()),
        });
        let output = alias.output_column_ref(&mut arena);
        let Operator::Project(op) = &mut project.operator else {
            unreachable!()
        };
        op.exprs[0] = alias;
        assert_eq!(scan_column(&project, output, &arena)?, None);
        Ok(())
    }

    #[test]
    fn column_origin_only_follows_the_preserved_left_input() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (left, key) = input("l", 10, &mut arena);
        let (right, other) = input("r", 10, &mut arena);
        let marker = arena.alloc_column(ColumnCatalog::new(
            "marker".into(),
            true,
            ColumnDesc::new(LogicalType::Boolean, None, false, None)?,
        ));
        let mut plan = MarkApplyOperator::build_exists(left, right, marker, vec![]);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::MarkApply,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        let left_column = key.unpack_bound_col(&arena, false).unwrap().0;
        let right_column = other.unpack_bound_col(&arena, false).unwrap().0;
        assert_eq!(
            scan_column(&plan, left_column, &arena)?,
            Some((&"l".into(), 0))
        );
        assert_eq!(scan_column(&plan, right_column, &arena)?, None);
        assert_eq!(scan_column(&plan, marker, &arena)?, None);
        plan.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
        assert_eq!(scan_column(&plan, left_column, &arena)?, None);
        Ok(())
    }

    #[test]
    fn column_ndv_is_independent_of_an_unrelated_filtering_index() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut left, l, index) = indexed_input("l", 12, Some(2), &mut arena);
        let (mut right, r) = input("r", 110, &mut arena);
        let mut meta = arena.index(index).clone();
        meta.column_ids = vec![1];
        let index = arena.alloc_index(meta);
        let PlanImpl::IndexScan(info) = &mut left.physical_option.as_mut().unwrap().plan else {
            unreachable!()
        };
        info.meta = index;
        let mut cache = StatisticsMetaCache::default();
        for (table, ndv) in [("l", 30), ("r", 70)] {
            let mut builder = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION)?;
            for value in 0..ndv {
                builder.append(&DataValue::Int32(value));
            }
            cache.columns.insert((table.into(), 0), builder.build());
        }
        let loader = StatisticMetaLoader::new(&cache);
        let ndvs = column_ndv(&left, &right, &[(l, r)], Some(&loader), &arena)?;
        assert_eq!(ndvs, Some((12, 70)));
        let left_schema = left.output_schema(&mut arena);
        let right_schema = right.output_schema(&mut arena);
        assert_eq!(
            join_rows(
                &JoinInputStatistics {
                    rows: 12,
                    schema: left_schema,
                    index: Some((arena.index(index), Some(2)))
                },
                &JoinInputStatistics {
                    rows: 110,
                    schema: right_schema,
                    index: None
                },
                || std::iter::once((l, r)),
                ndvs,
                &arena
            ),
            19
        );
        Ok(())
    }

    #[test]
    fn ordinary_join_keys_use_the_strongest_single_divisor() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut left, l) = input("l", 1000, &mut arena);
        let (mut right, r) = input("r", 200, &mut arena);
        let mut keys = vec![(l, r)];
        let mut extra_keys = Vec::new();
        for (table, plan) in [("l", &mut left), ("r", &mut right)] {
            let mut column = ColumnCatalog::new(
                "extra".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
            );
            column.set_ref_table(table.into(), 1, false);
            let column = arena.alloc_column(column);
            let Operator::TableScan(scan) = &mut plan.operator else {
                unreachable!()
            };
            scan.columns.push(column);
            extra_keys.push(arena.alloc_expression(ScalarExpression::column_expr(column, 1)));
        }
        keys.push((extra_keys[0], extra_keys[1]));
        let mut cache = StatisticsMetaCache::default();
        for (table, id, ndv) in [("l", 0, 100), ("r", 0, 100), ("l", 1, 80), ("r", 1, 70)] {
            let mut builder = HistogramMetaBuilder::new(DEFAULT_HLL_PRECISION)?;
            for value in 0..ndv {
                builder.append(&DataValue::Int32(value));
            }
            cache.columns.insert((table.into(), id), builder.build());
        }
        let ndvs = column_ndv(
            &left,
            &right,
            &keys,
            Some(&StatisticMetaLoader::new(&cache)),
            &arena,
        )?;
        assert_eq!(ndvs, Some((100, 100)));
        assert_eq!(
            estimate_join_rows(
                JoinInputEstimate {
                    rows: 1000,
                    ndv: Some(100),
                    unique: false
                },
                JoinInputEstimate {
                    rows: 200,
                    ndv: Some(100),
                    unique: false
                }
            ),
            2000
        );
        keys.reverse();
        assert_eq!(
            column_ndv(
                &left,
                &right,
                &keys,
                Some(&StatisticMetaLoader::new(&cache)),
                &arena
            )?,
            ndvs
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
            schema: &left_schema,
            index: Some((arena.index(li), Some(100))),
        };
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: &right_schema,
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
            schema: &right_schema,
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
            schema: &left_schema,
            index: Some((&unique_left, None)),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            200
        );
        unique_right.column_ids.push(1);
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: &right_schema,
            index: Some((&unique_right, Some(100))),
        };
        let l_input = JoinInputStatistics {
            rows: 1000,
            schema: &left_schema,
            index: Some((arena.index(li), Some(100))),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, || std::iter::once((l, r)), None, &arena),
            200000
        );
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: &right_schema,
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
    fn follow_keeps_index_statistics_but_join_results_do_not() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (left, l, li) = indexed_input("l", 1000, Some(100), &mut arena);
        let (right, r, _) = indexed_input("r", 200, Some(100), &mut arena);
        let mut left = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![l])),
            Childrens::Only(Box::new(left)),
        );
        left.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        assert_eq!(index_statistics(&left), Some((li, Some(100))));
        let mut join = JoinOperator::build(
            left,
            right,
            JoinCondition::On {
                on: vec![(l, r)],
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
        assert!(!GreedyJoinReorder.apply(&mut join, &mut arena, None)?);
        assert_eq!(join.estimated_rows(), Some(2000));
        assert_eq!(index_statistics(&join), None);
        Ok(())
    }

    #[test]
    fn greedy_uses_ndv_before_pruning_connected_candidates() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (left, l, _) = indexed_input("l", 1000, Some(100), &mut arena);
        let (right, r, _) = indexed_input("r", 200, Some(100), &mut arena);
        let (third, c) = input("c", 20, &mut arena);
        let mut lr = JoinOperator::build(
            left,
            right,
            JoinCondition::On {
                on: vec![(l, r)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        lr.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let mut plan = JoinOperator::build(
            lr,
            third,
            JoinCondition::On {
                on: vec![(r, c)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        let (lr, _) = plan.childrens.twins();
        assert_eq!(lr.referenced_table(), vec!["l".into(), "r".into()]);
        assert_eq!(lr.estimated_rows(), Some(2000));
        assert_eq!(index_statistics(lr), None);
        assert_eq!(plan.estimated_rows(), Some(40000));
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
    fn startup_estimates_follow_inputs_and_final_implementation() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut scan, key) = input("source", 16, &mut arena);
        assert_eq!(scan.estimated_startup_work()?, Some(0));
        assert_eq!(scan.estimated_work()?, Some(64));
        let mut sort = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![SortField {
                    expr: key,
                    asc: true,
                    nulls_first: false,
                }],
            }),
            Childrens::Only(Box::new(scan.clone())),
        );
        sort.physical_option = Some(PhysicalOption::new(
            PlanImpl::Sort,
            SortOption::None,
            RowCountOption::Follow,
        ));
        assert_eq!(sort.estimated_startup_work()?, Some(192));
        assert_eq!(sort.estimated_work()?, Some(256));
        let mut project = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![key])),
            Childrens::Only(Box::new(sort)),
        );
        project.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        assert_eq!(project.estimated_startup_work()?, Some(192));
        assert_eq!(project.estimated_work()?, Some(256));

        let mut aggregate = LogicalPlan::new(
            Operator::Aggregate(AggregateOperator {
                agg_calls: vec![key],
                groupby_exprs: vec![key],
                is_distinct: false,
                force_spill: false,
            }),
            Childrens::Only(Box::new(scan)),
        );
        aggregate.physical_option = Some(PhysicalOption::new(
            PlanImpl::HashAggregate,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        assert_eq!(aggregate.estimated_startup_work()?, Some(160));
        assert_eq!(aggregate.estimated_work()?, None);
        aggregate.physical_option = Some(PhysicalOption::new(
            PlanImpl::StreamAggregate,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        assert_eq!(aggregate.estimated_startup_work()?, Some(0));
        Ok(())
    }

    #[test]
    fn startup_unknown_and_overflow_do_not_become_zero() -> Result<(), DatabaseError> {
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
        assert_eq!(sort.estimated_startup_work()?, None);
        assert_eq!(sort.estimated_work()?, None);

        let (a, _) = input("a", usize::MAX, &mut arena);
        let (b, _) = input("b", 2, &mut arena);
        let mut cross = JoinOperator::build(a, b, JoinCondition::None, JoinType::Cross, false);
        cross.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        assert_eq!(cross.estimated_work()?, Some(usize::MAX));
        assert_eq!(cross.estimated_rows(), Some(usize::MAX));
        assert!(!OrientNestedLoopJoin.apply(&mut cross, &mut arena, None)?);
        Ok(())
    }

    #[test]
    fn nested_loop_cost_combines_startup_and_driver_rows() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        for (left_rows, right_rows, swap) in [
            (100, 1, true),
            (1, 100, false),
            (10, 10, false),
            (10, 0, true),
            (0, 10, false),
        ] {
            let (left, _) = input("left", left_rows, &mut arena);
            let (right, _) = input("right", right_rows, &mut arena);
            let mut plan =
                JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::NestLoopJoin,
                SortOption::None,
                RowCountOption::Unknown,
            ));
            plan.estimated_startup_work()?;
            assert_eq!(
                plan.estimated_rows(),
                Some(left_rows.saturating_mul(right_rows))
            );
            let schema = plan.output_schema(&mut arena).clone();
            assert_eq!(
                OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?,
                swap
            );
            assert_eq!(plan.output_schema(&mut arena), &schema);
        }

        // A large startup does not outweigh a driver that already executes the
        // right side once. Missing HashJoin output rows remain unknown as well.
        for unknown in [false, true] {
            let (left, _) = input("single", 1, &mut arena);
            let mut right = materialized_right(&mut arena);
            if unknown {
                let Childrens::Only(join) = right.childrens.as_mut() else {
                    unreachable!()
                };
                join.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
                join.reset_output_schema_cache();
            }
            let mut plan =
                JoinOperator::build(left, right, JoinCondition::None, JoinType::Cross, false);
            plan.physical_option = Some(PhysicalOption::new(
                PlanImpl::NestLoopJoin,
                SortOption::None,
                RowCountOption::Unknown,
            ));
            assert!(!OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?);
        }
        Ok(())
    }

    #[test]
    fn nested_cross_preserves_parameterized_input_work() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let mut expensive = parameterized_input(100, 2, &mut arena);
        assert_eq!(expensive.estimated_work()?, Some(508));
        let (peer, _) = input("peer", 3, &mut arena);
        let mut cross =
            JoinOperator::build(expensive, peer, JoinCondition::None, JoinType::Cross, false);
        cross.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        // Read the expensive child once, peer twice, then emit six rows.
        assert_eq!(cross.estimated_work()?, Some(508 + 2 * 12 + 6 * 4));
        let (outer, _) = input("outer", 2, &mut arena);
        let mut plan =
            JoinOperator::build(cross, outer, JoinCondition::None, JoinType::Cross, false);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::NestLoopJoin,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let original = plan.clone();
        assert!(!OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan, original);
        Ok(())
    }

    #[test]
    fn nested_loop_moves_materialization_left_and_restores_positions() -> Result<(), DatabaseError>
    {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (mut left, _) = input("a", 100, &mut arena);
        let mut right = materialized_right(&mut arena);
        assert_eq!(left.estimated_startup_work()?, Some(0));
        assert_eq!(right.estimated_startup_work()?, Some(90));
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
        assert!(OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?);
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
        assert!(!OrientNestedLoopJoin.apply(&mut join, &mut arena, None)?);
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
            assert!(!OrientNestedLoopJoin.apply(&mut plan, &mut arena, None)?);
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
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
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
    fn edge_order_ties_keep_original_input_order() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let (a, ak) = input("a", 1, &mut arena);
        let (b, bk) = input("b", 1, &mut arena);
        let (c, ck) = input("c", 1, &mut arena);
        let (d, dk) = input("d", 1, &mut arena);
        let (e, ek) = input("e", 10, &mut arena);
        let bc = JoinOperator::build(
            b,
            c,
            JoinCondition::On {
                on: vec![(bk, ck)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let abc = JoinOperator::build(a, bc, JoinCondition::None, JoinType::Cross, false);
        let abcd = JoinOperator::build(
            abc,
            d,
            JoinCondition::On {
                on: vec![(ak, dk)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let mut plan = JoinOperator::build(
            abcd,
            e,
            JoinCondition::On {
                on: vec![(ak, ek), (bk, ek)],
                filter: None,
            },
            JoinType::Inner,
            false,
        );
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan.output_schema(&mut arena), &schema);
        // AD (mask 9) precedes BC (mask 6) in inputs, despite their reverse tree order.
        let join = plan.childrens.only();
        let (left, right) = join.childrens.twins();
        assert_eq!(
            left.referenced_table(),
            vec!["a".into(), "d".into(), "e".into()]
        );
        assert_eq!(right.referenced_table(), vec!["b".into(), "c".into()]);
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
    fn connected_pair_precedes_a_smaller_cartesian_product() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let mut plan = three_inputs(&mut arena);
        if let Childrens::Twins { left, .. } = plan.childrens.as_mut() {
            if let Childrens::Twins { left, right } = left.childrens.as_mut() {
                left.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
                    rows: 10,
                    ndv: None,
                };
                right.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
                    rows: 1000,
                    ndv: None,
                };
            }
        }
        let schema = plan.output_schema(&mut arena).clone();
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
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
        assert!(!GreedyJoinReorder.apply(&mut cross, &mut arena, None)?);
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
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
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
        assert!(GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
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
    fn unestimated_region_defers_cartesian_products_by_input_order() -> Result<(), DatabaseError> {
        // FROM a, c, d, b WHERE a = b AND c = b: FROM order crosses a, c and d
        // before any join key applies. Without estimates the region is rebuilt
        // by connectivity and input position, and only d stays a cross join.
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
            let (ab, c) = abc.childrens.twins();
            assert_eq!(c.referenced_table(), vec!["c".into()]);
            let (a, b) = ab.childrens.twins();
            assert_eq!(a.referenced_table(), vec!["a".into()]);
            assert_eq!(b.referenced_table(), vec!["b".into()]);
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
            Operator::Project(ProjectOperator::new(exprs)),
            Childrens::Only(Box::new(join)),
        );
        let original_parent = plan.operator.clone();
        let Childrens::Only(join) = plan.childrens.as_mut() else {
            unreachable!()
        };
        assert!(GreedyJoinReorder.apply(join, &mut arena, None)?);
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
        assert!(!GreedyJoinReorder.apply(&mut plan, &mut arena, None)?);
        assert_eq!(plan, original);
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
