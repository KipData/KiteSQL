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
use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::visitor::ExprVisitor;
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::expression::{AliasType, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::visitor::OperatorVisitor;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::{Childrens, ExprRef, LogicalPlan, MetaArena, PlanArena};
use crate::types::index::{IndexMeta, IndexMetaRef, IndexType};
use crate::types::tuple::Schema;
use crate::types::ColumnId;
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
                    // T(input) = startup + rows * row-work. Count the inner
                    // startup again for every outer row; shared output work is
                    // omitted from this local direction comparison.
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

struct ScanColumnVisitor<'plan, 'arena, A> {
    column: Option<ColumnRef>,
    source: Option<(&'plan TableName, ColumnId)>,
    arena: &'arena A,
}

impl<'plan, A: MetaArena> OperatorVisitor<'plan> for ScanColumnVisitor<'plan, '_, A> {
    fn visit_plan(&mut self, plan: &'plan LogicalPlan) -> Result<(), DatabaseError> {
        if self.column.is_none() {
            return Ok(());
        }
        self.visit_operator(&plan.operator, plan.physical_option.as_ref())?;
        let follow = match &plan.operator {
            Operator::Project(_) => true,
            Operator::Join(_) | Operator::Aggregate(_) | Operator::Limit(_) | Operator::TopK(_) => {
                false
            }
            _ => plan
                .physical_option
                .as_ref()
                .is_some_and(|option| matches!(option.row_count, RowCountOption::Follow)),
        };
        if follow && self.column.is_some() {
            self.visit_children(&plan.childrens)?;
        }
        Ok(())
    }

    fn visit_children(&mut self, childrens: &'plan Childrens) -> Result<(), DatabaseError> {
        match childrens {
            Childrens::Only(input) | Childrens::Twins { left: input, .. } => self.visit_plan(input),
            Childrens::None => Ok(()),
        }
    }

    fn visit_table_scan(&mut self, scan: &'plan TableScanOperator) -> Result<(), DatabaseError> {
        if scan.limit == (None, None) {
            if let Some(source) = self.column.and_then(|column| {
                scan.columns
                    .iter()
                    .find(|source| self.arena.same_column(**source, column))
            }) {
                self.source = self
                    .arena
                    .column(*source)
                    .id()
                    .map(|id| (&scan.table_name, id));
            }
        }
        self.column = None;
        Ok(())
    }

    fn visit_project(&mut self, project: &'plan ProjectOperator) -> Result<(), DatabaseError> {
        let Some(column) = self.column.take() else {
            return Ok(());
        };
        let arena = self.arena;
        for &candidate in &project.exprs {
            let source = match arena.expression(candidate) {
                ScalarExpression::Alias {
                    expr,
                    alias: AliasType::Name(_),
                } if arena.column(column).id().is_none()
                    && arena.column(column).table_name().is_none()
                    && candidate.output_name(arena) == arena.column(column).name() =>
                {
                    *expr
                }
                ScalarExpression::Alias {
                    expr,
                    alias: AliasType::Expr(alias),
                } if matches!(arena.expression(*alias), ScalarExpression::ColumnRef { column: target, .. }
                        if arena.same_column(*target, column)) =>
                {
                    *expr
                }
                ScalarExpression::ColumnRef { column: target, .. }
                    if arena.same_column(*target, column) =>
                {
                    candidate
                }
                _ => continue,
            };
            return ExprVisitor::visit(self, source, arena);
        }
        Ok(())
    }
}

impl<A: MetaArena> ExprVisitor<A> for ScanColumnVisitor<'_, '_, A> {
    fn visit_expression(
        &mut self,
        expr: &ScalarExpression,
        _arena: &A,
    ) -> Result<bool, DatabaseError> {
        Ok(matches!(
            expr,
            ScalarExpression::Alias { .. } | ScalarExpression::ColumnRef { .. }
        ))
    }

    fn visit_alias(
        &mut self,
        expr: ExprRef,
        _alias: &AliasType,
        arena: &A,
    ) -> Result<(), DatabaseError> {
        // The alias target describes the output, not the source column.
        self.visit(expr, arena)
    }

    fn visit_column_ref(&mut self, column: &ColumnRef) -> Result<(), DatabaseError> {
        self.column = Some(*column);
        Ok(())
    }
}

fn scan_column<'plan>(
    plan: &'plan LogicalPlan,
    column: ColumnRef,
    arena: &PlanArena,
) -> Result<Option<(&'plan TableName, ColumnId)>, DatabaseError> {
    let mut visitor = ScanColumnVisitor {
        column: Some(column),
        source: None,
        arena,
    };
    visitor.visit_plan(plan)?;
    Ok(visitor.source)
}

fn column_statistics(
    left: &LogicalPlan,
    right: &LogicalPlan,
    keys: &[(ExprRef, ExprRef)],
    loader: Option<&StatisticMetaLoader<'_>>,
    arena: &PlanArena,
) -> Result<Option<(usize, usize)>, DatabaseError> {
    let Some(loader) = loader else {
        return Ok(None);
    };
    let (Some(left_rows), Some(right_rows)) = (left.estimated_rows(), right.estimated_rows())
    else {
        return Ok(None);
    };
    let mut result = None;
    let mut divisor = 0;
    for &(left_key, right_key) in keys {
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

fn covers_index(
    index: &IndexMeta,
    other: &JoinInputStatistics,
    keys: &[(ExprRef, ExprRef)],
    require_other_index: bool,
    arena: &PlanArena,
) -> bool {
    index.column_ids.iter().all(|id| {
        keys.iter().any(|&(left, right)| {
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

fn join_rows(
    left: &JoinInputStatistics,
    right: &JoinInputStatistics,
    keys: &[(ExprRef, ExprRef)],
    columns: Option<(usize, usize)>,
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
        if covers_index(left_index, right, keys, true, arena)
            && covers_index(right_index, left, keys, true, arena)
        {
            left_estimate.ndv = Some(left_ndv);
            right_estimate.ndv = Some(right_ndv);
        }
    }
    if left_estimate.ndv.is_none() || right_estimate.ndv.is_none() {
        if let Some((left_ndv, right_ndv)) = columns {
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
                && covers_index(index, other, keys, false, arena)
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

fn inspect<'plan>(
    plan: &'plan mut LogicalPlan,
    schemas: &mut Vec<&'plan Schema>,
    keys: &mut Vec<(ExprRef, ExprRef)>,
    arena: &mut PlanArena,
    loader: Option<&StatisticMetaLoader<'_>>,
) -> Result<Option<usize>, DatabaseError> {
    if can_expand(plan) {
        let (Operator::Join(join), Childrens::Twins { left, right }) =
            (&plan.operator, plan.childrens.as_mut())
        else {
            unreachable!()
        };
        if let JoinCondition::On { on, .. } = &join.on {
            keys.extend_from_slice(on);
        }
        let left_stats = index_statistics(left);
        let right_stats = index_statistics(right);
        let on = match &join.on {
            JoinCondition::On { on, .. } => on.as_slice(),
            JoinCondition::None => &[],
        };
        let columns = column_statistics(left, right, on, loader, arena)?;
        let start = schemas.len();
        let Some(left_rows) = inspect(left, schemas, keys, arena, loader)? else {
            return Ok(None);
        };
        let middle = schemas.len();
        let Some(right_rows) = inspect(right, schemas, keys, arena, loader)? else {
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
            on,
            columns,
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
        let Some(rows) = plan.estimated_rows() else {
            return Ok(None);
        };
        schemas.push(plan.output_schema(arena));
        Ok(Some(rows))
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
        let original_schema = plan.output_schema(arena).clone();
        let mut schemas = Vec::new();
        let mut keys = Vec::new();
        if inspect(plan, &mut schemas, &mut keys, arena, loader)?.is_none() || schemas.len() < 3 {
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
                let left_stats = index_statistics(left);
                for (offset, (right, right_rows)) in suffix.iter_mut().enumerate() {
                    let j = i + 1 + offset;
                    let right_stats = index_statistics(right);
                    let columns = column_statistics(left, right, &keys, loader, arena)?;
                    let left_schema = left.output_schema(arena);
                    let right_schema = right.output_schema(arena);
                    let product = join_rows(
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
                        &keys,
                        columns,
                        arena,
                    );
                    if let Some(previous) = best {
                        // Equal row estimates retain the earlier pair in enumeration order.
                        if !previous.cross_join && product >= previous.rows {
                            continue;
                        }
                    }
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
                RowCountOption::Estimated {
                    rows: product,
                    ndv: None,
                },
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
    use crate::expression::BinaryOperator;
    use crate::optimizer::core::histogram::{
        HistogramMetaBuilder, ANALYZE_STATISTICS_RELATIVE_ERROR,
    };
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

    #[test]
    fn column_statistics_are_read_from_scan_without_an_index() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let (mut left, l) = input("l", 1000, &mut arena);
        let (mut right, r) = input("r", 200, &mut arena);
        let mut cache = StatisticsMetaCache::default();
        for (table, rows) in [("l", 1000), ("r", 200)] {
            let mut builder = HistogramMetaBuilder::new(ANALYZE_STATISTICS_RELATIVE_ERROR)?;
            for value in 0..rows {
                builder.append(&DataValue::Int32(value % 100));
            }
            cache.columns.insert((table.into(), 0), builder.build());
        }
        let loader = StatisticMetaLoader::new(&cache);
        assert_eq!(
            column_statistics(&left, &right, &[(l, r)], Some(&loader), &arena)?,
            Some((100, 100))
        );
        assert_eq!(
            column_statistics(&left, &right, &[(l, r)], None, &arena)?,
            None
        );
        assert_eq!(
            column_statistics(&left, &right, &[], Some(&loader), &arena)?,
            None
        );
        left.physical_option.as_mut().unwrap().row_count =
            RowCountOption::Estimated { rows: 8, ndv: None };
        assert_eq!(
            column_statistics(&left, &right, &[(l, r)], Some(&loader), &arena)?,
            Some((8, 100))
        );
        left.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
            rows: 1000,
            ndv: None,
        };
        let mut nulls = HistogramMetaBuilder::new(ANALYZE_STATISTICS_RELATIVE_ERROR)?;
        nulls.append(&DataValue::Null);
        cache.columns.insert(("r".into(), 0), nulls.build());
        assert_eq!(
            column_statistics(
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
            column_statistics(
                &left,
                &right,
                &[(l, r)],
                Some(&StatisticMetaLoader::new(&cache)),
                &arena
            )?,
            None
        );
        let mut builder = HistogramMetaBuilder::new(ANALYZE_STATISTICS_RELATIVE_ERROR)?;
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
                &[(l, r)],
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
                "column statistics".into(),
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
            Operator::Project(ProjectOperator { exprs: vec![alias] }),
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
            Operator::Project(ProjectOperator { exprs: vec![alias] }),
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
            Operator::Project(ProjectOperator { exprs: vec![alias] }),
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
            let mut builder = HistogramMetaBuilder::new(ANALYZE_STATISTICS_RELATIVE_ERROR)?;
            for value in 0..ndv {
                builder.append(&DataValue::Int32(value));
            }
            cache.columns.insert((table.into(), 0), builder.build());
        }
        let loader = StatisticMetaLoader::new(&cache);
        let columns = column_statistics(&left, &right, &[(l, r)], Some(&loader), &arena)?;
        assert_eq!(columns, Some((12, 70)));
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
                &[(l, r)],
                columns,
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
            let mut builder = HistogramMetaBuilder::new(ANALYZE_STATISTICS_RELATIVE_ERROR)?;
            for value in 0..ndv {
                builder.append(&DataValue::Int32(value));
            }
            cache.columns.insert((table.into(), id), builder.build());
        }
        let columns = column_statistics(
            &left,
            &right,
            &keys,
            Some(&StatisticMetaLoader::new(&cache)),
            &arena,
        )?;
        assert_eq!(columns, Some((100, 100)));
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
            column_statistics(
                &left,
                &right,
                &keys,
                Some(&StatisticMetaLoader::new(&cache)),
                &arena
            )?,
            columns
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
        assert_eq!(join_rows(&l_input, &r_input, &[(l, r)], None, &arena), 2000);
        assert_eq!(join_rows(&l_input, &r_input, &[], None, &arena), 200000);
        let mut unique_right = arena.index(ri).clone();
        unique_right.ty = IndexType::Unique;
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: &right_schema,
            index: Some((&unique_right, None)),
        };
        assert_eq!(join_rows(&l_input, &r_input, &[(l, r)], None, &arena), 1000);
        let mut unique_left = arena.index(li).clone();
        unique_left.ty = IndexType::PrimaryKey { is_multiple: false };
        let l_input = JoinInputStatistics {
            rows: 1000,
            schema: &left_schema,
            index: Some((&unique_left, None)),
        };
        assert_eq!(join_rows(&l_input, &r_input, &[(l, r)], None, &arena), 200);
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
            join_rows(&l_input, &r_input, &[(l, r)], None, &arena),
            200000
        );
        let r_input = JoinInputStatistics {
            rows: 200,
            schema: &right_schema,
            index: Some((arena.index(ri), Some(100))),
        };
        assert_eq!(
            join_rows(&l_input, &r_input, &[(expr_key, r)], None, &arena),
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
            Operator::Project(ProjectOperator { exprs: vec![l] }),
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
            join_rows(&l_input, &r_input, &[(l, r)], None, &arena),
            200000
        );
        assert_eq!(
            join_rows(&l_input, &r_input, &[(l, r), (l2, r2)], None, &arena),
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
            Operator::Project(ProjectOperator { exprs: vec![key] }),
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
        let mut plan = three_inputs(&mut arena);
        if let Childrens::Twins { left, right } = plan.childrens.as_mut() {
            right.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
                rows: 10,
                ndv: None,
            };
            if let Childrens::Twins { left, .. } = left.childrens.as_mut() {
                left.physical_option.as_mut().unwrap().row_count = RowCountOption::Estimated {
                    rows: 10,
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
        assert_eq!(usize::MAX.saturating_mul(2), usize::MAX);
        assert_eq!(0_usize.saturating_mul(20), 0);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
