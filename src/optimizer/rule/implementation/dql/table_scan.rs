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
use crate::expression::range_detacher::Range;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{BestPhysicalOption, ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::types::index::{IndexLookup, IndexType};
use std::sync::LazyLock;

static TABLE_SCAN_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::TableScan(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct SeqScanImplementation;

impl MatchPattern for SeqScanImplementation {
    fn pattern(&self) -> &Pattern {
        &TABLE_SCAN_PATTERN
    }
}

impl ImplementationRule for SeqScanImplementation {
    fn update_best_option(
        &self,
        op: &Operator,
        arena: &crate::planner::PlanArena,
        loader: &StatisticMetaLoader<'_>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        if let Operator::TableScan(scan_op) = op {
            let cost = scan_op
                .index_infos
                .iter()
                .find(|index_info| {
                    matches!(
                        arena.index(index_info.meta).ty,
                        IndexType::PrimaryKey { .. }
                    )
                })
                .map(|index_info| loader.load(&scan_op.table_name, arena.index(index_info.meta).id))
                .transpose()?
                .flatten()
                .map(|statistics_meta| statistics_meta.histogram().values_len());

            crate::optimizer::core::rule::keep_best_physical_option(
                best_physical_option,
                PhysicalOption::new(PlanImpl::SeqScan, SortOption::None),
                cost,
            );
            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}

pub struct IndexScanImplementation;

impl MatchPattern for IndexScanImplementation {
    fn pattern(&self) -> &Pattern {
        &TABLE_SCAN_PATTERN
    }
}

impl ImplementationRule for IndexScanImplementation {
    fn update_best_option(
        &self,
        op: &Operator,
        arena: &crate::planner::PlanArena,
        loader: &StatisticMetaLoader<'_>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        if let Operator::TableScan(scan_op) = op {
            for index_info in scan_op.index_infos.iter() {
                let Some(IndexLookup::Static(range)) = &index_info.lookup else {
                    continue;
                };
                let mut cost = None;

                let index_meta = arena.index(index_info.meta);
                if let Some(mut row_count) =
                    loader.collect_count(&scan_op.table_name, index_meta.id, range)?
                {
                    row_count = adjust_index_row_count(index_meta.ty, range, row_count);
                    if index_info.covered_deserializers.is_none()
                        && !matches!(index_meta.ty, IndexType::PrimaryKey { .. })
                    {
                        // need to return table query(non-covering index)
                        row_count *= 2;
                    }
                    cost = Some(row_count);
                }

                if let Some(row_count) = cost {
                    // bonus = 0.5 * (rows * log2(rows)) / log2(hint_sum), capped to rows/2
                    // c = rows - bonus, min rows/2
                    // c
                    // rows   |\
                    //        | \
                    //        |  \
                    //        |   \______
                    // rows/2 |__________\______
                    //        |
                    //        |
                    //        └────────────────────── hint_sum
                    //         2    4    8   16   32
                    let hint_sum = index_info
                        .sort_elimination_hint
                        .map(|hint| hint.cover_num())
                        .unwrap_or(0)
                        + index_info
                            .stream_distinct_hint
                            .map(|hint| hint.cover_num())
                            .unwrap_or(0);
                    if hint_sum > 0 {
                        // TODO: use histogram correlation to refine the cost of ordered index
                        // scans. A hint here means an ancestor has an ordering requirement that
                        // index_info.sort_option can satisfy; correlation can estimate whether the
                        // underlying row access is closer to sequential IO or random IO.
                        let rows = row_count.max(1) as f64;
                        let raw_bonus = rows * rows.log2();
                        let hint_weight = (hint_sum as f64).log2().max(1.0);
                        // TODO: replace this heuristic with accurate row-count driven sort cost once available.
                        let bonus = (raw_bonus / hint_weight * 0.5) as usize;
                        let min_cost = row_count.div_ceil(2);
                        cost = Some(row_count.saturating_sub(bonus).max(min_cost));
                    }
                }

                crate::optimizer::core::rule::keep_best_physical_option(
                    best_physical_option,
                    PhysicalOption::new(
                        PlanImpl::IndexScan(Box::new(index_info.clone())),
                        index_info.sort_option.clone(),
                    ),
                    cost,
                );
            }

            Ok(())
        } else {
            unreachable!("invalid operator!")
        }
    }
}

fn adjust_index_row_count(index_type: IndexType, range: &Range, row_count: usize) -> usize {
    let row_count = unique_eq_row_count(index_type, range).unwrap_or(row_count);
    if row_count == 0 && !matches!(range, Range::Dummy) {
        1
    } else {
        row_count
    }
}

fn unique_eq_row_count(index_type: IndexType, range: &Range) -> Option<usize> {
    match range {
        Range::Dummy => Some(0),
        Range::Eq(value)
            if !value.is_null()
                && matches!(index_type, IndexType::PrimaryKey { .. } | IndexType::Unique) =>
        {
            Some(1)
        }
        Range::SortedRanges(ranges) => ranges.iter().try_fold(0usize, |count, range| {
            unique_eq_row_count(index_type, range).map(|row_count| count + row_count)
        }),
        _ => None,
    }
}
