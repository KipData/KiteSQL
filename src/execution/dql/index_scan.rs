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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor};
use crate::expression::range_detacher::{IndexRangeColumn, Range, RangeDetacher};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::SortOption;
use crate::planner::MetaArena;
use crate::storage::{IndexIter, IndexRanges, Iter, Transaction};
use crate::types::index::{IndexInfo, IndexLookup, RuntimeIndexProbe};
use std::borrow::Cow;

pub(crate) struct IndexScan<'a, T: Transaction + 'a> {
    op: &'a TableScanOperator,
    info: &'a IndexInfo,
    lookup: &'a IndexLookup,
    iter: Option<IndexIter<'a, T>>,
}

impl<'a, T: Transaction + 'a> IndexScan<'a, T> {
    pub(crate) fn new(
        op: &'a TableScanOperator,
        info: &'a IndexInfo,
        lookup: &'a IndexLookup,
    ) -> Self {
        Self {
            op,
            info,
            lookup,
            iter: None,
        }
    }

    pub(crate) fn ranges(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<IndexRanges<'a>, DatabaseError> {
        let info = self.info;
        let mut range = match self.lookup {
            IndexLookup::Static(range) => Cow::Borrowed(range),
            IndexLookup::Probe => Cow::Owned(match arena.pop_runtime_probe() {
                RuntimeIndexProbe::Eq(value) => Range::Eq(value),
                RuntimeIndexProbe::Scope { min, max } => Range::Scope { min, max },
            }),
        };

        if plan_arena.has_bound_params() && range.has_parameter() {
            let plan_arena = &*plan_arena;
            range
                .to_mut()
                .bind_parameters(&|id| plan_arena.bound_param(id))?;
        }
        if let (
            Some(predicate),
            SortOption::OrderBy {
                ignore_prefix_len, ..
            },
        ) = (info.residual_predicate, &info.sort_option)
        {
            if let Some(specialized) = RangeDetacher::<IndexRangeColumn, _>::specialize_range(
                info.meta,
                &range,
                predicate,
                *ignore_prefix_len,
                plan_arena,
            )? {
                range = Cow::Owned(specialized);
            }
        }
        Ok(range.into())
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for IndexScan<'a, T> {
    type Input = (&'a TableScanOperator, &'a IndexInfo, &'a IndexLookup);

    fn into_executor(
        (op, info, lookup): Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::IndexScan(IndexScan::new(op, info, lookup)))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for IndexScan<'a, T> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        let iter = match &mut self.iter {
            Some(iter) => iter,
            None => {
                let ranges = self.ranges(arena, plan_arena)?;
                let TableScanOperator {
                    table_name,
                    columns,
                    limit,
                    with_pk,
                    ..
                } = self.op;
                let state = arena.local_state(plan_arena);
                self.iter.insert(state.transaction().read_by_index(
                    state.context.table_cache,
                    state.plan_arena,
                    table_name.clone(),
                    *limit,
                    columns,
                    self.info.meta,
                    ranges,
                    *with_pk,
                    self.info.covered_deserializers.as_deref(),
                    self.info.cover_mapping.as_deref(),
                )?)
            }
        };

        let state = arena.local_state(plan_arena);
        if iter.next_tuple_into(state.table_codec, &mut state.result.tuple)? {
            arena.resume();
        } else {
            arena.finish();
        }
        Ok(())
    }
}
