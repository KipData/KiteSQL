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
use crate::execution::dql::seq_scan::next_scan_tuple;
use crate::execution::{
    ExecBuilder, ExecContext, ExecNode, ExecutorNode, IterSlot, ReadExecutor, StorageIter,
};
use crate::expression::range_detacher::{IndexRangeColumn, Range, RangeDetacher, RangeValue};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::SortOption;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::{IndexScanView, StorageTransaction, TupleMapping};
use crate::types::index::{IndexInfo, IndexLookup, RuntimeIndexProbe};
use std::borrow::Cow;

pub(crate) struct IndexScan<'a> {
    op: &'a TableScanOperator,
    info: &'a IndexInfo,
    lookup: &'a IndexLookup,
    iter: IterSlot,
    /// Borrows the plan's static range, or owns one that parameters bind into.
    ranges: Cow<'a, Range>,
    cover_mapping: Option<TupleMapping>,
}

impl<'a> IndexScan<'a> {
    pub(crate) fn new(
        op: &'a TableScanOperator,
        info: &'a IndexInfo,
        lookup: &'a IndexLookup,
        iter: IterSlot,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> Self {
        let cover_mapping = info
            .covered
            .then(|| {
                TupleMapping::cover(
                    plan_arena.index(info.meta),
                    &op.deserializers,
                    info.cover_mapping.as_deref(),
                )
            })
            .flatten();
        Self {
            op,
            info,
            lookup,
            iter,
            ranges: Cow::Owned(Range::Dummy),
            cover_mapping,
        }
    }

    /// Binds the ranges of this execution into `ranges`.
    pub(crate) fn bind_ranges<T: StorageTransaction + 'a>(
        info: &'a IndexInfo,
        lookup: &'a IndexLookup,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        ranges: &mut Cow<'a, Range>,
    ) -> Result<(), DatabaseError> {
        match lookup {
            IndexLookup::Static(template) if !template.has_parameter() => {
                *ranges = Cow::Borrowed(template)
            }
            IndexLookup::Static(template) => {
                let plan_arena = &*plan_arena;
                let bind = |range: &mut Range| {
                    range.bind_from(template, &|id| plan_arena.bound_param(id), &|id| {
                        plan_arena.init_value(id)
                    })
                };
                match ranges {
                    Cow::Owned(range) => bind(range)?,
                    Cow::Borrowed(_) => {
                        let mut range = Range::Dummy;
                        bind(&mut range)?;
                        *ranges = Cow::Owned(range);
                    }
                }
            }
            IndexLookup::Probe => match (&mut *ranges, arena.pop_runtime_probe()) {
                (Cow::Owned(Range::Eq(value)), RuntimeIndexProbe::Eq(probe)) => {
                    *value = RangeValue::Value(probe)
                }
                (ranges, probe) => *ranges = Cow::Owned(probe.into()),
            },
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
                ranges,
                predicate,
                *ignore_prefix_len,
                plan_arena,
            )? {
                *ranges = Cow::Owned(specialized);
            }
        }
        Ok(())
    }
}

impl<'a> ReadExecutor<'a> for IndexScan<'a> {
    type Input = (&'a TableScanOperator, &'a IndexInfo, &'a IndexLookup);

    fn into_executor<B: ExecBuilder>(
        (op, info, lookup): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let iter = arena.alloc_iter();
        ExecNode::IndexScan(Box::new(IndexScan::new(op, info, lookup, iter, plan_arena)))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for IndexScan<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let slot = self.iter;
        if arena.iter_mut(slot).is_empty() {
            return Ok(());
        }
        if let IndexLookup::Probe = self.lookup {
            Self::bind_ranges(self.info, self.lookup, arena, plan_arena, &mut self.ranges)?;
        }
        arena.iter_mut(slot).as_index().rewind(self.op.limit);
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if arena.iter_mut(self.iter).is_empty() {
            Self::bind_ranges(self.info, self.lookup, arena, plan_arena, &mut self.ranges)?;
            let TableScanOperator {
                table_id,
                deserializers,
                limit,
                with_pk,
                ..
            } = self.op;
            let state = arena.local_state(plan_arena);
            let iter = state.transaction().read_by_index(
                state.plan_arena,
                *table_id,
                *limit,
                self.info.meta,
                self.info.is_reverse(),
                *with_pk,
                deserializers,
                self.info.covered,
            )?;
            state.iters[self.iter] = StorageIter::Index(iter);
        }

        let view = IndexScanView {
            range: &self.ranges,
            cover_mapping: self.cover_mapping.as_ref(),
        };
        let state = arena.local_state(plan_arena);
        let iter = state.iters[self.iter].as_index();
        let bound_buffers = state.bound_buffers;
        let output = state.result.output_mut();
        if next_scan_tuple(
            self.op,
            |table_codec, tuple| iter.next_tuple_into(view, bound_buffers, table_codec, tuple),
            state.table_codec,
            output,
            plan_arena,
        )? {
            arena.resume();
        } else {
            arena.finish();
        }
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::db::test::build_table;
    use crate::execution::ExecStatus;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{Storage, TableCache, TupleDeserializers};
    use crate::types::index::RuntimeIndexProbe;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;

    #[test]
    fn reset_rebinds_probe_after_partial_read_eof_and_missing_key() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut tx = storage.transaction()?;
        let mut names = crate::storage::NameMapping::default();
        let mut tables = TableCache::default();
        let cell = TableArenaCell::default();
        let mut meta = PlanArena::new(&cell);
        build_table(&mut names, &mut tables, &mut tx, &mut meta)?;
        let meta = PlanArena::new(&cell);
        let serializers = [
            LogicalType::Integer.serializable(),
            LogicalType::Boolean.serializable(),
            LogicalType::Integer.serializable(),
        ];
        let mut codec = TableCodec::default();
        for id in 1..=4 {
            tx.append_tuple(
                &mut codec,
                0,
                &Tuple::new(
                    Some(DataValue::Int32(id)),
                    vec![
                        DataValue::Int32(id),
                        DataValue::Boolean(true),
                        DataValue::Int32(id * 10),
                    ],
                )
                .into(),
                &serializers,
                false,
            )?;
        }
        let table = names.table(&tables, "t1").unwrap();
        let op = TableScanOperator {
            table_id: 0,
            deserializers: TupleDeserializers::new(table, &meta, true),
            limit: (None, None),
            index_infos: vec![],
            with_pk: true,
            predicate: None,
        };
        let info = IndexInfo {
            meta: *table.indexes().next().unwrap(),
            sort_option: SortOption::None,
            lookup: None,
            residual_predicate: None,
            covered: false,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        };
        let mut exec_buffer = crate::execution::ExecBuffer::default();
        let mut exec_arena = crate::execution::ExecArena::new(&tables, &tx);
        let mut arena = crate::execution::ExecContext::new(&mut exec_buffer, &mut exec_arena);
        let mut meta = ExecMetaArena::new(meta);
        let slot = arena.alloc_iter();
        let mut scan = IndexScan::new(&op, &info, &IndexLookup::Probe, slot, &meta);
        arena.push_runtime_probe(RuntimeIndexProbe::Scope {
            min: Bound::Included(DataValue::Int32(1)),
            max: Bound::Included(DataValue::Int32(4)),
        });
        scan.next_tuple(&mut arena, &mut meta)?;
        assert_eq!(arena.result_tuple()[0], DataValue::Int32(1));
        let iterator = &arena.arena.iters[slot] as *const _;
        for (probe, expected) in [
            (RuntimeIndexProbe::Eq(DataValue::Int32(3)), vec![3]),
            (RuntimeIndexProbe::Eq(DataValue::Int32(99)), vec![]),
            (
                RuntimeIndexProbe::Scope {
                    min: Bound::Included(DataValue::Int32(1)),
                    max: Bound::Included(DataValue::Int32(4)),
                },
                vec![1, 2, 3, 4],
            ),
            (
                RuntimeIndexProbe::Scope {
                    min: Bound::Excluded(DataValue::Int32(1)),
                    max: Bound::Excluded(DataValue::Int32(4)),
                },
                vec![2, 3],
            ),
            (RuntimeIndexProbe::Eq(DataValue::Int32(1)), vec![1]),
        ] {
            arena.push_runtime_probe(probe);
            scan.reset(&mut arena, &mut meta)?;
            // An initialized scan is ready now, not at its next pull.
            assert!(arena.buffer.runtime_probe_stack.is_empty());
            for id in expected {
                scan.next_tuple(&mut arena, &mut meta)?;
                assert_eq!(arena.result_tuple()[0], DataValue::Int32(id));
                assert_eq!(&arena.arena.iters[slot] as *const _, iterator);
            }
            scan.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(arena.buffer.result.status, Some(ExecStatus::End));
            assert!(arena.buffer.runtime_probe_stack.is_empty());
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
