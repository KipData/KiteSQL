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
use crate::expression::range_detacher::{IndexRangeColumn, RangeDetacher};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::SortOption;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::{IndexIter, IndexRanges, Iter, StorageTransaction};
use crate::types::index::{IndexInfo, IndexLookup};
use std::borrow::Cow;

pub(crate) struct IndexScan<'a, T: StorageTransaction + 'a> {
    op: &'a TableScanOperator,
    info: &'a IndexInfo,
    lookup: &'a IndexLookup,
    iter: Option<IndexIter<'a, T>>,
}

impl<'a, T: StorageTransaction + 'a> IndexScan<'a, T> {
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
        info: &'a IndexInfo,
        lookup: &'a IndexLookup,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<IndexRanges<'a>, DatabaseError> {
        let mut range = match lookup {
            IndexLookup::Static(range) => Cow::Borrowed(range),
            IndexLookup::Probe => Cow::Owned(arena.pop_runtime_probe().into()),
        };

        if range.has_parameter() {
            let plan_arena = &*plan_arena;
            range
                .to_mut()
                .bind_parameters(&|id| plan_arena.bound_param(id), &|id| {
                    plan_arena.init_value(id)
                })?;
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
        Ok(IndexRanges::from(range).reversed(info.is_reverse()))
    }
}

impl<'a, T: StorageTransaction + 'a> ReadExecutor<'a, T> for IndexScan<'a, T> {
    type Input = (&'a TableScanOperator, &'a IndexInfo, &'a IndexLookup);

    fn into_executor(
        (op, info, lookup): Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::IndexScan(Box::new(IndexScan::new(
            op, info, lookup,
        ))))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for IndexScan<'a, T> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if let Some(iter) = self.iter.as_mut() {
            match self.lookup {
                IndexLookup::Static(_) => iter.rewind(self.op.limit),
                IndexLookup::Probe if self.info.residual_predicate.is_none() => {
                    iter.reset_probe(arena.pop_runtime_probe(), self.op.limit)
                }
                IndexLookup::Probe => {
                    let ranges = Self::ranges(self.info, self.lookup, arena, plan_arena)?;
                    iter.reset(ranges, self.op.limit);
                }
            }
        }
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let iter = match &mut self.iter {
            Some(iter) => iter,
            None => {
                let ranges = Self::ranges(self.info, self.lookup, arena, plan_arena)?;
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
        if iter.next_tuple_into(state.table_codec, state.result.output_mut())? {
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
    use crate::execution::{empty_context, ExecStatus};
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::index::RuntimeIndexProbe;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;

    #[test]
    fn reset_rebinds_probe_after_partial_read_eof_and_missing_key() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut tx = storage.transaction()?;
        let mut tables = TableCache::default();
        let cell = TableArenaCell::default();
        let mut meta = PlanArena::new(&cell);
        build_table(&mut tables, &mut tx, &mut meta)?;
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
                "t1",
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
        let table = tables.get("t1").unwrap();
        let op = TableScanOperator {
            table_name: "t1".into(),
            columns: table.columns().copied().collect(),
            limit: (None, None),
            index_infos: vec![],
            with_pk: true,
        };
        let info = IndexInfo {
            meta: *table.indexes().next().unwrap(),
            sort_option: SortOption::None,
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        };
        let views = ViewCache::default();
        let statistics = StatisticsMetaCache::default();
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(empty_context(&tables, &views, &statistics), &tx);
        let mut meta = ExecMetaArena::new(meta);
        let mut scan = IndexScan::new(&op, &info, &IndexLookup::Probe);
        arena.push_runtime_probe(RuntimeIndexProbe::Scope {
            min: Bound::Included(DataValue::Int32(1)),
            max: Bound::Included(DataValue::Int32(4)),
        });
        scan.next_tuple(&mut arena, &mut meta)?;
        assert_eq!(arena.result_tuple()[0], DataValue::Int32(1));
        let iterator = scan.iter.as_ref().unwrap() as *const _;
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
            assert!(arena.runtime_probe_stack.is_empty());
            for id in expected {
                scan.next_tuple(&mut arena, &mut meta)?;
                assert_eq!(arena.result_tuple()[0], DataValue::Int32(id));
                assert_eq!(scan.iter.as_ref().unwrap() as *const _, iterator);
            }
            scan.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(arena.result.status, Some(ExecStatus::End));
            assert!(arena.runtime_probe_stack.is_empty());
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
