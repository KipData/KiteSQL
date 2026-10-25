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
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::{Iter, Transaction, TupleIter};

pub(crate) struct SeqScan<'a, T: Transaction + 'a> {
    op: &'a TableScanOperator,
    iter: Option<TupleIter<'a, T>>,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SeqScan<'a, T> {
    type Input = &'a TableScanOperator;

    fn into_executor(
        op: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::SeqScan(SeqScan { op, iter: None }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for SeqScan<'a, T> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if let Some(iter) = &mut self.iter {
            let state = arena.local_state(plan_arena);
            iter.reset(
                state.transaction(),
                state.table_codec,
                &self.op.table_name,
                self.op.limit,
            )?;
        }
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let state = arena.local_state(plan_arena);
        let iter = match &mut self.iter {
            Some(iter) => iter,
            None => {
                let TableScanOperator {
                    table_name,
                    columns,
                    limit,
                    with_pk,
                    ..
                } = self.op;
                self.iter.insert(state.transaction().read(
                    state.table_codec,
                    state.plan_arena,
                    state.context.table_cache,
                    table_name.clone(),
                    *limit,
                    columns,
                    *with_pk,
                )?)
            }
        };

        if iter.next_tuple_into(state.table_codec, &mut state.result.tuple)? {
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
    use crate::execution::empty_context;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn reset_keeps_seq_iterator_and_restores_offset_limit() -> Result<(), DatabaseError> {
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
                ),
                &serializers,
                false,
            )?;
        }
        let op = TableScanOperator {
            table_name: "t1".into(),
            columns: tables.get("t1").unwrap().columns().copied().collect(),
            limit: (Some(1), Some(2)),
            index_infos: vec![],
            with_pk: true,
        };
        let views = ViewCache::default();
        let statistics = StatisticsMetaCache::default();
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(empty_context(&tables, &views, &statistics), &tx);
        let mut meta = ExecMetaArena::new(meta);
        let mut scan = SeqScan {
            op: &op,
            iter: None,
        };
        scan.next_tuple(&mut arena, &mut meta)?;
        assert_eq!(arena.result_tuple().values[0], DataValue::Int32(2));
        let iterator = scan.iter.as_ref().unwrap() as *const _;
        for _ in 0..3 {
            scan.reset(&mut arena, &mut meta)?;
            scan.reset(&mut arena, &mut meta)?;
            for id in [2, 3] {
                scan.next_tuple(&mut arena, &mut meta)?;
                assert_eq!(arena.result_tuple().values[0], DataValue::Int32(id));
                assert_eq!(scan.iter.as_ref().unwrap() as *const _, iterator);
            }
            scan.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(arena.result.status, Some(crate::execution::ExecStatus::End));
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
