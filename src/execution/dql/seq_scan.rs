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
use crate::execution::{
    ExecBuilder, ExecContext, ExecNode, ExecutorNode, IterSlot, ReadExecutor, StorageIter,
};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::{Iter, StorageTransaction};

pub(crate) struct SeqScan<'a> {
    op: &'a TableScanOperator,
    iter: IterSlot,
}

impl<'a> ReadExecutor<'a> for SeqScan<'a> {
    type Input = &'a TableScanOperator;

    fn into_executor<B: ExecBuilder>(
        op: Self::Input,
        arena: &mut B,
        _plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let iter = arena.alloc_iter();
        ExecNode::SeqScan(Box::new(SeqScan { op, iter }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for SeqScan<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let state = arena.local_state(plan_arena);
        let transaction = state.transaction();
        let iter = &mut state.iters[self.iter];
        if !iter.is_empty() {
            iter.as_tuple()
                .reset(transaction, state.table_codec, self.op.limit)?;
        }
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let state = arena.local_state(plan_arena);
        if state.iters[self.iter].is_empty() {
            let TableScanOperator {
                table_id,
                deserializers,
                limit,
                ..
            } = self.op;
            let iter =
                state
                    .transaction()
                    .read(state.table_codec, deserializers, *table_id, *limit)?;
            state.iters[self.iter] = StorageIter::Tuple(iter);
        }
        let iter = state.iters[self.iter].as_tuple();

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
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{Storage, TableCache, TupleDeserializers};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn reset_keeps_seq_iterator_and_restores_offset_limit() -> Result<(), DatabaseError> {
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
        let op = TableScanOperator {
            table_id: 0,
            deserializers: TupleDeserializers::new(
                names.table(&tables, "t1").unwrap(),
                &meta,
                true,
            ),
            limit: (Some(1), Some(2)),
            index_infos: vec![],
            with_pk: true,
        };
        let mut exec_buffer = crate::execution::ExecBuffer::default();
        let mut exec_arena = crate::execution::ExecArena::new(&tables, &tx);
        let mut arena = crate::execution::ExecContext::new(&mut exec_buffer, &mut exec_arena);
        let mut meta = ExecMetaArena::new(meta);
        let mut scan = SeqScan {
            op: &op,
            iter: arena.alloc_iter(),
        };
        scan.next_tuple(&mut arena, &mut meta)?;
        assert_eq!(arena.result_tuple()[0], DataValue::Int32(2));
        let iterator = &arena.arena.iters[scan.iter] as *const _;
        for _ in 0..3 {
            scan.reset(&mut arena, &mut meta)?;
            scan.reset(&mut arena, &mut meta)?;
            for id in [2, 3] {
                scan.next_tuple(&mut arena, &mut meta)?;
                assert_eq!(arena.result_tuple()[0], DataValue::Int32(id));
                assert_eq!(&arena.arena.iters[scan.iter] as *const _, iterator);
            }
            scan.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(
                arena.buffer.result.status,
                Some(crate::execution::ExecStatus::End)
            );
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
