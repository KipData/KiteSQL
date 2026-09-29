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
use crate::planner::MetaArena;
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
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
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
