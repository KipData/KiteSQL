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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{Iter, Transaction, TupleIter};

pub(crate) struct SeqScan<'a, T: Transaction + 'a> {
    op: Option<TableScanOperator>,
    iter: Option<TupleIter<'a, T>>,
}

impl<'a, T: Transaction + 'a> From<TableScanOperator> for SeqScan<'a, T> {
    fn from(op: TableScanOperator) -> Self {
        SeqScan {
            op: Some(op),
            iter: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SeqScan<'a, T> {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::SeqScan(self))
    }
}

impl<'a, T: Transaction + 'a> SeqScan<'a, T> {
    pub(crate) fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        if self.iter.is_none() {
            let Some(TableScanOperator {
                table_name,
                columns,
                limit,
                with_pk,
                ..
            }) = self.op.take()
            else {
                arena.finish();
                return Ok(());
            };
            self.iter = Some(arena.transaction_mut().read(
                arena.table_cache(),
                table_name,
                limit,
                columns,
                with_pk,
            )?);
        }

        if self
            .iter
            .as_mut()
            .expect("seq scan iterator initialized")
            .next_tuple_into(arena.result_tuple_mut())?
        {
            arena.resume();
        } else {
            arena.finish();
        }
        Ok(())
    }
}
