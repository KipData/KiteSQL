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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, WriteExecutor};
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropIndex {
    op: Option<DropIndexOperator>,
}

impl From<DropIndexOperator> for DropIndex {
    fn from(op: DropIndexOperator) -> Self {
        Self { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropIndex {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::DropIndex(self))
    }
}

impl DropIndex {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        let Some(DropIndexOperator {
            table_name,
            index_name,
            if_exists,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        arena.transaction_mut().drop_index(
            arena.table_cache(),
            arena.meta_cache(),
            table_name,
            &index_name,
            if_exists,
        )?;

        TupleBuilder::build_result_into(arena.result_tuple_mut(), index_name.to_string());
        arena.resume();
        Ok(())
    }
}
