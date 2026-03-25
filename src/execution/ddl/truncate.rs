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
use crate::planner::operator::truncate::TruncateOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct Truncate {
    op: Option<TruncateOperator>,
}

impl From<TruncateOperator> for Truncate {
    fn from(op: TruncateOperator) -> Self {
        Truncate { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Truncate {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::Truncate(self))
    }
}

impl Truncate {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<crate::types::tuple::Tuple>, DatabaseError> {
        let Some(TruncateOperator { table_name }) = self.op.take() else {
            return Ok(None);
        };
        arena.transaction_mut().drop_data(&table_name)?;

        Ok(Some(TupleBuilder::build_result(format!("{table_name}"))))
    }
}
