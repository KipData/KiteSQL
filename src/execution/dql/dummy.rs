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
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

pub struct Dummy {
    row: Option<Tuple>,
}

impl Default for Dummy {
    fn default() -> Self {
        Self {
            row: Some(Tuple::new(None, Vec::new())),
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Dummy {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::Dummy(self))
    }
}

impl Dummy {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        let Some(row) = self.row.take() else {
            arena.finish();
            return Ok(());
        };
        arena.produce_tuple(row);
        Ok(())
    }
}
