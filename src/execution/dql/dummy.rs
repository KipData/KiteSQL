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
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
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

impl<'a, T: StorageTransaction + 'a> ReadExecutor<'a, T> for Dummy {
    type Input = ();

    fn into_executor(
        _: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = Dummy::default();
        arena.push(ExecNode::Dummy(Box::new(executor)))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Dummy {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        _arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.row = Some(Tuple::default());
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        _: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let Some(row) = self.row.take() else {
            arena.finish();
            return Ok(());
        };
        arena.produce(row.into());
        Ok(())
    }
}
