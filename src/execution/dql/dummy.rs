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
    ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext, ReadExecutor,
};
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
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::Dummy(input))
    }
}

impl<'a> ExecutorNode<'a> for Dummy {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        _: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(row) = self.row.take() else {
            runtime.finish();
            return Ok(());
        };
        runtime.produce_tuple(row);
        Ok(())
    }
}
