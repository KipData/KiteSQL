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
    build_read, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    ReadExecutor,
};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: ExecId,
    skipped: usize,
    emitted: usize,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Limit {
    type Input = (LimitOperator, LogicalPlan);

    fn into_executor(
        (LimitOperator { offset, limit }, input): Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::Limit(Limit {
            offset,
            limit,
            input,
            skipped: 0,
            emitted: 0,
        }))
    }
}

impl<'a> ExecutorNode<'a> for Limit {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit.unwrap_or(usize::MAX);

        if limit == 0 || self.emitted >= limit {
            runtime.finish();
            return Ok(());
        }

        loop {
            if !runtime.next_tuple(self.input, plan_arena)? {
                runtime.finish();
                return Ok(());
            }

            if self.skipped < offset {
                self.skipped += 1;
                continue;
            }

            self.emitted += 1;
            runtime.resume();
            return Ok(());
        }
    }
}
