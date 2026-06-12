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
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
pub struct Union {
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
    left_input: ExecId,
    right_input: ExecId,
    reading_left: bool,
}

impl From<(LogicalPlan, LogicalPlan)> for Union {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Union {
            left_plan: left_input,
            right_plan: right_input,
            left_input: 0,
            right_input: 0,
            reading_left: true,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Union {
    type Input = (LogicalPlan, LogicalPlan);

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut exec = Self::from(input);
        exec.left_input = build_read(arena, plan_arena, exec.left_plan.take(), cache, transaction);
        exec.right_input = build_read(
            arena,
            plan_arena,
            exec.right_plan.take(),
            cache,
            transaction,
        );
        arena.push(ExecNode::Union(exec))
    }
}

impl<'a> ExecutorNode<'a> for Union {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.reading_left {
            if runtime.next_tuple(self.left_input, plan_arena)? {
                runtime.resume();
                return Ok(());
            }
            self.reading_left = false;
        }
        if runtime.next_tuple(self.right_input, plan_arena)? {
            runtime.resume();
        } else {
            runtime.finish();
        }
        Ok(())
    }
}
