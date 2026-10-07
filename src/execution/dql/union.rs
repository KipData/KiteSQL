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
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::Transaction;
pub struct Union {
    left_input: ExecId,
    right_input: ExecId,
    reading_left: bool,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Union {
    type Input = (&'a LogicalPlan, &'a LogicalPlan);

    fn into_executor(
        (left_plan, right_plan): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let left_input = build_read(arena, plan_arena, left_plan, cache, transaction);
        let right_input = build_read(arena, plan_arena, right_plan, cache, transaction);
        arena.push(ExecNode::Union(Union {
            left_input,
            right_input,
            reading_left: true,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Union {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if self.reading_left {
            if arena.next_tuple(self.left_input, plan_arena)? {
                arena.resume();
                return Ok(());
            }
            self.reading_left = false;
        }
        if arena.next_tuple(self.right_input, plan_arena)? {
            arena.resume();
        } else {
            arena.finish();
        }
        Ok(())
    }
}
