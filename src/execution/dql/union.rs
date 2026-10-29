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
use crate::execution::{build_read, ExecArena, ExecNode, ExecutorNode, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
pub struct Union<'a> {
    left_input: ExecNode<'a>,
    right_input: ExecNode<'a>,
    reading_left: bool,
}

impl<'a, T: StorageTransaction + 'a> ReadExecutor<'a, T> for Union<'a> {
    type Input = (&'a LogicalPlan, &'a LogicalPlan);

    fn into_executor(
        (left_plan, right_plan): Self::Input,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let left_input = build_read(arena, plan_arena, left_plan);
        let right_input = build_read(arena, plan_arena, right_plan);
        ExecNode::Union(Box::new(Union {
            left_input,
            right_input,
            reading_left: true,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Union<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.reading_left = true;
        self.left_input.reset(arena, plan_arena)?;
        self.right_input.reset(arena, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if self.reading_left {
            if self.left_input.next_tuple(arena, plan_arena)? {
                arena.resume();
                return Ok(());
            }
            self.reading_left = false;
        }
        if self.right_input.next_tuple(arena, plan_arena)? {
            arena.resume();
        } else {
            arena.finish();
        }
        Ok(())
    }
}
