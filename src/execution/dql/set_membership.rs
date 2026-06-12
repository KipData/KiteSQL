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
use crate::planner::operator::set_membership::SetMembershipKind;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use ahash::{HashMap, HashMapExt};

pub struct SetMembership {
    kind: SetMembershipKind,
    left_plan: LogicalPlan,
    right_plan: LogicalPlan,
    left_input: ExecId,
    right_input: ExecId,
    right_counts: HashMap<Tuple, usize>,
    built: bool,
}

impl From<(SetMembershipKind, LogicalPlan, LogicalPlan)> for SetMembership {
    fn from(
        (kind, left_input, right_input): (SetMembershipKind, LogicalPlan, LogicalPlan),
    ) -> Self {
        SetMembership {
            kind,
            left_plan: left_input,
            right_plan: right_input,
            left_input: 0,
            right_input: 0,
            right_counts: HashMap::new(),
            built: false,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SetMembership {
    type Input = (SetMembershipKind, LogicalPlan, LogicalPlan);

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
        arena.push(ExecNode::SetMembership(exec))
    }
}

impl<'a> ExecutorNode<'a> for SetMembership {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if !self.built {
            while runtime.next_tuple(self.right_input, plan_arena)? {
                *self
                    .right_counts
                    .entry(runtime.result_tuple().clone())
                    .or_insert(0) += 1;
            }
            self.built = true;
        }

        loop {
            if !runtime.next_tuple(self.left_input, plan_arena)? {
                runtime.finish();
                return Ok(());
            }

            let matched = self.consume_right_match(runtime.result_tuple());
            let should_emit = match self.kind {
                SetMembershipKind::Except => !matched,
                SetMembershipKind::Intersect => matched,
            };

            if should_emit {
                runtime.resume();
                return Ok(());
            }
        }
    }
}

impl SetMembership {
    fn consume_right_match(&mut self, tuple: &Tuple) -> bool {
        if let Some(count) = self.right_counts.get_mut(tuple) {
            if *count > 0 {
                *count -= 1;
                return true;
            }
        }

        false
    }
}
