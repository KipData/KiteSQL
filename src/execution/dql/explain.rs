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
    ExecArena, ExecId, ExecNode, ExecutorNode, ReadExecutionContext, ReadExecutor,
};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;

pub struct Explain {
    plan: LogicalPlan,
    emitted: bool,
}

impl From<LogicalPlan> for Explain {
    fn from(plan: LogicalPlan) -> Self {
        Explain {
            plan,
            emitted: false,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Explain {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::Explain(self))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Explain {
    type Input = LogicalPlan;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::Explain(Explain {
            plan: input,
            emitted: false,
        }))
    }

    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        Explain::next_tuple(self, arena, plan_arena)
    }
}

impl Explain {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.emitted {
            arena.finish();
            return Ok(());
        }

        let plan = self.plan.explain(plan_arena, 0);
        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.push(DataValue::Utf8 {
            value: plan,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });

        self.emitted = true;
        arena.resume();
        Ok(())
    }
}
