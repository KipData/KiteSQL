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
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
pub struct Filter {
    predicate: ScalarExpression,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Filter {
    type Input = (FilterOperator, LogicalPlan);

    fn into_executor(
        (FilterOperator { predicate, .. }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::Filter(Filter { predicate, input }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Filter {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            if !arena.next_tuple(self.input, plan_arena)? {
                arena.finish();
                return Ok(());
            };
            let tuple = arena.result_tuple();
            if self.predicate.eval(Some(tuple))?.is_true()? {
                arena.resume();
                return Ok(());
            }
        }
    }
}
