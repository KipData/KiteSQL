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
use crate::planner::operator::project::ProjectOperator;
use crate::planner::MetaArena;
use crate::planner::{ExprRef, LogicalPlan};
use crate::storage::Transaction;

pub struct Projection {
    exprs: Vec<ExprRef>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Projection {
    type Input = (ProjectOperator, LogicalPlan);

    fn into_executor(
        (ProjectOperator { exprs }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::Projection(Projection { exprs, input }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Projection {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        if !arena.next_tuple(self.input, plan_arena)? {
            arena.finish();
            return Ok(());
        }

        arena.rewrite(&self.exprs, plan_arena, None)?;
        arena.resume();
        Ok(())
    }
}
