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
use crate::execution::dql::aggregate::{
    prepare_accumulators, update_accumulators, write_aggregate_output, Accumulator,
};
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
pub struct SimpleAggExecutor<'a> {
    agg_calls: &'a [ExprRef],
    input: ExecNode<'a>,
    accs: Vec<Box<dyn Accumulator>>,
    returned: bool,
}

impl<'a> ReadExecutor<'a> for SimpleAggExecutor<'a> {
    type Input = (&'a AggregateOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (AggregateOperator { agg_calls, .. }, input): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input = build_read(arena, plan_arena, input);
        ExecNode::SimpleAgg(Box::new(SimpleAggExecutor {
            agg_calls,
            input,
            accs: Vec::new(),
            returned: false,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for SimpleAggExecutor<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.returned = false;
        self.input.reset(arena, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if self.returned {
            arena.finish();
            return Ok(());
        }

        prepare_accumulators(&mut self.accs, self.agg_calls, plan_arena)?;
        while self.input.next_tuple(arena, plan_arena)? {
            let tuple = arena.result_tuple();
            update_accumulators(&mut self.accs, self.agg_calls, tuple, plan_arena)?;
        }

        write_aggregate_output(arena.output_tuple_mut(), &mut self.accs, [])?;
        self.returned = true;
        arena.resume();
        Ok(())
    }
}
