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
use crate::execution::dql::aggregate::create_accumulators;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    ReadExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
pub struct SimpleAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    input: ExecId,
    returned: bool,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SimpleAggExecutor {
    type Input = (AggregateOperator, LogicalPlan);

    fn into_executor(
        (AggregateOperator { agg_calls, .. }, input): Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::SimpleAgg(SimpleAggExecutor {
            agg_calls,
            input,
            returned: false,
        }))
    }
}

impl<'a> ExecutorNode<'a> for SimpleAggExecutor {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.returned {
            runtime.finish();
            return Ok(());
        }

        let mut accs = create_accumulators(&self.agg_calls)?;

        while runtime.next_tuple(self.input, plan_arena)? {
            let tuple = runtime.result_tuple();
            for (acc, expr) in accs.iter_mut().zip(self.agg_calls.iter()) {
                let ScalarExpression::AggCall { args, .. } = expr else {
                    unreachable!()
                };
                if args.len() > 1 {
                    return Err(DatabaseError::UnsupportedStmt(
                        "currently aggregate functions only support a single Column as a parameter"
                            .to_string(),
                    ));
                }

                let value = args[0].eval(Some(tuple))?;
                acc.update_value(&value)?;
            }
        }

        let output = runtime.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.reserve(accs.len());
        for acc in accs {
            output.values.push(acc.evaluate()?);
        }
        self.returned = true;
        runtime.resume();
        Ok(())
    }
}
