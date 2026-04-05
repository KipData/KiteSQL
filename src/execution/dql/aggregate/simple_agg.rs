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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
pub struct SimpleAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    input: ExecId,
    returned: bool,
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for SimpleAggExecutor {
    type Input = (AggregateOperator, LogicalPlan);

    fn into_executor(
        (AggregateOperator { agg_calls, .. }, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        let input = build_read(arena, input, cache, transaction);
        arena.push(ExecNode::SimpleAgg(SimpleAggExecutor {
            agg_calls,
            input,
            returned: false,
        }))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        if self.returned {
            arena.finish();
            return Ok(());
        }

        let mut accs = create_accumulators(&self.agg_calls)?;

        while arena.next_tuple(self.input)? {
            let tuple = arena.result_tuple();
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

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.reserve(accs.len());
        for acc in accs {
            output.values.push(acc.evaluate()?);
        }
        self.returned = true;
        arena.resume();
        Ok(())
    }
}
