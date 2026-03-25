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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use itertools::Itertools;

pub struct SimpleAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    input_schema: SchemaRef,
    input_plan: Option<LogicalPlan>,
    input: Option<ExecId>,
}

impl From<(AggregateOperator, LogicalPlan)> for SimpleAggExecutor {
    fn from(
        (AggregateOperator { agg_calls, .. }, mut input): (AggregateOperator, LogicalPlan),
    ) -> Self {
        SimpleAggExecutor {
            agg_calls,
            input_schema: input.output_schema().clone(),
            input_plan: Some(input),
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SimpleAggExecutor {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            self.input_plan
                .take()
                .expect("simple aggregate input plan initialized"),
            cache,
            transaction,
        ));
        arena.push(ExecNode::SimpleAgg(self))
    }
}

impl SimpleAggExecutor {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let Some(input) = self.input.take() else {
            return Ok(None);
        };

        let mut accs = create_accumulators(&self.agg_calls)?;

        while let Some(tuple) = arena.next_tuple(input)? {
            let values: Vec<DataValue> = self
                .agg_calls
                .iter()
                .map(|expr| match expr {
                    ScalarExpression::AggCall { args, .. } => {
                        args[0].eval(Some((&tuple, &self.input_schema)))
                    }
                    _ => unreachable!(),
                })
                .try_collect()?;

            for (acc, value) in accs.iter_mut().zip(values.iter()) {
                acc.update_value(value)?;
            }
        }

        let values = accs.into_iter().map(|acc| acc.evaluate()).try_collect()?;
        Ok(Some(Tuple::new(None, values)))
    }
}
