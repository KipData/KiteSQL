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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::SchemaRef;
pub struct Filter {
    predicate: ScalarExpression,
    input_schema: SchemaRef,
    input_plan: Option<LogicalPlan>,
    input: ExecId,
}

impl From<(FilterOperator, LogicalPlan)> for Filter {
    fn from((FilterOperator { predicate, .. }, mut input): (FilterOperator, LogicalPlan)) -> Self {
        let input_schema = input.output_schema().clone();
        Filter {
            predicate,
            input_schema,
            input_plan: Some(input),
            input: 0,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Filter {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = build_read(
            arena,
            self.input_plan
                .take()
                .expect("filter input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::Filter(self))
    }
}

impl Filter {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        loop {
            if !arena.next_tuple(self.input)? {
                arena.finish();
                return Ok(());
            };
            let tuple = arena.result_tuple();

            if self
                .predicate
                .eval(Some((tuple, &self.input_schema)))?
                .is_true()?
            {
                arena.resume();
                return Ok(());
            }
        }
    }
}
