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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode};
use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;

pub struct ScalarSubquery {
    input: ExecId,
    value_count: usize,
    returned: bool,
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ScalarSubquery {
    type Input = (ScalarSubqueryOperator, LogicalPlan);

    fn into_executor(
        (_, mut input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        let value_count = input.output_schema().len();
        let input = build_read(arena, input, cache, transaction);
        arena.push(ExecNode::ScalarSubquery(Self {
            input,
            value_count,
            returned: false,
        }))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        if self.returned {
            arena.finish();
            return Ok(());
        }
        self.returned = true;

        let has_first = arena.next_tuple(self.input)?;
        if !has_first {
            let output = arena.result_tuple_mut();
            output.pk = None;
            output.values.clear();
            output
                .values
                .extend((0..self.value_count).map(|_| DataValue::Null));
            arena.resume();
            return Ok(());
        }

        if arena.next_tuple(self.input)? {
            return Err(DatabaseError::InvalidValue(
                "scalar subquery returned more than one row".to_string(),
            ));
        }

        arena.resume();
        Ok(())
    }
}
