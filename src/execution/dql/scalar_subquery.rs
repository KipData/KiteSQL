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
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::value::DataValue;

pub struct ScalarSubquery<'a> {
    input: ExecNode<'a>,
    value_count: usize,
    returned: bool,
}

impl<'a> ReadExecutor<'a> for ScalarSubquery<'a> {
    type Input = (&'a ScalarSubqueryOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (_, input): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let value_count = input.read_schema().len();
        let input = build_read(arena, plan_arena, input);
        ExecNode::ScalarSubquery(Box::new(Self {
            input,
            value_count,
            returned: false,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for ScalarSubquery<'a> {
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
        let has_next = self.input.next_tuple(arena, plan_arena)?;
        if self.returned {
            if has_next {
                return Err(DatabaseError::InvalidValue(
                    "scalar subquery returned more than one row".to_string(),
                ));
            }
            arena.finish();
            return Ok(());
        }
        self.returned = true;

        if !has_next {
            let output = arena.output_tuple_mut();
            output.pk = None;
            output.values.clear();
            output
                .values
                .extend((0..self.value_count).map(|_| DataValue::Null));
            arena.resume();
            return Ok(());
        }

        arena.resume();
        Ok(())
    }
}
