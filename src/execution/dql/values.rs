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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor};
use crate::planner::operator::values::ValuesOperator;
use crate::planner::ExprRef;
use crate::storage::Transaction;
use crate::types::tuple::Schema;

pub struct Values {
    rows: std::vec::IntoIter<Vec<ExprRef>>,
    schema_ref: Schema,
}

impl From<ValuesOperator> for Values {
    fn from(ValuesOperator { rows, schema_ref }: ValuesOperator) -> Self {
        Values {
            rows: rows.into_iter(),
            schema_ref,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Values {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::Values(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Values {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(expressions) = self.rows.next() else {
            arena.finish();
            return Ok(());
        };

        let mut values = Vec::with_capacity(expressions.len());
        for (i, expr) in expressions.into_iter().enumerate() {
            let ty = plan_arena.column(self.schema_ref[i]).datatype();
            values.push(
                plan_arena
                    .expression(expr)
                    .eval::<&crate::types::tuple::Tuple>(plan_arena, None)?
                    .cast(ty)?,
            );
        }

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values = values;
        arena.resume();
        Ok(())
    }
}
