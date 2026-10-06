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
use crate::planner::MetaArena;
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::value::DataValue;

pub struct Values<'a> {
    rows: std::slice::Iter<'a, ExprRef>,
    remaining_rows: usize,
    schema_ref: &'a Schema,
}

impl<'a> From<&'a ValuesOperator> for Values<'a> {
    fn from(
        ValuesOperator {
            rows,
            row_count,
            schema_ref,
        }: &'a ValuesOperator,
    ) -> Self {
        Values {
            rows: rows.iter(),
            remaining_rows: *row_count,
            schema_ref,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Values<'a> {
    type Input = &'a ValuesOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = Values::from(input);
        arena.push(ExecNode::Values(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Values<'a> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        if self.remaining_rows == 0 {
            arena.finish();
            return Ok(());
        }
        self.remaining_rows -= 1;
        let width = self.schema_ref.len();

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        for (i, expr) in self.rows.by_ref().take(width).enumerate() {
            let ty = plan_arena.column(self.schema_ref[i]).datatype();
            output.values.push(
                plan_arena
                    .expression(*expr)
                    .eval::<[DataValue]>(plan_arena, None)?
                    .into_owned()
                    .cast(ty)?,
            );
        }

        arena.resume();
        Ok(())
    }
}
