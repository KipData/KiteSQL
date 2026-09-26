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
use crate::types::tuple::{Schema, Tuple};

pub struct Values {
    rows: std::vec::IntoIter<ExprRef>,
    remaining_rows: usize,
    schema_ref: Schema,
}

impl From<ValuesOperator> for Values {
    fn from(
        ValuesOperator {
            rows,
            row_count,
            schema_ref,
        }: ValuesOperator,
    ) -> Self {
        Values {
            rows: rows.into_iter(),
            remaining_rows: row_count,
            schema_ref,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Values {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
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
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        if self.remaining_rows == 0 {
            arena.finish();
            return Ok(());
        }
        self.remaining_rows -= 1;
        let width = self.schema_ref.len();

        let mut output = Tuple::new(None, Vec::with_capacity(width));
        for (i, expr) in self.rows.by_ref().take(width).enumerate() {
            let ty = plan_arena.column(self.schema_ref[i]).datatype();
            output.values.push(
                plan_arena
                    .expression(expr)
                    .eval(plan_arena, None)?
                    .into_owned()
                    .cast(ty)?,
            );
        }

        arena.produce_tuple(output);
        Ok(())
    }
}
