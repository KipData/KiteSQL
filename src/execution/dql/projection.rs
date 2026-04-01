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

use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode};
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
pub struct Projection {
    exprs: Vec<ScalarExpression>,
    input_schema: SchemaRef,
    input: ExecId,
    scratch: Tuple,
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Projection {
    type Input = (ProjectOperator, LogicalPlan);

    fn into_executor(
        (ProjectOperator { exprs }, mut input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        let input_schema = input.output_schema().clone();
        let input = build_read(arena, input, cache, transaction);
        arena.push(ExecNode::Projection(Projection {
            exprs,
            input_schema,
            input,
            scratch: Tuple::default(),
        }))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        if !arena.next_tuple(self.input)? {
            arena.finish();
            return Ok(());
        }

        std::mem::swap(&mut self.scratch, arena.result_tuple_mut());
        let tuple = &self.scratch;
        let output = arena.result_tuple_mut();
        output.pk.clone_from(&tuple.pk);
        output.values.clear();
        output.values.reserve(self.exprs.len());
        for expr in self.exprs.iter() {
            output
                .values
                .push(expr.eval(Some((tuple, &self.input_schema)))?);
        }
        arena.resume();
        Ok(())
    }
}

impl Projection {
    pub fn projection(
        tuple: &Tuple,
        exprs: &[ScalarExpression],
        schema: &[ColumnRef],
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let mut values = Vec::with_capacity(exprs.len());

        for expr in exprs.iter() {
            values.push(expr.eval(Some((tuple, schema)))?);
        }
        Ok(values)
    }
}
