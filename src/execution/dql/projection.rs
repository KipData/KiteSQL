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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
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
    input_plan: Option<LogicalPlan>,
    input: ExecId,
    scratch: Tuple,
}

impl From<(ProjectOperator, LogicalPlan)> for Projection {
    fn from((ProjectOperator { exprs }, mut input): (ProjectOperator, LogicalPlan)) -> Self {
        Projection {
            exprs,
            input_schema: input.output_schema().clone(),
            input_plan: Some(input),
            input: 0,
            scratch: Tuple::default(),
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Projection {
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
                .expect("projection input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::Projection(self))
    }
}

impl Projection {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
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
