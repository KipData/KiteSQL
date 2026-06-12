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
    ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext, ReadExecutor,
};
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::value::DataValue;
use std::mem;

pub struct Values {
    rows: std::vec::IntoIter<Vec<DataValue>>,
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
    type Input = ValuesOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::Values(Values::from(input)))
    }
}

impl<'a> ExecutorNode<'a> for Values {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(mut values) = self.rows.next() else {
            runtime.finish();
            return Ok(());
        };

        for (i, value) in values.iter_mut().enumerate() {
            let ty = plan_arena.column(self.schema_ref[i]).datatype();

            *value = mem::replace(value, DataValue::Null).cast(&ty)?;
        }

        let output = runtime.result_tuple_mut();
        output.pk = None;
        output.values = values;
        runtime.resume();
        Ok(())
    }
}
