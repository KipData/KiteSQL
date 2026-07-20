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
#[cfg(not(target_arch = "wasm32"))]
use crate::execution::spill::{SpillReader, SpillVec};
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor};
use crate::planner::operator::values::ValuesOperator;
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::value::DataValue;
use std::mem;

pub struct Values {
    #[cfg(target_arch = "wasm32")]
    rows: std::vec::IntoIter<Vec<DataValue>>,
    #[cfg(not(target_arch = "wasm32"))]
    rows: SpillReader<Vec<DataValue>>,
    schema_ref: Schema,
}

impl From<ValuesOperator> for Values {
    fn from(ValuesOperator { rows, schema_ref }: ValuesOperator) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let rows = SpillVec::from(rows).into_iter();
        #[cfg(target_arch = "wasm32")]
        let rows = rows.into_iter();

        Values { rows, schema_ref }
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
        #[cfg(not(target_arch = "wasm32"))]
        let next_row = self.rows.next().transpose()?;
        #[cfg(target_arch = "wasm32")]
        let next_row = self.rows.next();

        let Some(mut values) = next_row else {
            arena.finish();
            return Ok(());
        };

        for (i, value) in values.iter_mut().enumerate() {
            let ty = plan_arena.column(self.schema_ref[i]).datatype();

            *value = mem::replace(value, DataValue::Null).cast(ty)?;
        }

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values = values;
        arena.resume();
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn large_values_are_read_back_from_spill_in_order() -> Result<(), DatabaseError> {
        let rows = (0..=1024)
            .map(|value| vec![DataValue::Int32(value as i32)])
            .collect::<Vec<_>>();
        let mut values = Values::from(ValuesOperator {
            rows: rows.clone(),
            schema_ref: Vec::new(),
        });
        let mut restored = Vec::new();
        while let Some(row) = values.rows.next().transpose()? {
            restored.push(row);
        }

        assert_eq!(restored, rows);
        Ok(())
    }
}
