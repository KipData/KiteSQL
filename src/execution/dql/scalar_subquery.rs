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
use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct ScalarSubquery {
    input_plan: Option<LogicalPlan>,
    input: Option<ExecId>,
    value_count: usize,
}

impl From<(ScalarSubqueryOperator, LogicalPlan)> for ScalarSubquery {
    fn from((_, mut input): (ScalarSubqueryOperator, LogicalPlan)) -> Self {
        Self {
            value_count: input.output_schema().len(),
            input_plan: Some(input),
            input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ScalarSubquery {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            self.input_plan
                .take()
                .expect("scalar subquery input plan initialized"),
            cache,
            transaction,
        ));
        arena.push(ExecNode::ScalarSubquery(self))
    }
}

impl ScalarSubquery {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let Some(input) = self.input.take() else {
            return Ok(None);
        };

        let first = arena.next_tuple(input)?;
        let Some(first) = first else {
            return Ok(Some(Tuple::new(
                None,
                vec![DataValue::Null; self.value_count],
            )));
        };

        if arena.next_tuple(input)?.is_some() {
            return Err(DatabaseError::InvalidValue(
                "scalar subquery returned more than one row".to_string(),
            ));
        }

        Ok(Some(first))
    }
}
