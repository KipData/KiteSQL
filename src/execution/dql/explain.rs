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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::{DataValue, Utf8Type};
use sqlparser::ast::CharLengthUnits;

pub struct Explain {
    plan: Option<LogicalPlan>,
}

impl From<LogicalPlan> for Explain {
    fn from(plan: LogicalPlan) -> Self {
        Explain { plan: Some(plan) }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Explain {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::Explain(self))
    }
}

impl Explain {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        let Some(plan) = self.plan.take() else {
            arena.finish();
            return Ok(());
        };

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.push(DataValue::Utf8 {
            value: plan.explain(0),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });

        arena.resume();
        Ok(())
    }
}
