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
    RuntimeCursorId,
};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;

pub struct ShowTables {
    pub(crate) cursor: Option<RuntimeCursorId>,
}

impl<'a, T: crate::storage::Transaction + 'a> ReadExecutor<'a, T> for ShowTables {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::ShowTables(input))
    }
}

impl<'a> ExecutorNode<'a> for ShowTables {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        _: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.cursor.is_none() {
            self.cursor = Some(runtime.open_table_iter()?);
        }

        let Some(table_name) =
            runtime.next_table_name(self.cursor.expect("table cursor initialized"))?
        else {
            runtime.finish();
            return Ok(());
        };

        let output = runtime.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.push(DataValue::Utf8 {
            value: table_name,
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });

        runtime.resume();
        Ok(())
    }
}
