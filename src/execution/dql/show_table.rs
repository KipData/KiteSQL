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
use crate::storage::{TableIter, Transaction};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;

pub struct ShowTables<'a, T: Transaction + 'a> {
    pub(crate) metas: Option<TableIter<'a, T>>,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ShowTables<'a, T> {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::ShowTables(input))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ShowTables<'a, T> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.metas.is_none() {
            let mut state = arena.local_state(plan_arena);
            let (transaction, table_codec) = state.transaction_codec();
            self.metas = Some(transaction.tables(table_codec)?);
        }

        let Some(table) = self
            .metas
            .as_mut()
            .expect("show tables iterator initialized")
            .try_next(plan_arena)?
        else {
            arena.finish();
            return Ok(());
        };

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values.clear();
        output.values.push(DataValue::Utf8 {
            value: table.table_name.to_string(),
            ty: Utf8Type::Variable(None),
            unit: CharLengthUnits::Characters,
        });

        arena.resume();
        Ok(())
    }
}
