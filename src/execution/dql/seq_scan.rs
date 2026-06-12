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
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::Transaction;

pub(crate) struct SeqScan {
    op: Option<TableScanOperator>,
    cursor: Option<RuntimeCursorId>,
}

impl From<TableScanOperator> for SeqScan {
    fn from(op: TableScanOperator) -> Self {
        Self {
            op: Some(op),
            cursor: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SeqScan {
    type Input = TableScanOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::SeqScan(SeqScan::from(input)))
    }
}

impl<'a> ExecutorNode<'a> for SeqScan {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if self.cursor.is_none() {
            let Some(op) = self.op.take() else {
                runtime.finish();
                return Ok(());
            };
            self.cursor = Some(runtime.open_seq_scan(plan_arena, op)?);
        }

        if runtime.next_scan_tuple(self.cursor.expect("seq scan cursor initialized"))? {
            runtime.resume();
        } else {
            runtime.finish();
        }
        Ok(())
    }
}
