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
    DDLApply, ExecArena, ExecId, ExecNode, ExecRuntime, ExecutorNode, ReadExecutionContext,
    WriteExecutor,
};
use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropTable {
    op: Option<DropTableOperator>,
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropTable {
    type Input = crate::planner::operator::drop_table::DropTableOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::DropTable(Self::from(input)))
    }
}
impl<'a> ExecutorNode<'a> for DropTable {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        _: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(DropTableOperator {
            table_name,
            if_exists,
        }) = self.op.take()
        else {
            runtime.finish();
            return Ok(());
        };

        if runtime.transaction_drop_table(table_name.clone(), if_exists)? {
            runtime.push_ddl_apply(DDLApply::DropTable {
                name: table_name.clone(),
            });
        }

        TupleBuilder::build_result_into(runtime.result_tuple_mut(), format!("{table_name}"));
        runtime.resume();
        Ok(())
    }
}
