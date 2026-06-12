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
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropIndex {
    op: Option<DropIndexOperator>,
}

impl From<DropIndexOperator> for DropIndex {
    fn from(op: DropIndexOperator) -> Self {
        Self { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropIndex {
    type Input = crate::planner::operator::drop_index::DropIndexOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::DropIndex(Self::from(input)))
    }
}
impl<'a> ExecutorNode<'a> for DropIndex {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(DropIndexOperator {
            table_name,
            index_name,
            if_exists,
        }) = self.op.take()
        else {
            runtime.finish();
            return Ok(());
        };

        let dropped = runtime.transaction_drop_index(
            plan_arena,
            table_name.clone(),
            &index_name,
            if_exists,
        )?;
        if let Some((table, index_id)) = dropped {
            runtime.push_ddl_apply(DDLApply::upsert_table(table, false));
            runtime.push_ddl_apply(DDLApply::RemoveStatisticsMeta {
                table_name: table_name.clone(),
                index_id,
            });
        }

        TupleBuilder::build_result_into(runtime.result_tuple_mut(), index_name.to_string());
        runtime.resume();
        Ok(())
    }
}
