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
    DDLApply, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, WriteExecutor,
};
use crate::planner::operator::drop_view::DropViewOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropView {
    op: Option<DropViewOperator>,
}

impl From<DropViewOperator> for DropView {
    fn from(op: DropViewOperator) -> Self {
        DropView { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropView {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::DropView(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for DropView {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        _: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(DropViewOperator {
            view_name,
            if_exists,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        let (transaction, table_codec) = arena.transaction_codec_mut();
        if transaction.drop_view(table_codec, view_name.clone(), if_exists)? {
            arena.push_ddl_apply(DDLApply::DropView {
                name: view_name.clone(),
            });
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), format!("{view_name}"));
        arena.resume();
        Ok(())
    }
}
