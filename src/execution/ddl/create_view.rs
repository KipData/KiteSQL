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
use crate::planner::operator::create_view::CreateViewOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct CreateView {
    op: Option<CreateViewOperator>,
}

impl From<CreateViewOperator> for CreateView {
    fn from(op: CreateViewOperator) -> Self {
        CreateView { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateView {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::CreateView(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CreateView {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(CreateViewOperator { view, or_replace }) = self.op.take() else {
            arena.finish();
            return Ok(());
        };
        let view_name = view.name.to_string();
        let (transaction, table_codec) = arena.transaction_codec_mut();
        let view = transaction.create_view(table_codec, plan_arena, view, or_replace)?;
        arena.push_ddl_apply(DDLApply::upsert_view(view));

        TupleBuilder::build_result_into(arena.result_tuple_mut(), view_name);
        arena.resume();
        Ok(())
    }
}
