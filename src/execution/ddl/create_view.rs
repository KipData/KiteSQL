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
use crate::execution::{ExecArena, ExecId, ExecNode, ReadExecutionContext, WriteExecutor};
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
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::CreateView(self))
    }
}

impl CreateView {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        let Some(CreateViewOperator { view, or_replace }) = self.op.take() else {
            arena.finish();
            return Ok(());
        };
        let view_name = view.name.to_string();
        let (transaction, context) = arena.write_context_mut();
        let view_cache = context.view_cache_mut();
        transaction.create_view(view_cache, view, or_replace)?;

        TupleBuilder::build_result_into(arena.result_tuple_mut(), view_name);
        arena.resume();
        Ok(())
    }
}
