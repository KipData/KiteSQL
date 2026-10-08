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
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::Transaction;

pub struct CreateView<'a> {
    op: &'a CreateViewOperator,
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateView<'a> {
    type Input = &'a CreateViewOperator;

    fn into_executor(
        op: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::CreateView(CreateView { op }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CreateView<'a> {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let CreateViewOperator { view, or_replace } = self.op;
        let (transaction, table_codec) = arena.transaction_codec_mut();
        let view = transaction.create_view(table_codec, plan_arena, view.clone(), *or_replace)?;
        arena.push_ddl_apply(DDLApply::upsert_view(view));

        arena.finish();
        Ok(())
    }
}
