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
use crate::planner::MetaArena;
use crate::storage::Transaction;

pub struct DropView<'a> {
    op: &'a DropViewOperator,
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropView<'a> {
    type Input = &'a DropViewOperator;

    fn into_executor(
        op: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::DropView(DropView { op }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for DropView<'a> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        _: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        let DropViewOperator {
            view_name,
            if_exists,
        } = self.op;

        let (transaction, table_codec) = arena.transaction_codec_mut();
        if transaction.drop_view(table_codec, view_name.clone(), *if_exists)? {
            arena.push_ddl_apply(DDLApply::DropView {
                name: view_name.clone(),
            });
        }

        arena.finish();
        Ok(())
    }
}
