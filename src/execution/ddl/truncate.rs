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
    ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, WriteExecutor,
};
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::MetaArena;
use crate::storage::Transaction;

pub struct Truncate<'a> {
    op: &'a TruncateOperator,
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Truncate<'a> {
    type Input = &'a TruncateOperator;

    fn into_executor(
        op: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut (dyn MetaArena + 'a),
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::Truncate(Truncate { op }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Truncate<'a> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        let TruncateOperator { table_name } = self.op;
        let mut state = arena.local_state(plan_arena);
        let (transaction, table_codec) = state.transaction_codec_mut();
        transaction.drop_data(table_codec, table_name)?;

        arena.finish();
        Ok(())
    }
}
