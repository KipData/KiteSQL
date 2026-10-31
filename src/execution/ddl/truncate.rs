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
use crate::execution::{ExecBuilder, ExecContext, ExecNode, ExecutorNode, WriteExecutor};
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;

pub struct Truncate<'a> {
    op: &'a TruncateOperator,
}

impl<'a> WriteExecutor<'a> for Truncate<'a> {
    type Input = &'a TruncateOperator;

    fn into_executor<B: ExecBuilder>(
        op: Self::Input,
        _arena: &mut B,
        _plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        ExecNode::Truncate(Box::new(Truncate { op }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Truncate<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        _arena: &mut ExecContext<'_, 'a, '_, T>,
        _plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let TruncateOperator { table_name } = self.op;
        let mut state = arena.local_state(plan_arena);
        let (transaction, table_codec) = state.transaction_codec_mut();
        transaction.drop_data(table_codec, table_name)?;

        arena.finish();
        Ok(())
    }
}
