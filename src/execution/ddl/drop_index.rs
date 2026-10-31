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
use crate::execution::{DDLApply, ExecBuilder, ExecContext, ExecNode, ExecutorNode, WriteExecutor};
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;

pub struct DropIndex<'a> {
    op: &'a DropIndexOperator,
}

impl<'a> WriteExecutor<'a> for DropIndex<'a> {
    type Input = &'a DropIndexOperator;

    fn into_executor<B: ExecBuilder>(
        op: Self::Input,
        _arena: &mut B,
        _plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        ExecNode::DropIndex(Box::new(DropIndex { op }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for DropIndex<'a> {
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
        let DropIndexOperator {
            table_name,
            index_name,
            if_exists,
        } = self.op;

        let dropped = {
            let (transaction, table_codec) = arena.transaction_codec_mut();
            transaction.drop_index(table_codec, plan_arena, table_name, index_name, *if_exists)?
        };
        if let Some((table, index_id)) = dropped {
            let table_id = table.id();
            arena.push_ddl_apply(DDLApply::upsert_table(table, false));
            arena.push_ddl_apply(DDLApply::RemoveIndexStatistics { table_id, index_id });
        }

        arena.finish();
        Ok(())
    }
}
