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
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::DropIndex(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for DropIndex {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(DropIndexOperator {
            table_name,
            index_name,
            if_exists,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        let dropped = {
            let (transaction, table_codec) = arena.transaction_codec_mut();
            transaction.drop_index(
                table_codec,
                plan_arena,
                table_name.clone(),
                &index_name,
                if_exists,
            )?
        };
        if let Some((table, index_id)) = dropped {
            arena.push_ddl_apply(DDLApply::upsert_table(table, false));
            arena.push_ddl_apply(DDLApply::RemoveStatisticsMeta {
                table_name: table_name.clone(),
                index_id,
            });
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), index_name.to_string());
        arena.resume();
        Ok(())
    }
}
