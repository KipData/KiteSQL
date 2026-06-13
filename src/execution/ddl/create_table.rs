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
use crate::planner::operator::create_table::CreateTableOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;

pub struct CreateTable {
    op: Option<CreateTableOperator>,
}

impl From<CreateTableOperator> for CreateTable {
    fn from(op: CreateTableOperator) -> Self {
        CreateTable { op: Some(op) }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateTable {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut crate::planner::PlanArena<'a>,
        _: ExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        let executor = input;
        arena.push(ExecNode::CreateTable(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for CreateTable {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(CreateTableOperator {
            table_name,
            columns,
            if_not_exists,
        }) = self.op.take()
        else {
            arena.finish();
            return Ok(());
        };

        let (transaction, table_codec) = arena.transaction_codec_mut();
        let table = transaction.create_table(
            table_codec,
            plan_arena,
            table_name.clone(),
            columns,
            if_not_exists,
        )?;
        if let Some(table) = table {
            arena.push_ddl_apply(DDLApply::upsert_table(table, false));
        }

        TupleBuilder::build_result_into(arena.result_tuple_mut(), format!("{table_name}"));
        arena.resume();
        Ok(())
    }
}
