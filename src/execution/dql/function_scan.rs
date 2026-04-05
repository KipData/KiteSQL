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
use crate::execution::{ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode, ReadExecutor};
use crate::expression::function::table::TableFunction;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

pub struct FunctionScan {
    table_function: TableFunction,
    iter: Option<Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>>,
}

impl From<FunctionScanOperator> for FunctionScan {
    fn from(op: FunctionScanOperator) -> Self {
        FunctionScan {
            table_function: op.table_function,
            iter: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for FunctionScan {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::FunctionScan(self))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for FunctionScan {
    type Input = FunctionScanOperator;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::FunctionScan(FunctionScan::from(input)))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        FunctionScan::next_tuple(self, arena)
    }
}

impl FunctionScan {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        if self.iter.is_none() {
            let TableFunction { args, inner } = &self.table_function;
            self.iter = Some(inner.eval(args)?);
        }

        let tuple = self.iter.as_mut().and_then(Iterator::next).transpose()?;
        let Some(tuple) = tuple else {
            arena.finish();
            return Ok(());
        };
        arena.produce_tuple(tuple);
        Ok(())
    }
}
