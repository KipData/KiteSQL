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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input_plan: Option<LogicalPlan>,
    input: ExecId,
    skipped: usize,
    emitted: usize,
}

impl From<(LimitOperator, LogicalPlan)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, LogicalPlan)) -> Self {
        Limit {
            offset,
            limit,
            input_plan: Some(input),
            input: 0,
            skipped: 0,
            emitted: 0,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Limit {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = build_read(
            arena,
            self.input_plan
                .take()
                .expect("limit input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::Limit(self))
    }
}

impl Limit {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        let offset = self.offset.unwrap_or(0);
        let limit = self.limit.unwrap_or(usize::MAX);

        if limit == 0 || self.emitted >= limit {
            arena.finish();
            return Ok(());
        }

        loop {
            if !arena.next_tuple(self.input)? {
                arena.finish();
                return Ok(());
            }

            if self.skipped < offset {
                self.skipped += 1;
                continue;
            }

            self.emitted += 1;
            arena.resume();
            return Ok(());
        }
    }
}
