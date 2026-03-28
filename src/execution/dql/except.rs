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
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use ahash::{HashMap, HashMapExt};
pub struct Except {
    left_plan: Option<LogicalPlan>,
    right_plan: Option<LogicalPlan>,
    left_input: ExecId,
    right_input: ExecId,
    except_col: HashMap<Tuple, usize>,
    built: bool,
}

impl From<(LogicalPlan, LogicalPlan)> for Except {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Except {
            left_plan: Some(left_input),
            right_plan: Some(right_input),
            left_input: 0,
            right_input: 0,
            except_col: HashMap::new(),
            built: false,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Except {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.left_input = build_read(
            arena,
            self.left_plan
                .take()
                .expect("except left input plan initialized"),
            cache,
            transaction,
        );
        self.right_input = build_read(
            arena,
            self.right_plan
                .take()
                .expect("except right input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::Except(self))
    }
}

impl Except {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        if !self.built {
            while arena.next_tuple(self.right_input)? {
                *self
                    .except_col
                    .entry(arena.result_tuple().clone())
                    .or_insert(0) += 1;
            }
            self.built = true;
        }

        loop {
            if !arena.next_tuple(self.left_input)? {
                arena.finish();
                return Ok(());
            }
            let tuple = arena.result_tuple();

            if let Some(count) = self.except_col.get_mut(tuple) {
                if *count > 0 {
                    *count -= 1;
                    continue;
                }
            }

            arena.resume();
            return Ok(());
        }
    }
}
