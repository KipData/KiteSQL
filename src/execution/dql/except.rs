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

use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use ahash::{HashSet, HashSetExt};
pub struct Except {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(LogicalPlan, LogicalPlan)> for Except {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Except {
            left_input,
            right_input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Except {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Except {
                left_input,
                right_input,
            } = self;

            let mut coroutine = build_read(right_input, cache, transaction);

            let mut except_col = HashSet::new();

            for tuple in coroutine.by_ref() {
                let tuple = throw!(co, tuple);
                except_col.insert(tuple);
            }

            let coroutine = build_read(left_input, cache, transaction);

            for tuple in coroutine {
                let tuple = throw!(co, tuple);
                if !except_col.contains(&tuple) {
                    co.yield_(Ok(tuple)).await;
                }
            }
        })
    }
}
