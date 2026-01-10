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
pub struct Union {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
}

impl From<(LogicalPlan, LogicalPlan)> for Union {
    fn from((left_input, right_input): (LogicalPlan, LogicalPlan)) -> Self {
        Union {
            left_input,
            right_input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Union {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Union {
                left_input,
                right_input,
            } = self;
            let mut left = build_read(left_input, cache, transaction);

            for tuple in left.by_ref() {
                co.yield_(tuple).await;
            }
            let right = build_read(right_input, cache, transaction);

            for tuple in right {
                co.yield_(tuple).await;
            }
        })
    }
}
