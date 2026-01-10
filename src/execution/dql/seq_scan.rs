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

use crate::execution::{spawn_executor, Executor, ReadExecutor};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{Iter, StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;

pub(crate) struct SeqScan {
    op: TableScanOperator,
}

impl From<TableScanOperator> for SeqScan {
    fn from(op: TableScanOperator) -> Self {
        SeqScan { op }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SeqScan {
    fn execute(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let TableScanOperator {
                table_name,
                columns,
                limit,
                with_pk,
                ..
            } = self.op;

            let mut iter = throw!(
                co,
                unsafe { &mut (*transaction) }.read(
                    table_cache,
                    table_name,
                    limit,
                    columns,
                    with_pk
                )
            );

            while let Some(tuple) = throw!(co, iter.next_tuple()) {
                co.yield_(Ok(tuple)).await;
            }
        })
    }
}
