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

use crate::execution::{spawn_executor, Executor, WriteExecutor};
use crate::planner::operator::drop_view::DropViewOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropView {
    op: DropViewOperator,
}

impl From<DropViewOperator> for DropView {
    fn from(op: DropViewOperator) -> Self {
        DropView { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropView {
    fn execute_mut(
        self,
        (table_cache, view_cache, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let DropViewOperator {
                view_name,
                if_exists,
            } = self.op;

            throw!(
                co,
                unsafe { &mut (*transaction) }.drop_view(
                    view_cache,
                    table_cache,
                    view_name.clone(),
                    if_exists
                )
            );

            co.yield_(Ok(TupleBuilder::build_result(format!("{view_name}"))))
                .await;
        })
    }
}
