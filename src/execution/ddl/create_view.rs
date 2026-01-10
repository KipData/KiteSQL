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
use crate::planner::operator::create_view::CreateViewOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct CreateView {
    op: CreateViewOperator,
}

impl From<CreateViewOperator> for CreateView {
    fn from(op: CreateViewOperator) -> Self {
        CreateView { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateView {
    fn execute_mut(
        self,
        (_, view_cache, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let CreateViewOperator { view, or_replace } = self.op;

            let result_tuple = TupleBuilder::build_result(format!("{}", view.name));
            throw!(
                co,
                unsafe { &mut (*transaction) }.create_view(view_cache, view, or_replace)
            );

            co.yield_(Ok(result_tuple)).await;
        })
    }
}
