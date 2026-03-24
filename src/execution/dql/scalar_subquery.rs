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
use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

pub struct ScalarSubquery {
    input: LogicalPlan,
}

impl From<(ScalarSubqueryOperator, LogicalPlan)> for ScalarSubquery {
    fn from((_, input): (ScalarSubqueryOperator, LogicalPlan)) -> Self {
        Self { input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ScalarSubquery {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let ScalarSubquery { mut input } = self;
            let value_count = input.output_schema().len();
            let mut executor = build_read(input, cache, transaction);

            let first = executor.next();
            let Some(first) = first else {
                co.yield_(Ok(Tuple::new(None, vec![DataValue::Null; value_count])))
                    .await;
                return;
            };
            let first = throw!(co, first);

            if executor.next().is_some() {
                throw!(
                    co,
                    Err(DatabaseError::InvalidValue(
                        "scalar subquery returned more than one row".to_string(),
                    ))
                );
            }

            co.yield_(Ok(first)).await;
        })
    }
}
