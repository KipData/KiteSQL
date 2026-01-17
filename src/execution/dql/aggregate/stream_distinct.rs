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
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use itertools::Itertools;

pub struct StreamDistinctExecutor {
    groupby_exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(AggregateOperator, LogicalPlan)> for StreamDistinctExecutor {
    fn from((op, input): (AggregateOperator, LogicalPlan)) -> Self {
        StreamDistinctExecutor {
            groupby_exprs: op.groupby_exprs,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for StreamDistinctExecutor {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let StreamDistinctExecutor {
                groupby_exprs,
                mut input,
            } = self;

            let schema_ref = input.output_schema().clone();
            let mut executor = build_read(input, cache, transaction);
            let mut last_keys: Option<Vec<DataValue>> = None;

            for result in executor.by_ref() {
                let tuple = throw!(co, result);
                let group_keys: Vec<DataValue> = throw!(
                    co,
                    groupby_exprs
                        .iter()
                        .map(|expr| expr.eval(Some((&tuple, &schema_ref))))
                        .try_collect()
                );

                if last_keys.as_ref() != Some(&group_keys) {
                    last_keys = Some(group_keys.clone());
                    co.yield_(Ok(Tuple::new(None, group_keys))).await;
                }
            }
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::aggregate::stream_distinct::StreamDistinctExecutor;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[allow(clippy::type_complexity)]
    fn build_test_storage() -> Result<
        (
            Arc<TableCache>,
            Arc<ViewCache>,
            Arc<StatisticsMetaCache>,
            TempDir,
            RocksStorage,
        ),
        DatabaseError,
    > {
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;

        Ok((table_cache, view_cache, meta_cache, temp_dir, storage))
    }

    #[test]
    fn stream_distinct_single_column_sorted() -> Result<(), DatabaseError> {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let schema_ref = Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
            "c1".to_string(),
            true,
            desc,
        ))]);

        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
                    vec![DataValue::Int32(1)],
                    vec![DataValue::Int32(1)],
                    vec![DataValue::Int32(2)],
                    vec![DataValue::Int32(2)],
                    vec![DataValue::Int32(3)],
                ],
                schema_ref: schema_ref.clone(),
            }),
            Childrens::None,
        );
        let agg = AggregateOperator {
            groupby_exprs: vec![ScalarExpression::column_expr(schema_ref[0].clone())],
            agg_calls: vec![],
            is_distinct: true,
        };

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(
            StreamDistinctExecutor::from((agg, input))
                .execute((&table_cache, &view_cache, &meta_cache), &mut transaction),
        )?;

        let actual = tuples
            .into_iter()
            .flat_map(|tuple| tuple.values)
            .flat_map(|value| value.i32())
            .collect_vec();
        assert_eq!(actual, vec![1, 2, 3]);

        Ok(())
    }

    #[test]
    fn stream_distinct_multi_column_sorted() -> Result<(), DatabaseError> {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let schema_ref = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c2".to_string(), true, desc)),
        ]);

        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
                    vec![DataValue::Int32(1), DataValue::Int32(1)],
                    vec![DataValue::Int32(1), DataValue::Int32(1)],
                    vec![DataValue::Int32(1), DataValue::Int32(2)],
                    vec![DataValue::Int32(2), DataValue::Int32(1)],
                    vec![DataValue::Int32(2), DataValue::Int32(1)],
                ],
                schema_ref: schema_ref.clone(),
            }),
            Childrens::None,
        );
        let agg = AggregateOperator {
            groupby_exprs: vec![
                ScalarExpression::column_expr(schema_ref[0].clone()),
                ScalarExpression::column_expr(schema_ref[1].clone()),
            ],
            agg_calls: vec![],
            is_distinct: true,
        };

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(
            StreamDistinctExecutor::from((agg, input))
                .execute((&table_cache, &view_cache, &meta_cache), &mut transaction),
        )?;

        let actual = tuples
            .into_iter()
            .map(|tuple| {
                tuple
                    .values
                    .into_iter()
                    .flat_map(|value| value.i32())
                    .collect_vec()
            })
            .collect_vec();
        assert_eq!(actual, vec![vec![1, 1], vec![1, 2], vec![2, 1]]);

        Ok(())
    }
}
