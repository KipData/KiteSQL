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

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, spawn_executor, Executor, WriteExecutor};
use crate::expression::{BindPosition, ScalarExpression};
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::IndexId;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;
use std::borrow::Cow;
use std::fmt::{self, Formatter};
use std::sync::Arc;

const DEFAULT_NUM_OF_BUCKETS: usize = 100;

pub struct Analyze {
    table_name: TableName,
    input: LogicalPlan,
    histogram_buckets: Option<usize>,
}

impl From<(AnalyzeOperator, LogicalPlan)> for Analyze {
    fn from(
        (
            AnalyzeOperator {
                table_name,
                index_metas,
                histogram_buckets,
            },
            input,
        ): (AnalyzeOperator, LogicalPlan),
    ) -> Self {
        let _ = index_metas;
        Analyze {
            table_name,
            input,
            histogram_buckets,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Analyze {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Analyze {
                table_name,
                mut input,
                histogram_buckets,
            } = self;

            let schema = input.output_schema().clone();
            let mut builders = Vec::new();
            let table = throw!(
                co,
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .cloned()
                .ok_or(DatabaseError::TableNotFound)
            );

            for index in table.indexes() {
                builders.push(State {
                    is_bound_position: false,
                    index_id: index.id,
                    exprs: throw!(co, index.column_exprs(&table)),
                    builder: HistogramBuilder::new(index, None),
                    histogram_buckets,
                });
            }

            let mut coroutine = build_read(input, cache, transaction);

            for tuple in coroutine.by_ref() {
                let tuple = throw!(co, tuple);

                for State {
                    is_bound_position,
                    exprs,
                    builder,
                    ..
                } in builders.iter_mut()
                {
                    if !*is_bound_position {
                        throw!(
                            co,
                            BindPosition::bind_exprs(
                                exprs.iter_mut(),
                                || schema.iter().map(Cow::Borrowed),
                                |a, b| a == b
                            )
                        );
                        *is_bound_position = true;
                    }
                    let values = throw!(co, Projection::projection(&tuple, exprs, &schema));

                    if values.len() == 1 {
                        throw!(co, builder.append(&values[0]));
                    } else {
                        throw!(
                            co,
                            builder.append(&Arc::new(DataValue::Tuple(values, false)))
                        );
                    }
                }
            }
            drop(coroutine);
            let values = throw!(
                co,
                Self::persist_statistics_meta(&table_name, builders, cache.2, transaction)
            );

            co.yield_(Ok(Tuple::new(None, values))).await;
        })
    }
}

struct State {
    is_bound_position: bool,
    index_id: IndexId,
    exprs: Vec<ScalarExpression>,
    builder: HistogramBuilder,
    histogram_buckets: Option<usize>,
}

impl Analyze {
    fn persist_statistics_meta<T: Transaction>(
        table_name: &TableName,
        builders: Vec<State>,
        cache: &StatisticsMetaCache,
        transaction: *mut T,
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let mut values = Vec::with_capacity(builders.len());

        for State {
            index_id,
            builder,
            histogram_buckets,
            ..
        } in builders
        {
            let (histogram, sketch) =
                builder.build(histogram_buckets.unwrap_or(DEFAULT_NUM_OF_BUCKETS))?;
            let meta = StatisticsMeta::new(histogram, sketch);

            unsafe { &mut (*transaction) }.save_statistics_meta(cache, table_name, meta)?;
            values.push(DataValue::Utf8 {
                value: format!("{table_name}/{index_id}"),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            });
        }

        Ok(values)
    }
}

impl fmt::Display for AnalyzeOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let indexes = self.index_metas.iter().map(|index| &index.name).join(", ");

        write!(f, "Analyze {} -> [{}]", self.table_name, indexes)?;

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::errors::DatabaseError;
    use crate::execution::dml::analyze::DEFAULT_NUM_OF_BUCKETS;
    use crate::storage::{InnerIter, Storage, Transaction};
    use std::ops::Bound;
    use tempfile::TempDir;

    #[test]
    fn test_analyze() -> Result<(), DatabaseError> {
        test_statistics_meta_roundtrip()?;
        test_meta_loader_uses_cache()?;
        test_clean_expired_index()?;

        Ok(())
    }

    fn test_statistics_meta_roundtrip() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let buckets = 10;
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .histogram_buckets(buckets)
            .build()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
        kite_sql.run("create index b_index on t1 (b)")?.done()?;
        kite_sql.run("create index p_index on t1 (a, b)")?.done()?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            kite_sql
                .run(format!("insert into t1 values({i}, {})", i % 20))?
                .done()?;
        }
        kite_sql.run("analyze table t1")?.done()?;

        let transaction = kite_sql.storage.transaction()?;
        let table_name = "t1".to_string().into();
        let loader = transaction.meta_loader(kite_sql.state.meta_cache());

        let statistics_meta_pk_index = loader.load(&table_name, 0)?.unwrap();
        assert_eq!(statistics_meta_pk_index.index_id(), 0);
        assert_eq!(statistics_meta_pk_index.histogram().values_len(), 101);
        assert_eq!(statistics_meta_pk_index.histogram().buckets_len(), buckets);

        let statistics_meta_b_index = loader.load(&table_name, 1)?.unwrap();
        assert_eq!(statistics_meta_b_index.index_id(), 1);
        assert_eq!(statistics_meta_b_index.histogram().values_len(), 101);
        assert_eq!(statistics_meta_b_index.histogram().buckets_len(), buckets);

        let statistics_meta_p_index = loader.load(&table_name, 2)?.unwrap();
        assert_eq!(statistics_meta_p_index.index_id(), 2);
        assert_eq!(statistics_meta_p_index.histogram().values_len(), 101);
        assert_eq!(statistics_meta_p_index.histogram().buckets_len(), buckets);

        Ok(())
    }

    fn test_meta_loader_uses_cache() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
        kite_sql.run("create index b_index on t1 (b)")?.done()?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            kite_sql
                .run(format!("insert into t1 values({i}, {i})"))?
                .done()?;
        }
        kite_sql.run("analyze table t1")?.done()?;

        let table_name = "t1".to_string().into();
        let mut transaction = kite_sql.storage.transaction()?;
        assert!(transaction
            .meta_loader(kite_sql.state.meta_cache())
            .load(&table_name, 1)?
            .is_some());

        let (min, max) = unsafe { &*transaction.table_codec() }.statistics_index_bound("t1", 1);
        let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        while let Some((key, _)) = iter.try_next()? {
            keys.push(key);
        }
        drop(iter);
        for key in keys {
            transaction.remove(&key)?;
        }

        assert!(transaction
            .meta_loader(kite_sql.state.meta_cache())
            .load(&table_name, 1)?
            .is_some());

        Ok(())
    }

    fn test_clean_expired_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
        kite_sql.run("create index b_index on t1 (b)")?.done()?;
        kite_sql.run("create index p_index on t1 (a, b)")?.done()?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            kite_sql
                .run(format!("insert into t1 values({i}, {i})"))?
                .done()?;
        }
        kite_sql.run("analyze table t1")?.done()?;

        let transaction = kite_sql.storage.transaction()?;
        let (min, max) = unsafe { &*transaction.table_codec() }.statistics_bound("t1");
        let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
        let mut count = 0;
        while iter.try_next()?.is_some() {
            count += 1;
        }
        assert!(count > 3);
        drop(iter);

        kite_sql.run("alter table t1 drop column b")?.done()?;
        kite_sql.run("analyze table t1")?.done()?;

        let transaction = kite_sql.storage.transaction()?;
        let (min, max) = unsafe { &*transaction.table_codec() }.statistics_bound("t1");
        let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
        let mut keys = 0;
        while iter.try_next()?.is_some() {
            keys += 1;
        }
        assert_eq!(keys, 1 + DEFAULT_NUM_OF_BUCKETS.min(DEFAULT_NUM_OF_BUCKETS));

        Ok(())
    }
}
