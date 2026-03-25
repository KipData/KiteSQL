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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, Transaction};
use crate::types::index::IndexId;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;
use std::fmt::{self, Formatter};
use std::sync::Arc;

const DEFAULT_NUM_OF_BUCKETS: usize = 100;

pub struct Analyze {
    table_name: TableName,
    input_schema: SchemaRef,
    input_plan: Option<LogicalPlan>,
    input: Option<ExecId>,
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
            mut input,
        ): (AnalyzeOperator, LogicalPlan),
    ) -> Self {
        let _ = index_metas;
        Analyze {
            table_name,
            input_schema: input.output_schema().clone(),
            input_plan: Some(input),
            input: None,
            histogram_buckets,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Analyze {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            self.input_plan
                .take()
                .expect("analyze input plan initialized"),
            cache,
            transaction,
        ));
        arena.push(ExecNode::Analyze(self))
    }
}

impl Analyze {
    pub(crate) fn next_tuple<'a, T: Transaction>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let Some(input) = self.input.take() else {
            return Ok(None);
        };

        let mut builders = Vec::new();
        let table = arena
            .transaction_mut()
            .table(arena.table_cache(), self.table_name.clone())?
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;

        for index in table.indexes() {
            builders.push(State {
                index_id: index.id,
                exprs: index.column_exprs(&table)?,
                builder: HistogramBuilder::new(index, None),
                histogram_buckets: self.histogram_buckets,
            });
        }

        while let Some(tuple) = arena.next_tuple(input)? {
            for State { exprs, builder, .. } in builders.iter_mut() {
                let values = Projection::projection(&tuple, exprs, &self.input_schema)?;

                if values.len() == 1 {
                    builder.append(&values[0])?;
                } else {
                    builder.append(&Arc::new(DataValue::Tuple(values, false)))?;
                }
            }
        }
        let values = Self::persist_statistics_meta(
            &self.table_name,
            builders,
            arena.meta_cache(),
            arena.transaction_mut(),
        )?;

        Ok(Some(Tuple::new(None, values)))
    }
}

struct State {
    index_id: IndexId,
    exprs: Vec<ScalarExpression>,
    builder: HistogramBuilder,
    histogram_buckets: Option<usize>,
}

impl Analyze {
    fn persist_statistics_meta<U: Transaction>(
        table_name: &TableName,
        builders: Vec<State>,
        cache: &StatisticsMetaCache,
        transaction: &mut U,
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
            let meta = StatisticsMeta::new(histogram);

            transaction.save_statistics_meta(cache, table_name, meta, sketch)?;
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
    use crate::expression::range_detacher::Range;
    use crate::optimizer::core::cm_sketch::COUNT_MIN_SKETCH_STORAGE_PAGE_LEN;
    use crate::storage::{InnerIter, Storage, Transaction};
    use crate::types::value::DataValue;
    use std::ops::Bound;
    use tempfile::TempDir;

    #[test]
    fn test_analyze() -> Result<(), DatabaseError> {
        test_statistics_meta_roundtrip()?;
        test_meta_loader_uses_cache()?;
        test_meta_loader_negative_cache()?;
        test_clean_expired_index()?;

        Ok(())
    }

    fn test_statistics_meta_roundtrip() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let buckets = 10;
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .histogram_buckets(buckets)
            .build_rocksdb()?;

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
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

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
        let transaction = kite_sql.storage.transaction()?;
        let loader = transaction.meta_loader(kite_sql.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_some());
        assert_eq!(
            loader.collect_count(&table_name, 1, &Range::Eq(DataValue::Int32(7)))?,
            Some(1)
        );
        drop(transaction);

        let mut transaction = kite_sql.storage.transaction()?;
        let keys: Vec<Vec<u8>> = unsafe { &*transaction.table_codec() }
            .with_statistics_index_bound("t1", 1, |min, max| {
                let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
                let mut keys = Vec::new();
                while let Some((key, _)) = iter.try_next()? {
                    keys.push(key.to_vec());
                }
                Ok(keys)
            })?;
        for key in keys {
            transaction.remove(&key)?;
        }

        let transaction = kite_sql.storage.transaction()?;
        let loader = transaction.meta_loader(kite_sql.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_some());
        assert_eq!(
            loader.collect_count(&table_name, 1, &Range::Eq(DataValue::Int32(7)))?,
            Some(1)
        );

        Ok(())
    }

    fn test_meta_loader_negative_cache() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
        kite_sql.run("create index b_index on t1 (b)")?.done()?;

        let table_name = "t1".to_string().into();
        let transaction = kite_sql.storage.transaction()?;
        let loader = transaction.meta_loader(kite_sql.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_none());

        let entry = kite_sql
            .state
            .meta_cache()
            .get(&(table_name.clone(), 1))
            .expect("missing statistics cache entry");
        assert!(entry.is_none());

        Ok(())
    }

    fn test_clean_expired_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

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
        let count =
            unsafe { &*transaction.table_codec() }.with_statistics_bound("t1", |min, max| {
                let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
                let mut count = 0;
                while iter.try_next()?.is_some() {
                    count += 1;
                }
                Ok(count)
            })?;
        assert!(count > 3);

        kite_sql.run("alter table t1 drop column b")?.done()?;
        kite_sql.run("analyze table t1")?.done()?;

        let transaction = kite_sql.storage.transaction()?;
        let keys =
            unsafe { &*transaction.table_codec() }.with_statistics_bound("t1", |min, max| {
                let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
                let mut keys = 0;
                while iter.try_next()?.is_some() {
                    keys += 1;
                }
                Ok(keys)
            })?;
        let table_name = "t1".to_string().into();
        let loader = transaction.meta_loader(kite_sql.state.meta_cache());
        let statistics_meta = loader.load(&table_name, 0)?.unwrap();
        let statistics_sketch = transaction
            .statistics_sketch(table_name.as_ref(), 0)?
            .unwrap();
        let expected_keys = 1
            + 1
            + statistics_sketch.storage_page_count(COUNT_MIN_SKETCH_STORAGE_PAGE_LEN)
            + statistics_meta.histogram().buckets_len();
        assert_eq!(keys, expected_keys);

        Ok(())
    }
}
