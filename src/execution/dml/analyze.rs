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
use crate::execution::{
    build_read, DDLApply, ExecArena, ExecId, ExecNode, ReadExecutionContext, WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::{table_codec::TableCodec, Transaction};
use crate::types::index::IndexId;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;
use itertools::Itertools;
use std::fmt::{self, Formatter};

const DEFAULT_NUM_OF_BUCKETS: usize = 100;

pub struct Analyze {
    table_name: TableName,
    input_plan: LogicalPlan,
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
            input,
        ): (AnalyzeOperator, LogicalPlan),
    ) -> Self {
        let _ = index_metas;
        Analyze {
            table_name,
            input_plan: input,
            input: None,
            histogram_buckets,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Analyze {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        self.input = Some(build_read(
            arena,
            plan_arena,
            self.input_plan.take(),
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
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let mut builders = {
            let table = arena
                .transaction()
                .table(arena.table_cache(), self.table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            table
                .indexes()
                .map(|index| {
                    let index = plan_arena.index(*index);
                    Ok(State {
                        index_id: index.id,
                        exprs: index.column_exprs(table, plan_arena)?,
                        builder: HistogramBuilder::new(index, None),
                        histogram_buckets: self.histogram_buckets,
                    })
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?
        };

        while arena.next_tuple(input, plan_arena)? {
            let tuple = arena.result_tuple();
            for State { exprs, builder, .. } in builders.iter_mut() {
                let values = Projection::projection(tuple, exprs)?;

                if values.len() == 1 {
                    builder.append(&values[0])?;
                } else {
                    builder.append(&DataValue::Tuple(values, false))?;
                }
            }
        }
        let mut state = arena.local_state(plan_arena);
        let (transaction, table_codec, ddl_apply) = state.write_transaction_codec_ddl_apply_mut();
        let values = Self::persist_statistics_meta(
            &self.table_name,
            builders,
            ddl_apply,
            transaction,
            table_codec,
        )?;

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values = values;
        arena.resume();
        Ok(())
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
        applies: &mut Vec<DDLApply>,
        transaction: &mut U,
        table_codec: &mut TableCodec,
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

            transaction.save_statistics_meta(table_codec, table_name, meta.clone())?;
            applies.push(DDLApply::UpsertStatisticsMeta {
                table_name: table_name.clone(),
                index_id,
                meta,
            });
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
        let indexes = self.index_metas.iter().join(", ");

        write!(f, "Analyze {} -> [{}]", self.table_name, indexes)?;

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::db::{DataBaseBuilder, Database};
    use crate::errors::DatabaseError;
    use crate::execution::dml::analyze::DEFAULT_NUM_OF_BUCKETS;
    use crate::expression::range_detacher::Range;
    use crate::optimizer::core::cm_sketch::COUNT_MIN_SKETCH_STORAGE_PAGE_LEN;
    use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
    use crate::storage::{table_codec::TableCodec, InnerIter, Storage, Transaction};
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

    fn create_table<S: Storage>(
        database: &mut Database<S>,
        sql: &str,
    ) -> Result<(), DatabaseError> {
        database.ddl(sql)
    }

    fn create_index<S: Storage>(
        database: &mut Database<S>,
        sql: &str,
    ) -> Result<(), DatabaseError> {
        database.ddl(sql)
    }

    fn test_statistics_meta_roundtrip() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let buckets = 10;
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path())
            .histogram_buckets(buckets)
            .build_rocksdb()?;

        create_table(&mut kite_sql, "create table t1 (a int primary key, b int)")?;
        create_index(&mut kite_sql, "create index b_index on t1 (b)")?;
        create_index(&mut kite_sql, "create index p_index on t1 (a, b)")?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            kite_sql
                .run(format!("insert into t1 values({i}, {})", i % 20))?
                .done()?;
        }
        kite_sql.analyze("t1")?;

        let table_name = "t1".to_string().into();
        let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());

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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        create_table(&mut kite_sql, "create table t1 (a int primary key, b int)")?;
        create_index(&mut kite_sql, "create index b_index on t1 (b)")?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            kite_sql
                .run(format!("insert into t1 values({i}, {i})"))?
                .done()?;
        }
        kite_sql.analyze("t1")?;

        let table_name = "t1".to_string().into();
        let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_some());
        assert_eq!(
            loader.collect_count(&table_name, 1, &Range::Eq(DataValue::Int32(7)))?,
            Some(1)
        );
        let mut transaction = kite_sql.storage.transaction()?;
        let mut table_codec = TableCodec::default();
        let keys: Vec<Vec<u8>> = table_codec.with_statistics_index_bound("t1", 1, |min, max| {
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

        let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_some());
        assert_eq!(
            loader.collect_count(&table_name, 1, &Range::Eq(DataValue::Int32(7)))?,
            Some(1)
        );

        Ok(())
    }

    fn test_meta_loader_negative_cache() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        create_table(&mut kite_sql, "create table t1 (a int primary key, b int)")?;
        create_index(&mut kite_sql, "create index b_index on t1 (b)")?;

        let table_name = "t1".to_string().into();
        let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_none());

        assert!(!kite_sql.state.meta_cache().contains_key(&(table_name, 1)));

        Ok(())
    }

    fn test_clean_expired_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        create_table(&mut kite_sql, "create table t1 (a int primary key, b int)")?;
        create_index(&mut kite_sql, "create index b_index on t1 (b)")?;
        create_index(&mut kite_sql, "create index p_index on t1 (a, b)")?;

        for i in 0..DEFAULT_NUM_OF_BUCKETS + 1 {
            kite_sql
                .run(format!("insert into t1 values({i}, {i})"))?
                .done()?;
        }
        kite_sql.analyze("t1")?;

        let count = {
            let transaction = kite_sql.storage.transaction()?;
            let mut table_codec = TableCodec::default();
            table_codec.with_statistics_bound("t1", |min, max| {
                let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
                let mut count = 0;
                while iter.try_next()?.is_some() {
                    count += 1;
                }
                Ok(count)
            })?
        };
        assert!(count > 3);

        kite_sql.ddl("alter table t1 drop column b")?;
        kite_sql.analyze("t1")?;

        let transaction = kite_sql.storage.transaction()?;
        let mut table_codec = TableCodec::default();
        let keys = table_codec.with_statistics_bound("t1", |min, max| {
            let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;
            let mut keys = 0;
            while iter.try_next()?.is_some() {
                keys += 1;
            }
            Ok(keys)
        })?;
        let table_name = "t1".to_string().into();
        let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());
        let statistics_meta = loader.load(&table_name, 0)?.unwrap();
        let expected_keys = 1
            + 1
            + statistics_meta
                .sketch()
                .storage_page_count(COUNT_MIN_SKETCH_STORAGE_PAGE_LEN)
            + statistics_meta.histogram().buckets_len();
        assert_eq!(keys, expected_keys);

        Ok(())
    }
}
