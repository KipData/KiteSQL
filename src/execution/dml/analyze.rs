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
use crate::execution::{
    build_read, DDLApply, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode,
    WriteExecutor,
};
use crate::expression::ScalarExpression;
use crate::iter_ext::Itertools;
use crate::optimizer::core::histogram::{
    HistogramBuilder, HistogramMetaBuilder, DEFAULT_HLL_PRECISION,
    DEFAULT_STATISTICS_RELATIVE_ERROR,
};
use crate::optimizer::core::statistics_meta::IndexStatistics;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::{table_codec::TableCodec, StorageTransaction};
use crate::types::index::IndexId;
use crate::types::tuple::TupleLike;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{CharLengthUnits, ColumnId};
use ordered_float::OrderedFloat;
use std::fmt::{self, Formatter};

const DEFAULT_NUM_OF_BUCKETS: usize = 10;

pub struct Analyze<'a> {
    table_name: &'a TableName,
    input: Option<ExecId>,
    histogram_buckets: Option<usize>,
    hll_precision: Option<u8>,
    statistics_relative_error: Option<OrderedFloat<f64>>,
}

impl<'a, T: StorageTransaction + 'a> WriteExecutor<'a, T> for Analyze<'a> {
    type Input = (&'a AnalyzeOperator, &'a LogicalPlan);

    fn into_executor(
        (
            AnalyzeOperator {
                table_name,
                histogram_buckets,
                hll_precision,
                statistics_relative_error,
                ..
            },
            input_plan,
        ): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = Some(build_read(
            arena,
            plan_arena,
            input_plan,
            cache,
            transaction,
        ));
        arena.push(ExecNode::Analyze(Analyze {
            table_name,
            input,
            histogram_buckets: *histogram_buckets,
            hll_precision: *hll_precision,
            statistics_relative_error: *statistics_relative_error,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for Analyze<'a> {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.input.take() else {
            arena.finish();
            return Ok(());
        };

        let hll_precision = self.hll_precision.unwrap_or(DEFAULT_HLL_PRECISION);
        let relative_error = self
            .statistics_relative_error
            .map_or(DEFAULT_STATISTICS_RELATIVE_ERROR, |error| error.0);
        let (mut columns, mut builders) = {
            let table = arena
                .transaction()
                .table(arena.table_cache(), self.table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let columns = table
                .columns()
                .enumerate()
                .map(|(position, column)| {
                    let column = plan_arena.column(*column);
                    let Some(column_id) = column.id() else {
                        return Err(DatabaseError::invalid_column(column.name()));
                    };
                    Ok((
                        column_id,
                        position,
                        HistogramMetaBuilder::new(hll_precision)?,
                    ))
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?;
            let indexes = table
                .indexes()
                .map(|index| {
                    let index = plan_arena.index(*index);
                    let index_id = index.id;
                    let builder = HistogramBuilder::new(index, relative_error, hll_precision)?;
                    let exprs = index.column_exprs(table).collect::<Result<Vec<_>, _>>()?;
                    Ok(State {
                        index_id,
                        exprs,
                        builder,
                        histogram_buckets: self.histogram_buckets,
                    })
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?;
            (columns, indexes)
        };

        while arena.next_tuple(input, plan_arena)? {
            let tuple = arena.take_tuple();
            for (_, position, builder) in &mut columns {
                builder.append(tuple.value_at(*position));
            }
            for State { exprs, builder, .. } in builders.iter_mut() {
                let value = match exprs.as_slice() {
                    [expr] => expr.eval(plan_arena, Some(&tuple))?.into_owned(),
                    _ => {
                        arena.rewrite(exprs, plan_arena, Some(&tuple))?;
                        DataValue::Tuple(arena.result_mut().take_values())
                    }
                };
                builder.append(value)?;
            }
        }
        let mut state = arena.local_state(plan_arena);
        let (transaction, table_codec, ddl_apply) = state.write_transaction_codec_ddl_apply_mut();
        let values = Self::persist_statistics(
            self.table_name,
            columns,
            builders,
            ddl_apply,
            transaction,
            table_codec,
            plan_arena,
        )?;

        arena.produce(crate::types::tuple::Tuple::new(None, values).into());
        Ok(())
    }
}

struct State {
    index_id: IndexId,
    exprs: Vec<ScalarExpression>,
    builder: HistogramBuilder,
    histogram_buckets: Option<usize>,
}

impl<'a> Analyze<'a> {
    fn persist_statistics<U: StorageTransaction>(
        table_name: &TableName,
        columns: Vec<(ColumnId, usize, HistogramMetaBuilder)>,
        builders: Vec<State>,
        applies: &mut Vec<DDLApply>,
        transaction: &mut U,
        table_codec: &mut TableCodec,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let mut values = Vec::with_capacity(builders.len());
        for (column_id, _, builder) in columns {
            let meta = builder.build();
            transaction.save_column_statistics(
                table_codec,
                table_name,
                column_id,
                &meta,
                plan_arena,
            )?;
            applies.push(DDLApply::UpsertColumnStatistics {
                table_name: table_name.clone(),
                column_id,
                meta,
            });
        }

        for State {
            index_id,
            builder,
            histogram_buckets,
            ..
        } in builders
        {
            let (histogram, sketch, top_n) =
                builder.build(histogram_buckets.unwrap_or(DEFAULT_NUM_OF_BUCKETS))?;
            let meta = IndexStatistics::new(histogram, sketch, top_n);

            transaction.save_index_statistics(table_codec, table_name, meta.clone(), plan_arena)?;
            applies.push(DDLApply::UpsertIndexStatistics {
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

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::db::{CatalogKind, DataBaseBuilder, Database};
    use crate::errors::DatabaseError;
    use crate::execution::dml::analyze::DEFAULT_NUM_OF_BUCKETS;
    use crate::expression::range_detacher::Range;
    use crate::iter_ext::Itertools;
    use crate::optimizer::core::cm_sketch::COUNT_MIN_SKETCH_STORAGE_PAGE_LEN;
    use crate::optimizer::core::histogram::{HistogramMetaBuilder, DEFAULT_HLL_PRECISION};
    use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
    use crate::storage::{table_codec::TableCodec, InnerIter, Storage, StorageTransaction};
    use crate::types::value::DataValue;
    use std::ops::Bound;
    use tempfile::TempDir;

    #[test]
    fn test_analyze() -> Result<(), DatabaseError> {
        test_column_and_index_statistics_roundtrip()?;
        test_meta_loader_uses_cache()?;
        test_meta_loader_negative_cache()?;
        test_clean_expired_index()?;
        test_hll_precision_is_configurable()?;
        test_statistics_relative_error_is_configurable()?;

        Ok(())
    }

    fn test_hll_precision_is_configurable() -> Result<(), DatabaseError> {
        let rows = 1000;
        let ndv_with = |precision: u8| -> Result<usize, DatabaseError> {
            let mut builder = HistogramMetaBuilder::new(precision)?;
            for i in 0..rows {
                builder.append(&DataValue::Int32(i));
            }
            Ok(builder.build().distinct_values_len())
        };
        let (coarse, default) = (ndv_with(4)?, ndv_with(DEFAULT_HLL_PRECISION)?);
        // Precision 4 has 16 registers, so its estimate must differ from the default sketch.
        assert_ne!(coarse, default);

        let values = (0..rows).map(|i| format!("({i})")).join(", ");
        for (builder, expected) in [
            (DataBaseBuilder::path("."), default),
            (DataBaseBuilder::path(".").hll_precision(4), coarse),
        ] {
            let mut kite_sql = builder.build_in_memory()?;
            kite_sql.ddl("create table t1 (a int primary key)")?;
            kite_sql
                .run(format!("insert into t1 values {values}"))?
                .done()?;
            kite_sql.analyze("t1")?;

            let table_name = "t1".to_string().into();
            let a_id = *kite_sql
                .state
                .table_cache()
                .get(&table_name)
                .unwrap()
                .get_column_id_by_name("a")
                .unwrap();
            let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());
            let column = loader.load_column(&table_name, a_id).unwrap();
            assert_eq!(column.distinct_values_len(), expected);
            let index = loader.load(&table_name, 0)?.unwrap();
            assert_eq!(index.histogram().distinct_values_len(), expected);
        }
        Ok(())
    }

    fn test_statistics_relative_error_is_configurable() -> Result<(), DatabaseError> {
        // Count-Min width is next_power_of_two(2 / error): 0.001 -> 2048, 0.01 -> 256,
        // and error_bound(n) = ceil(2n / width).
        for (builder, width) in [
            (DataBaseBuilder::path("."), 2048),
            (
                DataBaseBuilder::path(".").statistics_relative_error(0.01),
                256,
            ),
        ] {
            let mut kite_sql = builder.build_in_memory()?;
            kite_sql.ddl("create table t1 (a int primary key)")?;
            // ANALYZE needs at least DEFAULT_NUM_OF_BUCKETS rows.
            let values = (0..DEFAULT_NUM_OF_BUCKETS)
                .map(|i| format!("({i})"))
                .join(", ");
            kite_sql
                .run(format!("insert into t1 values {values}"))?
                .done()?;
            kite_sql.analyze("t1")?;

            let table_name = "t1".to_string().into();
            let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());
            let index = loader.load(&table_name, 0)?.unwrap();
            assert_eq!(index.sketch().error_bound(width), 2);
        }
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

    fn test_column_and_index_statistics_roundtrip() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let buckets = 10;
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path())
            .histogram_buckets(buckets)
            .build_rocksdb()?;

        create_table(
            &mut kite_sql,
            "create table t1 (a int primary key, b int, c varchar)",
        )?;
        create_index(&mut kite_sql, "create index b_index on t1 (b)")?;
        create_index(&mut kite_sql, "create index p_index on t1 (a, b)")?;

        // Keep this fixture independent of the default histogram bucket count.
        for i in 0..101 {
            kite_sql
                .run(format!("insert into t1 values({i}, {}, NULL)", i % 20))?
                .done()?;
        }
        kite_sql.analyze("t1")?;

        let table_name = "t1".to_string().into();
        let loader = StatisticMetaLoader::new(kite_sql.state.meta_cache());

        let table = kite_sql.state.table_cache().get(&table_name).unwrap();
        let a_id = *table.get_column_id_by_name("a").unwrap();
        let b_id = *table.get_column_id_by_name("b").unwrap();
        let c_id = *table.get_column_id_by_name("c").unwrap();
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
        for (column, distinct) in [(a_id, 101), (b_id, 20)] {
            let meta = loader.load_column(&table_name, column).unwrap();
            assert_eq!(meta.values_len(), 101);
            assert_eq!(meta.distinct_values_len(), distinct);
            assert_eq!(meta.buckets_len(), 0);
        }
        let unindexed = loader.load_column(&table_name, c_id).unwrap();
        assert_eq!(unindexed.values_len(), 0);
        assert_eq!(unindexed.null_count(), 101);
        assert_eq!(unindexed.distinct_values_len(), 0);
        let expected_frequency = loader.collect_count(
            &table_name,
            1,
            &Range::Eq(DataValue::Int32(7)),
            &mut Vec::new(),
        )?;

        // Reload the stored parts instead of reusing ANALYZE's in-memory cache.
        drop(kite_sql);
        let mut reopened = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        reopened.load(CatalogKind::Table(table_name.clone()))?;
        let loader = StatisticMetaLoader::new(reopened.state.meta_cache());
        for index in 0..=2 {
            let meta = loader.load(&table_name, index)?.unwrap();
            assert_eq!(meta.index_id(), index);
            assert_eq!(meta.histogram().values_len(), 101);
            assert_eq!(meta.histogram().buckets_len(), buckets);
        }
        assert_eq!(
            loader.collect_count(
                &table_name,
                1,
                &Range::Eq(DataValue::Int32(7)),
                &mut Vec::new()
            )?,
            expected_frequency
        );
        assert_eq!(
            loader
                .load_column(&table_name, a_id)
                .unwrap()
                .distinct_values_len(),
            101
        );
        assert_eq!(
            loader
                .load_column(&table_name, b_id)
                .unwrap()
                .distinct_values_len(),
            20
        );
        assert_eq!(
            loader.load_column(&table_name, c_id).unwrap().null_count(),
            101
        );
        reopened.ddl("drop index t1.b_index")?;
        let loader = StatisticMetaLoader::new(reopened.state.meta_cache());
        assert!(loader.load(&table_name, 1)?.is_none());
        assert_eq!(
            loader
                .load_column(&table_name, b_id)
                .unwrap()
                .distinct_values_len(),
            20
        );
        let tx = reopened.storage.transaction()?;
        assert!(tx
            .index_statistics(
                &mut TableCodec::default(),
                "t1",
                1,
                reopened.state.table_arena().borrow_mut()
            )?
            .is_none());
        assert_eq!(
            tx.column_statistics(
                &mut TableCodec::default(),
                "t1",
                b_id,
                reopened.state.table_arena().borrow_mut()
            )?
            .unwrap()
            .distinct_values_len(),
            20
        );
        drop(tx);
        reopened.ddl("alter table t1 drop column c")?;
        let tx = reopened.storage.transaction()?;
        assert!(tx
            .column_statistics(
                &mut TableCodec::default(),
                "t1",
                c_id,
                reopened.state.table_arena().borrow_mut()
            )?
            .is_none());
        assert!(tx
            .column_statistics(
                &mut TableCodec::default(),
                "t1",
                a_id,
                reopened.state.table_arena().borrow_mut()
            )?
            .is_some());
        drop(tx);
        let mut iter = reopened.run("select a from t1 where b=7 order by a")?;
        let mut rows = Vec::new();
        while iter
            .next_tuple(|_, row| rows.push(row[0].clone()))?
            .is_some()
        {}
        iter.done()?;
        assert_eq!(rows, [7, 27, 47, 67, 87].map(DataValue::Int32));

        // A missing root must not publish partially reconstructed statistics.
        let mut codec = TableCodec::default();
        let mut tx = reopened.storage.transaction()?;
        codec.with_index_statistics(
            "t1",
            2,
            None,
            reopened.state.table_arena().borrow(),
            |key, _| tx.remove(key),
        )?;
        tx.commit()?;
        drop(reopened);
        let mut reopened = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        assert!(matches!(reopened.load(CatalogKind::Table(table_name)),
            Err(DatabaseError::InvalidValue(message)) if message == "statistics meta is incomplete"));
        assert!(reopened.state.meta_cache().indexes.is_empty());
        assert!(reopened.state.meta_cache().columns.is_empty());
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
            loader
                .collect_count(
                    &table_name,
                    1,
                    &Range::Eq(DataValue::Int32(7)),
                    &mut Vec::new()
                )?
                .map(|(rows, _)| rows),
            Some(1)
        );
        let mut transaction = kite_sql.storage.transaction()?;
        let mut table_codec = TableCodec::default();
        let keys: Vec<Vec<u8>> = table_codec.with_index_statistics_bound("t1", 1, |min, max| {
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
            loader
                .collect_count(
                    &table_name,
                    1,
                    &Range::Eq(DataValue::Int32(7)),
                    &mut Vec::new()
                )?
                .map(|(rows, _)| rows),
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

        assert!(!kite_sql
            .state
            .meta_cache()
            .indexes
            .contains_key(&(table_name, 1)));

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
        let index_statistics = loader.load(&table_name, 0)?.unwrap();
        let expected_keys = 2
            + 1
            + index_statistics
                .sketch()
                .storage_page_count(COUNT_MIN_SKETCH_STORAGE_PAGE_LEN)
            + 1
            + index_statistics.histogram().buckets_len();
        assert_eq!(keys, expected_keys);
        drop(transaction);
        kite_sql.ddl("drop table t1")?;
        assert!(kite_sql.state.meta_cache().columns.is_empty());
        assert!(kite_sql.state.meta_cache().indexes.is_empty());
        let transaction = kite_sql.storage.transaction()?;
        table_codec.with_statistics_bound("t1", |min, max| {
            assert!(transaction
                .range(Bound::Included(min), Bound::Included(max))?
                .try_next()?
                .is_none());
            Ok(())
        })?;
        Ok(())
    }
}
// GRCOV_EXCL_STOP
