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
use crate::paths::require_statistics_base_dir;
#[cfg(target_arch = "wasm32")]
use crate::paths::{wasm_remove_storage_key, wasm_set_storage_item, wasm_storage_keys_with_prefix};
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::{IndexId, IndexMetaRef};
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;
use std::borrow::Cow;
use std::collections::HashSet;
#[cfg(not(target_arch = "wasm32"))]
use std::ffi::OsStr;
use std::fmt::{self, Formatter};
#[cfg(not(target_arch = "wasm32"))]
use std::fs::{self, DirEntry};
#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
use std::sync::Arc;

const DEFAULT_NUM_OF_BUCKETS: usize = 100;

pub struct Analyze {
    table_name: TableName,
    input: LogicalPlan,
    index_metas: Vec<IndexMetaRef>,
}

impl From<(AnalyzeOperator, LogicalPlan)> for Analyze {
    fn from(
        (
            AnalyzeOperator {
                table_name,
                index_metas,
            },
            input,
        ): (AnalyzeOperator, LogicalPlan),
    ) -> Self {
        Analyze {
            table_name,
            input,
            index_metas,
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
                index_metas,
            } = self;

            let schema = input.output_schema().clone();
            let mut builders = Vec::with_capacity(index_metas.len());
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
            #[cfg(target_arch = "wasm32")]
            let values = throw!(
                co,
                Self::persist_statistics_meta_wasm(&table_name, builders, cache.2, transaction)
            );

            #[cfg(not(target_arch = "wasm32"))]
            let values = throw!(
                co,
                Self::persist_statistics_meta_native(&table_name, builders, cache.2, transaction)
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
}

impl Analyze {
    #[cfg(not(target_arch = "wasm32"))]
    fn persist_statistics_meta_native<T: Transaction>(
        table_name: &TableName,
        builders: Vec<State>,
        cache: &StatisticsMetaCache,
        transaction: *mut T,
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let dir_path = Self::build_statistics_meta_path(table_name)?;
        fs::create_dir_all(&dir_path).map_err(DatabaseError::IO)?;

        let mut values = Vec::with_capacity(builders.len());
        let mut active_index_paths = HashSet::new();

        for State {
            index_id, builder, ..
        } in builders
        {
            let index_file = OsStr::new(&index_id.to_string()).to_os_string();
            let path = dir_path.join(&index_file);
            let temp_path = path.with_extension("tmp");
            let path_str: String = path.to_string_lossy().into();

            let (histogram, sketch) = builder.build(DEFAULT_NUM_OF_BUCKETS)?;
            let meta = StatisticsMeta::new(histogram, sketch);

            meta.to_file(&temp_path)?;
            values.push(DataValue::Utf8 {
                value: path_str.clone(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            });
            unsafe { &mut (*transaction) }.save_table_meta(cache, table_name, path_str, meta)?;
            fs::rename(&temp_path, &path).map_err(DatabaseError::IO)?;

            active_index_paths.insert(index_file);
        }

        // clean expired index
        for entry in fs::read_dir(dir_path).map_err(DatabaseError::IO)? {
            let entry: DirEntry = entry.map_err(DatabaseError::IO)?;

            if !active_index_paths.remove(&entry.file_name()) {
                fs::remove_file(entry.path()).map_err(DatabaseError::IO)?;
            }
        }

        Ok(values)
    }

    #[cfg(target_arch = "wasm32")]
    fn persist_statistics_meta_wasm<T: Transaction>(
        table_name: &TableName,
        builders: Vec<State>,
        cache: &StatisticsMetaCache,
        transaction: *mut T,
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let prefix = Self::build_statistics_meta_prefix(table_name)?;
        let mut values = Vec::with_capacity(builders.len());
        let mut active_keys = HashSet::new();

        for State {
            index_id, builder, ..
        } in builders
        {
            let key = format!("{prefix}/{index_id}");
            let (histogram, sketch) = builder.build(DEFAULT_NUM_OF_BUCKETS)?;
            let meta = StatisticsMeta::new(histogram, sketch);
            let encoded = meta.to_storage_string()?;

            wasm_set_storage_item(&key, &encoded)?;
            values.push(DataValue::Utf8 {
                value: key.clone(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            });
            unsafe { &mut (*transaction) }.save_table_meta(cache, table_name, key.clone(), meta)?;
            active_keys.insert(key);
        }

        let keys = wasm_storage_keys_with_prefix(&(prefix.clone() + "/"))?;

        for key in keys {
            if !active_keys.contains(&key) {
                wasm_remove_storage_key(&key)?;
            }
        }

        Ok(values)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn build_statistics_meta_path(table_name: &TableName) -> Result<PathBuf, DatabaseError> {
        Ok(require_statistics_base_dir().join(table_name.as_ref()))
    }

    #[cfg(target_arch = "wasm32")]
    fn build_statistics_meta_prefix(table_name: &TableName) -> Result<String, DatabaseError> {
        Ok(require_statistics_base_dir()
            .join(table_name.as_ref())
            .to_string_lossy()
            .into_owned())
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
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use crate::paths::require_statistics_base_dir;
    use crate::storage::rocksdb::RocksTransaction;
    use std::ffi::OsStr;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_analyze() -> Result<(), DatabaseError> {
        test_statistics_meta()?;
        test_clean_expired_index()?;

        Ok(())
    }

    fn test_statistics_meta() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let base_dir = require_statistics_base_dir();
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

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

        let dir_path = base_dir.join("t1");

        let mut paths = Vec::new();

        for entry in fs::read_dir(&dir_path)? {
            paths.push(entry?.path());
        }
        paths.sort();

        let statistics_meta_pk_index = StatisticsMeta::from_file::<RocksTransaction>(&paths[0])?;

        assert_eq!(statistics_meta_pk_index.index_id(), 0);
        assert_eq!(statistics_meta_pk_index.histogram().values_len(), 101);

        let statistics_meta_b_index = StatisticsMeta::from_file::<RocksTransaction>(&paths[1])?;

        assert_eq!(statistics_meta_b_index.index_id(), 1);
        assert_eq!(statistics_meta_b_index.histogram().values_len(), 101);

        let statistics_meta_p_index = StatisticsMeta::from_file::<RocksTransaction>(&paths[2])?;

        assert_eq!(statistics_meta_p_index.index_id(), 2);
        assert_eq!(statistics_meta_p_index.histogram().values_len(), 101);

        Ok(())
    }

    fn test_clean_expired_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let base_dir = require_statistics_base_dir();
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

        let dir_path = base_dir.join("t1");

        let mut file_names = Vec::new();

        for entry in fs::read_dir(&dir_path)? {
            file_names.push(entry?.file_name());
        }
        file_names.sort();

        assert_eq!(file_names.len(), 3);
        assert_eq!(file_names[0], OsStr::new("0"));
        assert_eq!(file_names[1], OsStr::new("1"));
        assert_eq!(file_names[2], OsStr::new("2"));

        kite_sql.run("alter table t1 drop column b")?.done()?;
        kite_sql.run("analyze table t1")?.done()?;

        let mut entries = fs::read_dir(&dir_path)?;

        assert_eq!(entries.next().unwrap()?.file_name(), OsStr::new("0"));
        assert!(entries.next().is_none());

        Ok(())
    }
}
