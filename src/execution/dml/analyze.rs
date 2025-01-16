use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::optimizer::core::histogram::HistogramBuilder;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::IndexMetaRef;
use crate::types::tuple::Tuple;
use crate::types::value::{DataValue, Utf8Type};
use itertools::Itertools;
use sqlparser::ast::CharLengthUnits;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::Formatter;
use std::fs::DirEntry;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;
use std::{fmt, fs};

const DEFAULT_NUM_OF_BUCKETS: usize = 100;
const DEFAULT_STATISTICS_META_PATH: &str = "kite_sql_statistics_metas";

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
        Box::new(
            #[coroutine]
            move || {
                let Analyze {
                    table_name,
                    mut input,
                    index_metas,
                } = self;

                let schema = input.output_schema().clone();
                let mut builders = Vec::with_capacity(index_metas.len());
                let table = throw!(throw!(
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .cloned()
                .ok_or(DatabaseError::TableNotFound));

                for index in table.indexes() {
                    builders.push((
                        index.id,
                        throw!(index.column_exprs(&table)),
                        HistogramBuilder::new(index, None),
                    ));
                }

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);

                    for (_, exprs, builder) in builders.iter_mut() {
                        let values = throw!(Projection::projection(&tuple, exprs, &schema));

                        if values.len() == 1 {
                            throw!(builder.append(&values[0]));
                        } else {
                            throw!(builder.append(&Arc::new(DataValue::Tuple(values, false))));
                        }
                    }
                }
                drop(coroutine);
                let mut values = Vec::with_capacity(builders.len());
                let dir_path = dirs::home_dir()
                    .expect("Your system does not have a Config directory!")
                    .join(DEFAULT_STATISTICS_META_PATH)
                    .join(table_name.as_str());
                // For DEBUG
                // println!("Statistics Path: {:#?}", dir_path);
                throw!(fs::create_dir_all(&dir_path).map_err(DatabaseError::IO));

                let mut active_index_paths = HashSet::new();

                for (index_id, _, builder) in builders {
                    let index_file = OsStr::new(&index_id.to_string()).to_os_string();
                    let path = dir_path.join(&index_file);
                    let temp_path = path.with_extension("tmp");
                    let path_str: String = path.to_string_lossy().into();

                    let (histogram, sketch) = throw!(builder.build(DEFAULT_NUM_OF_BUCKETS));
                    let meta = StatisticsMeta::new(histogram, sketch);

                    throw!(meta.to_file(&temp_path));
                    values.push(DataValue::Utf8 {
                        value: path_str.clone(),
                        ty: Utf8Type::Variable(None),
                        unit: CharLengthUnits::Characters,
                    });
                    throw!(unsafe { &mut (*transaction) }.save_table_meta(
                        cache.2,
                        &table_name,
                        path_str,
                        meta
                    ));
                    throw!(fs::rename(&temp_path, &path).map_err(DatabaseError::IO));

                    active_index_paths.insert(index_file);
                }

                // clean expired index
                for entry in throw!(fs::read_dir(dir_path).map_err(DatabaseError::IO)) {
                    let entry: DirEntry = throw!(entry.map_err(DatabaseError::IO));

                    if !active_index_paths.remove(&entry.file_name()) {
                        throw!(fs::remove_file(entry.path()).map_err(DatabaseError::IO));
                    }
                }

                yield Ok(Tuple::new(None, values));
            },
        )
    }
}

impl fmt::Display for AnalyzeOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let indexes = self.index_metas.iter().map(|index| &index.name).join(", ");

        write!(f, "Analyze {} -> [{}]", self.table_name, indexes)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::errors::DatabaseError;
    use crate::execution::dml::analyze::{DEFAULT_NUM_OF_BUCKETS, DEFAULT_STATISTICS_META_PATH};
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
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

        let dir_path = dirs::home_dir()
            .expect("Your system does not have a Config directory!")
            .join(DEFAULT_STATISTICS_META_PATH)
            .join("t1");

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

        let dir_path = dirs::home_dir()
            .expect("Your system does not have a Config directory!")
            .join(DEFAULT_STATISTICS_META_PATH)
            .join("t1");

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
