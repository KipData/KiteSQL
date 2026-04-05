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
use crate::storage::table_codec::{Bytes, TableCodec};
use crate::storage::{CheckpointableStorage, InnerIter, Storage, Transaction};
#[cfg(feature = "unsafe_txdb_checkpoint")]
use librocksdb_sys as ffi;
use rocksdb::{
    checkpoint::Checkpoint,
    statistics::{StatsLevel, Ticker},
    DBPinnableSlice, DBRawIteratorWithThreadMode, OptimisticTransactionDB, Options, ReadOptions,
    SliceTransform, TransactionDB,
};
use std::collections::Bound;
#[cfg(feature = "unsafe_txdb_checkpoint")]
use std::ffi::{c_char, c_void, CStr, CString};
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// Table data keys are `{table_hash(8)}{type_tag(1)}...`, so use hash+type as prefix.
const ROCKSDB_FIXED_PREFIX_LEN: usize = 9;
const ROCKSDB_BLOOM_BITS_PER_KEY: f64 = 10.0;
const ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO: f64 = 0.10;
#[cfg(feature = "unsafe_txdb_checkpoint")]
const ROCKSDB_TRANSACTION_DB_INNER_OFFSET: usize = 0x30;

/// A lightweight snapshot of key RocksDB runtime indicators for tuning and diagnostics.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RocksDbMetrics {
    pub block_cache_hit: Option<u64>,
    pub block_cache_miss: Option<u64>,
    pub block_cache_data_hit: Option<u64>,
    pub block_cache_data_miss: Option<u64>,
    pub block_cache_index_hit: Option<u64>,
    pub block_cache_index_miss: Option<u64>,
    pub block_cache_filter_hit: Option<u64>,
    pub block_cache_filter_miss: Option<u64>,
    pub stall_micros: Option<u64>,
    pub compaction_pending_bytes: Option<u64>,
    pub write_amp: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StorageConfig {
    pub enable_statistics: bool,
}

impl RocksDbMetrics {
    #[inline]
    pub fn block_cache_hit_rate(&self) -> Option<f64> {
        let hit = self.block_cache_hit?;
        let miss = self.block_cache_miss?;
        let total = hit + miss;
        if total == 0 {
            return None;
        }

        Some(hit as f64 / total as f64)
    }

    #[inline]
    pub fn delta_since(&self, base: &Self) -> Self {
        Self {
            block_cache_hit: subtract_optional_u64(self.block_cache_hit, base.block_cache_hit),
            block_cache_miss: subtract_optional_u64(self.block_cache_miss, base.block_cache_miss),
            block_cache_data_hit: subtract_optional_u64(
                self.block_cache_data_hit,
                base.block_cache_data_hit,
            ),
            block_cache_data_miss: subtract_optional_u64(
                self.block_cache_data_miss,
                base.block_cache_data_miss,
            ),
            block_cache_index_hit: subtract_optional_u64(
                self.block_cache_index_hit,
                base.block_cache_index_hit,
            ),
            block_cache_index_miss: subtract_optional_u64(
                self.block_cache_index_miss,
                base.block_cache_index_miss,
            ),
            block_cache_filter_hit: subtract_optional_u64(
                self.block_cache_filter_hit,
                base.block_cache_filter_hit,
            ),
            block_cache_filter_miss: subtract_optional_u64(
                self.block_cache_filter_miss,
                base.block_cache_filter_miss,
            ),
            stall_micros: subtract_optional_u64(self.stall_micros, base.stall_micros),
            compaction_pending_bytes: self.compaction_pending_bytes,
            write_amp: self.write_amp,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        [
            self.block_cache_hit,
            self.block_cache_miss,
            self.block_cache_data_hit,
            self.block_cache_data_miss,
            self.block_cache_index_hit,
            self.block_cache_index_miss,
            self.block_cache_filter_hit,
            self.block_cache_filter_miss,
            self.stall_micros,
            self.compaction_pending_bytes,
        ]
        .into_iter()
        .flatten()
        .all(|value| value == 0)
    }
}

impl Display for RocksDbMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "<RocksDB Metrics>")?;
        writeln!(
            f,
            "block_cache_hit={} block_cache_miss={} block_cache_hit_rate={}",
            format_optional_u64(self.block_cache_hit),
            format_optional_u64(self.block_cache_miss),
            format_optional_pct(self.block_cache_hit_rate()),
        )?;
        writeln!(
            f,
            "block_cache_data_hit={} block_cache_data_miss={} block_cache_index_hit={} block_cache_index_miss={} block_cache_filter_hit={} block_cache_filter_miss={}",
            format_optional_u64(self.block_cache_data_hit),
            format_optional_u64(self.block_cache_data_miss),
            format_optional_u64(self.block_cache_index_hit),
            format_optional_u64(self.block_cache_index_miss),
            format_optional_u64(self.block_cache_filter_hit),
            format_optional_u64(self.block_cache_filter_miss),
        )?;
        write!(
            f,
            "stall_micros={} pending_compaction_bytes={} write_amp={}",
            format_optional_u64(self.stall_micros),
            format_optional_u64(self.compaction_pending_bytes),
            format_optional_f64(self.write_amp),
        )
    }
}

#[derive(Clone)]
pub struct OptimisticRocksStorage {
    pub inner: Arc<OptimisticTransactionDB>,
    options: Arc<Options>,
    config: StorageConfig,
}

impl OptimisticRocksStorage {
    pub fn new(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        Self::with_config(path, StorageConfig::default())
    }

    pub fn with_config(
        path: impl Into<PathBuf> + Send,
        config: StorageConfig,
    ) -> Result<Self, DatabaseError> {
        let options = Arc::new(default_opts(config));
        let storage = OptimisticTransactionDB::open(options.as_ref(), path.into())?;

        Ok(OptimisticRocksStorage {
            inner: Arc::new(storage),
            options,
            config,
        })
    }
}

#[derive(Clone)]
pub struct RocksStorage {
    pub inner: Arc<TransactionDB<rocksdb::MultiThreaded>>,
    options: Arc<Options>,
    config: StorageConfig,
}

impl RocksStorage {
    pub fn new(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        Self::with_config(path, StorageConfig::default())
    }

    pub fn with_config(
        path: impl Into<PathBuf> + Send,
        config: StorageConfig,
    ) -> Result<Self, DatabaseError> {
        let options = Arc::new(default_opts(config));
        let txn_opts = rocksdb::TransactionDBOptions::default();
        let storage = TransactionDB::<rocksdb::MultiThreaded>::open(
            options.as_ref(),
            &txn_opts,
            path.into(),
        )?;

        Ok(RocksStorage {
            inner: Arc::new(storage),
            options,
            config,
        })
    }
}

fn collect_metrics(
    options: &Options,
    int_property: impl Fn(&str) -> Result<Option<u64>, rocksdb::Error>,
) -> RocksDbMetrics {
    let block_cache_hit = Some(options.get_ticker_count(Ticker::BlockCacheHit));
    let block_cache_miss = Some(options.get_ticker_count(Ticker::BlockCacheMiss));
    let block_cache_data_hit = Some(options.get_ticker_count(Ticker::BlockCacheDataHit));
    let block_cache_data_miss = Some(options.get_ticker_count(Ticker::BlockCacheDataMiss));
    let block_cache_index_hit = Some(options.get_ticker_count(Ticker::BlockCacheIndexHit));
    let block_cache_index_miss = Some(options.get_ticker_count(Ticker::BlockCacheIndexMiss));
    let block_cache_filter_hit = Some(options.get_ticker_count(Ticker::BlockCacheFilterHit));
    let block_cache_filter_miss = Some(options.get_ticker_count(Ticker::BlockCacheFilterMiss));
    let stall_micros = Some(options.get_ticker_count(Ticker::StallMicros));
    let compaction_pending_bytes = int_property("rocksdb.estimate-pending-compaction-bytes")
        .ok()
        .flatten();
    let write_amp = read_write_amp(options.get_statistics().as_deref());

    RocksDbMetrics {
        block_cache_hit,
        block_cache_miss,
        block_cache_data_hit,
        block_cache_data_miss,
        block_cache_index_hit,
        block_cache_index_miss,
        block_cache_filter_hit,
        block_cache_filter_miss,
        stall_micros,
        compaction_pending_bytes,
        write_amp,
    }
}

fn read_write_amp(stats: Option<&str>) -> Option<f64> {
    if let Some(stats) = stats {
        for line in stats.lines() {
            if line.to_ascii_lowercase().contains("write amplification") {
                if let Some(number) = parse_last_f64(line) {
                    return Some(number);
                }
            }
        }
    }

    None
}

fn parse_last_f64(input: &str) -> Option<f64> {
    input
        .split(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-'))
        .filter(|token| !token.is_empty() && *token != "." && *token != "-" && *token != "-.")
        .rev()
        .find_map(|token| token.parse::<f64>().ok())
}

fn format_optional_u64(value: Option<u64>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_f64(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.3}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_pct(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}%", v * 100.0))
        .unwrap_or_else(|| "n/a".to_string())
}

fn subtract_optional_u64(lhs: Option<u64>, rhs: Option<u64>) -> Option<u64> {
    match (lhs, rhs) {
        (Some(lhs), Some(rhs)) => Some(lhs.saturating_sub(rhs)),
        (Some(lhs), None) => Some(lhs),
        _ => None,
    }
}

fn default_opts(config: StorageConfig) -> Options {
    let mut bb = rocksdb::BlockBasedOptions::default();
    bb.set_block_cache(&rocksdb::Cache::new_lru_cache(40 * 1_024 * 1_024));
    bb.set_bloom_filter(ROCKSDB_BLOOM_BITS_PER_KEY, true);
    bb.set_whole_key_filtering(false);

    let mut opts = rocksdb::Options::default();
    opts.set_block_based_table_factory(&bb);
    opts.create_if_missing(true);
    if config.enable_statistics {
        opts.enable_statistics();
        opts.set_statistics_level(StatsLevel::ExceptDetailedTimers);
    }
    opts.set_memtable_prefix_bloom_ratio(ROCKSDB_MEMTABLE_PREFIX_BLOOM_RATIO);
    opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(
        ROCKSDB_FIXED_PREFIX_LEN,
    ));
    opts
}

fn prepare_checkpoint_dir(path: &Path) -> Result<(), DatabaseError> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    format!(
                        "checkpoint target path '{}' already exists and is not a directory",
                        path.display()
                    ),
                )
                .into());
            }

            if fs::read_dir(path)?.next().is_some() {
                return Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    format!(
                        "checkpoint target directory '{}' must be empty",
                        path.display()
                    ),
                )
                .into());
            }

            fs::remove_dir(path)?;
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn cleanup_failed_checkpoint_dir(path: &Path) -> Result<(), DatabaseError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

#[cfg(not(feature = "unsafe_txdb_checkpoint"))]
fn unsupported_transactiondb_checkpoint_error() -> DatabaseError {
    DatabaseError::UnsupportedStmt(format!(
        "rocksdb TransactionDB checkpoint is disabled; enable the `unsafe_txdb_checkpoint` feature to opt in to the current implementation",
    ))
}

#[cfg(feature = "unsafe_txdb_checkpoint")]
fn rocksdb_error_from_ptr(err: *mut c_char) -> DatabaseError {
    unsafe {
        let message = CStr::from_ptr(err).to_string_lossy().into_owned();
        ffi::rocksdb_free(err.cast::<c_void>());
        io::Error::other(message).into()
    }
}

#[cfg(feature = "unsafe_txdb_checkpoint")]
fn checkpoint_path_to_cstring(path: &Path) -> Result<CString, DatabaseError> {
    CString::new(path.to_string_lossy().into_owned()).map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "checkpoint path '{}' contains an interior NUL byte",
                path.display()
            ),
        )
        .into()
    })
}

#[cfg(feature = "unsafe_txdb_checkpoint")]
fn create_transactiondb_checkpoint(
    db: &TransactionDB<rocksdb::MultiThreaded>,
    path: &Path,
) -> Result<(), DatabaseError> {
    let path = checkpoint_path_to_cstring(path)?;
    let mut err: *mut c_char = std::ptr::null_mut();

    // `rocksdb` 0.23 does not expose a safe checkpoint API for `TransactionDB`.
    // This fallback is intentionally gated behind the `unsafe_txdb_checkpoint`
    // feature so callers must opt in explicitly.
    let db_ptr = unsafe {
        *(std::ptr::from_ref(db)
            .cast::<u8>()
            .add(ROCKSDB_TRANSACTION_DB_INNER_OFFSET)
            .cast::<*mut ffi::rocksdb_transactiondb_t>())
    };
    let checkpoint =
        unsafe { ffi::rocksdb_transactiondb_checkpoint_object_create(db_ptr, &mut err) };
    if !err.is_null() {
        return Err(rocksdb_error_from_ptr(err));
    }
    if checkpoint.is_null() {
        return Err(io::Error::other("Could not create checkpoint object.").into());
    }

    unsafe {
        ffi::rocksdb_checkpoint_create(checkpoint, path.as_ptr(), 0, &mut err);
        ffi::rocksdb_checkpoint_object_destroy(checkpoint);
    }
    if !err.is_null() {
        return Err(rocksdb_error_from_ptr(err));
    }

    Ok(())
}

impl Storage for OptimisticRocksStorage {
    type Metrics = RocksDbMetrics;

    type TransactionType<'a>
        = OptimisticRocksTransaction<'a>
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError> {
        Ok(OptimisticRocksTransaction {
            tx: self.inner.transaction(),
            table_codec: Default::default(),
        })
    }

    fn metrics(&self) -> Option<Self::Metrics> {
        if !self.config.enable_statistics {
            return None;
        }

        Some(collect_metrics(self.options.as_ref(), |name| {
            self.inner.property_int_value(name)
        }))
    }
}

impl Storage for RocksStorage {
    type Metrics = RocksDbMetrics;

    type TransactionType<'a>
        = RocksTransaction<'a>
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError> {
        Ok(RocksTransaction {
            tx: self.inner.transaction(),
            table_codec: Default::default(),
        })
    }

    fn metrics(&self) -> Option<Self::Metrics> {
        if !self.config.enable_statistics {
            return None;
        }

        Some(collect_metrics(self.options.as_ref(), |name| {
            self.inner.property_int_value(name)
        }))
    }
}

impl CheckpointableStorage for OptimisticRocksStorage {
    fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<(), DatabaseError> {
        let path = path.as_ref();
        prepare_checkpoint_dir(path)?;

        let checkpoint = Checkpoint::new(self.inner.as_ref())?;
        if let Err(err) = checkpoint.create_checkpoint(path) {
            cleanup_failed_checkpoint_dir(path)?;
            return Err(err.into());
        }

        Ok(())
    }
}

impl CheckpointableStorage for RocksStorage {
    fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<(), DatabaseError> {
        let path = path.as_ref();
        prepare_checkpoint_dir(path)?;

        #[cfg(feature = "unsafe_txdb_checkpoint")]
        {
            if let Err(err) = create_transactiondb_checkpoint(self.inner.as_ref(), path) {
                cleanup_failed_checkpoint_dir(path)?;
                return Err(err);
            }

            return Ok(());
        }

        #[cfg(not(feature = "unsafe_txdb_checkpoint"))]
        {
            return Err(unsupported_transactiondb_checkpoint_error());
        }
    }
}

pub struct OptimisticRocksTransaction<'db> {
    tx: rocksdb::Transaction<'db, OptimisticTransactionDB>,
    table_codec: TableCodec,
}

pub struct RocksTransaction<'db> {
    tx: rocksdb::Transaction<'db, TransactionDB<rocksdb::MultiThreaded>>,
    table_codec: TableCodec,
}

#[macro_export]
macro_rules! impl_transaction {
    ($tx:ident, $iter:ident) => {
        impl<'storage> Transaction for $tx<'storage> {
            type BorrowedBytes<'a>
                = DBPinnableSlice<'a>
            where
                Self: 'a;

            type IterType<'iter>
                = $iter<'storage, 'iter>
            where
                Self: 'iter;

            #[inline]
            fn table_codec(&self) -> *const TableCodec {
                &self.table_codec
            }

            #[inline]
            fn get_borrowed<'a>(
                &'a self,
                key: &[u8],
            ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError> {
                Ok(self.tx.get_pinned(key)?)
            }

            #[inline]
            fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
                self.tx.put(key, value)?;

                Ok(())
            }

            #[inline]
            fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
                self.tx.delete(key)?;

                Ok(())
            }

            // Tips: rocksdb has weak support for `Include` and `Exclude`, so precision will be lost
            #[inline]
            fn range<'a, 'key>(
                &'a self,
                min: Bound<&'key [u8]>,
                max: Bound<&'key [u8]>,
            ) -> Result<Self::IterType<'a>, DatabaseError> {
                let mut read_opts = ReadOptions::default();
                if let (
                    Bound::Included(min_bytes) | Bound::Excluded(min_bytes),
                    Bound::Included(max_bytes) | Bound::Excluded(max_bytes),
                ) = (&min, &max)
                {
                    let len = min_bytes
                        .iter()
                        .zip(max_bytes.iter())
                        .take_while(|(x, y)| x == y)
                        .count();
                    if len >= ROCKSDB_FIXED_PREFIX_LEN {
                        read_opts.set_prefix_same_as_start(true);
                    }
                }

                let mut iter = self.tx.raw_iterator_opt(read_opts);
                match &min {
                    Bound::Included(bytes) => iter.seek(*bytes),
                    Bound::Excluded(bytes) => {
                        iter.seek(*bytes);
                        if iter.key() == Some(*bytes) {
                            iter.next();
                        }
                    }
                    Bound::Unbounded => iter.seek_to_first(),
                }

                Ok($iter {
                    upper: owned_bound(max),
                    iter,
                    advanced: false,
                    done: false,
                })
            }

            fn commit(self) -> Result<(), DatabaseError> {
                self.tx.commit()?;
                Ok(())
            }
        }
    };
}

impl_transaction!(RocksTransaction, RocksIter);
impl_transaction!(OptimisticRocksTransaction, OptimisticRocksIter);

pub struct OptimisticRocksIter<'txn, 'iter> {
    upper: Bound<Bytes>,
    iter: DBRawIteratorWithThreadMode<'iter, rocksdb::Transaction<'txn, OptimisticTransactionDB>>,
    advanced: bool,
    done: bool,
}

impl InnerIter for OptimisticRocksIter<'_, '_> {
    #[inline]
    fn try_next(&mut self) -> Result<Option<crate::storage::KeyValueRef<'_>>, DatabaseError> {
        next(
            &mut self.iter,
            &self.upper,
            &mut self.advanced,
            &mut self.done,
        )
    }
}

pub struct RocksIter<'txn, 'iter> {
    upper: Bound<Bytes>,
    iter: DBRawIteratorWithThreadMode<
        'iter,
        rocksdb::Transaction<'txn, TransactionDB<rocksdb::MultiThreaded>>,
    >,
    advanced: bool,
    done: bool,
}

impl InnerIter for RocksIter<'_, '_> {
    #[inline]
    fn try_next(&mut self) -> Result<Option<crate::storage::KeyValueRef<'_>>, DatabaseError> {
        next(
            &mut self.iter,
            &self.upper,
            &mut self.advanced,
            &mut self.done,
        )
    }
}

#[inline]
fn next<'a, D: rocksdb::DBAccess>(
    iter: &'a mut DBRawIteratorWithThreadMode<'_, D>,
    upper: &Bound<Bytes>,
    advanced: &mut bool,
    done: &mut bool,
) -> Result<Option<crate::storage::KeyValueRef<'a>>, DatabaseError> {
    if *done {
        return Ok(None);
    }
    if *advanced {
        iter.next();
    }
    if !iter.valid() {
        *done = true;
        iter.status()?;
        return Ok(None);
    }

    let Some((key, value)) = iter.item() else {
        *done = true;
        iter.status()?;
        return Ok(None);
    };
    let upper_bound_check = match upper {
        Bound::Included(upper) => key <= upper.as_slice(),
        Bound::Excluded(upper) => key < upper.as_slice(),
        Bound::Unbounded => true,
    };
    if !upper_bound_check {
        *done = true;
        return Ok(None);
    }

    *advanced = true;
    Ok(Some((key, value)))
}

fn owned_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.to_vec()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, TableName};
    use crate::db::DataBaseBuilder;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{
        IndexImplEnum, IndexImplParams, IndexIter, IndexIterState, IterBounds, PrimaryKeyIndexImpl,
        Storage, Transaction,
    };
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::collections::{BTreeMap, Bound};
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_parse_write_amp_from_stats_line() {
        assert_eq!(
            super::parse_last_f64("Cumulative writes: 89 writes, Write Amplification: 1.72"),
            Some(1.72)
        );
        assert_eq!(super::parse_last_f64("no numeric token"), None);
    }

    #[test]
    fn test_collect_rocksdb_metrics_snapshot() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .storage_statistics(true)
            .build_rocksdb()?;
        kite_sql
            .run("create table t_metrics (a int primary key, b int)")?
            .done()?;
        kite_sql
            .run("insert into t_metrics values (1, 10), (2, 20), (3, 30)")?
            .done()?;
        kite_sql
            .run("select * from t_metrics where a = 2")?
            .done()?;

        let metrics = kite_sql.storage_metrics().unwrap();
        let _ = metrics.block_cache_hit_rate();

        Ok(())
    }

    #[test]
    fn test_in_rocksdb_storage_works_with_data() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let columns = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            )),
        ]);

        let source_columns = columns
            .iter()
            .map(|col_ref| ColumnCatalog::clone(col_ref))
            .collect_vec();
        let _ = transaction.create_table(
            &table_cache,
            "test".to_string().into(),
            source_columns,
            false,
        )?;

        let table_catalog = transaction.table(&table_cache, "test".to_string().into())?;
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_column_id_by_name("c1").is_some());

        transaction.append_tuple(
            "test",
            Tuple::new(
                Some(DataValue::Int32(1)),
                vec![DataValue::Int32(1), DataValue::Boolean(true)],
            ),
            &[
                LogicalType::Integer.serializable(),
                LogicalType::Boolean.serializable(),
            ],
            false,
        )?;
        transaction.append_tuple(
            "test",
            Tuple::new(
                Some(DataValue::Int32(2)),
                vec![DataValue::Int32(2), DataValue::Boolean(true)],
            ),
            &[
                LogicalType::Integer.serializable(),
                LogicalType::Boolean.serializable(),
            ],
            false,
        )?;

        let mut read_columns = BTreeMap::new();
        read_columns.insert(0, columns[0].clone());

        let mut iter = transaction.read(
            &table_cache,
            "test".to_string().into(),
            (Some(1), Some(1)),
            read_columns,
            true,
        )?;

        let option_1 = crate::storage::next_tuple_for_test(&mut iter)?;
        assert_eq!(option_1.unwrap().pk, Some(DataValue::Int32(2)));

        let option_2 = crate::storage::next_tuple_for_test(&mut iter)?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[test]
    fn test_index_iter_pk() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t1 (a int primary key)")?
            .done()?;
        kite_sql
            .run("insert into t1 (a) values (0), (1), (2), (3), (4)")?
            .done()?;
        let transaction = kite_sql.storage.transaction()?;

        let table_name: TableName = "t1".to_string().into();
        let table = transaction
            .table(kite_sql.state.table_cache(), table_name.clone())?
            .unwrap()
            .clone();
        let a_column_id = table.get_column_id_by_name("a").unwrap();
        let tuple_ids = vec![
            DataValue::Int32(0),
            DataValue::Int32(2),
            DataValue::Int32(3),
            DataValue::Int32(4),
        ];
        let deserializers = table
            .columns()
            .map(|column| column.datatype().serializable())
            .collect_vec();
        let mut iter: IndexIter<'_, _> = IndexIter {
            bounds: IterBounds::new(0, None),
            params: IndexImplParams {
                index_meta: Arc::new(IndexMeta {
                    id: 0,
                    column_ids: vec![*a_column_id],
                    table_name,
                    pk_ty: LogicalType::Integer,
                    value_ty: LogicalType::Integer,
                    name: "pk_a".to_string(),
                    ty: IndexType::PrimaryKey { is_multiple: false },
                }),
                table_name: &table.name,
                deserializers,
                total_len: table.columns_len(),
                tx: &transaction,
                cover_mapping: None,
                with_pk: true,
            },
            ranges: vec![
                Range::Eq(DataValue::Int32(0)),
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(2)),
                    max: Bound::Included(DataValue::Int32(4)),
                },
            ]
            .into(),
            state: IndexIterState::Init,
            inner: IndexImplEnum::PrimaryKey(PrimaryKeyIndexImpl),
            encode_min_buffer: Vec::new(),
            encode_max_buffer: Vec::new(),
        };
        let mut result = Vec::new();

        while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, tuple_ids);

        Ok(())
    }

    #[test]
    fn test_read_by_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        kite_sql
            .run("create table t1 (a int primary key, b int unique)")?
            .done()?;
        kite_sql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2), (3, 4)")?
            .done()?;
        let transaction = kite_sql.storage.transaction().unwrap();

        let table = transaction
            .table(kite_sql.state.table_cache(), "t1".to_string().into())?
            .unwrap()
            .clone();
        {
            let mut iter = transaction
                .read_by_index(
                    kite_sql.state.table_cache(),
                    "t1".to_string().into(),
                    (Some(0), Some(1)),
                    table.columns().cloned().enumerate().collect(),
                    table.indexes[0].clone(),
                    vec![Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(0)),
                        max: Bound::Unbounded,
                    }],
                    true,
                    None,
                    None,
                )
                .unwrap();

            while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
                assert_eq!(tuple.pk, Some(DataValue::Int32(1)));
                assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(1)])
            }
        }
        // projection
        {
            let mut columns: BTreeMap<_, _> = table.columns().cloned().enumerate().collect();
            let _ = columns.pop_last();

            let mut iter = transaction
                .read_by_index(
                    kite_sql.state.table_cache(),
                    "t1".to_string().into(),
                    (Some(0), Some(1)),
                    columns,
                    table.indexes[0].clone(),
                    vec![Range::Scope {
                        min: Bound::Excluded(DataValue::Int32(3)),
                        max: Bound::Unbounded,
                    }],
                    true,
                    None,
                    None,
                )
                .unwrap();

            while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
                assert_eq!(tuple.pk, Some(DataValue::Int32(3)));
                assert_eq!(tuple.values, vec![DataValue::Int32(3)])
            }
        }

        Ok(())
    }

    #[test]
    fn test_read_by_index_cover() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        kite_sql
            .run("create table t1 (a int primary key, b int unique)")?
            .done()?;
        kite_sql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2), (3, 4)")?
            .done()?;
        kite_sql.run("create index idx_b_a on t1(b, a)")?.done()?;

        let mut transaction = kite_sql.storage.transaction().unwrap();
        let table = transaction
            .table(kite_sql.state.table_cache(), "t1".to_string().into())?
            .unwrap()
            .clone();
        let columns_vec: Vec<_> = table.columns().cloned().collect();
        let a_cover_column = columns_vec
            .iter()
            .find(|column| column.name() == "a")
            .unwrap()
            .clone();
        let b_cover_column = columns_vec
            .iter()
            .find(|column| column.name() == "b")
            .unwrap()
            .clone();
        let unique_index = table
            .indexes
            .iter()
            .find(|index| matches!(index.ty, IndexType::Unique))
            .unwrap()
            .clone();
        let (b_pos, b_column) = table
            .columns()
            .cloned()
            .enumerate()
            .find(|(_, column)| column.name() == "b")
            .unwrap();
        let mut columns = BTreeMap::new();
        columns.insert(b_pos, b_column.clone());
        let covered_deserializers = vec![b_column.datatype().serializable()];

        // ensure cover mapping can reorder index values to match scan order
        let composite_index = table
            .indexes
            .iter()
            .find(|index| index.name == "idx_b_a")
            .unwrap()
            .clone();
        let mut reordered_columns = BTreeMap::new();
        reordered_columns.insert(0, a_cover_column.clone());
        reordered_columns.insert(1, b_cover_column.clone());
        let reordered_deserializers = vec![
            a_cover_column.datatype().serializable(),
            b_cover_column.datatype().serializable(),
        ];
        let a_id = a_cover_column.id().unwrap();
        let b_id = b_cover_column.id().unwrap();
        let cover_mapping = vec![
            composite_index
                .column_ids
                .iter()
                .position(|id| id == &a_id)
                .unwrap(),
            composite_index
                .column_ids
                .iter()
                .position(|id| id == &b_id)
                .unwrap(),
        ];

        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            "t1".to_string().into(),
            (None, None),
            reordered_columns,
            composite_index,
            vec![Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Unbounded,
            }],
            false,
            Some(reordered_deserializers),
            Some(cover_mapping),
        )?;
        let first_tuple = crate::storage::next_tuple_for_test(&mut iter)?.unwrap();
        assert_eq!(
            first_tuple.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        drop(iter);

        let target_pk = DataValue::Int32(3);
        let covered_value = DataValue::Int32(4);
        transaction.remove_tuple("t1", &target_pk)?;

        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            "t1".to_string().into(),
            (Some(0), Some(1)),
            columns,
            unique_index,
            vec![Range::Eq(covered_value.clone())],
            false,
            Some(covered_deserializers),
            None,
        )?;

        let mut tuples = Vec::new();
        while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
            tuples.push(tuple);
        }

        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].pk, None);
        assert_eq!(tuples[0].values, vec![covered_value]);

        // primary key index should ignore covered-deserializer hint and still return rows
        let pk_index = table
            .indexes
            .iter()
            .find(|index| index.name == "pk_index")
            .unwrap()
            .clone();
        let mut pk_columns = BTreeMap::new();
        pk_columns.insert(0, a_cover_column.clone());
        let pk_deserializers = vec![a_cover_column.datatype().serializable()];
        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            "t1".to_string().into(),
            (None, None),
            pk_columns,
            pk_index,
            vec![Range::Scope {
                min: Bound::Unbounded,
                max: Bound::Unbounded,
            }],
            false,
            Some(pk_deserializers),
            Some(vec![0]),
        )?;
        let mut row_count = 0;
        while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
            assert_eq!(tuple.values.len(), 1);
            row_count += 1;
        }
        assert_eq!(row_count, 3);

        Ok(())
    }
}
