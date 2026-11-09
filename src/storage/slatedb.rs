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

//! A storage over [SlateDB](https://slatedb.io), an LSM tree kept in object storage. It runs on
//! the async path only: SlateDB drives its I/O and background work on tokio, so open the
//! database and use it inside a tokio runtime.
//!
//! With the `slatedb-s3` feature, [`SlateDbStorage::open_s3`] opens it on Amazon S3 or an
//! S3-compatible store described by an [`S3Config`].
//!
//! Scans read their SSTs as [`SlateDbScanOptions`] set, and [`SlateDbCacheOptions`] sizes the
//! block cache [`SlateDbStorage::open`] builds and can add a local disk tier to it. For other
//! SlateDB settings, build the [`Db`] with [`Db::builder`] and wrap it with
//! [`SlateDbStorage::new`].
//!
//! Each KiteSQL transaction is a SlateDB snapshot-isolation transaction. Reads see the snapshot
//! taken when it began plus its own writes; a commit that conflicts with a write committed since
//! fails, and the transaction must be retried.

use crate::errors::DatabaseError;
use crate::storage::{
    EmptyStorageMetrics, InnerIter, KeyValueRef, Storage, StorageTransaction,
    TransactionIsolationLevel,
};
use crate::types::serialize::BytesPool;
use slatedb::bytes::Bytes;
use slatedb::config::ScanOptions;
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::{
    CachedEntry, DbCache, SplitCache, DEFAULT_BLOCK_CACHE_CAPACITY, DEFAULT_META_CACHE_CAPACITY,
};
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use slatedb::{
    BlockCachePolicy, CacheTarget, Db, DbIterator, DbTransaction, IsolationLevel, IterationOrder,
    KeyValue,
};
use std::collections::Bound;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct SlateDbStorage {
    db: Arc<Db>,
    await_durable: bool,
    scan_options: SlateDbScanOptions,
    cache: Option<Arc<dyn DbCache>>,
}

/// The block cache of a database [`SlateDbStorage::open`] opens. The default keeps
/// SlateDB's: data blocks in 512MiB of memory, and the blocks compactions write left out.
#[derive(Clone, Debug, Default)]
pub struct SlateDbCacheOptions {
    /// The memory for data blocks; 512MiB when unset.
    pub memory_bytes: Option<u64>,
    /// A local directory where data blocks go when memory evicts them, so that reads of them skip
    /// object storage.
    pub disk: Option<SlateDbDiskCache>,
    /// Put the data blocks compactions write into the cache. Without it, compacting rows drops
    /// them from the cache, and their next reads go to object storage.
    pub cache_compaction_output: bool,
}

/// The disk tier of the block cache.
#[derive(Clone, Debug)]
pub struct SlateDbDiskCache {
    /// The directory of the cache files.
    pub dir: PathBuf,
    /// The bytes the cache files may take. The files grow as they fill, so a large capacity
    /// costs nothing up front.
    pub capacity_bytes: usize,
}

/// How a scan reads the SSTs it covers from object storage.
///
/// The defaults suit object storage, where each request costs tens of milliseconds. SlateDB's own
/// defaults fetch one block (4KiB) per request, one request at a time, and keep nothing in the
/// block cache, so a full scan costs one request per block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlateDbScanOptions {
    /// The bytes a scan fetches from an SST in one request, at least one block. 256KiB by
    /// default.
    pub read_ahead_bytes: usize,
    /// The requests one scan keeps in flight. 4 by default.
    pub max_fetch_tasks: usize,
    /// Whether the blocks a scan fetches go into the block cache, so that later scans of the same
    /// range skip the request. On by default; a large scan then evicts what the cache held.
    pub cache_blocks: bool,
}

impl Default for SlateDbScanOptions {
    fn default() -> Self {
        Self {
            read_ahead_bytes: 256 * 1024,
            max_fetch_tasks: 4,
            cache_blocks: true,
        }
    }
}

impl SlateDbStorage {
    /// Wraps an open SlateDB database. Commits wait until the write is durable in object
    /// storage; see [`await_durable`](Self::await_durable).
    pub fn new(db: Arc<Db>) -> Self {
        Self {
            db,
            await_durable: true,
            scan_options: SlateDbScanOptions::default(),
            cache: None,
        }
    }

    /// Whether a commit waits until its write is durable in object storage (the default). Without
    /// it a commit returns once the write is applied in memory, and a crash may lose the latest
    /// commits.
    pub fn await_durable(mut self, await_durable: bool) -> Self {
        self.await_durable = await_durable;
        self
    }

    /// How scans read SSTs from object storage; see [`SlateDbScanOptions`].
    pub fn scan_options(mut self, scan_options: SlateDbScanOptions) -> Self {
        self.scan_options = scan_options;
        self
    }

    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    /// Closes the database, flushing what is still in memory to object storage, and then the
    /// cache [`open`](Self::open) built for it.
    pub async fn close(&self) -> Result<(), DatabaseError> {
        self.db.close().await?;
        if let Some(cache) = &self.cache {
            cache.close().await?;
        }
        Ok(())
    }

    /// Opens the SlateDB database at `path` in `object_store`, creating it if it does not exist,
    /// with the block cache `cache` describes. Close it with [`close`](Self::close), which closes
    /// the cache too.
    pub async fn open(
        path: impl Into<Path>,
        object_store: Arc<dyn ObjectStore>,
        cache: SlateDbCacheOptions,
    ) -> Result<Self, DatabaseError> {
        let in_memory = |max_capacity| -> Arc<dyn DbCache> {
            Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity,
                ..FoyerCacheOptions::default()
            }))
        };
        let memory_bytes = cache.memory_bytes.unwrap_or(DEFAULT_BLOCK_CACHE_CAPACITY);
        let blocks = match cache.disk {
            None if cache.memory_bytes.is_none() => None,
            None => Some(in_memory(memory_bytes)),
            Some(disk) => {
                use foyer::{
                    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder,
                    PsyncIoEngineConfig,
                };
                let disk_error = |err: foyer::Error| {
                    DatabaseError::IO(std::io::Error::other(format!("disk cache: {err}")))
                };
                std::fs::create_dir_all(&disk.dir)?;
                let device = FsDeviceBuilder::new(&disk.dir)
                    .with_capacity(disk.capacity_bytes)
                    .build()
                    .map_err(disk_error)?;
                let hybrid = HybridCacheBuilder::new()
                    .with_name("kitesql_slatedb_blocks")
                    .memory(memory_bytes as usize)
                    .with_weighter(|_, entry: &CachedEntry| entry.size())
                    .storage()
                    .with_io_engine_config(PsyncIoEngineConfig::new())
                    .with_engine_config(BlockEngineConfig::new(device))
                    .build()
                    .await
                    .map_err(disk_error)?;
                Some(Arc::new(FoyerHybridCache::new_with_cache(hybrid)) as Arc<dyn DbCache>)
            }
        };
        // Without its own block cache, the database keeps SlateDB's default one, and closes it.
        let db_cache = blocks.map(|blocks| -> Arc<dyn DbCache> {
            Arc::new(
                SplitCache::new()
                    .with_block_cache(Some(blocks))
                    .with_meta_cache(Some(in_memory(DEFAULT_META_CACHE_CAPACITY)))
                    .build(),
            )
        });

        let mut builder = Db::builder(path, object_store);
        if let Some(db_cache) = &db_cache {
            builder = builder.with_db_cache(db_cache.clone(), 0);
        }
        if cache.cache_compaction_output {
            builder = builder.with_block_cache_policy(
                BlockCachePolicy::default().with_compaction_output_targets(&[
                    CacheTarget::data::<&[u8], _>(..),
                    CacheTarget::Index,
                    CacheTarget::Filters,
                ]),
            );
        }
        let mut storage = Self::new(Arc::new(builder.build().await?));
        storage.cache = db_cache;
        Ok(storage)
    }

    /// [`open`](Self::open)s the SlateDB database at `path` in the S3 bucket `config` describes.
    #[cfg(feature = "slatedb-s3")]
    pub async fn open_s3(
        path: impl Into<Path>,
        config: &S3Config,
        cache: SlateDbCacheOptions,
    ) -> Result<Self, DatabaseError> {
        Self::open(path, config.object_store()?, cache).await
    }
}

/// An Amazon S3 bucket, or one of an S3-compatible store such as MinIO or R2. Settings left
/// unset fall back to the standard `AWS_*` environment variables; credentials then fall back to
/// the default AWS chain, such as instance metadata.
#[cfg(feature = "slatedb-s3")]
#[derive(Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    /// The endpoint of an S3-compatible store, such as `http://127.0.0.1:9000` for a local MinIO.
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// Allows a plain `http://` endpoint.
    pub allow_http: bool,
}

#[cfg(feature = "slatedb-s3")]
impl S3Config {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            ..Self::default()
        }
    }

    /// Builds the object store for the bucket. It does not connect yet.
    pub fn object_store(&self) -> Result<Arc<dyn ObjectStore>, DatabaseError> {
        use slatedb::object_store::aws::AmazonS3Builder;

        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&self.bucket);
        if let Some(region) = &self.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key_id) = &self.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &self.session_token {
            builder = builder.with_token(session_token);
        }
        if self.allow_http {
            builder = builder.with_allow_http(true);
        }
        Ok(Arc::new(builder.build()?))
    }
}

// Leaves the credentials out of logs.
#[cfg(feature = "slatedb-s3")]
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let redacted = |secret: &Option<String>| secret.as_ref().map(|_| "<redacted>");
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &redacted(&self.secret_access_key))
            .field("session_token", &redacted(&self.session_token))
            .field("allow_http", &self.allow_http)
            .finish()
    }
}

impl Storage for SlateDbStorage {
    type Metrics = EmptyStorageMetrics;

    type TransactionType<'a>
        = SlateDbTransaction
    where
        Self: 'a;

    fn transaction_with_isolation(
        &self,
        isolation: TransactionIsolationLevel,
        _read_only: bool,
    ) -> Result<Self::TransactionType<'_>, DatabaseError> {
        self.validate_transaction_isolation(isolation)?;
        Ok(SlateDbTransaction {
            txn: self.db.begin(IsolationLevel::Snapshot)?,
            await_durable: self.await_durable,
            scan_options: self.scan_options,
            statements: 0,
        })
    }
}

pub struct SlateDbTransaction {
    txn: DbTransaction,
    await_durable: bool,
    scan_options: SlateDbScanOptions,
    statements: u64,
}

pub struct SlateDbIter {
    iter: DbIterator,
    // The entry returned last, which the returned slices borrow.
    current: Option<KeyValue>,
}

impl InnerIter for SlateDbIter {
    async fn try_next(&mut self) -> Result<Option<KeyValueRef<'_>>, DatabaseError> {
        self.current = self.iter.next().await?;
        Ok(self
            .current
            .as_ref()
            .map(|entry| (entry.key.as_ref(), entry.value.as_ref())))
    }
}

impl SlateDbTransaction {
    async fn scan(
        &self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        order: IterationOrder,
    ) -> Result<SlateDbIter, DatabaseError> {
        let range = (
            min.map(Bytes::copy_from_slice),
            max.map(Bytes::copy_from_slice),
        );
        let SlateDbScanOptions {
            read_ahead_bytes,
            max_fetch_tasks,
            cache_blocks,
        } = self.scan_options;
        let options = ScanOptions {
            order,
            read_ahead_bytes,
            max_fetch_tasks,
            cache_blocks,
            ..ScanOptions::default()
        };
        Ok(SlateDbIter {
            iter: self.txn.scan_with_options(range, &options).await?,
            current: None,
        })
    }
}

impl StorageTransaction for SlateDbTransaction {
    type BorrowedBytes<'a>
        = Bytes
    where
        Self: 'a;

    type IterType<'a>
        = SlateDbIter
    where
        Self: 'a;

    type RevIterType<'a>
        = SlateDbIter
    where
        Self: 'a;

    type PointCursor<'a>
        = ()
    where
        Self: 'a;

    fn next_statement_stamp(&mut self) -> Result<u64, DatabaseError> {
        const STATEMENT_BITS: u32 = 12;
        self.statements += 1;
        let sequence = self.txn.seqnum();
        if self.statements >= 1 << STATEMENT_BITS || sequence >= 1 << (u64::BITS - STATEMENT_BITS) {
            return Err(DatabaseError::InvalidValue(
                "statement stamps exhausted".into(),
            ));
        }
        Ok((sequence << STATEMENT_BITS) | self.statements)
    }

    fn point_cursor(&self) -> Result<Self::PointCursor<'_>, DatabaseError> {
        Ok(())
    }

    async fn get_borrowed(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError> {
        Ok(self.txn.get(key).await?)
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        Ok(self.txn.put(key, value)?)
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        Ok(self.txn.delete(key)?)
    }

    async fn range(
        &self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        _pool: &mut BytesPool,
    ) -> Result<SlateDbIter, DatabaseError> {
        self.scan(min, max, IterationOrder::Ascending).await
    }

    async fn range_rev(
        &self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        _pool: &mut BytesPool,
    ) -> Result<SlateDbIter, DatabaseError> {
        self.scan(min, max, IterationOrder::Descending).await
    }

    async fn commit(self) -> Result<(), DatabaseError> {
        let handle = self.txn.commit().await?;
        if let (Some(handle), true) = (handle, self.await_durable) {
            handle.await_durable().await?;
        }
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::serialize::BytesPool;
    use slatedb::object_store::memory::InMemory;
    use slatedb::ErrorKind;

    async fn open() -> Result<SlateDbStorage, DatabaseError> {
        let db = Db::open("kitesql-test", Arc::new(InMemory::new())).await?;
        Ok(SlateDbStorage::new(Arc::new(db)))
    }

    async fn collect(mut iter: SlateDbIter) -> Result<Vec<Vec<u8>>, DatabaseError> {
        let mut keys = Vec::new();
        while let Some((key, _)) = iter.try_next().await? {
            keys.push(key.to_vec());
        }
        Ok(keys)
    }

    #[tokio::test]
    async fn reads_own_writes_and_commits() -> Result<(), DatabaseError> {
        let storage = open().await?;
        let mut pool = BytesPool::default();
        let mut txn = storage.transaction()?;
        for key in [b"a", b"b", b"c"] {
            txn.set(key, b"v")?;
        }
        txn.remove(b"b")?;
        assert_eq!(txn.get_borrowed(b"a").await?.as_deref(), Some(&b"v"[..]));
        assert_eq!(txn.get_borrowed(b"b").await?, None);
        let iter = txn
            .range(Bound::Included(b"a"), Bound::Unbounded, &mut pool)
            .await?;
        assert_eq!(collect(iter).await?, [b"a".to_vec(), b"c".to_vec()]);
        let iter = txn
            .range_rev(Bound::Excluded(b"a"), Bound::Included(b"c"), &mut pool)
            .await?;
        assert_eq!(collect(iter).await?, [b"c".to_vec()]);
        txn.commit().await?;

        let txn = storage.transaction()?;
        assert_eq!(txn.get_borrowed(b"c").await?.as_deref(), Some(&b"v"[..]));
        Ok(())
    }

    // Scans of keys flushed to an SST read them back in order, whatever the read-ahead.
    #[tokio::test]
    async fn scans_read_flushed_ssts() -> Result<(), DatabaseError> {
        let db = Arc::new(Db::open("kitesql-test", Arc::new(InMemory::new())).await?);
        let keys = (0..2000u32)
            .map(|i| format!("{i:05}").into_bytes())
            .collect::<Vec<_>>();
        let mut txn = SlateDbStorage::new(db.clone()).transaction()?;
        for key in &keys {
            txn.set(key, &[7; 64])?;
        }
        txn.commit().await?;
        db.flush_with_options(slatedb::config::FlushOptions {
            flush_type: slatedb::config::FlushType::MemTable,
        })
        .await?;

        let mut pool = BytesPool::default();
        for scan_options in [
            SlateDbScanOptions::default(),
            SlateDbScanOptions {
                read_ahead_bytes: 1,
                max_fetch_tasks: 1,
                cache_blocks: false,
            },
        ] {
            let txn = SlateDbStorage::new(db.clone())
                .scan_options(scan_options)
                .transaction()?;
            let iter = txn
                .range(Bound::Unbounded, Bound::Unbounded, &mut pool)
                .await?;
            assert_eq!(collect(iter).await?, keys);
            let iter = txn
                .range_rev(Bound::Included(b"01000"), Bound::Unbounded, &mut pool)
                .await?;
            assert_eq!(
                collect(iter).await?,
                keys[1000..].iter().rev().cloned().collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    // A database with a disk tier reads back what it wrote, and closes its cache with it.
    #[tokio::test]
    async fn opens_with_a_disk_cache() -> Result<(), DatabaseError> {
        let dir = tempfile::tempdir()?;
        let cache = SlateDbCacheOptions {
            memory_bytes: Some(1 << 20),
            disk: Some(SlateDbDiskCache {
                dir: dir.path().join("cache"),
                capacity_bytes: 64 << 20,
            }),
            cache_compaction_output: true,
        };
        let storage =
            SlateDbStorage::open("kitesql-test", Arc::new(InMemory::new()), cache).await?;
        let mut txn = storage.transaction()?;
        txn.set(b"k", b"v")?;
        txn.commit().await?;
        storage
            .db()
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await?;
        let txn = storage.transaction()?;
        assert_eq!(txn.get_borrowed(b"k").await?.as_deref(), Some(&b"v"[..]));
        drop(txn);
        storage.close().await?;
        assert!(dir.path().join("cache").read_dir()?.next().is_some());
        Ok(())
    }

    #[tokio::test]
    async fn dropping_rolls_back() -> Result<(), DatabaseError> {
        let storage = open().await?;
        let mut txn = storage.transaction()?;
        txn.set(b"k", b"v")?;
        drop(txn);
        assert_eq!(storage.transaction()?.get_borrowed(b"k").await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn conflicting_commit_fails() -> Result<(), DatabaseError> {
        let storage = open().await?.await_durable(false);
        let mut first = storage.transaction()?;
        let mut second = storage.transaction()?;
        first.set(b"k", b"1")?;
        second.set(b"k", b"2")?;
        first.commit().await?;
        assert!(matches!(
            second.commit().await,
            Err(DatabaseError::SlateDB(err)) if err.kind() == ErrorKind::Transaction
        ));
        let txn = storage.transaction()?;
        assert_eq!(txn.get_borrowed(b"k").await?.as_deref(), Some(&b"1"[..]));
        Ok(())
    }

    #[cfg(feature = "slatedb-s3")]
    #[test]
    fn s3_config_builds_a_store_and_hides_secrets() -> Result<(), DatabaseError> {
        let config = S3Config {
            endpoint: Some("http://127.0.0.1:9000".into()),
            access_key_id: Some("key-id".into()),
            secret_access_key: Some("top-secret".into()),
            allow_http: true,
            ..S3Config::new("kitesql")
        };
        config.object_store()?;
        let debug = format!("{config:?}");
        assert!(debug.contains("key-id") && !debug.contains("top-secret"));
        Ok(())
    }

    #[tokio::test]
    async fn statement_stamps_are_distinct() -> Result<(), DatabaseError> {
        let storage = open().await?;
        let mut txn = storage.transaction()?;
        let first = txn.next_statement_stamp()?;
        let second = txn.next_statement_stamp()?;
        assert!(first != 0 && first != second);
        txn.set(b"k", b"v")?;
        txn.commit().await?;
        // A later transaction starts from a later snapshot, so its stamps differ too.
        let mut later = storage.transaction()?;
        assert!(![first, second].contains(&later.next_statement_stamp()?));
        Ok(())
    }
}
// GRCOV_EXCL_STOP
