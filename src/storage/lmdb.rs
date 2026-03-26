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
use crate::storage::{reuse_bound_as_excluded, InnerIter, Storage, Transaction};
use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, RoCursor, RwTransaction,
    Transaction as _, WriteFlags,
};
use std::cmp::Ordering;
use std::collections::Bound;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

const DEFAULT_MAP_SIZE: usize = 16 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LmdbConfig {
    pub enable_statistics: bool,
    pub map_size: usize,
    pub flags: EnvironmentFlags,
    pub max_readers: Option<u32>,
    pub max_dbs: Option<u32>,
}

impl Default for LmdbConfig {
    fn default() -> Self {
        Self {
            enable_statistics: false,
            map_size: DEFAULT_MAP_SIZE,
            flags: EnvironmentFlags::empty(),
            max_readers: None,
            max_dbs: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LmdbMetrics {
    pub map_size: usize,
    pub page_size: u32,
    pub depth: u32,
    pub branch_pages: usize,
    pub leaf_pages: usize,
    pub overflow_pages: usize,
    pub entries: usize,
}

impl Display for LmdbMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "<LMDB Metrics>")?;
        writeln!(
            f,
            "map_size={} page_size={} depth={}",
            self.map_size, self.page_size, self.depth
        )?;
        write!(
            f,
            "branch_pages={} leaf_pages={} overflow_pages={} entries={}",
            self.branch_pages, self.leaf_pages, self.overflow_pages, self.entries
        )
    }
}

#[derive(Clone)]
pub struct LmdbStorage {
    env: Arc<Environment>,
    db: Database,
    config: LmdbConfig,
}

impl LmdbStorage {
    pub fn new(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        Self::with_config(path, LmdbConfig::default())
    }

    pub fn with_config(
        path: impl Into<PathBuf> + Send,
        config: LmdbConfig,
    ) -> Result<Self, DatabaseError> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut builder = Environment::new();
        builder.set_map_size(config.map_size);
        builder.set_flags(config.flags);
        if let Some(max_readers) = config.max_readers {
            builder.set_max_readers(max_readers);
        }
        if let Some(max_dbs) = config.max_dbs {
            builder.set_max_dbs(max_dbs);
        }
        let env = builder.open(&path).map_err(map_lmdb_err)?;
        let db = env
            .create_db(None, DatabaseFlags::empty())
            .map_err(map_lmdb_err)?;

        Ok(Self {
            env: Arc::new(env),
            db,
            config,
        })
    }
}

impl Storage for LmdbStorage {
    type Metrics = LmdbMetrics;

    type TransactionType<'a>
        = LmdbTransaction<'a>
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError> {
        let tx = self.env.begin_rw_txn().map_err(map_lmdb_err)?;

        Ok(LmdbTransaction {
            tx,
            db: self.db,
            table_codec: Default::default(),
        })
    }

    fn metrics(&self) -> Option<Self::Metrics> {
        if !self.config.enable_statistics {
            return None;
        }
        let stat = self.env.stat().ok()?;

        Some(LmdbMetrics {
            map_size: self.config.map_size,
            page_size: stat.page_size(),
            depth: stat.depth(),
            branch_pages: stat.branch_pages(),
            leaf_pages: stat.leaf_pages(),
            overflow_pages: stat.overflow_pages(),
            entries: stat.entries(),
        })
    }
}

pub struct LmdbTransaction<'env> {
    tx: RwTransaction<'env>,
    db: Database,
    table_codec: TableCodec,
}

pub struct LmdbIter<'txn> {
    _cursor: RoCursor<'txn>,
    iter: lmdb::Iter<'txn>,
    pending: Option<(&'txn [u8], &'txn [u8])>,
    max: Bound<Bytes>,
    done: bool,
}

impl LmdbIter<'_> {
    fn next_visible(&mut self) -> Option<(&[u8], &[u8])> {
        if let Some(entry) = self.pending.take() {
            if within_upper_bound(entry.0, &self.max) {
                return Some(entry);
            }
            self.done = true;
            return None;
        }

        while let Some((key, value)) = self.iter.next() {
            if !within_upper_bound(key, &self.max) {
                self.done = true;
                return None;
            }
            return Some((key, value));
        }

        self.done = true;
        None
    }
}

impl InnerIter for LmdbIter<'_> {
    fn try_next(&mut self) -> Result<Option<(&[u8], &[u8])>, DatabaseError> {
        if self.done {
            return Ok(None);
        }
        Ok(self.next_visible())
    }
}

impl Transaction for LmdbTransaction<'_> {
    type BorrowedBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    type IterType<'a>
        = LmdbIter<'a>
    where
        Self: 'a;

    fn table_codec(&self) -> *const TableCodec {
        &self.table_codec
    }

    fn get_borrowed<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError> {
        match self.tx.get(self.db, &key) {
            Ok(value) => Ok(Some(value)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(map_lmdb_err(err)),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.tx
            .put(self.db, &key, &value, lmdb::WriteFlags::empty())
            .map_err(map_lmdb_err)?;
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        match self.tx.del(self.db, &key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(map_lmdb_err(err)),
        }
    }

    fn range<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
    ) -> Result<Self::IterType<'txn>, DatabaseError> {
        let mut cursor = self.tx.open_ro_cursor(self.db).map_err(map_lmdb_err)?;
        let (pending, done) = initial_entry(&mut cursor, &min).map_err(map_lmdb_err)?;
        let iter = cursor.iter();

        Ok(LmdbIter {
            _cursor: cursor,
            iter,
            pending,
            max: owned_bound(max),
            done,
        })
    }

    fn remove_range(&mut self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Result<(), DatabaseError> {
        let mut cursor = self.tx.open_rw_cursor(self.db).map_err(map_lmdb_err)?;
        let upper = owned_bound(max);
        let mut lower = owned_bound(min);
        let mut seek_key = Bytes::new();

        loop {
            let entry = cursor_seek(&mut cursor, &lower, &mut seek_key).map_err(map_lmdb_err)?;
            let Some((key, _)) = entry else {
                return Ok(());
            };
            if !within_upper_bound(key, &upper) {
                return Ok(());
            }

            reuse_bound_as_excluded(&mut lower, key);
            cursor.del(WriteFlags::empty()).map_err(map_lmdb_err)?;
        }
    }

    fn commit(self) -> Result<(), DatabaseError> {
        self.tx.commit().map_err(map_lmdb_err)?;
        Ok(())
    }
}

fn initial_entry<'txn>(
    cursor: &mut RoCursor<'txn>,
    min: &Bound<&[u8]>,
) -> Result<(Option<(&'txn [u8], &'txn [u8])>, bool), lmdb::Error> {
    match min {
        Bound::Unbounded => Ok((None, false)),
        Bound::Included(min) => match cursor.get(Some(*min), None, lmdb_sys::MDB_SET_RANGE) {
            Ok((key, value)) => Ok((Some((key.unwrap_or_default(), value)), false)),
            Err(lmdb::Error::NotFound) => Ok((None, true)),
            Err(err) => Err(err),
        },
        Bound::Excluded(min) => match cursor.get(Some(*min), None, lmdb_sys::MDB_SET_RANGE) {
            Ok((key, value)) => {
                let key = key.unwrap_or_default();
                if key == *min {
                    Ok((None, false))
                } else {
                    Ok((Some((key, value)), false))
                }
            }
            Err(lmdb::Error::NotFound) => Ok((None, true)),
            Err(err) => Err(err),
        },
    }
}

fn cursor_seek<'txn>(
    cursor: &mut lmdb::RwCursor<'txn>,
    lower: &Bound<Bytes>,
    seek_key: &mut Bytes,
) -> Result<Option<(&'txn [u8], &'txn [u8])>, lmdb::Error> {
    match lower {
        Bound::Unbounded => match cursor.get(None, None, lmdb_sys::MDB_FIRST) {
            Ok((key, value)) => Ok(Some((key.unwrap_or_default(), value))),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err),
        },
        Bound::Included(min) => {
            match cursor.get(Some(min.as_slice()), None, lmdb_sys::MDB_SET_RANGE) {
                Ok((key, value)) => Ok(Some((key.unwrap_or_default(), value))),
                Err(lmdb::Error::NotFound) => Ok(None),
                Err(err) => Err(err),
            }
        }
        Bound::Excluded(min) => {
            seek_key.clear();
            seek_key.extend_from_slice(min.as_slice());
            seek_key.push(0);
            match cursor.get(Some(seek_key.as_slice()), None, lmdb_sys::MDB_SET_RANGE) {
                Ok((key, value)) => Ok(Some((key.unwrap_or_default(), value))),
                Err(lmdb::Error::NotFound) => Ok(None),
                Err(err) => Err(err),
            }
        }
    }
}

fn owned_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.to_vec()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn within_upper_bound(key: &[u8], max: &Bound<Bytes>) -> bool {
    match max {
        Bound::Included(max) => key.cmp(max.as_slice()) != Ordering::Greater,
        Bound::Excluded(max) => key.cmp(max.as_slice()) == Ordering::Less,
        Bound::Unbounded => true,
    }
}

fn map_lmdb_err(err: impl std::fmt::Display) -> DatabaseError {
    DatabaseError::InvalidValue(format!("lmdb: {err}"))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{LmdbConfig, LmdbStorage};
    use crate::db::DataBaseBuilder;
    use lmdb::EnvironmentFlags;
    use tempfile::TempDir;

    #[test]
    fn lmdb_backend_smoke() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let kite_sql = DataBaseBuilder::path(db_path).build_lmdb().unwrap();

        kite_sql
            .run("create table t1 (a int primary key, b int)")
            .unwrap()
            .done()
            .unwrap();
        kite_sql
            .run("insert into t1 values (1, 10), (2, 20), (3, 30)")
            .unwrap()
            .done()
            .unwrap();

        let mut iter = kite_sql.run("select b from t1 where a = 2").unwrap();
        let tuple = iter.next().unwrap().unwrap();
        assert_eq!(tuple.values[0].to_string(), "20");
        iter.done().unwrap();
    }

    #[test]
    fn build_with_lmdb_storage() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let storage = LmdbStorage::new(db_path).unwrap();
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .build_with_storage(storage)
            .unwrap();

        kite_sql
            .run("create table t1 (a int primary key)")
            .unwrap()
            .done()
            .unwrap();
    }

    #[test]
    fn collect_lmdb_metrics_snapshot() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let kite_sql = DataBaseBuilder::path(db_path)
            .storage_statistics(true)
            .lmdb_flags(EnvironmentFlags::NO_SYNC)
            .lmdb_map_size(64 * 1024 * 1024)
            .build_lmdb()
            .unwrap();

        kite_sql
            .run("create table t_metrics (a int primary key, b int)")
            .unwrap()
            .done()
            .unwrap();
        kite_sql
            .run("insert into t_metrics values (1, 10), (2, 20), (3, 30)")
            .unwrap()
            .done()
            .unwrap();

        let metrics = kite_sql.storage_metrics().unwrap();
        assert_eq!(metrics.map_size, 64 * 1024 * 1024);
        assert!(metrics.entries > 0);
    }

    #[test]
    fn build_lmdb_with_config() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let storage = LmdbStorage::with_config(
            db_path,
            LmdbConfig {
                map_size: 32 * 1024 * 1024,
                flags: EnvironmentFlags::NO_SYNC,
                ..LmdbConfig::default()
            },
        )
        .unwrap();
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .build_with_storage(storage)
            .unwrap();

        kite_sql
            .run("create table t1 (a int primary key)")
            .unwrap()
            .done()
            .unwrap();
    }
}
