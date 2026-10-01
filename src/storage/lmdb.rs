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
use crate::storage::table_codec::Bytes;
use crate::storage::{
    bytes_bound_as_slice, owned_bound, InnerIter, KeyValueRef, Storage, Transaction,
    TransactionIsolationLevel,
};
use lmdb::{
    Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, RoCursor, RwTransaction,
    Transaction as _, WriteFlags,
};
use std::collections::Bound;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::ops::RangeBounds;
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
        let env = builder.open(&path)?;
        let db = env.create_db(None, DatabaseFlags::empty())?;

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

    fn transaction_with_isolation(
        &self,
        isolation: TransactionIsolationLevel,
    ) -> Result<Self::TransactionType<'_>, DatabaseError> {
        self.validate_transaction_isolation(isolation)?;
        let tx = self.env.begin_rw_txn()?;

        Ok(LmdbTransaction {
            tx,
            db: self.db,
            statements: 0,
        })
    }

    fn default_transaction_isolation(&self) -> TransactionIsolationLevel {
        TransactionIsolationLevel::RepeatableRead
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
    statements: u64,
}

pub struct LmdbIter<'txn> {
    cursor: RoCursor<'txn>,
    scan: Scan,
}

struct Scan {
    step: Step,
    range: BytesRange,
    reverse: bool,
}

enum Step {
    Seek,
    Last,
    Move,
    Done,
}

type BytesRange = (Bound<Bytes>, Bound<Bytes>);

impl Scan {
    fn new(min: Bound<&[u8]>, max: Bound<&[u8]>, reverse: bool) -> Self {
        Self {
            step: Step::Seek,
            range: (owned_bound(min), owned_bound(max)),
            reverse,
        }
    }

    fn start(&self) -> Bound<&[u8]> {
        if self.reverse {
            bytes_bound_as_slice(&self.range.1)
        } else {
            bytes_bound_as_slice(&self.range.0)
        }
    }

    fn before_start(&self, key: &[u8]) -> bool {
        if self.reverse {
            !(Bound::Unbounded, self.start()).contains(key)
        } else {
            !(self.start(), Bound::Unbounded).contains(key)
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        (
            bytes_bound_as_slice(&self.range.0),
            bytes_bound_as_slice(&self.range.1),
        )
            .contains(key)
    }

    fn next<'txn, C: Cursor<'txn>>(
        &mut self,
        cursor: &C,
    ) -> Result<Option<KeyValueRef<'txn>>, DatabaseError> {
        loop {
            let (key, op) = match self.step {
                Step::Seek => match self.start() {
                    Bound::Included(key) | Bound::Excluded(key) => {
                        (Some(key), lmdb_sys::MDB_SET_RANGE)
                    }
                    Bound::Unbounded if self.reverse => (None, lmdb_sys::MDB_LAST),
                    Bound::Unbounded => (None, lmdb_sys::MDB_FIRST),
                },
                Step::Last => (None, lmdb_sys::MDB_LAST),
                Step::Move if self.reverse => (None, lmdb_sys::MDB_PREV),
                Step::Move => (None, lmdb_sys::MDB_NEXT),
                Step::Done => return Ok(None),
            };
            let positioning = !matches!(self.step, Step::Move);
            let Some(entry) = cursor_get(cursor, key, op)? else {
                self.step = if self.reverse && key.is_some() && matches!(self.step, Step::Seek) {
                    Step::Last
                } else {
                    Step::Done
                };
                continue;
            };
            self.step = Step::Move;
            if self.contains(entry.0) {
                return Ok(Some(entry));
            }
            if !(positioning && self.before_start(entry.0)) {
                self.step = Step::Done;
            }
        }
    }
}

impl InnerIter for LmdbIter<'_> {
    fn try_next(&mut self) -> Result<Option<KeyValueRef<'_>>, DatabaseError> {
        self.scan.next(&self.cursor)
    }
}

fn cursor_get<'txn, C: Cursor<'txn>>(
    cursor: &C,
    key: Option<&[u8]>,
    op: lmdb_sys::MDB_cursor_op,
) -> Result<Option<KeyValueRef<'txn>>, lmdb::Error> {
    match cursor.get(key, None, op) {
        Ok((key, value)) => Ok(Some((key.unwrap_or_default(), value))),
        Err(lmdb::Error::NotFound) => Ok(None),
        Err(err) => Err(err),
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

    type RevIterType<'a>
        = LmdbIter<'a>
    where
        Self: 'a;
    fn next_statement_stamp(&mut self) -> Result<u64, DatabaseError> {
        const STATEMENT_BITS: u32 = 20;
        self.statements += 1;
        // SAFETY: `self.tx` is a live transaction handle owned by this value.
        let txn_id = unsafe { lmdb_sys::mdb_txn_id(self.tx.txn()) } as u64;
        if self.statements >= 1 << STATEMENT_BITS || txn_id >= 1 << (u64::BITS - STATEMENT_BITS) {
            return Err(DatabaseError::InvalidValue(
                "statement stamps exhausted".into(),
            ));
        }
        Ok((txn_id << STATEMENT_BITS) | self.statements)
    }

    fn get_borrowed<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError> {
        match self.tx.get(self.db, &key) {
            Ok(value) => Ok(Some(value)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.tx
            .put(self.db, &key, &value, lmdb::WriteFlags::empty())?;
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        match self.tx.del(self.db, &key, None) {
            Ok(()) | Err(lmdb::Error::NotFound) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    fn range<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
    ) -> Result<Self::IterType<'txn>, DatabaseError> {
        Ok(LmdbIter {
            cursor: self.tx.open_ro_cursor(self.db)?,
            scan: Scan::new(min, max, false),
        })
    }

    fn range_rev<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
    ) -> Result<Self::RevIterType<'txn>, DatabaseError> {
        Ok(LmdbIter {
            cursor: self.tx.open_ro_cursor(self.db)?,
            scan: Scan::new(min, max, true),
        })
    }

    fn remove_range(&mut self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Result<(), DatabaseError> {
        let mut cursor = self.tx.open_rw_cursor(self.db)?;
        let mut scan = Scan::new(min, max, false);
        while scan.next(&cursor)?.is_some() {
            cursor.del(WriteFlags::empty())?;
        }
        Ok(())
    }

    fn commit(self) -> Result<(), DatabaseError> {
        self.tx.commit()?;
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{LmdbConfig, LmdbStorage};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use lmdb::EnvironmentFlags;
    use tempfile::TempDir;

    #[test]
    fn lmdb_backend_smoke() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let mut kite_sql = DataBaseBuilder::path(db_path).build_lmdb().unwrap();

        kite_sql
            .ddl("create table t1 (a int primary key, b int)")
            .unwrap();
        kite_sql
            .load(CatalogKind::Table("t1".to_string().into()))
            .unwrap();
        kite_sql
            .run("insert into t1 values (1, 10), (2, 20), (3, 30)")
            .unwrap()
            .done()
            .unwrap();

        let mut iter = kite_sql.run("select b from t1 where a = 2").unwrap();
        let tuple = iter.next_tuple(|_, tuple| tuple.clone()).unwrap().unwrap();
        assert_eq!(tuple.values[0].to_string(), "20");
        iter.done().unwrap();
    }

    #[test]
    fn lmdb_remove_range() {
        use crate::storage::Storage;
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = LmdbStorage::new(temp_dir.path().join("kite_sql.lmdb")).unwrap();
        let mut transaction = storage.transaction().unwrap();
        crate::storage::check_remove_range(&mut transaction).unwrap();
    }

    #[test]
    fn lmdb_range_rev_matches_range() {
        use crate::storage::Storage;
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = LmdbStorage::new(temp_dir.path().join("kite_sql.lmdb")).unwrap();
        let mut transaction = storage.transaction().unwrap();
        crate::storage::check_range_rev_matches_range(&mut transaction).unwrap();
    }

    #[test]
    fn explicit_transaction_does_not_rescan_own_writes() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db = DataBaseBuilder::path(temp_dir.path()).build_lmdb().unwrap();
        crate::storage::check_explicit_transaction_does_not_rescan_own_writes(db).unwrap();
    }

    #[test]
    fn build_with_lmdb_storage() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let storage = LmdbStorage::new(db_path).unwrap();
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path())
            .build_with_storage(storage)
            .unwrap();

        kite_sql.ddl("create table t1 (a int primary key)").unwrap();
    }

    #[test]
    fn collect_lmdb_metrics_snapshot() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let mut kite_sql = DataBaseBuilder::path(db_path)
            .storage_statistics(true)
            .lmdb_flags(EnvironmentFlags::NO_SYNC)
            .lmdb_map_size(64 * 1024 * 1024)
            .build_lmdb()
            .unwrap();

        kite_sql
            .ddl("create table t_metrics (a int primary key, b int)")
            .unwrap();
        kite_sql
            .load(CatalogKind::Table("t_metrics".to_string().into()))
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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path())
            .build_with_storage(storage)
            .unwrap();

        kite_sql.ddl("create table t1 (a int primary key)").unwrap();
    }
}
