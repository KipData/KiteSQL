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
    bounds_contain, bytes_bound_as_slice, InnerIter, KeyValueRef, Storage, StorageTransaction,
    TransactionIsolationLevel,
};
use crate::types::mapped::MappedScope;
use crate::types::serialize::BytesPool;
use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags, RoTransaction, RwTransaction};
use std::collections::Bound;
use std::fmt::{self, Display, Formatter};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn mdb_result(code: i32) -> Result<(), lmdb::Error> {
    if code == 0 {
        Ok(())
    } else {
        Err(lmdb::Error::from_err_code(code))
    }
}

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
        // Read transactions may outlive the statement call (result iterators) and
        // several may be open on one thread, so they must not be bound to TLS slots.
        builder.set_flags(config.flags | EnvironmentFlags::NO_TLS);
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
        = LmdbTransaction
    where
        Self: 'a;

    fn transaction_with_isolation(
        &self,
        isolation: TransactionIsolationLevel,
        read_only: bool,
    ) -> Result<Self::TransactionType<'_>, DatabaseError> {
        self.validate_transaction_isolation(isolation)?;
        let (tx, mapped) = if read_only {
            // SAFETY: a read-only transaction reads its snapshot of the memory map, which stays
            // mapped and unchanged until the transaction ends, and the scope is dropped first.
            let mapped = unsafe { MappedScope::new() };
            let ro_tx = self.env.begin_ro_txn()?;
            let raw =
                unsafe { std::mem::transmute::<RoTransaction<'_>, *mut lmdb_sys::MDB_txn>(ro_tx) };
            (LmdbTxn::Ro(raw), Some(mapped))
        } else {
            let rw_tx = self.env.begin_rw_txn()?;
            let raw =
                unsafe { std::mem::transmute::<RwTransaction<'_>, *mut lmdb_sys::MDB_txn>(rw_tx) };
            (LmdbTxn::Rw(raw), None)
        };
        Ok(LmdbTransaction {
            mapped,
            _env: self.env.clone(),
            tx: Some(tx),
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

pub struct LmdbTransaction {
    // Declared before `tx` so it is dropped before the snapshot its reads point into.
    mapped: Option<MappedScope>,
    _env: Arc<Environment>,
    tx: Option<LmdbTxn>,
    db: Database,
    statements: u64,
}

/// Read transactions do not take LMDB's single-writer lock.
enum LmdbTxn {
    Ro(*mut lmdb_sys::MDB_txn),
    Rw(*mut lmdb_sys::MDB_txn),
}

/// SAFETY: LMDB write transactions are thread-affine (bound to the thread that created them).
/// This implementation is safe only when:
/// 1. The transaction's futures are polled on a single thread (e.g., `block_on` on a connection thread)
/// 2. A work-stealing executor that moves write-txn futures across threads would be UNSAFE
///
/// Read transactions (NO_TLS flag set) may safely move between threads.
unsafe impl Send for LmdbTransaction {}
unsafe impl Sync for LmdbTransaction {}

impl Drop for LmdbTransaction {
    fn drop(&mut self) {
        if let Some(tx) = self.tx.take() {
            unsafe {
                lmdb_sys::mdb_txn_abort(tx.raw());
            }
        }
    }
}

impl LmdbTxn {
    fn raw(&self) -> *mut lmdb_sys::MDB_txn {
        match self {
            LmdbTxn::Ro(txn) | LmdbTxn::Rw(txn) => *txn,
        }
    }

    fn get(&self, db: Database, key: &[u8]) -> Result<&[u8], lmdb::Error> {
        let mut key_val = lmdb_sys::MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut std::ffi::c_void,
        };
        let mut data = lmdb_sys::MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        };
        unsafe {
            mdb_result(lmdb_sys::mdb_get(
                self.raw(),
                db.dbi(),
                &mut key_val,
                &mut data,
            ))?;
            Ok(std::slice::from_raw_parts(
                data.mv_data as *const u8,
                data.mv_size,
            ))
        }
    }

    unsafe fn open_ro_cursor_raw(
        &self,
        db: Database,
    ) -> Result<*mut lmdb_sys::MDB_cursor, lmdb::Error> {
        let mut cursor: *mut lmdb_sys::MDB_cursor = std::ptr::null_mut();
        mdb_result(lmdb_sys::mdb_cursor_open(self.raw(), db.dbi(), &mut cursor))?;
        Ok(cursor)
    }

    fn is_writable(&self) -> bool {
        matches!(self, LmdbTxn::Rw(_))
    }
}

pub struct LmdbIter {
    cursor: *mut lmdb_sys::MDB_cursor,
    scan: Scan,
}

unsafe impl Send for LmdbIter {}

pub struct PointCur(*mut lmdb_sys::MDB_cursor);

unsafe impl Send for PointCur {}
unsafe impl Sync for PointCur {}

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
    fn new(min: Bound<&[u8]>, max: Bound<&[u8]>, reverse: bool, pool: &mut BytesPool) -> Self {
        let mut owned = |bound: Bound<&[u8]>| bound.map(|key| pool.bytes(key));
        Self {
            step: Step::Seek,
            range: (owned(min), owned(max)),
            reverse,
        }
    }

    fn recycle(self, pool: &mut BytesPool) {
        let (min, max) = self.range;
        for bound in [min, max] {
            if let Bound::Included(bytes) | Bound::Excluded(bytes) = bound {
                pool.recycle_buffer(bytes);
            }
        }
    }

    fn before_start(&self, key: &[u8]) -> bool {
        let start = if self.reverse {
            bytes_bound_as_slice(&self.range.1)
        } else {
            bytes_bound_as_slice(&self.range.0)
        };
        if self.reverse {
            !bounds_contain(Bound::Unbounded, start, key)
        } else {
            !bounds_contain(start, Bound::Unbounded, key)
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        bounds_contain(
            bytes_bound_as_slice(&self.range.0),
            bytes_bound_as_slice(&self.range.1),
            key,
        )
    }

    fn next(
        &mut self,
        cursor: *mut lmdb_sys::MDB_cursor,
    ) -> Result<Option<KeyValueRef<'_>>, DatabaseError> {
        loop {
            let (key, op, positioning, is_seek, is_reverse) = {
                let start_bound = if matches!(self.step, Step::Seek) {
                    let min = bytes_bound_as_slice(&self.range.0);
                    let max = bytes_bound_as_slice(&self.range.1);
                    if self.reverse {
                        max
                    } else {
                        min
                    }
                } else {
                    Bound::Unbounded
                };
                let positioning = !matches!(self.step, Step::Move);
                let is_seek = matches!(self.step, Step::Seek);
                let is_reverse = self.reverse;

                let (key, op) = match self.step {
                    Step::Seek => match start_bound {
                        Bound::Included(key) | Bound::Excluded(key) => {
                            (Some(key), lmdb_sys::MDB_SET_RANGE)
                        }
                        Bound::Unbounded if is_reverse => (None, lmdb_sys::MDB_LAST),
                        Bound::Unbounded => (None, lmdb_sys::MDB_FIRST),
                    },
                    Step::Last => (None, lmdb_sys::MDB_LAST),
                    Step::Move if is_reverse => (None, lmdb_sys::MDB_PREV),
                    Step::Move => (None, lmdb_sys::MDB_NEXT),
                    Step::Done => return Ok(None),
                };
                (key, op, positioning, is_seek, is_reverse)
            };

            let Some(entry) = cursor_get(cursor, key, op)? else {
                self.step = if is_reverse && key.is_some() && is_seek {
                    Step::Last
                } else {
                    Step::Done
                };
                continue;
            };
            let (entry_key, entry_value) = entry;
            let contains = self.contains(entry_key);
            let before_start = positioning && self.before_start(entry_key);
            self.step = Step::Move;
            if contains {
                return Ok(Some((entry_key, entry_value)));
            }
            if !before_start {
                self.step = Step::Done;
            }
        }
    }
}

#[maybe_async]
impl InnerIter for LmdbIter {
    async fn try_next(&mut self) -> Result<Option<KeyValueRef<'_>>, DatabaseError> {
        self.scan.next(self.cursor)
    }

    fn recycle(self, pool: &mut BytesPool) {
        self.scan.recycle(pool);
        unsafe {
            lmdb_sys::mdb_cursor_close(self.cursor);
        }
    }
}

fn cursor_get(
    cursor: *mut lmdb_sys::MDB_cursor,
    key: Option<&[u8]>,
    op: lmdb_sys::MDB_cursor_op,
) -> Result<Option<KeyValueRef<'_>>, lmdb::Error> {
    let mut key_val = match key {
        Some(k) => lmdb_sys::MDB_val {
            mv_size: k.len(),
            mv_data: k.as_ptr() as *mut std::ffi::c_void,
        },
        None => lmdb_sys::MDB_val {
            mv_size: 0,
            mv_data: std::ptr::null_mut(),
        },
    };
    let mut data = lmdb_sys::MDB_val {
        mv_size: 0,
        mv_data: std::ptr::null_mut(),
    };
    unsafe {
        let rc = lmdb_sys::mdb_cursor_get(cursor, &mut key_val, &mut data, op);
        if rc == lmdb_sys::MDB_NOTFOUND {
            return Ok(None);
        }
        mdb_result(rc)?;
        let key_slice = std::slice::from_raw_parts(key_val.mv_data as *const u8, key_val.mv_size);
        let value_slice = std::slice::from_raw_parts(data.mv_data as *const u8, data.mv_size);
        Ok(Some((key_slice, value_slice)))
    }
}

#[maybe_async]
impl StorageTransaction for LmdbTransaction {
    type BorrowedBytes<'a>
        = &'a [u8]
    where
        Self: 'a;

    type IterType<'a>
        = LmdbIter
    where
        Self: 'a;

    type RevIterType<'a>
        = LmdbIter
    where
        Self: 'a;

    type PointCursor<'a>
        = PointCur
    where
        Self: 'a;
    fn next_statement_stamp(&mut self) -> Result<u64, DatabaseError> {
        const STATEMENT_BITS: u32 = 20;
        self.statements += 1;
        let txn_id = unsafe { lmdb_sys::mdb_txn_id(self.tx.as_ref().unwrap().raw()) } as u64;
        if self.statements >= 1 << STATEMENT_BITS || txn_id >= 1 << (u64::BITS - STATEMENT_BITS) {
            return Err(DatabaseError::InvalidValue(
                "statement stamps exhausted".into(),
            ));
        }
        Ok((txn_id << STATEMENT_BITS) | self.statements)
    }

    async fn get_borrowed<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError> {
        match self.tx.as_ref().unwrap().get(self.db, key) {
            Ok(value) => Ok(Some(value)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn mapped_scope(&self) -> Option<&MappedScope> {
        self.mapped.as_ref()
    }

    fn point_cursor(&self) -> Result<Self::PointCursor<'_>, DatabaseError> {
        unsafe {
            Ok(PointCur(
                self.tx.as_ref().unwrap().open_ro_cursor_raw(self.db)?,
            ))
        }
    }

    async fn get_at<'a>(
        &'a self,
        cursor: &Self::PointCursor<'a>,
        key: &[u8],
    ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError> {
        match cursor_get(cursor.0, Some(key), lmdb_sys::MDB_SET) {
            Ok(Some((_, value))) => unsafe {
                Ok(Some(std::mem::transmute::<&[u8], &'a [u8]>(value)))
            },
            Ok(None) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        let tx = self.tx.as_ref().unwrap();
        if !tx.is_writable() {
            return Err(DatabaseError::UnsupportedStmt(
                "cannot write in a read-only transaction".into(),
            ));
        }
        let mut key_val = lmdb_sys::MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut std::ffi::c_void,
        };
        let mut data = lmdb_sys::MDB_val {
            mv_size: value.len(),
            mv_data: value.as_ptr() as *mut std::ffi::c_void,
        };
        unsafe {
            mdb_result(lmdb_sys::mdb_put(
                tx.raw(),
                self.db.dbi(),
                &mut key_val,
                &mut data,
                0,
            ))?;
        }
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        let tx = self.tx.as_ref().unwrap();
        if !tx.is_writable() {
            return Err(DatabaseError::UnsupportedStmt(
                "cannot write in a read-only transaction".into(),
            ));
        }
        let mut key_val = lmdb_sys::MDB_val {
            mv_size: key.len(),
            mv_data: key.as_ptr() as *mut std::ffi::c_void,
        };
        unsafe {
            let rc = lmdb_sys::mdb_del(tx.raw(), self.db.dbi(), &mut key_val, std::ptr::null_mut());
            if rc == lmdb_sys::MDB_NOTFOUND {
                return Ok(());
            }
            mdb_result(rc)?;
        }
        Ok(())
    }

    async fn range<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
        pool: &mut BytesPool,
    ) -> Result<Self::IterType<'txn>, DatabaseError> {
        unsafe {
            Ok(LmdbIter {
                cursor: self.tx.as_ref().unwrap().open_ro_cursor_raw(self.db)?,
                scan: Scan::new(min, max, false, pool),
            })
        }
    }

    async fn range_rev<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
        pool: &mut BytesPool,
    ) -> Result<Self::RevIterType<'txn>, DatabaseError> {
        unsafe {
            Ok(LmdbIter {
                cursor: self.tx.as_ref().unwrap().open_ro_cursor_raw(self.db)?,
                scan: Scan::new(min, max, true, pool),
            })
        }
    }

    async fn remove_range(
        &mut self,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        pool: &mut BytesPool,
    ) -> Result<(), DatabaseError> {
        let tx = self.tx.as_ref().unwrap();
        if !tx.is_writable() {
            return Err(DatabaseError::UnsupportedStmt(
                "cannot write in a read-only transaction".into(),
            ));
        }
        unsafe {
            let cursor = tx.open_ro_cursor_raw(self.db)?;
            let mut scan = Scan::new(min, max, false, pool);
            while scan.next(cursor)?.is_some() {
                mdb_result(lmdb_sys::mdb_cursor_del(cursor, 0))?;
            }
            scan.recycle(pool);
            lmdb_sys::mdb_cursor_close(cursor);
        }
        Ok(())
    }

    async fn commit(mut self) -> Result<(), DatabaseError> {
        drop(self.mapped.take());
        let tx = self.tx.take().unwrap();
        unsafe {
            mdb_result(lmdb_sys::mdb_txn_commit(tx.raw()))?;
        }
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
#[maybe_async]
#[allow(unused_imports)]
mod tests {
    use super::{LmdbConfig, LmdbStorage};
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{Iter, ScanColumn, Storage, StorageTransaction, TupleDeserializers};
    use crate::types::tuple::Tuple;
    use crate::types::value::{DataValue, Utf8Type};
    use crate::types::{CharLengthUnits, LogicalType};
    use lmdb::EnvironmentFlags;
    use std::borrow::Cow;
    use tempfile::TempDir;

    #[sync_only]
    #[test]
    fn read_transactions_map_decoded_strings() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = LmdbStorage::new(temp_dir.path().join("kite_sql.lmdb")).unwrap();
        let isolation = storage.default_transaction_isolation();
        let types = [
            LogicalType::Varchar(None, CharLengthUnits::Characters),
            LogicalType::Char(4, CharLengthUnits::Characters),
        ];
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let columns = types
            .iter()
            .map(|ty| ScanColumn {
                column: arena.alloc_column(ColumnCatalog::new(
                    "c".to_string(),
                    false,
                    ColumnDesc::new(ty.clone(), None, false, None).unwrap(),
                )),
                deserializer: ty.serializable(),
                pinned: true,
            })
            .collect();
        let deserializers = TupleDeserializers {
            columns,
            total_len: types.len(),
            pk_ty: None,
        };
        let mut codec = TableCodec::default();
        let mut writer = storage.transaction().unwrap();
        let row = Tuple::new(
            Some(DataValue::Int32(1)),
            vec![
                DataValue::from("kite".to_string()),
                DataValue::Utf8 {
                    value: "db".into(),
                    ty: Utf8Type::Fixed(4),
                    unit: CharLengthUnits::Characters,
                },
            ],
        );
        let serializers = types.iter().map(LogicalType::serializable);
        writer
            .append_tuple(&mut codec, 0, &row.into(), serializers, false)
            .unwrap();
        writer.commit().unwrap();

        // Only a read-only transaction keeps its reads mapped until it ends.
        for read_only in [false, true] {
            let tx = storage
                .transaction_with_isolation(isolation, read_only)
                .unwrap();
            let mut iter = tx
                .read(&mut codec, &deserializers, 0, (None, None))
                .unwrap();
            let mut tuple = Tuple::default();
            assert!(iter.next_tuple_into(&mut codec, &mut tuple).unwrap());
            for (value, expected) in tuple.values.iter().zip(["kite", "db"]) {
                let DataValue::Utf8 { value, .. } = value else {
                    panic!("expected a string, got {value:?}")
                };
                assert_eq!(value, expected);
                assert_eq!(value.is_mapped(), read_only);
            }
        }
    }

    #[sync_only]
    #[test]
    fn transaction_rows_lend_strings_until_the_transaction_ends() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut db = DataBaseBuilder::path(temp_dir.path()).build_lmdb().unwrap();
        db.ddl("create table t (a int primary key, b varchar)")
            .unwrap();
        db.run("insert into t values (1, 'kite')")
            .unwrap()
            .done()
            .unwrap();

        // Only a read-only transaction's reads stay mapped; a computed string is always owned.
        for read_only in [false, true] {
            let mut tx = db.new_transaction(read_only).unwrap();
            let mut iter = tx.run("select b, upper(b) from t").unwrap();
            let (b, upper) = iter
                .next_tuple(|_, row| {
                    let b = row.take_txn_value(0).unwrap().into_str().unwrap();
                    let upper = row.take_txn_value(1).unwrap().into_str().unwrap();
                    (b, upper)
                })
                .unwrap()
                .unwrap();
            iter.done().unwrap();
            assert_eq!((b.as_ref(), upper.as_ref()), ("kite", "KITE"));
            assert_eq!(matches!(b, Cow::Borrowed(_)), read_only);
            assert!(matches!(upper, Cow::Owned(_)));
            drop((b, upper));
            tx.commit().unwrap();
        }
    }

    #[sync_only]
    #[test]
    fn rows_from_read_transactions_outlive_them() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut db = DataBaseBuilder::path(temp_dir.path()).build_lmdb().unwrap();
        db.ddl("create table t (a int primary key, b varchar)")
            .unwrap();
        db.run("insert into t values (1, 'kite')")
            .unwrap()
            .done()
            .unwrap();

        let mut reader = db.new_transaction(true).unwrap();
        let mut iter = reader.run("select b from t").unwrap();
        let row = iter.next_tuple(|_, row| row.take()).unwrap().unwrap();
        iter.done().unwrap();
        reader.commit().unwrap();
        let DataValue::Utf8 { value, .. } = &row[0] else {
            panic!("expected a string")
        };
        assert!(!value.is_mapped());
        assert_eq!(value, "kite");
    }

    #[sync_only]
    #[test]
    fn materializing_operators_keep_strings_mapped_across_executions() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut db = DataBaseBuilder::path(temp_dir.path()).build_lmdb().unwrap();
        db.ddl("create table t (a int primary key, b varchar, c int)")
            .unwrap();
        db.ddl("create table u (a int primary key, b varchar)")
            .unwrap();
        db.run("insert into t values (1, 'kite', 1), (2, 'db', 1), (3, 'kite', 2)")
            .unwrap()
            .done()
            .unwrap();
        db.run("insert into u values (1, 'kite')")
            .unwrap()
            .done()
            .unwrap();

        // Each query keeps strings in an operator: grouping, distinct, sort and join keys, joined
        // rows of a hash and a nested-loop join, an aggregate and a window result, an `IN`
        // subquery's materialized keys and a set operation's right rows. Every string a query
        // returns must still point into the read; strings an operator only compares, such as the
        // right rows, are covered by the reruns below.
        let queries = [
            "select b from t group by b order by b",
            "select distinct b from t order by b",
            "select max(b) from t",
            "select t.b, u.b from t join u on t.b = u.b order by t.a",
            "select max(b) over (partition by c) from t order by a",
            "select b from t where b in (select b from u) order by a",
            "select b from (select b from t intersect select b from u) s order by b",
            "select t.b, u.b from t join u on t.b >= u.b order by t.a, u.a",
        ];
        let mut programs = queries
            .iter()
            .map(|sql| db.program_sql(sql, &[]).unwrap())
            .collect::<Vec<_>>();
        let rounds: [(&[&str], [&[&str]; 8]); 2] = [
            (
                &[],
                [
                    &["db", "kite"],
                    &["db", "kite"],
                    &["kite"],
                    &["kite", "kite"],
                    &["kite", "kite", "kite"],
                    &["kite", "kite"],
                    &["kite"],
                    &["kite", "kite"],
                ],
            ),
            (
                &[
                    "insert into t values (4, 'lmdb', 2)",
                    "update t set b = 'sql' where a = 2",
                    "insert into u values (2, 'lmdb')",
                ],
                [
                    &["kite", "lmdb", "sql"],
                    &["kite", "lmdb", "sql"],
                    &["sql"],
                    &["kite", "kite", "lmdb"],
                    &["sql", "sql", "lmdb", "lmdb"],
                    &["kite", "kite", "lmdb"],
                    &["kite", "lmdb"],
                    &["kite", "sql", "sql", "kite", "lmdb", "lmdb"],
                ],
            ),
        ];

        // Every round runs the same programs in a new read-only transaction after writes, so a
        // string kept from the previous execution would be read after its transaction ended,
        // which debug builds catch.
        for (writes, expected) in rounds {
            for write in writes {
                db.run(write).unwrap().done().unwrap();
            }
            let mut tx = db.new_transaction(true).unwrap();
            for ((program, sql), expected) in programs.iter_mut().zip(queries).zip(expected) {
                let mut iter = program.execute(&mut tx, []).unwrap();
                let mut rows = Vec::new();
                while let Some(row) = iter
                    .next_tuple(|_, row| {
                        for value in row.iter() {
                            let DataValue::Utf8 { value, .. } = value else {
                                panic!("{sql}: expected a string, got {value:?}")
                            };
                            assert!(value.is_mapped(), "{sql}: {value} was copied");
                        }
                        row[0].to_string()
                    })
                    .unwrap()
                {
                    rows.push(row);
                }
                iter.done().unwrap();
                assert_eq!(rows, expected, "{sql}");
            }
            tx.commit().unwrap();
        }
    }

    #[sync_only]
    #[test]
    fn lmdb_backend_smoke() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let mut kite_sql = DataBaseBuilder::path(db_path).build_lmdb().unwrap();

        kite_sql
            .ddl("create table t1 (a int primary key, b int)")
            .unwrap();
        kite_sql.load(CatalogKind::Table("t1".to_string())).unwrap();
        kite_sql
            .run("insert into t1 values (1, 10), (2, 20), (3, 30)")
            .unwrap()
            .done()
            .unwrap();

        let mut iter = kite_sql.run("select b from t1 where a = 2").unwrap();
        let tuple = iter.next_tuple(|_, tuple| tuple.clone()).unwrap().unwrap();
        assert_eq!(tuple[0].to_string(), "20");
        iter.done().unwrap();
    }

    #[test]
    async fn lmdb_remove_range() {
        use crate::storage::Storage;
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = LmdbStorage::new(temp_dir.path().join("kite_sql.lmdb")).unwrap();
        let mut transaction = storage.transaction().unwrap();
        crate::storage::check_remove_range(&mut transaction)
            .await
            .unwrap();
    }

    #[test]
    async fn lmdb_range_rev_matches_range() {
        use crate::storage::Storage;
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = LmdbStorage::new(temp_dir.path().join("kite_sql.lmdb")).unwrap();
        let mut transaction = storage.transaction().unwrap();
        crate::storage::check_range_rev_matches_range(&mut transaction)
            .await
            .unwrap();
    }

    #[test]
    async fn explicit_transaction_does_not_rescan_own_writes() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db = DataBaseBuilder::path(temp_dir.path()).build_lmdb().unwrap();
        crate::storage::check_explicit_transaction_does_not_rescan_own_writes(db)
            .await
            .unwrap();
    }

    #[sync_only]
    #[test]
    fn read_transactions_skip_the_writer_lock() {
        use crate::storage::{Storage, StorageTransaction};
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = LmdbStorage::new(temp_dir.path().join("kite_sql.lmdb")).unwrap();
        let isolation = storage.default_transaction_isolation();

        // A writer holds LMDB's single write lock; readers still open on the same thread.
        let mut writer = storage.transaction().unwrap();
        writer.set(b"k", b"v").unwrap();
        let mut reader = storage.transaction_with_isolation(isolation, true).unwrap();
        let second = storage.transaction_with_isolation(isolation, true).unwrap();
        assert!(reader.get_borrowed(b"k").unwrap().is_none());
        assert!(second.get_borrowed(b"k").unwrap().is_none());
        assert!(reader.set(b"k", b"x").is_err());
        assert!(reader.remove(b"k").is_err());
        writer.commit().unwrap();
        drop((reader, second));

        let reader = storage.transaction_with_isolation(isolation, true).unwrap();
        assert_eq!(reader.get_borrowed(b"k").unwrap(), Some(&b"v"[..]));
    }

    #[sync_only]
    #[test]
    fn read_statements_and_plans_use_read_transactions() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut db = DataBaseBuilder::path(temp_dir.path()).build_lmdb().unwrap();
        db.ddl("create table t (a int primary key)").unwrap();
        db.run("insert into t values (1)").unwrap().done().unwrap();

        // An open write transaction does not block reads.
        let mut writer = db.new_transaction(false).unwrap();
        writer
            .run("insert into t values (2)")
            .unwrap()
            .done()
            .unwrap();
        let mut select = db.program_sql("select count(*) from t", &[]).unwrap();
        let mut insert = db.program_sql("insert into t values (3)", &[]).unwrap();
        assert!(select.read_only() && !insert.read_only());
        let mut first = db.run("select a from t").unwrap();
        let mut snapshot = db.new_transaction(true).unwrap();
        let mut second = select.execute(&mut snapshot, []).unwrap();
        let count = second.next_tuple(|_, t| t[0].to_string()).unwrap();
        assert_eq!(count.as_deref(), Some("1"));
        assert!(first.next_tuple(|_, _| ()).unwrap().is_some());
        // An explicit read-only transaction reads beside the writer and rejects writes.
        let mut reader = db.new_transaction(true).unwrap();
        assert!(reader.run("select a from t").unwrap().done().is_ok());
        assert!(reader.run("insert into t values (4)").is_err());
        assert!(insert.execute(&mut reader, []).is_err());
        drop((first, second, reader));
        drop(snapshot);
        writer.commit().unwrap();
    }

    #[sync_only]
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

    #[sync_only]
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
            .load(CatalogKind::Table("t_metrics".to_string()))
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

    #[sync_only]
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
// GRCOV_EXCL_STOP
