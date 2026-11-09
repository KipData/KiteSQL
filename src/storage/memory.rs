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

//! An in-memory storage for tests, tools and wasm. A transaction works on a snapshot of the map
//! taken when it begins, copied on its first write, and publishes it on commit; dropping it
//! rolls back. Concurrent writers are last-writer-wins, so it does not suit concurrent
//! workloads. On the async path every read suspends once before completing, so the engine meets
//! a real `Pending` on each read like it would against a remote store.

use crate::dual::dual;
use crate::errors::DatabaseError;
use crate::storage::table_codec::Bytes;
use crate::storage::{
    bytes_bound_as_slice, EmptyStorageMetrics, InnerIter, KeyValueRef, Storage, StorageTransaction,
    TransactionIsolationLevel,
};
use crate::types::serialize::BytesPool;
use std::collections::{BTreeMap, Bound};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

type Map = BTreeMap<Bytes, Arc<[u8]>>;

#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Arc<Mutex<Arc<Map>>>,
    statements: Arc<AtomicU64>,
    suspensions: Arc<AtomicU64>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of reads that suspended so far; always 0 on the sync path.
    pub fn suspensions(&self) -> u64 {
        self.suspensions.load(Ordering::Relaxed)
    }
}

impl Storage for MemoryStorage {
    type Metrics = EmptyStorageMetrics;

    type TransactionType<'a>
        = MemoryTransaction
    where
        Self: 'a;

    fn transaction_with_isolation(
        &self,
        isolation: TransactionIsolationLevel,
        _read_only: bool,
    ) -> Result<Self::TransactionType<'_>, DatabaseError> {
        self.validate_transaction_isolation(isolation)?;
        let snapshot = self
            .inner
            .lock()
            .expect("memory storage lock poisoned")
            .clone();
        Ok(MemoryTransaction {
            storage: self.clone(),
            map: Mutex::new(snapshot),
        })
    }
}

pub struct MemoryTransaction {
    storage: MemoryStorage,
    // Read through `&self` by open iterators while the executor writes through `&mut self`.
    map: Mutex<Arc<Map>>,
}

impl MemoryTransaction {
    fn map(&self) -> MutexGuard<'_, Arc<Map>> {
        self.map.lock().expect("memory transaction lock poisoned")
    }

    fn map_mut(&mut self) -> &mut Map {
        Arc::make_mut(
            self.map
                .get_mut()
                .expect("memory transaction lock poisoned"),
        )
    }
}

pub struct MemoryIter<'txn> {
    txn: &'txn MemoryTransaction,
    range: (Bound<Bytes>, Bound<Bytes>),
    reverse: bool,
    // The last entry returned. The next read resumes after its key, so writes made while the
    // iterator is open are seen; the value is a shared copy, so later writes cannot change it.
    current: Option<(Bytes, Arc<[u8]>)>,
    done: bool,
}

impl<'txn> MemoryIter<'txn> {
    fn new(
        txn: &'txn MemoryTransaction,
        min: Bound<&[u8]>,
        max: Bound<&[u8]>,
        reverse: bool,
        pool: &mut BytesPool,
    ) -> Self {
        let mut owned = |bound: Bound<&[u8]>| bound.map(|key| pool.bytes(key));
        Self {
            txn,
            range: (owned(min), owned(max)),
            reverse,
            current: None,
            done: false,
        }
    }
}

/// Returns `Pending` once, waking itself, then `Ready`.
#[dual]
#[async_only]
async fn suspend_once(suspensions: &AtomicU64) {
    suspensions.fetch_add(1, Ordering::Relaxed);
    let mut suspended = false;
    std::future::poll_fn(|cx| {
        if std::mem::replace(&mut suspended, true) {
            return std::task::Poll::Ready(());
        }
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    })
    .await
}

#[dual]
#[sync_only]
fn suspend_once(_: &AtomicU64) {}

#[dual]
impl InnerIter for MemoryIter<'_> {
    async fn try_next(&mut self) -> Result<Option<KeyValueRef<'_>>, DatabaseError> {
        suspend_once(&self.txn.storage.suspensions).await;
        if self.done {
            return Ok(None);
        }
        let (mut min, mut max) = (
            bytes_bound_as_slice(&self.range.0),
            bytes_bound_as_slice(&self.range.1),
        );
        if let Some((key, _)) = &self.current {
            if self.reverse {
                max = Bound::Excluded(key.as_slice());
            } else {
                min = Bound::Excluded(key.as_slice());
            }
        }
        let next = {
            let map = self.txn.map();
            let mut range = map.range::<[u8], _>((min, max));
            let entry = if self.reverse {
                range.next_back()
            } else {
                range.next()
            };
            entry.map(|(key, value)| (key.clone(), value.clone()))
        };
        let Some(entry) = next else {
            self.done = true;
            return Ok(None);
        };
        let (key, value) = self.current.insert(entry);
        Ok(Some((key.as_slice(), &value[..])))
    }

    fn recycle(self, pool: &mut BytesPool) {
        let (min, max) = self.range;
        for bound in [min, max] {
            if let Bound::Included(bytes) | Bound::Excluded(bytes) = bound {
                pool.recycle_buffer(bytes);
            }
        }
        if let Some((key, _)) = self.current {
            pool.recycle_buffer(key);
        }
    }
}

#[dual]
impl StorageTransaction for MemoryTransaction {
    type BorrowedBytes<'a>
        = Arc<[u8]>
    where
        Self: 'a;

    type IterType<'a>
        = MemoryIter<'a>
    where
        Self: 'a;

    type RevIterType<'a>
        = MemoryIter<'a>
    where
        Self: 'a;

    type PointCursor<'a>
        = ()
    where
        Self: 'a;

    fn point_cursor(&self) -> Result<Self::PointCursor<'_>, DatabaseError> {
        Ok(())
    }

    fn next_statement_stamp(&mut self) -> Result<u64, DatabaseError> {
        Ok(self.storage.statements.fetch_add(1, Ordering::Relaxed) + 1)
    }

    async fn get_borrowed(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, DatabaseError> {
        suspend_once(&self.storage.suspensions).await;
        Ok(self.map().get(key).cloned())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.map_mut().insert(key.to_vec(), value.into());
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        self.map_mut().remove(key);
        Ok(())
    }

    async fn range<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
        pool: &mut BytesPool,
    ) -> Result<MemoryIter<'txn>, DatabaseError> {
        suspend_once(&self.storage.suspensions).await;
        Ok(MemoryIter::new(self, min, max, false, pool))
    }

    async fn range_rev<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
        pool: &mut BytesPool,
    ) -> Result<MemoryIter<'txn>, DatabaseError> {
        suspend_once(&self.storage.suspensions).await;
        Ok(MemoryIter::new(self, min, max, true, pool))
    }

    async fn commit(self) -> Result<(), DatabaseError> {
        suspend_once(&self.storage.suspensions).await;
        let map = self
            .map
            .into_inner()
            .expect("memory transaction lock poisoned");
        *self
            .storage
            .inner
            .lock()
            .expect("memory storage lock poisoned") = map;
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, target_arch = "wasm32", feature = "wasm"))]
#[crate::dual::dual]
mod wasm_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableName};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::expression::range_detacher::Range;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{IndexScanView, Iter};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn memory_storage_roundtrip() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut transaction = storage.transaction()?;
        let mut names = crate::storage::NameMapping::default();
        let mut table_cache = crate::storage::TableCache::default();
        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let source_columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            ),
        ];
        if let Some(table) = transaction.create_table(
            &mut table_codec,
            &mut plan_arena,
            "test".to_string().into(),
            source_columns,
            false,
        )? {
            let table = table.transplant_to_table_arena(&plan_arena)?;
            names.insert_table(&mut table_cache, table);
        }
        let plan_arena = PlanArena::new(&table_arena);
        let table_name: TableName = "test".to_string().into();

        transaction.append_tuple(
            &mut table_codec,
            0,
            &Tuple::new(
                Some(DataValue::Int32(1)),
                vec![DataValue::Int32(1), DataValue::Boolean(true)],
            )
            .into(),
            &[
                LogicalType::Integer.serializable(),
                LogicalType::Boolean.serializable(),
            ],
            false,
        )?;
        transaction.append_tuple(
            &mut table_codec,
            0,
            &Tuple::new(
                Some(DataValue::Int32(2)),
                vec![DataValue::Int32(2), DataValue::Boolean(true)],
            )
            .into(),
            &[
                LogicalType::Integer.serializable(),
                LogicalType::Boolean.serializable(),
            ],
            false,
        )?;

        let read_column = names
            .table(&table_cache, &table_name)
            .unwrap()
            .columns()
            .next()
            .copied()
            .unwrap();
        let read_columns = vec![read_column];

        let deserializers = crate::storage::project_deserializers_for_test(
            &names,
            &table_cache,
            "test",
            &read_columns,
            &plan_arena,
            true,
        );
        let mut iter = transaction.read(
            &mut table_codec,
            &deserializers,
            names.table(&table_cache, "test").unwrap().id(),
            (Some(1), Some(1)),
        )?;

        let option_1 =
            crate::storage::next_tuple_for_test(|codec, tuple| iter.next_tuple_into(codec, tuple))?;
        assert_eq!(option_1.unwrap().pk, Some(DataValue::Int32(2)));

        let option_2 =
            crate::storage::next_tuple_for_test(|codec, tuple| iter.next_tuple_into(codec, tuple))?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[wasm_bindgen_test]
    fn memory_storage_read_by_index() -> Result<(), DatabaseError> {
        let mut kite_sql = DataBaseBuilder::path("./memory").build_in_memory()?;
        kite_sql.ddl("create table t1 (a int primary key, b int)")?;
        kite_sql.load(CatalogKind::Table("t1".to_string().into()))?;
        kite_sql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2), (3, 4)")?
            .done()?;

        let transaction = kite_sql.storage.transaction()?;
        let table_name: TableName = "t1".to_string().into();
        let table = transaction
            .table(
                kite_sql.state.names(),
                kite_sql.state.table_cache(),
                &table_name,
            )?
            .unwrap()
            .clone();
        let pk_index = table.indexes().next().unwrap().clone();
        let plan_arena = PlanArena::new(kite_sql.state.table_arena());
        let deserializers = crate::storage::project_deserializers_for_test(
            kite_sql.state.names(),
            kite_sql.state.table_cache(),
            &table_name,
            &table.columns().cloned().collect::<Vec<_>>(),
            &plan_arena,
            true,
        );
        let range = Range::Scope {
            min: Bound::Excluded(DataValue::Int32(0).into()),
            max: Bound::Included(DataValue::Int32(2).into()),
        };
        let mut iter = transaction.read_by_index(
            &plan_arena,
            kite_sql
                .state
                .names()
                .table(kite_sql.state.table_cache(), &table_name)
                .unwrap()
                .id(),
            (Some(0), None),
            pk_index,
            false,
            true,
            &deserializers,
            false,
        )?;

        let mut result = Vec::new();
        while let Some(tuple) = crate::storage::next_tuple_for_test(|codec, tuple| {
            iter.next_tuple_into(
                IndexScanView {
                    range: &range,
                    cover_mapping: None,
                },
                &mut Default::default(),
                codec,
                tuple,
            )
        })? {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, vec![DataValue::Int32(1), DataValue::Int32(2)]);

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[crate::dual::dual]
mod native_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableName};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::expression::range_detacher::Range;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{IndexScanView, Iter};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;

    #[test]
    async fn memory_explicit_transaction_does_not_rescan_own_writes() -> Result<(), DatabaseError> {
        let db = crate::db::DataBaseBuilder::path(".").build_in_memory()?;
        crate::storage::check_explicit_transaction_does_not_rescan_own_writes(db).await
    }

    #[test]
    async fn memory_remove_range() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut transaction = storage.transaction()?;
        crate::storage::check_remove_range(&mut transaction).await
    }

    #[test]
    async fn memory_range_rev_matches_range() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut transaction = storage.transaction()?;
        crate::storage::check_range_rev_matches_range(&mut transaction).await
    }

    #[test]
    async fn memory_storage_roundtrip() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut transaction = storage.transaction()?;
        let mut names = crate::storage::NameMapping::default();
        let mut table_cache = crate::storage::TableCache::default();
        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let source_columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            ),
        ];
        if let Some(table) = transaction
            .create_table(
                &mut table_codec,
                &mut plan_arena,
                "test".to_string(),
                source_columns,
                false,
            )
            .await?
        {
            let table = table.transplant_to_table_arena(&plan_arena)?;
            names.insert_table(&mut table_cache, table);
        }
        let plan_arena = PlanArena::new(&table_arena);
        let table_name: TableName = "test".to_string();

        transaction
            .append_tuple(
                &mut table_codec,
                0,
                &Tuple::new(
                    Some(DataValue::Int32(1)),
                    vec![DataValue::Int32(1), DataValue::Boolean(true)],
                )
                .into(),
                &[
                    LogicalType::Integer.serializable(),
                    LogicalType::Boolean.serializable(),
                ],
                false,
            )
            .await?;
        transaction
            .append_tuple(
                &mut table_codec,
                0,
                &Tuple::new(
                    Some(DataValue::Int32(2)),
                    vec![DataValue::Int32(2), DataValue::Boolean(true)],
                )
                .into(),
                &[
                    LogicalType::Integer.serializable(),
                    LogicalType::Boolean.serializable(),
                ],
                false,
            )
            .await?;

        let read_column = names
            .table(&table_cache, &table_name)
            .unwrap()
            .columns()
            .next()
            .copied()
            .unwrap();
        let read_columns = vec![read_column];

        let deserializers = crate::storage::project_deserializers_for_test(
            &names,
            &table_cache,
            "test",
            &read_columns,
            &plan_arena,
            true,
        );
        let mut iter = transaction
            .read(
                &mut table_codec,
                &deserializers,
                names.table(&table_cache, "test").unwrap().id(),
                (Some(1), Some(1)),
            )
            .await?;

        let option_1 = crate::storage::next_tuple_for_test(async |codec, tuple| {
            iter.next_tuple_into(codec, tuple).await
        })
        .await?;
        assert_eq!(option_1.unwrap().pk, Some(DataValue::Int32(2)));

        let option_2 = crate::storage::next_tuple_for_test(async |codec, tuple| {
            iter.next_tuple_into(codec, tuple).await
        })
        .await?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[test]
    async fn memory_storage_read_by_index() -> Result<(), DatabaseError> {
        let mut kite_sql = DataBaseBuilder::path("./memory").build_in_memory()?;
        kite_sql
            .ddl("create table t1 (a int primary key, b int)")
            .await?;
        kite_sql.load(CatalogKind::Table("t1".to_string())).await?;
        kite_sql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2), (3, 4)")
            .await?
            .done()
            .await?;

        let transaction = kite_sql.storage.transaction()?;
        let table_name: TableName = "t1".to_string();
        let table = transaction
            .table(
                kite_sql.state.names(),
                kite_sql.state.table_cache(),
                &table_name,
            )?
            .unwrap()
            .clone();
        let pk_index = *table.indexes().next().unwrap();
        let plan_arena = PlanArena::new(kite_sql.state.table_arena());
        let deserializers = crate::storage::project_deserializers_for_test(
            kite_sql.state.names(),
            kite_sql.state.table_cache(),
            &table_name,
            &table.columns().cloned().collect::<Vec<_>>(),
            &plan_arena,
            true,
        );
        let range = Range::Scope {
            min: Bound::Excluded(DataValue::Int32(0).into()),
            max: Bound::Included(DataValue::Int32(2).into()),
        };
        let mut iter = transaction.read_by_index(
            &plan_arena,
            kite_sql
                .state
                .names()
                .table(kite_sql.state.table_cache(), &table_name)
                .unwrap()
                .id(),
            (Some(0), None),
            pk_index,
            false,
            true,
            &deserializers,
            false,
        )?;

        let mut result = Vec::new();
        while let Some(tuple) = crate::storage::next_tuple_for_test(async |codec, tuple| {
            iter.next_tuple_into(
                IndexScanView {
                    range: &range,
                    cover_mapping: None,
                },
                &mut Default::default(),
                codec,
                tuple,
            )
            .await
        })
        .await?
        {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, vec![DataValue::Int32(1), DataValue::Int32(2)]);

        Ok(())
    }
}
// GRCOV_EXCL_STOP
