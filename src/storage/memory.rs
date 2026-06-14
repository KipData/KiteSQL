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
    EmptyStorageMetrics, InnerIter, Storage, Transaction, TransactionIsolationLevel,
};
use std::cell::{Ref, RefCell};
use std::collections::{BTreeMap, Bound, VecDeque};
use std::rc::Rc;

#[derive(Clone, Default)]
pub struct MemoryStorage {
    inner: Rc<RefCell<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
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
    ) -> Result<Self::TransactionType<'_>, DatabaseError> {
        self.validate_transaction_isolation(isolation)?;
        Ok(MemoryTransaction {
            inner: self.inner.clone(),
        })
    }

    fn default_transaction_isolation(&self) -> TransactionIsolationLevel {
        TransactionIsolationLevel::ReadCommitted
    }
}

pub struct MemoryTransaction {
    inner: Rc<RefCell<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

pub struct MemoryIter {
    entries: VecDeque<(Bytes, Bytes)>,
    current: Option<(Bytes, Bytes)>,
}

pub struct MemoryValue<'a> {
    value: Ref<'a, Vec<u8>>,
}

impl AsRef<[u8]> for MemoryValue<'_> {
    fn as_ref(&self) -> &[u8] {
        self.value.as_slice()
    }
}

impl InnerIter for MemoryIter {
    fn try_next(&mut self) -> Result<Option<crate::storage::KeyValueRef<'_>>, DatabaseError> {
        self.current = self.entries.pop_front();

        Ok(self
            .current
            .as_ref()
            .map(|(key, value)| (key.as_slice(), value.as_slice())))
    }
}

impl Transaction for MemoryTransaction {
    type BorrowedBytes<'a>
        = MemoryValue<'a>
    where
        Self: 'a;

    type IterType<'a>
        = MemoryIter
    where
        Self: 'a;
    fn get_borrowed<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError> {
        let map = self.inner.borrow();
        let Ok(value) = Ref::filter_map(map, |map| map.get(key)) else {
            return Ok(None);
        };

        Ok(Some(MemoryValue { value }))
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError> {
        self.inner.borrow_mut().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        self.inner.borrow_mut().remove(key);
        Ok(())
    }

    fn range<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
    ) -> Result<Self::IterType<'txn>, DatabaseError> {
        let map = self.inner.borrow();
        let start = match &min {
            Bound::Included(b) => Bound::Included(*b),
            Bound::Excluded(b) => Bound::Excluded(*b),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match &max {
            Bound::Included(b) => Bound::Included(*b),
            Bound::Excluded(b) => Bound::Excluded(*b),
            Bound::Unbounded => Bound::Unbounded,
        };

        let entries = map
            .range::<[u8], _>((start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(MemoryIter {
            entries,
            current: None,
        })
    }

    fn commit(self) -> Result<(), DatabaseError> {
        Ok(())
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableName};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::expression::range_detacher::Range;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::table_codec::TableCodec;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn memory_storage_roundtrip() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut transaction = storage.transaction()?;
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
            table_cache.insert(table.name().clone(), table);
        }
        let plan_arena = PlanArena::new(&table_arena);
        let table_name: TableName = "test".to_string().into();

        transaction.append_tuple(
            &mut table_codec,
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
            &mut table_codec,
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

        let read_column = table_cache
            .get(&table_name)
            .unwrap()
            .columns()
            .next()
            .copied()
            .unwrap();
        let read_columns = vec![read_column];

        let mut iter = transaction.read(
            &mut table_codec,
            &plan_arena,
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
            .table(kite_sql.state.table_cache(), table_name.clone())?
            .unwrap()
            .clone();
        let pk_index = table.indexes().next().unwrap().clone();
        let plan_arena = PlanArena::new(kite_sql.state.table_arena());
        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            &plan_arena,
            table_name,
            (Some(0), None),
            table.columns().cloned().collect(),
            pk_index,
            vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Included(DataValue::Int32(2)),
            }],
            true,
            None,
            None,
        )?;

        let mut result = Vec::new();
        while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, vec![DataValue::Int32(1), DataValue::Int32(2)]);

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableName};
    use crate::db::{CatalogKind, DataBaseBuilder};
    use crate::expression::range_detacher::Range;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::table_codec::TableCodec;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;

    #[test]
    fn memory_storage_roundtrip() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let mut transaction = storage.transaction()?;
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
            table_cache.insert(table.name().clone(), table);
        }
        let plan_arena = PlanArena::new(&table_arena);
        let table_name: TableName = "test".to_string().into();

        transaction.append_tuple(
            &mut table_codec,
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
            &mut table_codec,
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

        let read_column = table_cache
            .get(&table_name)
            .unwrap()
            .columns()
            .next()
            .copied()
            .unwrap();
        let read_columns = vec![read_column];

        let mut iter = transaction.read(
            &mut table_codec,
            &plan_arena,
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
            .table(kite_sql.state.table_cache(), table_name.clone())?
            .unwrap()
            .clone();
        let pk_index = *table.indexes().next().unwrap();
        let plan_arena = PlanArena::new(kite_sql.state.table_arena());
        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            &plan_arena,
            table_name,
            (Some(0), None),
            table.columns().cloned().collect(),
            pk_index,
            vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Included(DataValue::Int32(2)),
            }],
            true,
            None,
            None,
        )?;

        let mut result = Vec::new();
        while let Some(tuple) = crate::storage::next_tuple_for_test(&mut iter)? {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, vec![DataValue::Int32(1), DataValue::Int32(2)]);

        Ok(())
    }
}
