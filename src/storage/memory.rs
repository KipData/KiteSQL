use crate::errors::DatabaseError;
use crate::storage::table_codec::{BumpBytes, Bytes, TableCodec};
use crate::storage::{InnerIter, Storage, Transaction};
use std::cell::RefCell;
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
    type TransactionType<'a>
        = MemoryTransaction
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError> {
        Ok(MemoryTransaction {
            inner: self.inner.clone(),
            table_codec: Default::default(),
        })
    }
}

pub struct MemoryTransaction {
    inner: Rc<RefCell<BTreeMap<Vec<u8>, Vec<u8>>>>,
    table_codec: TableCodec,
}

pub struct MemoryIter {
    entries: VecDeque<(Bytes, Bytes)>,
}

impl InnerIter for MemoryIter {
    fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>, DatabaseError> {
        Ok(self.entries.pop_front())
    }
}

impl Transaction for MemoryTransaction {
    type IterType<'a>
        = MemoryIter
    where
        Self: 'a;

    fn table_codec(&self) -> *const TableCodec {
        &self.table_codec
    }

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError> {
        Ok(self.inner.borrow().get(key).cloned())
    }

    fn set(&mut self, key: BumpBytes, value: BumpBytes) -> Result<(), DatabaseError> {
        self.inner.borrow_mut().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError> {
        self.inner.borrow_mut().remove(key);
        Ok(())
    }

    fn range<'a>(
        &'a self,
        min: Bound<BumpBytes<'a>>,
        max: Bound<BumpBytes<'a>>,
    ) -> Result<Self::IterType<'a>, DatabaseError> {
        let map = self.inner.borrow();
        let start = match &min {
            Bound::Included(b) => Bound::Included(b.as_ref()),
            Bound::Excluded(b) => Bound::Excluded(b.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match &max {
            Bound::Included(b) => Bound::Included(b.as_ref()),
            Bound::Excluded(b) => Bound::Excluded(b.as_ref()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let entries = map
            .range::<[u8], _>((start, end))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        Ok(MemoryIter { entries })
    }

    fn commit(self) -> Result<(), DatabaseError> {
        Ok(())
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, TableName};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::expression::range_detacher::Range;
    use crate::storage::Iter;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::collections::{BTreeMap, Bound};
    use std::hash::RandomState;
    use std::sync::Arc;
    use wasm_bindgen_test::*;

    #[wasm_bindgen_test]
    fn memory_storage_roundtrip() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
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
        transaction.create_table(
            &table_cache,
            "test".to_string().into(),
            source_columns,
            false,
        )?;

        transaction.append_tuple(
            &"test".to_string(),
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
            &"test".to_string(),
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

        let option_1 = iter.next_tuple()?;
        assert_eq!(option_1.unwrap().pk, Some(DataValue::Int32(2)));

        let option_2 = iter.next_tuple()?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[wasm_bindgen_test]
    fn memory_storage_read_by_index() -> Result<(), DatabaseError> {
        let kite_sql = DataBaseBuilder::path("./memory").build_in_memory()?;
        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
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
        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            table_name,
            (Some(0), None),
            table.columns().cloned().enumerate().collect(),
            pk_index,
            vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Included(DataValue::Int32(2)),
            }],
            true,
        )?;

        let mut result = Vec::new();
        while let Some(tuple) = iter.next_tuple()? {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, vec![DataValue::Int32(1), DataValue::Int32(2)]);

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod native_tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, TableName};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::expression::range_detacher::Range;
    use crate::storage::Iter;
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::collections::{BTreeMap, Bound};
    use std::hash::RandomState;
    use std::sync::Arc;

    #[test]
    fn memory_storage_roundtrip() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
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
        transaction.create_table(
            &table_cache,
            "test".to_string().into(),
            source_columns,
            false,
        )?;

        transaction.append_tuple(
            &"test".to_string(),
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
            &"test".to_string(),
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

        let option_1 = iter.next_tuple()?;
        assert_eq!(option_1.unwrap().pk, Some(DataValue::Int32(2)));

        let option_2 = iter.next_tuple()?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[test]
    fn memory_storage_read_by_index() -> Result<(), DatabaseError> {
        let kite_sql = DataBaseBuilder::path("./memory").build_in_memory()?;
        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
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
        let mut iter = transaction.read_by_index(
            kite_sql.state.table_cache(),
            table_name,
            (Some(0), None),
            table.columns().cloned().enumerate().collect(),
            pk_index,
            vec![Range::Scope {
                min: Bound::Excluded(DataValue::Int32(0)),
                max: Bound::Included(DataValue::Int32(2)),
            }],
            true,
        )?;

        let mut result = Vec::new();
        while let Some(tuple) = iter.next_tuple()? {
            result.push(tuple.pk.unwrap());
        }

        assert_eq!(result, vec![DataValue::Int32(1), DataValue::Int32(2)]);

        Ok(())
    }
}
