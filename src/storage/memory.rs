#![cfg(target_arch = "wasm32")]

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
