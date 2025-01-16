use crate::errors::DatabaseError;
use crate::storage::table_codec::{BumpBytes, Bytes, TableCodec};
use crate::storage::{InnerIter, Storage, Transaction};
use rocksdb::{
    DBIteratorWithThreadMode, Direction, IteratorMode, OptimisticTransactionDB, SliceTransform,
};
use std::collections::Bound;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct RocksStorage {
    pub inner: Arc<OptimisticTransactionDB>,
}

impl RocksStorage {
    pub fn new(path: impl Into<PathBuf> + Send) -> Result<Self, DatabaseError> {
        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(40 * 1_024 * 1_024));
        bb.set_whole_key_filtering(false);

        let mut opts = rocksdb::Options::default();
        opts.set_block_based_table_factory(&bb);
        opts.create_if_missing(true);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(4));

        let storage = OptimisticTransactionDB::open(&opts, path.into())?;

        Ok(RocksStorage {
            inner: Arc::new(storage),
        })
    }
}

impl Storage for RocksStorage {
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
}

pub struct RocksTransaction<'db> {
    tx: rocksdb::Transaction<'db, OptimisticTransactionDB>,
    table_codec: TableCodec,
}

impl<'txn> Transaction for RocksTransaction<'txn> {
    type IterType<'iter>
        = RocksIter<'txn, 'iter>
    where
        Self: 'iter;

    #[inline]
    fn table_codec(&self) -> *const TableCodec {
        &self.table_codec
    }

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError> {
        Ok(self.tx.get(key)?)
    }

    #[inline]
    fn set(&mut self, key: BumpBytes, value: BumpBytes) -> Result<(), DatabaseError> {
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
    fn range<'a>(
        &'a self,
        min: Bound<BumpBytes<'a>>,
        max: Bound<BumpBytes<'a>>,
    ) -> Result<Self::IterType<'a>, DatabaseError> {
        let min = match min {
            Bound::Included(bytes) => Some(bytes),
            Bound::Excluded(mut bytes) => {
                // the prefix is the same, but the length is larger
                bytes.push(0u8);
                Some(bytes)
            }
            Bound::Unbounded => None,
        };
        let lower = min
            .as_ref()
            .map(|bytes| IteratorMode::From(bytes, Direction::Forward))
            .unwrap_or(IteratorMode::Start);

        if let (Some(min_bytes), Bound::Included(max_bytes) | Bound::Excluded(max_bytes)) =
            (&min, &max)
        {
            let len = min_bytes
                .iter()
                .zip(max_bytes.iter())
                .take_while(|(x, y)| x == y)
                .count();

            debug_assert!(len > 0);
            let mut iter = self.tx.prefix_iterator(&min_bytes[..len]);
            iter.set_mode(lower);

            return Ok(RocksIter { upper: max, iter });
        }
        let iter = self.tx.iterator(lower);

        Ok(RocksIter { upper: max, iter })
    }

    fn commit(self) -> Result<(), DatabaseError> {
        self.tx.commit()?;
        Ok(())
    }
}

pub struct RocksIter<'txn, 'iter> {
    upper: Bound<BumpBytes<'iter>>,
    iter: DBIteratorWithThreadMode<'iter, rocksdb::Transaction<'txn, OptimisticTransactionDB>>,
}

impl InnerIter for RocksIter<'_, '_> {
    #[inline]
    fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>, DatabaseError> {
        if let Some(result) = self.iter.by_ref().next() {
            let (key, value) = result?;
            let upper_bound_check = match &self.upper {
                Bound::Included(ref upper) => key.as_ref() <= upper.as_slice(),
                Bound::Excluded(ref upper) => key.as_ref() < upper.as_slice(),
                Bound::Unbounded => true,
            };
            if !upper_bound_check {
                return Ok(None);
            }
            return Ok(Some((Vec::from(key), Vec::from(value))));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{
        IndexImplEnum, IndexImplParams, IndexIter, IndexIterState, Iter, PrimaryKeyIndexImpl,
        Storage, Transaction,
    };
    use crate::types::index::{IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::collections::Bound;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

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
            .map(|col_ref| ColumnCatalog::clone(&col_ref))
            .collect_vec();
        let _ = transaction.create_table(
            &table_cache,
            Arc::new("test".to_string()),
            source_columns,
            false,
        )?;

        let table_catalog = transaction.table(&table_cache, Arc::new("test".to_string()))?;
        assert!(table_catalog.is_some());
        assert!(table_catalog
            .unwrap()
            .get_column_id_by_name(&"c1".to_string())
            .is_some());

        transaction.append_tuple(
            &"test".to_string(),
            Tuple::new(
                Some(Arc::new(vec![0])),
                vec![DataValue::Int32(1), DataValue::Boolean(true)],
            ),
            &[LogicalType::Integer, LogicalType::Boolean],
            false,
        )?;
        transaction.append_tuple(
            &"test".to_string(),
            Tuple::new(
                Some(Arc::new(vec![0])),
                vec![DataValue::Int32(2), DataValue::Boolean(true)],
            ),
            &[LogicalType::Integer, LogicalType::Boolean],
            false,
        )?;

        let mut iter = transaction.read(
            &table_cache,
            Arc::new("test".to_string()),
            (Some(1), Some(1)),
            vec![(0, columns[0].clone())],
        )?;

        let option_1 = iter.next_tuple()?;
        assert_eq!(option_1.unwrap().pk_indices, Some(Arc::new(vec![0])));

        let option_2 = iter.next_tuple()?;
        assert_eq!(option_2, None);

        Ok(())
    }

    #[test]
    fn test_index_iter_pk() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        kite_sql
            .run("create table t1 (a int primary key)")?
            .done()?;
        kite_sql
            .run("insert into t1 (a) values (0), (1), (2), (3), (4)")?
            .done()?;
        let transaction = kite_sql.storage.transaction()?;

        let table_name = Arc::new("t1".to_string());
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
        let pk_indices = Arc::new(vec![0]);
        let mut iter = IndexIter {
            offset: 0,
            limit: None,
            pk_indices: &pk_indices,
            params: IndexImplParams {
                tuple_schema_ref: table.schema_ref().clone(),
                projections: vec![0],
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
                table_types: table.types(),
                tx: &transaction,
            },
            ranges: vec![
                Range::Eq(DataValue::Int32(0)),
                Range::Scope {
                    min: Bound::Included(DataValue::Int32(2)),
                    max: Bound::Included(DataValue::Int32(4)),
                },
            ]
            .into_iter(),
            state: IndexIterState::Init,
            inner: IndexImplEnum::PrimaryKey(PrimaryKeyIndexImpl),
        };
        let mut result = Vec::new();

        while let Some(mut tuple) = iter.next_tuple()? {
            result.push(tuple.id().unwrap().clone());
        }

        assert_eq!(result, tuple_ids);

        Ok(())
    }

    #[test]
    fn test_read_by_index() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;
        kite_sql
            .run("create table t1 (a int primary key, b int unique)")?
            .done()?;
        kite_sql
            .run("insert into t1 (a, b) values (0, 0), (1, 1), (2, 2)")?
            .done()?;
        let transaction = kite_sql.storage.transaction().unwrap();

        let table = transaction
            .table(kite_sql.state.table_cache(), Arc::new("t1".to_string()))?
            .unwrap()
            .clone();
        let columns = table.columns().cloned().enumerate().collect_vec();
        let mut iter = transaction
            .read_by_index(
                kite_sql.state.table_cache(),
                Arc::new("t1".to_string()),
                (Some(0), Some(1)),
                columns,
                table.indexes[0].clone(),
                vec![Range::Scope {
                    min: Bound::Excluded(DataValue::Int32(0)),
                    max: Bound::Unbounded,
                }],
            )
            .unwrap();

        while let Some(tuple) = iter.next_tuple()? {
            assert_eq!(tuple.pk_indices, Some(Arc::new(vec![0])));
            assert_eq!(tuple.values, vec![DataValue::Int32(1), DataValue::Int32(1)])
        }

        Ok(())
    }
}
