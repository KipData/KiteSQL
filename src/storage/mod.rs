pub mod rocksdb;
pub(crate) mod table_codec;

use crate::catalog::view::View;
use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableMeta, TableName};
use crate::errors::DatabaseError;
use crate::execution::dml::analyze::Analyze;
use crate::expression::range_detacher::Range;
use crate::optimizer::core::statistics_meta::{StatisticMetaLoader, StatisticsMeta};
use crate::serdes::ReferenceTables;
use crate::storage::table_codec::{BumpBytes, Bytes, TableCodec};
use crate::types::index::{Index, IndexId, IndexMetaRef, IndexType};
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::DataValue;
use crate::types::{ColumnId, LogicalType};
use crate::utils::lru::SharedLruCache;
use itertools::Itertools;
use std::collections::{BTreeMap, Bound};
use std::io::Cursor;
use std::ops::SubAssign;
use std::sync::Arc;
use std::vec::IntoIter;
use std::{fs, mem};
use ulid::Generator;

pub(crate) type StatisticsMetaCache = SharedLruCache<(TableName, IndexId), StatisticsMeta>;
pub(crate) type TableCache = SharedLruCache<TableName, TableCatalog>;
pub(crate) type ViewCache = SharedLruCache<TableName, View>;

pub trait Storage: Clone {
    type TransactionType<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);

pub trait Transaction: Sized {
    type IterType<'a>: InnerIter
    where
        Self: 'a;

    fn table_codec(&self) -> *const TableCodec;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read<'a>(
        &'a self,
        table_cache: &'a TableCache,
        table_name: TableName,
        bounds: Bounds,
        mut columns: BTreeMap<usize, ColumnRef>,
        with_pk: bool,
    ) -> Result<TupleIter<'a, Self>, DatabaseError> {
        debug_assert!(columns.keys().all_unique());

        let table = self
            .table(table_cache, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let table_types = table.types();
        if columns.is_empty() || with_pk {
            for (i, column) in table.primary_keys() {
                columns.insert(*i, column.clone());
            }
        }
        let mut tuple_columns = Vec::with_capacity(columns.len());
        let mut projections = Vec::with_capacity(columns.len());
        for (projection, column) in columns {
            tuple_columns.push(column);
            projections.push(projection);
        }
        let remap_pk_indices = remap_pk_indices(&projections, table.primary_keys_indices());

        let (min, max) = unsafe { &*self.table_codec() }.tuple_bound(&table_name);
        let iter = self.range(Bound::Included(min), Bound::Included(max))?;

        Ok(TupleIter {
            offset: bounds.0.unwrap_or(0),
            limit: bounds.1,
            table_types,
            tuple_columns: Arc::new(tuple_columns),
            remap_pk_indices,
            projections,
            with_pk,
            iter,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn read_by_index<'a>(
        &'a self,
        table_cache: &'a TableCache,
        table_name: TableName,
        (offset_option, limit_option): Bounds,
        mut columns: BTreeMap<usize, ColumnRef>,
        index_meta: IndexMetaRef,
        ranges: Vec<Range>,
        with_pk: bool,
    ) -> Result<IndexIter<'a, Self>, DatabaseError> {
        debug_assert!(columns.keys().all_unique());

        let table = self
            .table(table_cache, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let table_types = table.types();
        let table_name = table.name.as_str();
        let offset = offset_option.unwrap_or(0);

        if columns.is_empty() || with_pk {
            for (i, column) in table.primary_keys() {
                columns.insert(*i, column.clone());
            }
        }
        let mut tuple_columns = Vec::with_capacity(columns.len());
        let mut projections = Vec::with_capacity(columns.len());
        for (projection, column) in columns {
            tuple_columns.push(column);
            projections.push(projection);
        }
        let remap_pk_indices = remap_pk_indices(&projections, table.primary_keys_indices());
        let inner = IndexImplEnum::instance(index_meta.ty);

        Ok(IndexIter {
            offset,
            limit: limit_option,
            remap_pk_indices,
            params: IndexImplParams {
                tuple_schema_ref: Arc::new(tuple_columns),
                projections,
                index_meta,
                table_name,
                table_types,
                with_pk,
                tx: self,
            },
            inner,
            ranges: ranges.into_iter(),
            state: IndexIterState::Init,
        })
    }

    fn add_index_meta(
        &mut self,
        table_cache: &TableCache,
        table_name: &TableName,
        index_name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<IndexId, DatabaseError> {
        if let Some(mut table) = self.table(table_cache, table_name.clone())?.cloned() {
            let index_meta = table.add_index_meta(index_name, column_ids, ty)?;
            let (key, value) =
                unsafe { &*self.table_codec() }.encode_index_meta(table_name, index_meta)?;
            self.set(key, value)?;
            table_cache.remove(table_name);

            Ok(index_meta.id)
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        if matches!(index.ty, IndexType::PrimaryKey { .. }) {
            return Ok(());
        }
        let (key, value) =
            unsafe { &*self.table_codec() }.encode_index(table_name, &index, tuple_id)?;

        if matches!(index.ty, IndexType::Unique) {
            if let Some(bytes) = self.get(&key)? {
                return if bytes != value.as_slice() {
                    Err(DatabaseError::DuplicateUniqueValue)
                } else {
                    Ok(())
                };
            }
        }
        self.set(key, value)?;

        Ok(())
    }

    fn del_index(
        &mut self,
        table_name: &str,
        index: &Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        if matches!(index.ty, IndexType::PrimaryKey { .. }) {
            return Ok(());
        }
        self.remove(&unsafe { &*self.table_codec() }.encode_index_key(
            table_name,
            index,
            Some(tuple_id),
        )?)?;

        Ok(())
    }

    fn append_tuple(
        &mut self,
        table_name: &str,
        mut tuple: Tuple,
        types: &[LogicalType],
        is_overwrite: bool,
    ) -> Result<(), DatabaseError> {
        let (key, value) =
            unsafe { &*self.table_codec() }.encode_tuple(table_name, &mut tuple, types)?;

        if !is_overwrite && self.get(&key)?.is_some() {
            return Err(DatabaseError::DuplicatePrimaryKey);
        }
        self.set(key, value)?;

        Ok(())
    }

    fn remove_tuple(&mut self, table_name: &str, tuple_id: &TupleId) -> Result<(), DatabaseError> {
        let key = unsafe { &*self.table_codec() }.encode_tuple_key(table_name, tuple_id)?;
        self.remove(&key)?;

        Ok(())
    }

    fn add_column(
        &mut self,
        table_cache: &TableCache,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<ColumnId, DatabaseError> {
        if let Some(mut table) = self.table(table_cache, table_name.clone())?.cloned() {
            if !column.nullable() && column.default_value()?.is_none() {
                return Err(DatabaseError::NeedNullAbleOrDefault);
            }

            for col in table.columns() {
                if col.name() == column.name() {
                    return if if_not_exists {
                        Ok(col.id().unwrap())
                    } else {
                        Err(DatabaseError::DuplicateColumn(column.name().to_string()))
                    };
                }
            }
            let mut generator = Generator::new();
            let col_id = table.add_column(column.clone(), &mut generator)?;

            if column.desc().is_unique() {
                let meta_ref = table.add_index_meta(
                    format!("uk_{}", column.name()),
                    vec![col_id],
                    IndexType::Unique,
                )?;
                let (key, value) =
                    unsafe { &*self.table_codec() }.encode_index_meta(table_name, meta_ref)?;
                self.set(key, value)?;
            }

            let column = table.get_column_by_id(&col_id).unwrap();
            let (key, value) = unsafe { &*self.table_codec() }
                .encode_column(column, &mut ReferenceTables::new())?;
            self.set(key, value)?;
            table_cache.remove(table_name);

            Ok(col_id)
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn drop_column(
        &mut self,
        table_cache: &TableCache,
        meta_cache: &StatisticsMetaCache,
        table_name: &TableName,
        column_name: &str,
    ) -> Result<(), DatabaseError> {
        if let Some(table_catalog) = self.table(table_cache, table_name.clone())?.cloned() {
            let column = table_catalog.get_column_by_name(column_name).unwrap();

            let (key, _) = unsafe { &*self.table_codec() }
                .encode_column(column, &mut ReferenceTables::new())?;
            self.remove(&key)?;

            for index_meta in table_catalog.indexes.iter() {
                if !index_meta.column_ids.contains(&column.id().unwrap()) {
                    continue;
                }
                let (index_meta_key, _) =
                    unsafe { &*self.table_codec() }.encode_index_meta(table_name, index_meta)?;
                self.remove(&index_meta_key)?;

                let (index_min, index_max) =
                    unsafe { &*self.table_codec() }.index_bound(table_name, index_meta.id)?;
                self._drop_data(index_min, index_max)?;

                self.remove_table_meta(meta_cache, table_name, index_meta.id)?;
            }
            table_cache.remove(table_name);

            Ok(())
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }

    fn create_view(
        &mut self,
        view_cache: &ViewCache,
        view: View,
        or_replace: bool,
    ) -> Result<(), DatabaseError> {
        let (view_key, value) = unsafe { &*self.table_codec() }.encode_view(&view)?;

        let already_exists = self.get(&view_key)?.is_some();
        if !or_replace && already_exists {
            return Err(DatabaseError::ViewExists);
        }
        if !already_exists {
            self.check_name_hash(&view.name)?;
        }
        self.set(view_key, value)?;
        let _ = view_cache.put(view.name.clone(), view);

        Ok(())
    }

    fn create_table(
        &mut self,
        table_cache: &TableCache,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<TableName, DatabaseError> {
        let mut table_catalog = TableCatalog::new(table_name.clone(), columns)?;

        for (_, column) in table_catalog.primary_keys() {
            TableCodec::check_primary_key_type(column.datatype())?;
        }

        let (table_key, value) = unsafe { &*self.table_codec() }
            .encode_root_table(&TableMeta::empty(table_name.clone()))?;
        if self.get(&table_key)?.is_some() {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(DatabaseError::TableExists);
        }
        self.check_name_hash(&table_name)?;
        self.create_index_meta_from_column(&mut table_catalog)?;
        self.set(table_key, value)?;

        let mut reference_tables = ReferenceTables::new();
        for column in table_catalog.columns() {
            let (key, value) =
                unsafe { &*self.table_codec() }.encode_column(column, &mut reference_tables)?;
            self.set(key, value)?;
        }
        debug_assert_eq!(reference_tables.len(), 1);
        table_cache.put(table_name.clone(), table_catalog);

        Ok(table_name)
    }

    fn check_name_hash(&mut self, table_name: &TableName) -> Result<(), DatabaseError> {
        let (hash_key, value) = unsafe { &*self.table_codec() }.encode_table_hash(table_name);
        if self.get(&hash_key)?.is_some() {
            return Err(DatabaseError::DuplicateSourceHash(table_name.to_string()));
        }
        self.set(hash_key, value)
    }

    fn drop_name_hash(&mut self, table_name: &TableName) -> Result<(), DatabaseError> {
        self.remove(&unsafe { &*self.table_codec() }.encode_table_hash_key(table_name))
    }

    fn drop_view(
        &mut self,
        view_cache: &ViewCache,
        table_cache: &TableCache,
        view_name: TableName,
        if_exists: bool,
    ) -> Result<(), DatabaseError> {
        self.drop_name_hash(&view_name)?;
        if self
            .view(table_cache, view_cache, view_name.clone())?
            .is_none()
        {
            if if_exists {
                return Ok(());
            } else {
                return Err(DatabaseError::ViewNotFound);
            }
        }

        self.remove(&unsafe { &*self.table_codec() }.encode_view_key(view_name.as_str()))?;
        view_cache.remove(&view_name);

        Ok(())
    }

    fn drop_index(
        &mut self,
        table_cache: &TableCache,
        table_name: TableName,
        index_name: &str,
        if_exists: bool,
    ) -> Result<(), DatabaseError> {
        let table = self
            .table(table_cache, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let Some(index_meta) = table.indexes.iter().find(|index| index.name == index_name) else {
            if if_exists {
                return Ok(());
            } else {
                return Err(DatabaseError::TableNotFound);
            }
        };
        match index_meta.ty {
            IndexType::PrimaryKey { .. } | IndexType::Unique => {
                return Err(DatabaseError::InvalidIndex)
            }
            IndexType::Normal | IndexType::Composite => (),
        }

        let index_id = index_meta.id;
        let index_meta_key =
            unsafe { &*self.table_codec() }.encode_index_meta_key(table_name.as_str(), index_id)?;
        self.remove(&index_meta_key)?;

        let (index_min, index_max) =
            unsafe { &*self.table_codec() }.index_bound(table_name.as_str(), index_id)?;
        self._drop_data(index_min, index_max)?;

        let statistics_min_key = unsafe { &*self.table_codec() }
            .encode_statistics_path_key(table_name.as_str(), index_id);
        self.remove(&statistics_min_key)?;

        table_cache.remove(&table_name);
        //  When dropping Index, the statistics file corresponding to the Index is not cleaned up and is processed uniformly by the Analyze Table.

        Ok(())
    }

    fn drop_table(
        &mut self,
        table_cache: &TableCache,
        table_name: TableName,
        if_exists: bool,
    ) -> Result<(), DatabaseError> {
        if self.table(table_cache, table_name.clone())?.is_none() {
            if if_exists {
                return Ok(());
            } else {
                return Err(DatabaseError::TableNotFound);
            }
        }
        self.drop_name_hash(&table_name)?;
        self.drop_data(table_name.as_str())?;

        let (column_min, column_max) =
            unsafe { &*self.table_codec() }.columns_bound(table_name.as_str());
        self._drop_data(column_min, column_max)?;

        let (index_meta_min, index_meta_max) =
            unsafe { &*self.table_codec() }.index_meta_bound(table_name.as_str());
        self._drop_data(index_meta_min, index_meta_max)?;

        self.remove(&unsafe { &*self.table_codec() }.encode_root_table_key(table_name.as_str()))?;
        table_cache.remove(&table_name);

        let _ = fs::remove_dir(Analyze::build_statistics_meta_path(&table_name));

        Ok(())
    }

    fn drop_data(&mut self, table_name: &str) -> Result<(), DatabaseError> {
        let (tuple_min, tuple_max) = unsafe { &*self.table_codec() }.tuple_bound(table_name);
        self._drop_data(tuple_min, tuple_max)?;

        let (index_min, index_max) = unsafe { &*self.table_codec() }.all_index_bound(table_name);
        self._drop_data(index_min, index_max)?;

        let (statistics_min, statistics_max) =
            unsafe { &*self.table_codec() }.statistics_bound(table_name);
        self._drop_data(statistics_min, statistics_max)?;

        Ok(())
    }

    fn view<'a>(
        &'a self,
        table_cache: &'a TableCache,
        view_cache: &'a ViewCache,
        view_name: TableName,
    ) -> Result<Option<&'a View>, DatabaseError> {
        if let Some(view) = view_cache.get(&view_name) {
            return Ok(Some(view));
        }
        let Some(bytes) = self.get(&unsafe { &*self.table_codec() }.encode_view_key(&view_name))?
        else {
            return Ok(None);
        };
        Ok(Some(view_cache.get_or_insert(view_name.clone(), |_| {
            TableCodec::decode_view(&bytes, (self, table_cache))
        })?))
    }

    fn views(&self, table_cache: &TableCache) -> Result<Vec<View>, DatabaseError> {
        let mut metas = vec![];
        let (min, max) = unsafe { &*self.table_codec() }.view_bound();
        let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;

        while let Some((_, value)) = iter.try_next().ok().flatten() {
            let meta = TableCodec::decode_view(&value, (self, table_cache))?;

            metas.push(meta);
        }

        Ok(metas)
    }

    fn table<'a>(
        &'a self,
        table_cache: &'a TableCache,
        table_name: TableName,
    ) -> Result<Option<&'a TableCatalog>, DatabaseError> {
        if let Some(table) = table_cache.get(&table_name) {
            return Ok(Some(table));
        }

        // `TableCache` is not theoretically used in `table_collect` because ColumnCatalog should not depend on other Column
        self.table_collect(&table_name)?
            .map(|(columns, indexes)| {
                table_cache.get_or_insert(table_name.clone(), |_| {
                    TableCatalog::reload(table_name, columns, indexes)
                })
            })
            .transpose()
    }

    fn table_metas(&self) -> Result<Vec<TableMeta>, DatabaseError> {
        let mut metas = vec![];
        let (min, max) = unsafe { &*self.table_codec() }.root_table_bound();
        let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;

        while let Some((_, value)) = iter.try_next().ok().flatten() {
            let meta = TableCodec::decode_root_table::<Self>(&value)?;

            metas.push(meta);
        }

        Ok(metas)
    }

    fn save_table_meta(
        &mut self,
        meta_cache: &StatisticsMetaCache,
        table_name: &TableName,
        path: String,
        statistics_meta: StatisticsMeta,
    ) -> Result<(), DatabaseError> {
        let index_id = statistics_meta.index_id();
        meta_cache.put((table_name.clone(), index_id), statistics_meta);

        let (key, value) = unsafe { &*self.table_codec() }.encode_statistics_path(
            table_name.as_str(),
            index_id,
            path,
        );
        self.set(key, value)?;

        Ok(())
    }

    fn table_meta_path(
        &self,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<Option<String>, DatabaseError> {
        let key = unsafe { &*self.table_codec() }.encode_statistics_path_key(table_name, index_id);
        self.get(&key)?
            .map(|bytes| TableCodec::decode_statistics_path(&bytes))
            .transpose()
    }

    fn remove_table_meta(
        &mut self,
        meta_cache: &StatisticsMetaCache,
        table_name: &TableName,
        index_id: IndexId,
    ) -> Result<(), DatabaseError> {
        let key = unsafe { &*self.table_codec() }.encode_statistics_path_key(table_name, index_id);
        self.remove(&key)?;

        meta_cache.remove(&(table_name.clone(), index_id));

        Ok(())
    }

    fn meta_loader<'a>(
        &'a self,
        meta_cache: &'a StatisticsMetaCache,
    ) -> StatisticMetaLoader<'a, Self>
    where
        Self: Sized,
    {
        StatisticMetaLoader::new(self, meta_cache)
    }

    #[allow(clippy::type_complexity)]
    fn table_collect(
        &self,
        table_name: &TableName,
    ) -> Result<Option<(Vec<ColumnRef>, Vec<IndexMetaRef>)>, DatabaseError> {
        let (table_min, table_max) = unsafe { &*self.table_codec() }.table_bound(table_name);
        let mut column_iter = self.range(
            Bound::Included(table_min.clone()),
            Bound::Included(table_max),
        )?;

        let mut columns = Vec::new();
        let mut index_metas = Vec::new();
        let mut reference_tables = ReferenceTables::new();
        let _ = reference_tables.push_or_replace(table_name);

        // Tips: only `Column`, `IndexMeta`, `TableMeta`
        while let Some((key, value)) = column_iter.try_next().ok().flatten() {
            if key.starts_with(&table_min) {
                let mut cursor = Cursor::new(value);
                columns.push(TableCodec::decode_column::<Self, _>(
                    &mut cursor,
                    &reference_tables,
                )?);
            } else {
                index_metas.push(Arc::new(TableCodec::decode_index_meta::<Self>(&value)?));
            }
        }

        Ok((!columns.is_empty()).then_some((columns, index_metas)))
    }

    fn _drop_data(&mut self, min: BumpBytes, max: BumpBytes) -> Result<(), DatabaseError> {
        let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;
        let mut data_keys = vec![];

        while let Some((key, _)) = iter.try_next()? {
            data_keys.push(key);
        }
        drop(iter);

        for key in data_keys {
            self.remove(&key)?
        }

        Ok(())
    }

    fn create_index_meta_from_column(
        &mut self,
        table: &mut TableCatalog,
    ) -> Result<(), DatabaseError> {
        let table_name = table.name.clone();
        let mut primary_keys = Vec::new();

        let schema_ref = table.schema_ref().clone();
        for col in schema_ref.iter() {
            let col_id = col.id().ok_or(DatabaseError::PrimaryKeyNotFound)?;
            let index_ty = if let Some(i) = col.desc().primary() {
                primary_keys.push((i, col_id));
                continue;
            } else if col.desc().is_unique() {
                IndexType::Unique
            } else {
                continue;
            };
            let meta_ref =
                table.add_index_meta(format!("uk_{}_index", col.name()), vec![col_id], index_ty)?;
            let (key, value) =
                unsafe { &*self.table_codec() }.encode_index_meta(&table_name, meta_ref)?;
            self.set(key, value)?;
        }
        let primary_keys = table
            .primary_keys()
            .iter()
            .map(|(_, column)| column.id().unwrap())
            .collect_vec();
        let pk_index_ty = IndexType::PrimaryKey {
            is_multiple: primary_keys.len() != 1,
        };
        let meta_ref = table.add_index_meta("pk_index".to_string(), primary_keys, pk_index_ty)?;
        let (key, value) =
            unsafe { &*self.table_codec() }.encode_index_meta(&table_name, meta_ref)?;
        self.set(key, value)?;

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Bytes>, DatabaseError>;

    fn set(&mut self, key: BumpBytes, value: BumpBytes) -> Result<(), DatabaseError>;

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError>;

    fn range<'a>(
        &'a self,
        min: Bound<BumpBytes<'a>>,
        max: Bound<BumpBytes<'a>>,
    ) -> Result<Self::IterType<'a>, DatabaseError>;

    fn commit(self) -> Result<(), DatabaseError>;
}

trait IndexImpl<'bytes, T: Transaction + 'bytes> {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        pk_indices: &[usize],
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError>;

    fn eq_to_res<'a>(
        &self,
        value: &DataValue,
        pk_indices: &[usize],
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError>;

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
    ) -> Result<BumpBytes<'bytes>, DatabaseError>;
}

enum IndexImplEnum {
    PrimaryKey(PrimaryKeyIndexImpl),
    Unique(UniqueIndexImpl),
    Normal(NormalIndexImpl),
    Composite(CompositeIndexImpl),
}

impl IndexImplEnum {
    fn instance(index_type: IndexType) -> IndexImplEnum {
        match index_type {
            IndexType::PrimaryKey { .. } => IndexImplEnum::PrimaryKey(PrimaryKeyIndexImpl),
            IndexType::Unique => IndexImplEnum::Unique(UniqueIndexImpl),
            IndexType::Normal => IndexImplEnum::Normal(NormalIndexImpl),
            IndexType::Composite => IndexImplEnum::Composite(CompositeIndexImpl),
        }
    }
}

struct PrimaryKeyIndexImpl;
struct UniqueIndexImpl;
struct NormalIndexImpl;
struct CompositeIndexImpl;

struct IndexImplParams<'a, T: Transaction> {
    tuple_schema_ref: Arc<Vec<ColumnRef>>,
    projections: Vec<usize>,

    index_meta: IndexMetaRef,
    table_name: &'a str,
    table_types: Vec<LogicalType>,
    with_pk: bool,
    tx: &'a T,
}

impl<T: Transaction> IndexImplParams<'_, T> {
    #[inline]
    pub(crate) fn value_ty(&self) -> &LogicalType {
        &self.index_meta.value_ty
    }

    #[inline]
    pub(crate) fn table_codec(&self) -> *const TableCodec {
        self.tx.table_codec()
    }

    pub(crate) fn try_cast(&self, mut val: DataValue) -> Result<DataValue, DatabaseError> {
        let value_ty = self.value_ty();

        if &val.logical_type() != value_ty {
            val = val.cast(value_ty)?;
        }
        Ok(val)
    }

    fn get_tuple_by_id(
        &self,
        pk_indices: &[usize],
        tuple_id: &TupleId,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let key = unsafe { &*self.table_codec() }.encode_tuple_key(self.table_name, tuple_id)?;

        self.tx
            .get(&key)?
            .map(|bytes| {
                TableCodec::decode_tuple(
                    &self.table_types,
                    pk_indices,
                    &self.projections,
                    &self.tuple_schema_ref,
                    &bytes,
                    self.with_pk,
                )
            })
            .transpose()
    }
}

enum IndexResult<'a, T: Transaction + 'a> {
    Tuple(Option<Tuple>),
    Scope(T::IterType<'a>),
}

impl<'bytes, T: Transaction + 'bytes> IndexImpl<'bytes, T> for IndexImplEnum {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        pk_indices: &[usize],
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.index_lookup(bytes, pk_indices, params),
            IndexImplEnum::Unique(inner) => inner.index_lookup(bytes, pk_indices, params),
            IndexImplEnum::Normal(inner) => inner.index_lookup(bytes, pk_indices, params),
            IndexImplEnum::Composite(inner) => inner.index_lookup(bytes, pk_indices, params),
        }
    }

    fn eq_to_res<'a>(
        &self,
        value: &DataValue,
        pk_indices: &[usize],
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.eq_to_res(value, pk_indices, params),
            IndexImplEnum::Unique(inner) => inner.eq_to_res(value, pk_indices, params),
            IndexImplEnum::Normal(inner) => inner.eq_to_res(value, pk_indices, params),
            IndexImplEnum::Composite(inner) => inner.eq_to_res(value, pk_indices, params),
        }
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
    ) -> Result<BumpBytes<'bytes>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.bound_key(params, value, is_upper),
            IndexImplEnum::Unique(inner) => inner.bound_key(params, value, is_upper),
            IndexImplEnum::Normal(inner) => inner.bound_key(params, value, is_upper),
            IndexImplEnum::Composite(inner) => inner.bound_key(params, value, is_upper),
        }
    }
}

impl<'bytes, T: Transaction + 'bytes> IndexImpl<'bytes, T> for PrimaryKeyIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        pk_indices: &[usize],
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        TableCodec::decode_tuple(
            &params.table_types,
            pk_indices,
            &params.projections,
            &params.tuple_schema_ref,
            bytes,
            params.with_pk,
        )
    }

    fn eq_to_res<'a>(
        &self,
        value: &DataValue,
        pk_indices: &[usize],
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let tuple = params
            .tx
            .get(&unsafe { &*params.table_codec() }.encode_tuple_key(params.table_name, value)?)?
            .map(|bytes| {
                TableCodec::decode_tuple(
                    &params.table_types,
                    pk_indices,
                    &params.projections,
                    &params.tuple_schema_ref,
                    &bytes,
                    params.with_pk,
                )
            })
            .transpose()?;
        Ok(IndexResult::Tuple(tuple))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        _: bool,
    ) -> Result<BumpBytes<'bytes>, DatabaseError> {
        unsafe { &*params.table_codec() }.encode_tuple_key(params.table_name, value)
    }
}

fn secondary_index_lookup<T: Transaction>(
    bytes: &Bytes,
    pk_indices: &[usize],
    params: &IndexImplParams<T>,
) -> Result<Tuple, DatabaseError> {
    let tuple_id = TableCodec::decode_index(bytes)?;
    params
        .get_tuple_by_id(pk_indices, &tuple_id)?
        .ok_or(DatabaseError::TupleIdNotFound(tuple_id))
}

impl<'bytes, T: Transaction + 'bytes> IndexImpl<'bytes, T> for UniqueIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        pk_indices: &[usize],
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, pk_indices, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &DataValue,
        pk_indices: &[usize],
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let Some(bytes) = params.tx.get(&self.bound_key(params, value, false)?)? else {
            return Ok(IndexResult::Tuple(None));
        };
        let tuple_id = TableCodec::decode_index(&bytes)?;
        let tuple = params
            .get_tuple_by_id(pk_indices, &tuple_id)?
            .ok_or(DatabaseError::TupleIdNotFound(tuple_id))?;
        Ok(IndexResult::Tuple(Some(tuple)))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        _: bool,
    ) -> Result<BumpBytes<'bytes>, DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Unique);

        unsafe { &*params.table_codec() }.encode_index_key(params.table_name, &index, None)
    }
}

impl<'bytes, T: Transaction + 'bytes> IndexImpl<'bytes, T> for NormalIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        pk_indices: &[usize],
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, pk_indices, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &DataValue,
        _: &[usize],
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let min = self.bound_key(params, value, false)?;
        let max = self.bound_key(params, value, true)?;

        let iter = params
            .tx
            .range(Bound::Included(min), Bound::Included(max))?;
        Ok(IndexResult::Scope(iter))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
    ) -> Result<BumpBytes<'bytes>, DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Normal);

        unsafe { &*params.table_codec() }.encode_index_bound_key(
            params.table_name,
            &index,
            is_upper,
        )
    }
}

impl<'bytes, T: Transaction + 'bytes> IndexImpl<'bytes, T> for CompositeIndexImpl {
    fn index_lookup(
        &self,
        bytes: &Bytes,
        pk_indices: &[usize],
        params: &IndexImplParams<T>,
    ) -> Result<Tuple, DatabaseError> {
        secondary_index_lookup(bytes, pk_indices, params)
    }

    fn eq_to_res<'a>(
        &self,
        value: &DataValue,
        _: &[usize],
        params: &IndexImplParams<'a, T>,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let min = self.bound_key(params, value, false)?;
        let max = self.bound_key(params, value, true)?;

        let iter = params
            .tx
            .range(Bound::Included(min), Bound::Included(max))?;
        Ok(IndexResult::Scope(iter))
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
    ) -> Result<BumpBytes<'bytes>, DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Composite);

        unsafe { &*params.table_codec() }.encode_index_bound_key(
            params.table_name,
            &index,
            is_upper,
        )
    }
}

pub struct TupleIter<'a, T: Transaction + 'a> {
    offset: usize,
    limit: Option<usize>,
    table_types: Vec<LogicalType>,
    tuple_columns: Arc<Vec<ColumnRef>>,
    remap_pk_indices: Vec<usize>,
    projections: Vec<usize>,
    with_pk: bool,
    iter: T::IterType<'a>,
}

impl<'a, T: Transaction + 'a> Iter for TupleIter<'a, T> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        while self.offset > 0 {
            if self.iter.try_next()?.is_none() {
                return Ok(None);
            }
            self.offset -= 1;
        }

        #[allow(clippy::never_loop)]
        while let Some((_, value)) = self.iter.try_next()? {
            if let Some(limit) = self.limit.as_mut() {
                if *limit == 0 {
                    return Ok(None);
                }
                *limit -= 1;
            }
            let tuple = TableCodec::decode_tuple(
                &self.table_types,
                &self.remap_pk_indices,
                &self.projections,
                &self.tuple_columns,
                &value,
                self.with_pk,
            )?;

            return Ok(Some(tuple));
        }

        Ok(None)
    }
}

pub struct IndexIter<'a, T: Transaction> {
    offset: usize,
    limit: Option<usize>,

    remap_pk_indices: Vec<usize>,
    params: IndexImplParams<'a, T>,
    inner: IndexImplEnum,
    // for buffering data
    ranges: IntoIter<Range>,
    state: IndexIterState<'a, T>,
}

pub enum IndexIterState<'a, T: Transaction + 'a> {
    Init,
    Range(T::IterType<'a>),
    Over,
}

impl<'a, T: Transaction + 'a> IndexIter<'a, T> {
    fn offset_move(offset: &mut usize) -> bool {
        if *offset > 0 {
            offset.sub_assign(1);

            true
        } else {
            false
        }
    }

    fn limit_sub(limit: &mut Option<usize>) {
        if let Some(num) = limit.as_mut() {
            num.sub_assign(1);
        }
    }
}

/// expression -> index value -> tuple
impl<T: Transaction> Iter for IndexIter<'_, T> {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        fn check_bound<'a>(value: &mut Bound<BumpBytes<'a>>, bound: BumpBytes<'a>) {
            if matches!(value, Bound::Unbounded) {
                let _ = mem::replace(value, Bound::Included(bound));
            }
        }
        if matches!(self.limit, Some(0)) {
            self.state = IndexIterState::Over;

            return Ok(None);
        }

        loop {
            match &mut self.state {
                IndexIterState::Init => {
                    let Some(binary) = self.ranges.next() else {
                        self.state = IndexIterState::Over;
                        continue;
                    };
                    match binary {
                        Range::Scope { min, max } => {
                            let table_name = &self.params.table_name;
                            let index_meta = &self.params.index_meta;
                            let bound_encode =
                                |bound: Bound<DataValue>,
                                 is_upper: bool|
                                 -> Result<_, DatabaseError> {
                                    match bound {
                                        Bound::Included(mut val) => {
                                            val = self.params.try_cast(val)?;

                                            Ok(Bound::Included(self.inner.bound_key(
                                                &self.params,
                                                &val,
                                                is_upper,
                                            )?))
                                        }
                                        Bound::Excluded(mut val) => {
                                            val = self.params.try_cast(val)?;

                                            Ok(Bound::Excluded(self.inner.bound_key(
                                                &self.params,
                                                &val,
                                                is_upper,
                                            )?))
                                        }
                                        Bound::Unbounded => Ok(Bound::Unbounded),
                                    }
                                };
                            let (bound_min, bound_max) =
                                if matches!(index_meta.ty, IndexType::PrimaryKey { .. }) {
                                    unsafe { &*self.params.table_codec() }.tuple_bound(table_name)
                                } else {
                                    unsafe { &*self.params.table_codec() }
                                        .index_bound(table_name, index_meta.id)?
                                };
                            let mut encode_min = bound_encode(min, false)?;
                            check_bound(&mut encode_min, bound_min);

                            let mut encode_max = bound_encode(max, true)?;
                            check_bound(&mut encode_max, bound_max);

                            let iter = self.params.tx.range(encode_min, encode_max)?;
                            self.state = IndexIterState::Range(iter);
                        }
                        Range::Eq(mut val) => {
                            val = self.params.try_cast(val)?;

                            match self.inner.eq_to_res(
                                &val,
                                &self.remap_pk_indices,
                                &self.params,
                            )? {
                                IndexResult::Tuple(tuple) => {
                                    if Self::offset_move(&mut self.offset) {
                                        continue;
                                    }
                                    Self::limit_sub(&mut self.limit);
                                    return Ok(tuple);
                                }
                                IndexResult::Scope(iter) => {
                                    self.state = IndexIterState::Range(iter);
                                }
                            }
                        }
                        _ => (),
                    }
                }
                IndexIterState::Range(iter) => {
                    while let Some((_, bytes)) = iter.try_next()? {
                        if Self::offset_move(&mut self.offset) {
                            continue;
                        }
                        Self::limit_sub(&mut self.limit);
                        let tuple = self.inner.index_lookup(
                            &bytes,
                            &self.remap_pk_indices,
                            &self.params,
                        )?;

                        return Ok(Some(tuple));
                    }
                    self.state = IndexIterState::Init;
                }
                IndexIterState::Over => return Ok(None),
            }
        }
    }
}

pub trait InnerIter {
    fn try_next(&mut self) -> Result<Option<(Bytes, Bytes)>, DatabaseError>;
}

pub trait Iter {
    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}

fn remap_pk_indices(projection: &[usize], pk_indices: &[usize]) -> Vec<usize> {
    pk_indices
        .iter()
        .filter_map(|pk| projection.binary_search(pk).ok())
        .collect()
}

#[cfg(test)]
mod test {
    use crate::binder::test::build_t1_table;
    use crate::catalog::view::View;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary};
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{
        IndexIter, InnerIter, Iter, StatisticsMetaCache, Storage, TableCache, Transaction,
    };
    use crate::types::index::{Index, IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::{ColumnId, LogicalType};
    use crate::utils::lru::SharedLruCache;
    use std::collections::{BTreeMap, Bound};
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn full_columns() -> BTreeMap<usize, ColumnRef> {
        let mut columns = BTreeMap::new();

        columns.insert(
            0,
            ColumnRef::from(ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            )),
        );
        columns.insert(
            1,
            ColumnRef::from(ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            )),
        );
        columns.insert(
            2,
            ColumnRef::from(ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
        );
        columns
    }
    fn build_tuples() -> Vec<Tuple> {
        vec![
            Tuple::new(
                Some(DataValue::Int32(0)),
                vec![
                    DataValue::Int32(0),
                    DataValue::Boolean(true),
                    DataValue::Int32(0),
                ],
            ),
            Tuple::new(
                Some(DataValue::Int32(1)),
                vec![
                    DataValue::Int32(1),
                    DataValue::Boolean(true),
                    DataValue::Int32(1),
                ],
            ),
            Tuple::new(
                Some(DataValue::Int32(2)),
                vec![
                    DataValue::Int32(2),
                    DataValue::Boolean(false),
                    DataValue::Int32(0),
                ],
            ),
        ]
    }

    #[test]
    fn test_table_create_drop() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;

        let fn_assert = |transaction: &mut RocksTransaction,
                         table_cache: &TableCache|
         -> Result<(), DatabaseError> {
            let table = transaction
                .table(&table_cache, Arc::new("t1".to_string()))?
                .unwrap();
            let c1_column_id = *table.get_column_id_by_name("c1").unwrap();
            let c2_column_id = *table.get_column_id_by_name("c2").unwrap();
            let c3_column_id = *table.get_column_id_by_name("c3").unwrap();

            assert_eq!(table.name.as_str(), "t1");
            assert_eq!(table.indexes.len(), 1);

            let primary_key_index_meta = &table.indexes[0];
            assert_eq!(primary_key_index_meta.id, 0);
            assert_eq!(primary_key_index_meta.column_ids, vec![c1_column_id]);
            assert_eq!(
                primary_key_index_meta.table_name,
                Arc::new("t1".to_string())
            );
            assert_eq!(primary_key_index_meta.pk_ty, LogicalType::Integer);
            assert_eq!(primary_key_index_meta.name, "pk_index".to_string());
            assert_eq!(
                primary_key_index_meta.ty,
                IndexType::PrimaryKey { is_multiple: false }
            );

            let mut column_iter = table.columns();
            let c1_column = column_iter.next().unwrap();
            assert_eq!(c1_column.nullable(), false);
            assert_eq!(
                c1_column.summary(),
                &ColumnSummary {
                    name: "c1".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c1_column_id,
                        table_name: Arc::new("t1".to_string()),
                        is_temp: false,
                    },
                }
            );
            assert_eq!(
                c1_column.desc(),
                &ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?
            );

            let c2_column = column_iter.next().unwrap();
            assert_eq!(c2_column.nullable(), false);
            assert_eq!(
                c2_column.summary(),
                &ColumnSummary {
                    name: "c2".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c2_column_id,
                        table_name: Arc::new("t1".to_string()),
                        is_temp: false,
                    },
                }
            );
            assert_eq!(
                c2_column.desc(),
                &ColumnDesc::new(LogicalType::Boolean, None, false, None)?
            );

            let c3_column = column_iter.next().unwrap();
            assert_eq!(c3_column.nullable(), false);
            assert_eq!(
                c3_column.summary(),
                &ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c3_column_id,
                        table_name: Arc::new("t1".to_string()),
                        is_temp: false,
                    },
                }
            );
            assert_eq!(
                c3_column.desc(),
                &ColumnDesc::new(LogicalType::Integer, None, false, None)?
            );

            Ok(())
        };
        fn_assert(&mut transaction, &table_cache)?;
        fn_assert(
            &mut transaction,
            &Arc::new(SharedLruCache::new(4, 1, RandomState::new())?),
        )?;

        Ok(())
    }

    #[test]
    fn test_tuple_append_delete() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;

        let tuples = build_tuples();
        for tuple in tuples.iter().cloned() {
            transaction.append_tuple(
                "t1",
                tuple,
                &[
                    LogicalType::Integer,
                    LogicalType::Boolean,
                    LogicalType::Integer,
                ],
                false,
            )?;
        }
        {
            let mut tuple_iter = transaction.read(
                &table_cache,
                Arc::new("t1".to_string()),
                (None, None),
                full_columns(),
                true,
            )?;

            assert_eq!(tuple_iter.next_tuple()?.unwrap(), tuples[0]);
            assert_eq!(tuple_iter.next_tuple()?.unwrap(), tuples[1]);
            assert_eq!(tuple_iter.next_tuple()?.unwrap(), tuples[2]);

            let (min, max) = table_codec.tuple_bound("t1");
            let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;

            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            assert!(iter.try_next()?.is_none());
        }

        transaction.remove_tuple("t1", &tuples[1].values[0])?;
        {
            let mut tuple_iter = transaction.read(
                &table_cache,
                Arc::new("t1".to_string()),
                (None, None),
                full_columns(),
                true,
            )?;

            assert_eq!(tuple_iter.next_tuple()?.unwrap(), tuples[0]);
            assert_eq!(tuple_iter.next_tuple()?.unwrap(), tuples[2]);

            let (min, max) = table_codec.tuple_bound("t1");
            let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;

            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            assert!(iter.try_next()?.is_none());
        }

        Ok(())
    }

    #[test]
    fn test_add_index_meta() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;
        let (c2_column_id, c3_column_id) = {
            let t1_table = transaction
                .table(&table_cache, Arc::new("t1".to_string()))?
                .unwrap();

            (
                *t1_table.get_column_id_by_name("c2").unwrap(),
                *t1_table.get_column_id_by_name("c3").unwrap(),
            )
        };

        let _ = transaction.add_index_meta(
            &table_cache,
            &Arc::new("t1".to_string()),
            "i1".to_string(),
            vec![c3_column_id],
            IndexType::Normal,
        )?;
        let _ = transaction.add_index_meta(
            &table_cache,
            &Arc::new("t1".to_string()),
            "i2".to_string(),
            vec![c3_column_id, c2_column_id],
            IndexType::Composite,
        )?;

        let fn_assert = |transaction: &mut RocksTransaction,
                         table_cache: &TableCache|
         -> Result<(), DatabaseError> {
            let table = transaction
                .table(&table_cache, Arc::new("t1".to_string()))?
                .unwrap();

            let i1_meta = table.indexes[1].clone();
            assert_eq!(i1_meta.id, 1);
            assert_eq!(i1_meta.column_ids, vec![c3_column_id]);
            assert_eq!(i1_meta.table_name, Arc::new("t1".to_string()));
            assert_eq!(i1_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i1_meta.name, "i1".to_string());
            assert_eq!(i1_meta.ty, IndexType::Normal);

            let i2_meta = table.indexes[2].clone();
            assert_eq!(i2_meta.id, 2);
            assert_eq!(i2_meta.column_ids, vec![c3_column_id, c2_column_id]);
            assert_eq!(i2_meta.table_name, Arc::new("t1".to_string()));
            assert_eq!(i2_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i2_meta.name, "i2".to_string());
            assert_eq!(i2_meta.ty, IndexType::Composite);

            Ok(())
        };
        fn_assert(&mut transaction, &table_cache)?;
        fn_assert(
            &mut transaction,
            &Arc::new(SharedLruCache::new(4, 1, RandomState::new())?),
        )?;
        {
            let (min, max) = table_codec.index_meta_bound("t1");
            let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;

            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            assert!(iter.try_next()?.is_none());
        }
        match transaction.drop_index(&table_cache, Arc::new("t1".to_string()), "pk_index", false) {
            Err(DatabaseError::InvalidIndex) => (),
            _ => unreachable!(),
        }
        transaction.drop_index(&table_cache, Arc::new("t1".to_string()), "i1", false)?;
        {
            let table = transaction
                .table(&table_cache, Arc::new("t1".to_string()))?
                .unwrap();
            let i2_meta = table.indexes[1].clone();
            assert_eq!(i2_meta.id, 2);
            assert_eq!(i2_meta.column_ids, vec![c3_column_id, c2_column_id]);
            assert_eq!(i2_meta.table_name, Arc::new("t1".to_string()));
            assert_eq!(i2_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i2_meta.name, "i2".to_string());
            assert_eq!(i2_meta.ty, IndexType::Composite);

            let (min, max) = table_codec.index_meta_bound("t1");
            let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;

            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            assert!(iter.try_next()?.is_none());
        }

        Ok(())
    }

    #[test]
    fn test_index_insert_delete() -> Result<(), DatabaseError> {
        fn build_index_iter<'a>(
            transaction: &'a RocksTransaction<'a>,
            table_cache: &'a Arc<TableCache>,
            index_column_id: ColumnId,
        ) -> Result<IndexIter<'a, RocksTransaction<'a>>, DatabaseError> {
            transaction.read_by_index(
                &table_cache,
                Arc::new("t1".to_string()),
                (None, None),
                full_columns(),
                Arc::new(IndexMeta {
                    id: 1,
                    column_ids: vec![index_column_id],
                    table_name: Arc::new("t1".to_string()),
                    pk_ty: LogicalType::Integer,
                    value_ty: LogicalType::Integer,
                    name: "i1".to_string(),
                    ty: IndexType::Normal,
                }),
                vec![Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                }],
                true,
            )
        }

        let table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;
        let t1_table = transaction
            .table(&table_cache, Arc::new("t1".to_string()))?
            .unwrap();
        let c3_column_id = *t1_table.get_column_id_by_name("c3").unwrap();

        let _ = transaction.add_index_meta(
            &table_cache,
            &Arc::new("t1".to_string()),
            "i1".to_string(),
            vec![c3_column_id],
            IndexType::Normal,
        )?;

        let tuples = build_tuples();
        let indexes = vec![
            (
                Arc::new(DataValue::Int32(0)),
                Index::new(1, &tuples[0].values[2], IndexType::Normal),
            ),
            (
                Arc::new(DataValue::Int32(1)),
                Index::new(1, &tuples[1].values[2], IndexType::Normal),
            ),
            (
                Arc::new(DataValue::Int32(2)),
                Index::new(1, &tuples[2].values[2], IndexType::Normal),
            ),
        ];
        for (tuple_id, index) in indexes.iter().cloned() {
            transaction.add_index("t1", index, &tuple_id)?;
        }
        for tuple in tuples.iter().cloned() {
            transaction.append_tuple(
                "t1",
                tuple,
                &[
                    LogicalType::Integer,
                    LogicalType::Boolean,
                    LogicalType::Integer,
                ],
                false,
            )?;
        }
        {
            let mut index_iter = build_index_iter(&transaction, &table_cache, c3_column_id)?;

            assert_eq!(index_iter.next_tuple()?.unwrap(), tuples[0]);
            assert_eq!(index_iter.next_tuple()?.unwrap(), tuples[2]);
            assert_eq!(index_iter.next_tuple()?.unwrap(), tuples[1]);

            let (min, max) = table_codec.index_bound("t1", 1)?;
            let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;

            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            assert!(iter.try_next()?.is_none());
        }
        transaction.del_index("t1", &indexes[0].1, &indexes[0].0)?;

        let mut index_iter = build_index_iter(&transaction, &table_cache, c3_column_id)?;

        assert_eq!(index_iter.next_tuple()?.unwrap(), tuples[2]);
        assert_eq!(index_iter.next_tuple()?.unwrap(), tuples[1]);

        let (min, max) = table_codec.index_bound("t1", 1)?;
        let mut iter = transaction.range(Bound::Included(min), Bound::Included(max))?;

        let (_, value) = iter.try_next()?.unwrap();
        dbg!(value);
        let (_, value) = iter.try_next()?.unwrap();
        dbg!(value);
        assert!(iter.try_next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_column_add_drop() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let meta_cache = StatisticsMetaCache::new(4, 1, RandomState::new())?;

        build_table(&table_cache, &mut transaction)?;
        let table_name = Arc::new("t1".to_string());

        let new_column = ColumnCatalog::new(
            "c4".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        let new_column_id =
            transaction.add_column(&table_cache, &table_name, &new_column, false)?;
        {
            assert!(transaction
                .add_column(&table_cache, &table_name, &new_column, false,)
                .is_err());
            assert_eq!(
                new_column_id,
                transaction.add_column(&table_cache, &table_name, &new_column, true,)?
            );
        }
        {
            let table = transaction
                .table(&table_cache, table_name.clone())?
                .unwrap();
            assert!(table.contains_column("c4"));

            let mut new_column = ColumnCatalog::new(
                "c4".to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
            );
            new_column.summary_mut().relation = ColumnRelation::Table {
                column_id: *table.get_column_id_by_name("c4").unwrap(),
                table_name: table_name.clone(),
                is_temp: false,
            };
            assert_eq!(
                table.get_column_by_name("c4"),
                Some(&ColumnRef::from(new_column))
            );
        }
        transaction.drop_column(&table_cache, &meta_cache, &table_name, "c4")?;
        {
            let table = transaction
                .table(&table_cache, table_name.clone())?
                .unwrap();
            assert!(!table.contains_column("c4"));
            assert!(table.get_column_by_name("c4").is_none());
        }

        Ok(())
    }

    #[test]
    fn test_view_create_drop() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;

        let view_name = Arc::new("v1".to_string());
        let view = View {
            name: view_name.clone(),
            plan: Box::new(
                table_state.plan("select c1, c3 from t1 inner join t2 on c1 = c3 and c1 > 1")?,
            ),
        };
        let mut transaction = table_state.storage.transaction()?;
        transaction.create_view(&table_state.view_cache, view.clone(), true)?;

        assert_eq!(
            &view,
            transaction
                .view(
                    &table_state.table_cache,
                    &table_state.view_cache,
                    view_name.clone(),
                )?
                .unwrap()
        );
        assert_eq!(
            &view,
            transaction
                .view(
                    &Arc::new(SharedLruCache::new(4, 1, RandomState::new())?),
                    &table_state.view_cache,
                    view_name.clone(),
                )?
                .unwrap()
        );

        transaction.drop_view(
            &table_state.view_cache,
            &table_state.table_cache,
            view_name.clone(),
            false,
        )?;
        transaction.drop_view(
            &table_state.view_cache,
            &table_state.table_cache,
            view_name.clone(),
            true,
        )?;
        assert!(transaction
            .view(&table_state.table_cache, &table_state.view_cache, view_name)?
            .is_none());

        Ok(())
    }
}
