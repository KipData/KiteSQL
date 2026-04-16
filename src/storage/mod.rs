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

#[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
pub mod lmdb;
pub mod memory;
#[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
pub mod rocksdb;
pub(crate) mod table_codec;

use crate::catalog::view::View;
use crate::catalog::{ColumnCatalog, ColumnRef, TableCatalog, TableMeta, TableName};
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::expression::ScalarExpression;
use crate::optimizer::core::cm_sketch::{
    CountMinSketch, CountMinSketchPage, COUNT_MIN_SKETCH_STORAGE_PAGE_LEN,
};
use crate::optimizer::core::statistics_meta::{StatisticMetaLoader, StatisticsMeta};
use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
use crate::serdes::ReferenceTables;
use crate::storage::table_codec::{
    BumpBytes, Bytes, StatisticsCodecType, TableCodec, BOUND_MAX_TAG,
};
use crate::types::index::{Index, IndexId, IndexMeta, IndexMetaRef, IndexType};
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, TupleMappingRef};
use crate::types::{ColumnId, LogicalType};
use crate::utils::lru::SharedLruCache;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{BTreeMap, Bound};
use std::fmt::{self, Display, Formatter};
use std::io::Cursor;
use std::mem;
use std::ops::SubAssign;
use std::path::Path;
use std::sync::Arc;

pub type KeyValueRef<'a> = (&'a [u8], &'a [u8]);
use ulid::Generator;

pub(crate) type StatisticsMetaCache = SharedLruCache<(TableName, IndexId), Option<StatisticsMeta>>;
pub(crate) type TableCache = SharedLruCache<TableName, TableCatalog>;
pub(crate) type ViewCache = SharedLruCache<TableName, View>;

/// Transaction isolation levels supported by KiteSQL.
///
/// See [`crate::docs::transaction_isolation`] for the storage support matrix
/// and the detailed visibility rules used by KiteSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionIsolationLevel {
    /// Statement-level snapshot isolation for ordinary reads.
    ReadCommitted,
    /// Transaction-level fixed snapshot for ordinary reads.
    RepeatableRead,
}

impl Display for TransactionIsolationLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TransactionIsolationLevel::ReadCommitted => f.write_str("read committed"),
            TransactionIsolationLevel::RepeatableRead => f.write_str("repeatable read"),
        }
    }
}

pub(crate) fn index_value_type(
    table: &TableCatalog,
    column_ids: &[ColumnId],
) -> Result<LogicalType, DatabaseError> {
    let mut value_types = Vec::with_capacity(column_ids.len());
    for column_id in column_ids {
        let value_type = table
            .get_column_by_id(column_id)
            .ok_or_else(|| DatabaseError::column_not_found(column_id.to_string()))?
            .datatype()
            .clone();
        value_types.push(value_type);
    }

    Ok(if value_types.len() == 1 {
        value_types.pop().unwrap()
    } else {
        LogicalType::Tuple(value_types)
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EmptyStorageMetrics;

impl Display for EmptyStorageMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("")
    }
}

pub trait Storage: Clone {
    type Metrics: Display;

    type TransactionType<'a>: Transaction
    where
        Self: 'a;

    fn transaction(&self) -> Result<Self::TransactionType<'_>, DatabaseError> {
        self.transaction_with_isolation(self.default_transaction_isolation())
    }

    fn transaction_with_isolation(
        &self,
        isolation: TransactionIsolationLevel,
    ) -> Result<Self::TransactionType<'_>, DatabaseError>;

    fn default_transaction_isolation(&self) -> TransactionIsolationLevel {
        TransactionIsolationLevel::ReadCommitted
    }

    fn validate_transaction_isolation(
        &self,
        isolation: TransactionIsolationLevel,
    ) -> Result<(), DatabaseError> {
        if isolation == self.default_transaction_isolation() {
            Ok(())
        } else {
            Err(DatabaseError::UnsupportedStmt(format!(
                "transaction isolation `{isolation}` is not supported by this storage"
            )))
        }
    }

    fn metrics(&self) -> Option<Self::Metrics> {
        None
    }
}

/// Optional capability for storage engines that can materialize an online
/// consistent checkpoint to the local filesystem.
pub trait CheckpointableStorage: Storage {
    fn create_checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<(), DatabaseError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
pub(crate) type Bounds = (Option<usize>, Option<usize>);

pub trait Transaction: Sized {
    type BorrowedBytes<'a>: AsRef<[u8]>
    where
        Self: 'a;

    type IterType<'a>: InnerIter
    where
        Self: 'a;

    fn table_codec(&self) -> *const TableCodec;

    fn begin_statement_scope(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn end_statement_scope(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

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
        if with_pk {
            for (i, column) in table.primary_keys() {
                columns.insert(*i, column.clone());
            }
        }
        let deserializers = Self::create_deserializers(&columns, table);
        let pk_ty = with_pk.then(|| table.primary_keys_type().clone());
        let offset = bounds.0.unwrap_or(0);

        unsafe { &*self.table_codec() }.with_tuple_bound(&table_name, |min, max| {
            let iter = self.range(Bound::Included(min), Bound::Included(max))?;

            Ok(TupleIter {
                bounds: IterBounds::new(offset, bounds.1),
                pk_ty,
                deserializers,
                total_len: table.columns_len(),
                iter,
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn read_by_index<'a, R>(
        &'a self,
        table_cache: &'a TableCache,
        table_name: TableName,
        (offset_option, limit_option): Bounds,
        mut columns: BTreeMap<usize, ColumnRef>,
        index_meta: IndexMetaRef,
        ranges: R,
        with_pk: bool,
        covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
        cover_mapping_indices: Option<Vec<usize>>,
    ) -> Result<IndexIter<'a, Self>, DatabaseError>
    where
        R: Into<IndexRanges>,
    {
        debug_assert!(columns.keys().all_unique());
        let table = self
            .table(table_cache, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let table_name = table.name.as_ref();
        let offset = offset_option.unwrap_or(0);

        if with_pk {
            for (i, column) in table.primary_keys() {
                columns.insert(*i, column.clone());
            }
        }
        let is_primary_index = matches!(index_meta.ty, IndexType::PrimaryKey { .. });
        let (inner, deserializers, cover_mapping) = match (
            covered_deserializers,
            cover_mapping_indices,
            is_primary_index,
        ) {
            (Some(deserializers), mapping, false) => {
                let tuple_len = match &index_meta.value_ty {
                    LogicalType::Tuple(tys) => tys.len(),
                    _ => 1,
                };
                let cover_mapping = mapping.map(|slots| TupleMapping::new(slots, tuple_len));

                (
                    IndexImplEnum::Covered(CoveredIndexImpl),
                    deserializers,
                    cover_mapping,
                )
            }
            _ => {
                let deserializers = Self::create_deserializers(&columns, table);
                (IndexImplEnum::instance(index_meta.ty), deserializers, None)
            }
        };

        Ok(IndexIter {
            bounds: IterBounds::new(offset, limit_option),
            params: IndexImplParams {
                index_meta,
                table_name,
                deserializers,
                total_len: table.columns_len(),
                tx: self,
                cover_mapping,
                with_pk,
            },
            inner,
            ranges: ranges.into(),
            state: IndexIterState::Init,
            encode_min_buffer: Bytes::new(),
            encode_max_buffer: Bytes::new(),
        })
    }

    fn create_deserializers(
        columns: &BTreeMap<usize, ColumnRef>,
        table: &TableCatalog,
    ) -> Vec<TupleValueSerializableImpl> {
        let mut deserializers = Vec::with_capacity(columns.len());
        let mut last_projection = None;
        for (projection, column) in columns.iter() {
            let (start, end) = last_projection
                .map(|last_projection| {
                    let start = last_projection + 1;
                    let len = projection - start;
                    (start, start + len)
                })
                .unwrap_or((0, *projection));
            for skip_column in table.schema_ref()[start..end].iter() {
                deserializers.push(skip_column.datatype().skip_serializable());
            }
            deserializers.push(column.datatype().serializable());
            last_projection = Some(*projection);
        }
        deserializers
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
            let value = unsafe { &*self.table_codec() }.encode_index_meta_value(index_meta)?;
            unsafe { &*self.table_codec() }.with_index_meta_key(
                table_name,
                index_meta.id,
                |key| self.set(key, value.as_slice()),
            )?;
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
        let mut value = BumpBytes::new_in(unsafe { &*self.table_codec() }.arena());
        bincode::serialize_into(&mut value, tuple_id)?;

        unsafe { &*self.table_codec() }.with_index_key(table_name, &index, Some(tuple_id), |key| {
            if matches!(index.ty, IndexType::Unique) {
                if let Some(bytes) = self.get_borrowed(key)? {
                    return if bytes.as_ref() != value.as_slice() {
                        Err(DatabaseError::DuplicateUniqueValue)
                    } else {
                        Ok(())
                    };
                }
            }
            self.set(key, value.as_slice())
        })
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
        unsafe { &*self.table_codec() }.with_index_key(
            table_name,
            index,
            Some(tuple_id),
            |key| self.remove(key),
        )?;

        Ok(())
    }

    fn append_tuple(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        serializers: &[TupleValueSerializableImpl],
        is_overwrite: bool,
    ) -> Result<(), DatabaseError> {
        let tuple_id = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;
        let value = tuple.serialize_to(serializers, unsafe { &*self.table_codec() }.arena())?;

        unsafe { &*self.table_codec() }.with_tuple_key(table_name, tuple_id, |key| {
            if !is_overwrite && self.exists(key)? {
                return Err(DatabaseError::DuplicatePrimaryKey);
            }
            self.set(key, value.as_slice())
        })
    }

    fn remove_tuple(&mut self, table_name: &str, tuple_id: &TupleId) -> Result<(), DatabaseError> {
        unsafe { &*self.table_codec() }.with_tuple_key(table_name, tuple_id, |key| self.remove(key))
    }

    fn rewrite_table_metadata(
        &mut self,
        table_cache: &TableCache,
        table: &TableCatalog,
    ) -> Result<(), DatabaseError> {
        let table_name = table.name().clone();
        unsafe { &*self.table_codec() }.with_columns_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        unsafe { &*self.table_codec() }
            .with_index_meta_bound(table_name.as_ref(), |min, max| {
                self.remove_range(Bound::Included(min), Bound::Included(max))
            })?;

        let mut reference_tables = ReferenceTables::new();
        let _ = reference_tables.push_or_replace(table.name());
        for column in table.columns() {
            let value = unsafe { &*self.table_codec() }
                .encode_column_value(column, &mut reference_tables)?;
            unsafe { &*self.table_codec() }
                .with_column_key(column, |key| self.set(key, value.as_slice()))?;
        }
        for index_meta in table.indexes() {
            let value = unsafe { &*self.table_codec() }.encode_index_meta_value(index_meta)?;
            unsafe { &*self.table_codec() }.with_index_meta_key(
                table.name(),
                index_meta.id,
                |key| self.set(key, value.as_slice()),
            )?;
        }
        table_cache.remove(table.name());

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn change_column(
        &mut self,
        table_cache: &TableCache,
        table_name: &TableName,
        old_column_name: &str,
        new_column_name: &str,
        new_data_type: &LogicalType,
        default_change: &DefaultChange,
        not_null_change: &NotNullChange,
    ) -> Result<TableCatalog, DatabaseError> {
        let table = self
            .table(table_cache, table_name.clone())?
            .cloned()
            .ok_or(DatabaseError::TableNotFound)?;
        let mut column_refs = Vec::with_capacity(table.columns_len());
        let mut found = false;

        if old_column_name != new_column_name && table.get_column_by_name(new_column_name).is_some()
        {
            return Err(DatabaseError::DuplicateColumn(new_column_name.to_string()));
        }

        for column in table.columns() {
            let mut new_column = ColumnCatalog::clone(column);
            if column.name() == old_column_name {
                found = true;
                new_column.set_name(new_column_name.to_string());
                new_column.desc_mut().column_datatype = new_data_type.clone();
                match default_change {
                    DefaultChange::NoChange => {
                        if let Some(default_expr) = new_column.desc().default.clone() {
                            new_column.desc_mut().default = Some(ScalarExpression::type_cast(
                                default_expr,
                                Cow::Borrowed(new_data_type),
                            )?);
                        }
                    }
                    DefaultChange::Set(default_expr) => {
                        new_column.desc_mut().default = Some(default_expr.clone());
                    }
                    DefaultChange::Drop => {
                        new_column.desc_mut().default = None;
                    }
                }
                match not_null_change {
                    NotNullChange::NoChange => {}
                    NotNullChange::Set => new_column.set_nullable(false),
                    NotNullChange::Drop => new_column.set_nullable(true),
                }
                if new_column.desc().is_primary() {
                    TableCodec::check_primary_key_type(new_data_type)?;
                    new_column.set_nullable(false);
                }
            }
            column_refs.push(ColumnRef::from(new_column));
        }
        if !found {
            return Err(DatabaseError::column_not_found(old_column_name.to_string()));
        }

        let temp_table = TableCatalog::reload(table_name.clone(), column_refs.clone(), vec![])?;
        let index_metas = table
            .indexes()
            .map(|index_meta| {
                Ok(Arc::new(IndexMeta {
                    id: index_meta.id,
                    column_ids: index_meta.column_ids.clone(),
                    table_name: table_name.clone(),
                    pk_ty: temp_table.primary_keys_type().clone(),
                    value_ty: index_value_type(&temp_table, &index_meta.column_ids)?,
                    name: index_meta.name.clone(),
                    ty: index_meta.ty,
                }))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        let updated_table = TableCatalog::reload(table_name.clone(), column_refs, index_metas)?;
        self.rewrite_table_metadata(table_cache, &updated_table)?;

        Ok(updated_table)
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
                let value = unsafe { &*self.table_codec() }.encode_index_meta_value(meta_ref)?;
                unsafe { &*self.table_codec() }.with_index_meta_key(
                    table_name,
                    meta_ref.id,
                    |key| self.set(key, value.as_slice()),
                )?;
            }

            let column = table.get_column_by_id(&col_id).unwrap();
            let value = unsafe { &*self.table_codec() }
                .encode_column_value(column, &mut ReferenceTables::new())?;
            unsafe { &*self.table_codec() }
                .with_column_key(column, |key| self.set(key, value.as_slice()))?;
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

            unsafe { &*self.table_codec() }.with_column_key(column, |key| self.remove(key))?;

            for index_meta in table_catalog.indexes.iter() {
                if !index_meta.column_ids.contains(&column.id().unwrap()) {
                    continue;
                }
                unsafe { &*self.table_codec() }.with_index_meta_key(
                    table_name,
                    index_meta.id,
                    |key| self.remove(key),
                )?;

                unsafe { &*self.table_codec() }.with_index_bound(
                    table_name,
                    index_meta.id,
                    |min, max| self.remove_range(Bound::Included(min), Bound::Included(max)),
                )?;

                self.remove_statistics_meta(meta_cache, table_name, index_meta.id)?;
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
        let value = unsafe { &*self.table_codec() }.encode_view_value(&view)?;

        let already_exists =
            unsafe { &*self.table_codec() }.with_view_key(&view.name, |key| self.exists(key))?;
        if !or_replace && already_exists {
            return Err(DatabaseError::ViewExists);
        }
        if !already_exists {
            self.check_name_hash(&view.name)?;
        }
        unsafe { &*self.table_codec() }
            .with_view_key(&view.name, |key| self.set(key, value.as_slice()))?;
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

        let value = unsafe { &*self.table_codec() }
            .encode_root_table_value(&TableMeta::empty(table_name.clone()))?;
        let exists = unsafe { &*self.table_codec() }
            .with_root_table_key(table_name.as_ref(), |key| self.exists(key))?;
        if exists {
            if if_not_exists {
                return Ok(table_name);
            }
            return Err(DatabaseError::TableExists);
        }
        self.check_name_hash(&table_name)?;
        self.create_index_meta_from_column(&mut table_catalog)?;
        unsafe { &*self.table_codec() }
            .with_root_table_key(table_name.as_ref(), |key| self.set(key, value.as_slice()))?;

        let mut reference_tables = ReferenceTables::new();
        for column in table_catalog.columns() {
            let value = unsafe { &*self.table_codec() }
                .encode_column_value(column, &mut reference_tables)?;
            unsafe { &*self.table_codec() }
                .with_column_key(column, |key| self.set(key, value.as_slice()))?;
        }
        debug_assert_eq!(reference_tables.len(), 1);
        table_cache.put(table_name.clone(), table_catalog);

        Ok(table_name)
    }

    fn check_name_hash(&mut self, table_name: &TableName) -> Result<(), DatabaseError> {
        if unsafe { &*self.table_codec() }
            .with_table_hash_key(table_name, |key| self.exists(key))?
        {
            return Err(DatabaseError::DuplicateSourceHash(table_name.to_string()));
        }
        unsafe { &*self.table_codec() }.with_table_hash_key(table_name, |key| self.set(key, &[]))
    }

    fn drop_name_hash(&mut self, table_name: &TableName) -> Result<(), DatabaseError> {
        unsafe { &*self.table_codec() }.with_table_hash_key(table_name, |key| self.remove(key))
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

        unsafe { &*self.table_codec() }
            .with_view_key(view_name.as_ref(), |key| self.remove(key))?;
        view_cache.remove(&view_name);

        Ok(())
    }

    fn drop_index(
        &mut self,
        table_cache: &TableCache,
        meta_cache: &StatisticsMetaCache,
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
            IndexType::PrimaryKey { .. } => return Err(DatabaseError::InvalidIndex),
            IndexType::Unique | IndexType::Normal | IndexType::Composite => (),
        }

        let index_id = index_meta.id;
        unsafe { &*self.table_codec() }.with_index_meta_key(
            table_name.as_ref(),
            index_id,
            |key| self.remove(key),
        )?;

        unsafe { &*self.table_codec() }.with_index_bound(
            table_name.as_ref(),
            index_id,
            |min, max| self.remove_range(Bound::Included(min), Bound::Included(max)),
        )?;

        self.remove_statistics_meta(meta_cache, &table_name, index_id)?;

        table_cache.remove(&table_name);

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
        self.drop_data(table_name.as_ref())?;

        unsafe { &*self.table_codec() }.with_columns_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        unsafe { &*self.table_codec() }
            .with_index_meta_bound(table_name.as_ref(), |min, max| {
                self.remove_range(Bound::Included(min), Bound::Included(max))
            })?;

        unsafe { &*self.table_codec() }
            .with_root_table_key(table_name.as_ref(), |key| self.remove(key))?;
        table_cache.remove(&table_name);

        Ok(())
    }

    fn drop_data(&mut self, table_name: &str) -> Result<(), DatabaseError> {
        unsafe { &*self.table_codec() }.with_tuple_bound(table_name, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        unsafe { &*self.table_codec() }.with_all_index_bound(table_name, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        unsafe { &*self.table_codec() }.with_statistics_bound(table_name, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })
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
        unsafe { &*self.table_codec() }.with_view_key(&view_name, |key| {
            let Some(bytes) = self.get_borrowed(key)? else {
                return Ok(None);
            };
            Ok(Some(view_cache.get_or_insert(view_name.clone(), |_| {
                TableCodec::decode_view(bytes.as_ref(), (self, table_cache))
            })?))
        })
    }

    fn views(&self, table_cache: &TableCache) -> Result<Vec<View>, DatabaseError> {
        let mut metas = vec![];
        unsafe { &*self.table_codec() }.with_view_bound(|min, max| {
            let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;

            while let Some((_, value)) = iter.try_next().ok().flatten() {
                let meta = TableCodec::decode_view(value, (self, table_cache))?;

                metas.push(meta);
            }

            Ok(metas)
        })
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
        unsafe { &*self.table_codec() }.with_root_table_bound(|min, max| {
            let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;

            while let Some((_, value)) = iter.try_next().ok().flatten() {
                let meta = TableCodec::decode_root_table::<Self>(value)?;

                metas.push(meta);
            }

            Ok(metas)
        })
    }

    fn save_statistics_meta(
        &mut self,
        meta_cache: &StatisticsMetaCache,
        table_name: &TableName,
        statistics_meta: StatisticsMeta,
    ) -> Result<(), DatabaseError> {
        let index_id = statistics_meta.index_id();
        let cached_meta = statistics_meta.clone();
        let (root, buckets, cm_sketch) = statistics_meta.into_parts();
        let value = unsafe { &*self.table_codec() }.encode_statistics_meta_value(&root)?;
        unsafe { &*self.table_codec() }.with_statistics_meta_key(
            table_name.as_ref(),
            index_id,
            |key| self.set(key, value.as_slice()),
        )?;
        let (sketch_meta, sketch_pages) =
            cm_sketch.into_storage_parts(COUNT_MIN_SKETCH_STORAGE_PAGE_LEN);
        let value =
            unsafe { &*self.table_codec() }.encode_statistics_sketch_meta_value(&sketch_meta)?;
        unsafe { &*self.table_codec() }.with_statistics_sketch_meta_key(
            table_name.as_ref(),
            index_id,
            |key| self.set(key, value.as_slice()),
        )?;
        for sketch_page in sketch_pages {
            let value = unsafe { &*self.table_codec() }
                .encode_statistics_sketch_page_value(&sketch_page)?;
            unsafe { &*self.table_codec() }.with_statistics_sketch_page_key(
                table_name.as_ref(),
                index_id,
                &sketch_page,
                |key| self.set(key, value.as_slice()),
            )?;
        }

        for (ordinal, bucket) in buckets.iter().enumerate() {
            let value = unsafe { &*self.table_codec() }.encode_statistics_bucket_value(bucket)?;
            unsafe { &*self.table_codec() }.with_statistics_bucket_key(
                table_name.as_ref(),
                index_id,
                ordinal as u32,
                |key| self.set(key, value.as_slice()),
            )?;
        }

        meta_cache.put((table_name.clone(), index_id), Some(cached_meta));

        Ok(())
    }

    fn statistics_meta(
        &self,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<Option<StatisticsMeta>, DatabaseError> {
        unsafe { &*self.table_codec() }.with_statistics_index_bound(
            table_name,
            index_id,
            |min, max| {
                let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;
                let mut root = None;
                let mut buckets = Vec::new();
                let mut sketch_meta = None;
                let mut sketch_pages = Vec::<CountMinSketchPage>::new();

                while let Some((key, value)) = iter.try_next()? {
                    match unsafe { &*self.table_codec() }.decode_statistics_codec_type(key)? {
                        StatisticsCodecType::Root => {
                            root = Some(TableCodec::decode_statistics_meta::<Self>(value)?);
                        }
                        StatisticsCodecType::SketchMeta => {
                            sketch_meta =
                                Some(TableCodec::decode_statistics_sketch_meta::<Self>(value)?);
                        }
                        StatisticsCodecType::SketchPage => {
                            sketch_pages
                                .push(TableCodec::decode_statistics_sketch_page::<Self>(value)?);
                        }
                        StatisticsCodecType::Bucket => {
                            buckets.push(TableCodec::decode_statistics_bucket::<Self>(value)?);
                        }
                    }
                }

                match (root, sketch_meta) {
                    (Some(root), Some(sketch_meta)) => {
                        let sketch = CountMinSketch::from_storage_parts(sketch_meta, sketch_pages)?;
                        StatisticsMeta::from_parts(root, buckets, sketch).map(Some)
                    }
                    (None, None) if buckets.is_empty() && sketch_pages.is_empty() => Ok(None),
                    _ => Err(DatabaseError::InvalidValue(
                        "statistics meta is incomplete".to_string(),
                    )),
                }
            },
        )
    }

    fn remove_statistics_meta(
        &mut self,
        meta_cache: &StatisticsMetaCache,
        table_name: &TableName,
        index_id: IndexId,
    ) -> Result<(), DatabaseError> {
        unsafe { &*self.table_codec() }.with_statistics_index_bound(
            table_name.as_ref(),
            index_id,
            |min, max| self.remove_range(Bound::Included(min), Bound::Included(max)),
        )?;

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
        unsafe { &*self.table_codec() }.with_table_bound(table_name, |table_min, table_max| {
            let mut column_iter =
                self.range(Bound::Included(table_min), Bound::Included(table_max))?;

            let mut columns = Vec::new();
            let mut index_metas = Vec::new();
            let mut reference_tables = ReferenceTables::new();
            let _ = reference_tables.push_or_replace(table_name);

            // Tips: only `Column`, `IndexMeta`, `TableMeta`
            while let Some((key, value)) = column_iter.try_next().ok().flatten() {
                if key.starts_with(table_min) {
                    let mut cursor = Cursor::new(value);
                    columns.push(TableCodec::decode_column::<Self, _>(
                        &mut cursor,
                        &reference_tables,
                    )?);
                } else {
                    index_metas.push(Arc::new(TableCodec::decode_index_meta::<Self>(value)?));
                }
            }

            Ok((!columns.is_empty()).then_some((columns, index_metas)))
        })
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
            let value = unsafe { &*self.table_codec() }.encode_index_meta_value(meta_ref)?;
            unsafe { &*self.table_codec() }.with_index_meta_key(
                &table_name,
                meta_ref.id,
                |key| self.set(key, value.as_slice()),
            )?;
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
        let value = unsafe { &*self.table_codec() }.encode_index_meta_value(meta_ref)?;
        unsafe { &*self.table_codec() }.with_index_meta_key(&table_name, meta_ref.id, |key| {
            self.set(key, value.as_slice())
        })?;

        Ok(())
    }

    fn get_borrowed<'a>(
        &'a self,
        key: &[u8],
    ) -> Result<Option<Self::BorrowedBytes<'a>>, DatabaseError>;

    fn exists(&self, key: &[u8]) -> Result<bool, DatabaseError> {
        Ok(self.get_borrowed(key)?.is_some())
    }

    fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), DatabaseError>;

    fn remove(&mut self, key: &[u8]) -> Result<(), DatabaseError>;

    fn range<'txn, 'key>(
        &'txn self,
        min: Bound<&'key [u8]>,
        max: Bound<&'key [u8]>,
    ) -> Result<Self::IterType<'txn>, DatabaseError>;

    fn remove_range(&mut self, min: Bound<&[u8]>, max: Bound<&[u8]>) -> Result<(), DatabaseError> {
        const DELETE_BATCH_SIZE: usize = 1024;

        let mut lower = owned_bound(min);
        let upper = owned_bound(max);
        let mut data_keys = Vec::with_capacity(DELETE_BATCH_SIZE);

        loop {
            data_keys.clear();
            let mut iter =
                self.range(bytes_bound_as_slice(&lower), bytes_bound_as_slice(&upper))?;

            while data_keys.len() < DELETE_BATCH_SIZE {
                let Some((key, _)) = iter.try_next()? else {
                    break;
                };
                data_keys.push(key.to_vec());
            }
            drop(iter);

            let Some(last_key) = data_keys.pop() else {
                return Ok(());
            };
            let batch_full = data_keys.len() + 1 == DELETE_BATCH_SIZE;

            for key in data_keys.drain(..) {
                self.remove(&key)?;
            }
            self.remove(&last_key)?;

            if !batch_full {
                return Ok(());
            }
            reuse_bound_as_excluded(&mut lower, &last_key);
        }
    }

    fn commit(self) -> Result<(), DatabaseError>;
}

fn owned_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.to_vec()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn reuse_bound_as_excluded(bound: &mut Bound<Bytes>, key: &[u8]) {
    let mut bytes = match mem::replace(bound, Bound::Unbounded) {
        Bound::Included(bytes) | Bound::Excluded(bytes) => bytes,
        Bound::Unbounded => Vec::new(),
    };
    bytes.clear();
    bytes.extend_from_slice(key);
    *bound = Bound::Excluded(bytes);
}

fn bytes_bound_as_slice(bound: &Bound<Bytes>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes.as_slice()),
        Bound::Excluded(bytes) => Bound::Excluded(bytes.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[inline]
fn fill_default_bound<'a>(bound: Bound<&'a [u8]>, default: &'a [u8]) -> Bound<&'a [u8]> {
    match bound {
        Bound::Included(bytes) => Bound::Included(bytes),
        Bound::Excluded(bytes) => Bound::Excluded(bytes),
        Bound::Unbounded => Bound::Included(default),
    }
}

#[inline]
fn encode_bound_key(buffer: &mut Bytes, key: &[u8], is_upper: bool) {
    buffer.clear();
    buffer.extend_from_slice(key);
    if is_upper {
        buffer.push(BOUND_MAX_TAG);
    }
}

#[inline]
fn encode_bound<'a>(
    bound: &Bound<DataValue>,
    is_upper: bool,
    buffer: &'a mut Bytes,
    params: &IndexImplParams<'_, impl Transaction>,
    inner: &IndexImplEnum,
) -> Result<Bound<&'a [u8]>, DatabaseError> {
    match bound {
        Bound::Included(val) => {
            inner.bound_key(params, val, is_upper, buffer)?;
            Ok(Bound::Included(buffer.as_slice()))
        }
        Bound::Excluded(val) => {
            inner.bound_key(params, val, is_upper, buffer)?;
            Ok(Bound::Excluded(buffer.as_slice()))
        }
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

trait IndexImpl<T: Transaction> {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError>;

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError>;

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError>;
}

enum IndexImplEnum {
    PrimaryKey(PrimaryKeyIndexImpl),
    Unique(UniqueIndexImpl),
    Normal(NormalIndexImpl),
    Composite(CompositeIndexImpl),
    Covered(CoveredIndexImpl),
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
struct CoveredIndexImpl;

struct TupleMapping {
    index_to_scan: Vec<usize>,
    target_len: usize,
}

impl TupleMapping {
    fn new(scan_to_index: Vec<usize>, tuple_len: usize) -> Self {
        let mut index_to_scan = vec![usize::MAX; tuple_len];

        for (scan_idx, index_idx) in scan_to_index.iter().enumerate() {
            index_to_scan[*index_idx] = scan_idx;
        }
        TupleMapping {
            index_to_scan,
            target_len: scan_to_index.len(),
        }
    }

    fn as_ref(&self) -> TupleMappingRef<'_> {
        TupleMappingRef::new(&self.index_to_scan, self.target_len)
    }
}

struct IndexImplParams<'a, T: Transaction> {
    index_meta: IndexMetaRef,
    table_name: &'a str,
    deserializers: Vec<TupleValueSerializableImpl>,
    total_len: usize,
    tx: &'a T,
    cover_mapping: Option<TupleMapping>,
    with_pk: bool,
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

    fn get_tuple_by_id_into(
        &self,
        tuple_id: &TupleId,
        tuple: &mut Tuple,
    ) -> Result<bool, DatabaseError> {
        unsafe { &*self.table_codec() }.with_tuple_key_unchecked(self.table_name, tuple_id, |key| {
            let Some(bytes) = self.tx.get_borrowed(key)? else {
                return Ok(false);
            };
            TableCodec::decode_tuple_into(
                tuple,
                &self.deserializers,
                Some(tuple_id.clone()),
                bytes.as_ref(),
                self.total_len,
            )?;
            Ok(true)
        })
    }
}

enum IndexResult<'a, T: Transaction + 'a> {
    Hit,
    Miss,
    Scope(T::IterType<'a>),
}

impl<T: Transaction> IndexImpl<T> for IndexImplEnum {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.index_lookup_into(tuple, key, value, params),
            IndexImplEnum::Unique(inner) => inner.index_lookup_into(tuple, key, value, params),
            IndexImplEnum::Normal(inner) => inner.index_lookup_into(tuple, key, value, params),
            IndexImplEnum::Composite(inner) => inner.index_lookup_into(tuple, key, value, params),
            IndexImplEnum::Covered(inner) => inner.index_lookup_into(tuple, key, value, params),
        }
    }

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => {
                inner.eq_to_res(tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Unique(inner) => {
                inner.eq_to_res(tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Normal(inner) => {
                inner.eq_to_res(tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Composite(inner) => {
                inner.eq_to_res(tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Covered(inner) => {
                inner.eq_to_res(tuple, value, params, encode_min, encode_max)
            }
        }
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => inner.bound_key(params, value, is_upper, out),
            IndexImplEnum::Unique(inner) => inner.bound_key(params, value, is_upper, out),
            IndexImplEnum::Normal(inner) => inner.bound_key(params, value, is_upper, out),
            IndexImplEnum::Composite(inner) => inner.bound_key(params, value, is_upper, out),
            IndexImplEnum::Covered(inner) => inner.bound_key(params, value, is_upper, out),
        }
    }
}

impl<T: Transaction> IndexImpl<T> for PrimaryKeyIndexImpl {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        let tuple_id = TableCodec::decode_tuple_key(key, &params.index_meta.pk_ty)?;
        TableCodec::decode_tuple_into(
            tuple,
            &params.deserializers,
            Some(tuple_id),
            value,
            params.total_len,
        )
    }

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        _: &mut Bytes,
        _: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let tuple_id = value.clone();
        let found = unsafe { &*params.table_codec() }.with_tuple_key_unchecked(
            params.table_name,
            value,
            |key| {
                let Some(bytes) = params.tx.get_borrowed(key)? else {
                    return Ok(false);
                };
                TableCodec::decode_tuple_into(
                    tuple,
                    &params.deserializers,
                    Some(tuple_id.clone()),
                    bytes.as_ref(),
                    params.total_len,
                )?;
                Ok(true)
            },
        )?;
        Ok(if found {
            IndexResult::Hit
        } else {
            IndexResult::Miss
        })
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        _: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        unsafe { &*params.table_codec() }.with_tuple_key_unchecked(
            params.table_name,
            value,
            |key| {
                out.clear();
                out.extend_from_slice(key);
                Ok(())
            },
        )
    }
}

#[inline(always)]
fn secondary_index_lookup<T: Transaction>(
    tuple: &mut Tuple,
    bytes: &[u8],
    params: &IndexImplParams<T>,
) -> Result<(), DatabaseError> {
    let tuple_id = TableCodec::decode_index(bytes)?;
    if params.get_tuple_by_id_into(&tuple_id, tuple)? {
        Ok(())
    } else {
        Err(DatabaseError::TupleIdNotFound(tuple_id))
    }
}

impl<T: Transaction> IndexImpl<T> for UniqueIndexImpl {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        _: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        secondary_index_lookup(tuple, value, params)
    }

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        _: &mut Bytes,
        _: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Unique);
        let Some(bytes) = unsafe { &*params.table_codec() }.with_index_key(
            params.table_name,
            &index,
            None,
            |key| params.tx.get_borrowed(key),
        )?
        else {
            return Ok(IndexResult::Miss);
        };
        let tuple_id = TableCodec::decode_index(bytes.as_ref())?;
        if params.get_tuple_by_id_into(&tuple_id, tuple)? {
            Ok(IndexResult::Hit)
        } else {
            Err(DatabaseError::TupleIdNotFound(tuple_id))
        }
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        _: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Unique);

        unsafe { &*params.table_codec() }.with_index_key(params.table_name, &index, None, |key| {
            out.clear();
            out.extend_from_slice(key);
            Ok(())
        })
    }
}

impl<T: Transaction> IndexImpl<T> for NormalIndexImpl {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        _: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        secondary_index_lookup(tuple, value, params)
    }

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        eq_to_res_scope(tuple, self, value, params, encode_min, encode_max)
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Normal);
        unsafe { &*params.table_codec() }.with_index_key(params.table_name, &index, None, |key| {
            encode_bound_key(out, key, is_upper);
            Ok(())
        })
    }
}

impl<T: Transaction> IndexImpl<T> for CompositeIndexImpl {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        _: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        secondary_index_lookup(tuple, value, params)
    }

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        eq_to_res_scope(tuple, self, value, params, encode_min, encode_max)
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta.id, value, IndexType::Composite);
        unsafe { &*params.table_codec() }.with_index_key(params.table_name, &index, None, |key| {
            encode_bound_key(out, key, is_upper);
            Ok(())
        })
    }
}

impl<T: Transaction> IndexImpl<T> for CoveredIndexImpl {
    fn index_lookup_into(
        &self,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        let mapping = params
            .cover_mapping
            .as_ref()
            .map(|mapping| mapping.as_ref());
        let key = TableCodec::decode_index_key(key, params.value_ty(), mapping)?;

        let tuple_id = if params.with_pk {
            Some(TableCodec::decode_index(value)?)
        } else {
            None
        };
        tuple.pk = tuple_id;
        tuple.values = match key {
            DataValue::Tuple(vals, _) => vals,
            v => vec![v],
        };
        Ok(())
    }

    fn eq_to_res<'a>(
        &self,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        eq_to_res_scope(tuple, self, value, params, encode_min, encode_max)
    }

    fn bound_key(
        &self,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta.id, value, params.index_meta.ty);
        unsafe { &*params.table_codec() }.with_index_key(params.table_name, &index, None, |key| {
            encode_bound_key(out, key, is_upper);
            Ok(())
        })
    }
}

#[inline(always)]
fn eq_to_res_scope<'a, T: Transaction + 'a>(
    tuple: &mut Tuple,
    index_impl: &impl IndexImpl<T>,
    value: &DataValue,
    params: &IndexImplParams<'a, T>,
    encode_min: &mut Bytes,
    encode_max: &mut Bytes,
) -> Result<IndexResult<'a, T>, DatabaseError> {
    let _ = tuple;
    index_impl.bound_key(params, value, false, encode_min)?;
    index_impl.bound_key(params, value, true, encode_max)?;

    let iter = params.tx.range(
        Bound::Included(encode_min.as_slice()),
        Bound::Included(encode_max.as_slice()),
    )?;
    Ok(IndexResult::Scope(iter))
}

pub struct TupleIter<'a, T: Transaction + 'a> {
    bounds: IterBounds,
    pk_ty: Option<LogicalType>,
    deserializers: Vec<TupleValueSerializableImpl>,
    total_len: usize,
    iter: T::IterType<'a>,
}

impl<'a, T: Transaction + 'a> Iter for TupleIter<'a, T> {
    fn next_tuple_into(&mut self, tuple: &mut Tuple) -> Result<bool, DatabaseError> {
        while self.bounds.consume_offset() {
            if self.iter.try_next()?.is_none() {
                return Ok(false);
            }
        }

        #[allow(clippy::never_loop)]
        while let Some((key, value)) = self.iter.try_next()? {
            if self.bounds.limit_reached() {
                return Ok(false);
            }
            self.bounds.consume_limit();
            let tuple_id = if let Some(pk_ty) = &self.pk_ty {
                Some(TableCodec::decode_tuple_key(key, pk_ty)?)
            } else {
                None
            };
            TableCodec::decode_tuple_into(
                tuple,
                &self.deserializers,
                tuple_id,
                value,
                self.total_len,
            )?;

            return Ok(true);
        }

        Ok(false)
    }
}

enum IndexRangesInner {
    One(Range),
    Many(Vec<Range>),
}

pub struct IndexRanges {
    inner: IndexRangesInner,
    next_idx: usize,
}

impl IndexRanges {
    fn next(&mut self) -> Option<&Range> {
        let range = match &self.inner {
            IndexRangesInner::One(range) => (self.next_idx == 0).then_some(range),
            IndexRangesInner::Many(ranges) => ranges.get(self.next_idx),
        };

        if range.is_some() {
            self.next_idx += 1;
        }

        range
    }
}

impl From<Vec<Range>> for IndexRanges {
    fn from(value: Vec<Range>) -> Self {
        Self {
            inner: IndexRangesInner::Many(value),
            next_idx: 0,
        }
    }
}

impl From<Range> for IndexRanges {
    fn from(value: Range) -> Self {
        Self {
            inner: IndexRangesInner::One(value),
            next_idx: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IterBounds {
    offset: usize,
    limit: Option<usize>,
}

impl IterBounds {
    fn new(offset: usize, limit: Option<usize>) -> Self {
        Self { offset, limit }
    }

    fn consume_offset(&mut self) -> bool {
        if self.offset > 0 {
            self.offset.sub_assign(1);
            true
        } else {
            false
        }
    }

    fn limit_reached(&self) -> bool {
        matches!(self.limit, Some(0))
    }

    fn consume_limit(&mut self) {
        if let Some(num) = self.limit.as_mut() {
            num.sub_assign(1);
        }
    }
}

pub struct IndexIter<'a, T: Transaction> {
    bounds: IterBounds,
    params: IndexImplParams<'a, T>,
    inner: IndexImplEnum,
    ranges: IndexRanges,
    state: IndexIterState<'a, T>,
    encode_min_buffer: Bytes,
    encode_max_buffer: Bytes,
}

pub enum IndexIterState<'a, T: Transaction + 'a> {
    Init,
    Range(T::IterType<'a>),
    Over,
}

impl<T: Transaction> Iter for IndexIter<'_, T> {
    fn next_tuple_into(&mut self, tuple: &mut Tuple) -> Result<bool, DatabaseError> {
        if self.bounds.limit_reached() {
            self.state = IndexIterState::Over;

            return Ok(false);
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
                            let encode_min = encode_bound(
                                min,
                                false,
                                &mut self.encode_min_buffer,
                                &self.params,
                                &self.inner,
                            )?;
                            let encode_max = encode_bound(
                                max,
                                true,
                                &mut self.encode_max_buffer,
                                &self.params,
                                &self.inner,
                            )?;

                            let table_codec = unsafe { &*self.params.table_codec() };
                            let tx = self.params.tx;
                            let open_iter = move |bound_min: &[u8], bound_max: &[u8]| {
                                tx.range(
                                    fill_default_bound(encode_min, bound_min),
                                    fill_default_bound(encode_max, bound_max),
                                )
                            };
                            let iter = if matches!(index_meta.ty, IndexType::PrimaryKey { .. }) {
                                table_codec.with_tuple_bound(table_name, open_iter)?
                            } else {
                                table_codec.with_index_bound(
                                    table_name,
                                    index_meta.id,
                                    open_iter,
                                )?
                            };
                            self.state = IndexIterState::Range(iter);
                        }
                        Range::Eq(val) => {
                            match self.inner.eq_to_res(
                                tuple,
                                val,
                                &self.params,
                                &mut self.encode_min_buffer,
                                &mut self.encode_max_buffer,
                            )? {
                                IndexResult::Hit => {
                                    if self.bounds.consume_offset() {
                                        continue;
                                    }
                                    self.bounds.consume_limit();
                                    return Ok(true);
                                }
                                IndexResult::Miss => return Ok(false),
                                IndexResult::Scope(iter) => {
                                    self.state = IndexIterState::Range(iter);
                                }
                            }
                        }
                        _ => (),
                    }
                }
                IndexIterState::Range(iter) => {
                    while let Some((key, value)) = iter.try_next()? {
                        if self.bounds.consume_offset() {
                            continue;
                        }
                        self.bounds.consume_limit();
                        self.inner
                            .index_lookup_into(tuple, key, value, &self.params)?;

                        return Ok(true);
                    }
                    self.state = IndexIterState::Init;
                }
                IndexIterState::Over => return Ok(false),
            }
        }
    }
}

pub trait InnerIter {
    fn try_next(&mut self) -> Result<Option<KeyValueRef<'_>>, DatabaseError>;
}

pub trait Iter {
    fn next_tuple_into(&mut self, tuple: &mut Tuple) -> Result<bool, DatabaseError>;
}

#[cfg(test)]
pub(crate) fn next_tuple_for_test<I: Iter>(iter: &mut I) -> Result<Option<Tuple>, DatabaseError> {
    let mut tuple = Tuple::default();
    if iter.next_tuple_into(&mut tuple)? {
        Ok(Some(tuple))
    } else {
        Ok(None)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::binder::test::build_t1_table;
    use crate::catalog::view::View;
    use crate::catalog::{
        ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary, TableName,
    };
    use crate::db::test::build_table;
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{
        IndexIter, InnerIter, StatisticsMetaCache, Storage, TableCache, Transaction,
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
                .table(table_cache, "t1".to_string().into())?
                .unwrap();
            let c1_column_id = *table.get_column_id_by_name("c1").unwrap();
            let c2_column_id = *table.get_column_id_by_name("c2").unwrap();
            let c3_column_id = *table.get_column_id_by_name("c3").unwrap();

            assert_eq!(table.name.as_ref(), "t1");
            assert_eq!(table.indexes.len(), 1);

            let primary_key_index_meta = &table.indexes[0];
            assert_eq!(primary_key_index_meta.id, 0);
            assert_eq!(primary_key_index_meta.column_ids, vec![c1_column_id]);
            assert_eq!(primary_key_index_meta.table_name, "t1".to_string().into());
            assert_eq!(primary_key_index_meta.pk_ty, LogicalType::Integer);
            assert_eq!(primary_key_index_meta.name, "pk_index".to_string());
            assert_eq!(
                primary_key_index_meta.ty,
                IndexType::PrimaryKey { is_multiple: false }
            );

            let mut column_iter = table.columns();
            let c1_column = column_iter.next().unwrap();
            assert!(!c1_column.nullable());
            assert_eq!(
                c1_column.summary(),
                &ColumnSummary {
                    name: "c1".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c1_column_id,
                        table_name: "t1".to_string().into(),
                        is_temp: false,
                    },
                }
            );
            assert_eq!(
                c1_column.desc(),
                &ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?
            );

            let c2_column = column_iter.next().unwrap();
            assert!(!c2_column.nullable());
            assert_eq!(
                c2_column.summary(),
                &ColumnSummary {
                    name: "c2".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c2_column_id,
                        table_name: "t1".to_string().into(),
                        is_temp: false,
                    },
                }
            );
            assert_eq!(
                c2_column.desc(),
                &ColumnDesc::new(LogicalType::Boolean, None, false, None)?
            );

            let c3_column = column_iter.next().unwrap();
            assert!(!c3_column.nullable());
            assert_eq!(
                c3_column.summary(),
                &ColumnSummary {
                    name: "c3".to_string(),
                    relation: ColumnRelation::Table {
                        column_id: c3_column_id,
                        table_name: "t1".to_string().into(),
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
                    LogicalType::Integer.serializable(),
                    LogicalType::Boolean.serializable(),
                    LogicalType::Integer.serializable(),
                ],
                false,
            )?;
        }
        {
            let mut tuple_iter = transaction.read(
                &table_cache,
                "t1".to_string().into(),
                (None, None),
                full_columns(),
                true,
            )?;

            assert_eq!(
                super::next_tuple_for_test(&mut tuple_iter)?.unwrap(),
                tuples[0]
            );
            assert_eq!(
                super::next_tuple_for_test(&mut tuple_iter)?.unwrap(),
                tuples[1]
            );
            assert_eq!(
                super::next_tuple_for_test(&mut tuple_iter)?.unwrap(),
                tuples[2]
            );

            let mut iter = table_codec.with_tuple_bound("t1", |min, max| {
                transaction.range(Bound::Included(min), Bound::Included(max))
            })?;

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
                "t1".to_string().into(),
                (None, None),
                full_columns(),
                true,
            )?;

            assert_eq!(
                super::next_tuple_for_test(&mut tuple_iter)?.unwrap(),
                tuples[0]
            );
            assert_eq!(
                super::next_tuple_for_test(&mut tuple_iter)?.unwrap(),
                tuples[2]
            );

            let mut iter = table_codec.with_tuple_bound("t1", |min, max| {
                transaction.range(Bound::Included(min), Bound::Included(max))
            })?;

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
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();

            (
                *t1_table.get_column_id_by_name("c2").unwrap(),
                *t1_table.get_column_id_by_name("c3").unwrap(),
            )
        };

        let _ = transaction.add_index_meta(
            &table_cache,
            &"t1".to_string().into(),
            "i1".to_string(),
            vec![c3_column_id],
            IndexType::Normal,
        )?;
        let _ = transaction.add_index_meta(
            &table_cache,
            &"t1".to_string().into(),
            "i2".to_string(),
            vec![c3_column_id, c2_column_id],
            IndexType::Composite,
        )?;

        let fn_assert = |transaction: &mut RocksTransaction,
                         table_cache: &TableCache|
         -> Result<(), DatabaseError> {
            let table = transaction
                .table(table_cache, "t1".to_string().into())?
                .unwrap();

            let i1_meta = table.indexes[1].clone();
            assert_eq!(i1_meta.id, 1);
            assert_eq!(i1_meta.column_ids, vec![c3_column_id]);
            assert_eq!(i1_meta.table_name, "t1".to_string().into());
            assert_eq!(i1_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i1_meta.name, "i1".to_string());
            assert_eq!(i1_meta.ty, IndexType::Normal);

            let i2_meta = table.indexes[2].clone();
            assert_eq!(i2_meta.id, 2);
            assert_eq!(i2_meta.column_ids, vec![c3_column_id, c2_column_id]);
            assert_eq!(i2_meta.table_name, "t1".to_string().into());
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
            let mut iter = table_codec.with_index_meta_bound("t1", |min, max| {
                transaction.range(Bound::Included(min), Bound::Included(max))
            })?;

            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            let (_, value) = iter.try_next()?.unwrap();
            dbg!(value);
            assert!(iter.try_next()?.is_none());
        }
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        match transaction.drop_index(
            &table_cache,
            &meta_cache,
            "t1".to_string().into(),
            "pk_index",
            false,
        ) {
            Err(DatabaseError::InvalidIndex) => (),
            _ => unreachable!(),
        }
        transaction.drop_index(
            &table_cache,
            &meta_cache,
            "t1".to_string().into(),
            "i1",
            false,
        )?;
        {
            let table = transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            let i2_meta = table.indexes[1].clone();
            assert_eq!(i2_meta.id, 2);
            assert_eq!(i2_meta.column_ids, vec![c3_column_id, c2_column_id]);
            assert_eq!(i2_meta.table_name, "t1".to_string().into());
            assert_eq!(i2_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i2_meta.name, "i2".to_string());
            assert_eq!(i2_meta.ty, IndexType::Composite);

            let mut iter = table_codec.with_index_meta_bound("t1", |min, max| {
                transaction.range(Bound::Included(min), Bound::Included(max))
            })?;

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
                table_cache,
                "t1".to_string().into(),
                (None, None),
                full_columns(),
                Arc::new(IndexMeta {
                    id: 1,
                    column_ids: vec![index_column_id],
                    table_name: "t1".to_string().into(),
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
                None,
                None,
            )
        }

        let table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        build_table(&table_cache, &mut transaction)?;
        let t1_table = transaction
            .table(&table_cache, "t1".to_string().into())?
            .unwrap();
        let c3_column_id = *t1_table.get_column_id_by_name("c3").unwrap();

        let _ = transaction.add_index_meta(
            &table_cache,
            &"t1".to_string().into(),
            "i1".to_string(),
            vec![c3_column_id],
            IndexType::Normal,
        )?;

        let tuples = build_tuples();
        let indexes = [
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
                    LogicalType::Integer.serializable(),
                    LogicalType::Boolean.serializable(),
                    LogicalType::Integer.serializable(),
                ],
                false,
            )?;
        }
        {
            let mut index_iter = build_index_iter(&transaction, &table_cache, c3_column_id)?;

            assert_eq!(
                super::next_tuple_for_test(&mut index_iter)?.unwrap(),
                tuples[0]
            );
            assert_eq!(
                super::next_tuple_for_test(&mut index_iter)?.unwrap(),
                tuples[2]
            );
            assert_eq!(
                super::next_tuple_for_test(&mut index_iter)?.unwrap(),
                tuples[1]
            );

            let mut iter = table_codec.with_index_bound("t1", 1, |min, max| {
                transaction.range(Bound::Included(min), Bound::Included(max))
            })?;

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

        assert_eq!(
            super::next_tuple_for_test(&mut index_iter)?.unwrap(),
            tuples[2]
        );
        assert_eq!(
            super::next_tuple_for_test(&mut index_iter)?.unwrap(),
            tuples[1]
        );

        let mut iter = table_codec.with_index_bound("t1", 1, |min, max| {
            transaction.range(Bound::Included(min), Bound::Included(max))
        })?;

        let (_, value) = iter.try_next()?.unwrap();
        dbg!(value);
        let (_, value) = iter.try_next()?.unwrap();
        dbg!(value);
        assert!(iter.try_next()?.is_none());

        Ok(())
    }

    #[test]
    fn test_reader_transaction_can_mix_index_and_heap_views() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let serializers = [
            LogicalType::Integer.serializable(),
            LogicalType::Boolean.serializable(),
            LogicalType::Integer.serializable(),
        ];

        let initial_tuple = Tuple::new(
            Some(DataValue::Int32(0)),
            vec![
                DataValue::Int32(0),
                DataValue::Boolean(true),
                DataValue::Int32(0),
            ],
        );
        let updated_tuple = Tuple::new(
            Some(DataValue::Int32(0)),
            vec![
                DataValue::Int32(0),
                DataValue::Boolean(true),
                DataValue::Int32(1),
            ],
        );

        let index_id = {
            let mut setup_tx = storage.transaction()?;
            build_table(&table_cache, &mut setup_tx)?;
            let table = setup_tx
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            let c3_column_id = *table.get_column_id_by_name("c3").unwrap();
            let index_id = setup_tx.add_index_meta(
                &table_cache,
                &"t1".to_string().into(),
                "i1".to_string(),
                vec![c3_column_id],
                IndexType::Normal,
            )?;

            setup_tx.add_index(
                "t1",
                Index::new(index_id, &initial_tuple.values[2], IndexType::Normal),
                initial_tuple.pk.as_ref().unwrap(),
            )?;
            setup_tx.append_tuple("t1", initial_tuple.clone(), &serializers, false)?;
            setup_tx.commit()?;

            index_id
        };

        let reader_tx = storage.transaction()?;
        let tuple_id = {
            let mut index_iter = table_codec.with_index_bound("t1", index_id, |min, max| {
                reader_tx.range(Bound::Included(min), Bound::Included(max))
            })?;
            let (_, value) = index_iter.try_next()?.unwrap();

            TableCodec::decode_index(value)?
        };

        let before_update = table_codec.with_tuple_key("t1", &tuple_id, |key| {
            let bytes = reader_tx.get_borrowed(key)?.expect("tuple should exist");
            let mut tuple = Tuple::default();

            TableCodec::decode_tuple_into(
                &mut tuple,
                &serializers,
                Some(tuple_id.clone()),
                bytes.as_ref(),
                3,
            )?;
            Ok(tuple)
        })?;
        assert_eq!(before_update.values[2], DataValue::Int32(0));

        let mut writer_tx = storage.transaction()?;
        writer_tx.del_index(
            "t1",
            &Index::new(index_id, &initial_tuple.values[2], IndexType::Normal),
            initial_tuple.pk.as_ref().unwrap(),
        )?;
        writer_tx.add_index(
            "t1",
            Index::new(index_id, &updated_tuple.values[2], IndexType::Normal),
            updated_tuple.pk.as_ref().unwrap(),
        )?;
        writer_tx.append_tuple("t1", updated_tuple.clone(), &serializers, true)?;
        writer_tx.commit()?;

        let after_update = table_codec.with_tuple_key("t1", &tuple_id, |key| {
            let bytes = reader_tx.get_borrowed(key)?.expect("tuple should exist");
            let mut tuple = Tuple::default();

            TableCodec::decode_tuple_into(
                &mut tuple,
                &serializers,
                Some(tuple_id.clone()),
                bytes.as_ref(),
                3,
            )?;
            Ok(tuple)
        })?;

        // The same reader transaction first observed the old secondary-index entry,
        // then observed the updated base-row contents after another transaction committed.
        assert_eq!(after_update.values[2], DataValue::Int32(1));

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
        let table_name: TableName = "t1".to_string().into();

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

        let view_name: TableName = "v1".to_string().into();
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
