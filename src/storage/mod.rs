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
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::expression::ScalarExpression;
use crate::optimizer::core::cm_sketch::{
    CountMinSketch, CountMinSketchPage, COUNT_MIN_SKETCH_STORAGE_PAGE_LEN,
};
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
use crate::planner::{MetaArena, PlanArena, TableArenaCell};
use crate::serdes::ReferenceTables;
use crate::storage::table_codec::{Bytes, StatisticsCodecType, TableCodec, BOUND_MAX_TAG};
use crate::types::index::{Index, IndexId, IndexMeta, IndexMetaRef, IndexType};
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, TupleMappingRef};
use crate::types::{ColumnId, LogicalType};
use ahash::HashMap;
use itertools::Itertools;
use std::borrow::{Borrow, Cow};
use std::collections::Bound;
use std::fmt::{self, Display, Formatter};
use std::io::Cursor;
use std::mem;
use std::ops::SubAssign;
use std::path::Path;

pub type KeyValueRef<'a> = (&'a [u8], &'a [u8]);
use ulid::Generator;

pub(crate) type StatisticsMetaCache = HashMap<(TableName, IndexId), StatisticsMeta>;
pub(crate) type TableCache = HashMap<TableName, TableCatalog>;
pub(crate) type ViewCache = HashMap<TableName, View>;

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
    arena: &impl MetaArena,
    column_ids: &[ColumnId],
) -> Result<LogicalType, DatabaseError> {
    let mut value_types = Vec::with_capacity(column_ids.len());
    for column_id in column_ids {
        let value_type = table
            .get_column_by_id(column_id)
            .map(|column| arena.column(column))
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
        table_codec: &mut TableCodec,
        arena: &PlanArena,
        table_cache: &TableCache,
        table_name: TableName,
        bounds: Bounds,
        columns: Vec<ColumnRef>,
        with_pk: bool,
    ) -> Result<TupleIter<'a, Self>, DatabaseError> {
        let table = self
            .table(table_cache, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let deserializers = Self::create_deserializers(&columns, table, arena, with_pk);
        let pk_ty = with_pk.then(|| table.primary_keys_type().clone());
        let offset = bounds.0.unwrap_or(0);

        table_codec.with_tuple_bound(&table_name, |min, max| {
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
        table_cache: &TableCache,
        arena: &PlanArena<'a>,
        table_name: TableName,
        (offset_option, limit_option): Bounds,
        columns: Vec<ColumnRef>,
        index_meta: IndexMetaRef,
        ranges: R,
        with_pk: bool,
        covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
        cover_mapping_indices: Option<Vec<usize>>,
    ) -> Result<IndexIter<'a, Self>, DatabaseError>
    where
        R: Into<IndexRanges>,
    {
        let index_meta_ref = index_meta;
        let index_meta = arena.index(index_meta_ref);
        let table = self
            .table(table_cache, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let offset = offset_option.unwrap_or(0);

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
                let deserializers = Self::create_deserializers(&columns, table, arena, with_pk);
                (IndexImplEnum::instance(index_meta.ty), deserializers, None)
            }
        };
        let total_len = table.columns_len();

        Ok(IndexIter {
            bounds: IterBounds::new(offset, limit_option),
            params: IndexImplParams {
                index_meta: index_meta_ref,
                meta_arena: arena.table_arena_cell().borrow(),
                table_name,
                deserializers,
                total_len,
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
        columns: &[ColumnRef],
        table: &TableCatalog,
        arena: &PlanArena,
        with_pk: bool,
    ) -> Vec<TupleValueSerializableImpl> {
        let mut pk_len = if with_pk {
            table.primary_keys().len()
        } else {
            0
        };
        let mut deserializers = Vec::with_capacity(table.columns_len());
        let mut columns = columns.iter().peekable();

        for table_column_ref in table.columns() {
            let table_column = arena.column(*table_column_ref);
            if columns.peek().is_none() && pk_len == 0 {
                break;
            }

            let is_primary_key = with_pk && table_column.desc().primary().is_some();
            if columns
                .peek()
                .is_some_and(|column| arena.same_column(**column, *table_column_ref))
            {
                deserializers.push(table_column.datatype().serializable());
                columns.next();
                if is_primary_key {
                    pk_len -= 1;
                }
            } else if is_primary_key {
                deserializers.push(table_column.datatype().serializable());
                pk_len -= 1;
            } else {
                deserializers.push(table_column.datatype().skip_serializable());
            }
        }
        debug_assert!(columns.next().is_none());
        debug_assert_eq!(pk_len, 0);
        deserializers
    }

    fn add_index_meta(
        &mut self,
        table_codec: &mut TableCodec,
        plan_arena: &mut PlanArena,
        table_name: &TableName,
        index_name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<(TableCatalog, IndexId), DatabaseError> {
        let mut table = self
            .load_table(table_codec, plan_arena, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let index_meta = table.add_index_meta(index_name, column_ids, ty, plan_arena)?;
        let index_meta = plan_arena.index(index_meta);
        let index_id = index_meta.id;
        table_codec.with_index_meta(table_name, index_id, Some(index_meta), |key, value| {
            self.set(key, value)
        })?;

        Ok((table, index_id))
    }

    fn add_index(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &str,
        index: Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        if matches!(index.ty, IndexType::PrimaryKey { .. }) {
            return Ok(());
        }
        table_codec.with_index(table_name, &index, Some(tuple_id), |key, value| {
            if matches!(index.ty, IndexType::Unique) {
                if let Some(bytes) = self.get_borrowed(key)? {
                    return if bytes.as_ref() != value {
                        Err(DatabaseError::DuplicateUniqueValue)
                    } else {
                        Ok(())
                    };
                }
            }
            self.set(key, value)
        })
    }

    fn del_index(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &str,
        index: &Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        if matches!(index.ty, IndexType::PrimaryKey { .. }) {
            return Ok(());
        }
        table_codec.with_index(table_name, index, Some(tuple_id), |key, _| self.remove(key))?;

        Ok(())
    }

    fn append_tuple<I, S>(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &str,
        tuple: Tuple,
        serializers: I,
        is_overwrite: bool,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<TupleValueSerializableImpl>,
    {
        let tuple_id = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;
        let mut serializers = serializers.into_iter();
        let mut write_value =
            |tuple: &Tuple, value: &mut Bytes| tuple.serialize_to(&mut serializers, value);
        table_codec.with_tuple(
            table_name,
            tuple_id,
            Some((&tuple, &mut write_value)),
            |key, value| {
                if !is_overwrite && self.exists(key)? {
                    return Err(DatabaseError::DuplicatePrimaryKey);
                }
                self.set(key, value)
            },
        )
    }

    fn remove_tuple(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &str,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        table_codec.with_tuple(table_name, tuple_id, None, |key, _| self.remove(key))
    }

    fn rewrite_table_metadata(
        &mut self,
        table_codec: &mut TableCodec,
        arena: &impl MetaArena,
        table: &TableCatalog,
    ) -> Result<(), DatabaseError> {
        let table_name = table.name().clone();
        table_codec.with_columns_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        table_codec.with_index_meta_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        for column in table.columns().map(|column| arena.column(*column)) {
            table_codec.with_column(&column, true, |key, value| self.set(key, value))?;
        }
        for index_meta in table.indexes() {
            let index_meta = arena.index(*index_meta);
            table_codec.with_index_meta(
                table_name.as_ref(),
                index_meta.id,
                Some(index_meta),
                |key, value| self.set(key, value),
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn change_column(
        &mut self,
        table_codec: &mut TableCodec,
        plan_arena: &mut PlanArena,
        table_name: &TableName,
        old_column_name: &str,
        new_column_name: &str,
        new_data_type: &LogicalType,
        default_change: &DefaultChange,
        not_null_change: &NotNullChange,
    ) -> Result<TableCatalog, DatabaseError> {
        let table = self
            .load_table(table_codec, plan_arena, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let mut column_catalogs = Vec::with_capacity(table.columns_len());
        let mut found = false;

        if old_column_name != new_column_name && table.get_column_by_name(new_column_name).is_some()
        {
            return Err(DatabaseError::DuplicateColumn(new_column_name.to_string()));
        }

        for column in table.columns().map(|column| plan_arena.column(*column)) {
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
                                plan_arena,
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
            column_catalogs.push(new_column);
        }
        if !found {
            return Err(DatabaseError::column_not_found(old_column_name.to_string()));
        }

        let temp_table = TableCatalog::reload(
            table_name.clone(),
            column_catalogs.clone().into_iter(),
            std::iter::empty(),
            plan_arena,
        )?;
        let index_metas = table
            .indexes()
            .map(|index_meta| {
                let index_meta = plan_arena.index(*index_meta);
                Ok(IndexMeta {
                    id: index_meta.id,
                    column_ids: index_meta.column_ids.clone(),
                    table_name: table_name.clone(),
                    pk_ty: temp_table.primary_keys_type().clone(),
                    value_ty: index_value_type(&temp_table, plan_arena, &index_meta.column_ids)?,
                    name: index_meta.name.clone(),
                    ty: index_meta.ty,
                })
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        let updated_table = TableCatalog::reload(
            table_name.clone(),
            column_catalogs.into_iter(),
            index_metas.into_iter(),
            plan_arena,
        )?;
        self.rewrite_table_metadata(table_codec, plan_arena, &updated_table)?;
        table_codec.with_statistics_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        Ok(updated_table)
    }

    fn add_column(
        &mut self,
        table_codec: &mut TableCodec,
        plan_arena: &mut PlanArena,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<(TableCatalog, ColumnId), DatabaseError> {
        let mut table = self
            .load_table(table_codec, plan_arena, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        if !column.nullable() && column.default_value()?.is_none() {
            return Err(DatabaseError::NeedNullAbleOrDefault);
        }

        for col in table.columns().map(|column| plan_arena.column(*column)) {
            if col.name() == column.name() {
                return if if_not_exists {
                    Ok((table, col.id().unwrap()))
                } else {
                    Err(DatabaseError::DuplicateColumn(column.name().to_string()))
                };
            }
        }
        let mut generator = Generator::new();
        let col_id = table.add_column(column.clone(), &mut generator, plan_arena)?;

        if column.desc().is_unique() {
            let meta_ref = table.add_index_meta(
                format!("uk_{}", column.name()),
                vec![col_id],
                IndexType::Unique,
                plan_arena,
            )?;
            let meta = plan_arena.index(meta_ref);
            table_codec.with_index_meta(table_name, meta.id, Some(meta), |key, value| {
                self.set(key, value)
            })?;
        }

        let column = plan_arena.column(table.get_column_by_id(&col_id).unwrap());
        table_codec.with_column(&column, true, |key, value| self.set(key, value))?;

        Ok((table, col_id))
    }

    fn drop_column(
        &mut self,
        table_codec: &mut TableCodec,
        plan_arena: &mut PlanArena,
        table_name: &TableName,
        column_name: &str,
    ) -> Result<TableCatalog, DatabaseError> {
        let table_catalog = self
            .load_table(table_codec, plan_arena, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let column_id = {
            let column = plan_arena.column(table_catalog.get_column_by_name(column_name).unwrap());
            let column_id = column.id().unwrap();
            table_codec.with_column(&column, false, |key, _| self.remove(key))?;
            column_id
        };

        for index_meta in table_catalog.indexes.iter() {
            let index_meta = plan_arena.index(*index_meta);
            if !index_meta.column_ids.contains(&column_id) {
                continue;
            }
            table_codec
                .with_index_meta(table_name, index_meta.id, None, |key, _| self.remove(key))?;

            table_codec.with_index_bound(table_name, index_meta.id, |min, max| {
                self.remove_range(Bound::Included(min), Bound::Included(max))
            })?;

            self.remove_statistics_meta(table_codec, table_name, index_meta.id)?;
        }
        self.load_table(table_codec, plan_arena, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)
    }

    fn create_view(
        &mut self,
        table_codec: &mut TableCodec,
        arena: &PlanArena,
        view: View,
        or_replace: bool,
    ) -> Result<View, DatabaseError> {
        let already_exists = table_codec.with_view(&view.name, |key, _| self.exists(key))?;
        if !or_replace && already_exists {
            return Err(DatabaseError::ViewExists);
        }
        if !already_exists {
            self.check_name_hash(table_codec, &view.name)?;
        }
        table_codec.with_view_value(&view.name, &view, arena, |key, value| self.set(key, value))?;

        Ok(view)
    }

    fn create_table(
        &mut self,
        table_codec: &mut TableCodec,
        plan_arena: &mut PlanArena,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<Option<TableCatalog>, DatabaseError> {
        let mut table_catalog = TableCatalog::new(table_name.clone(), columns, plan_arena)?;

        for (_, column) in table_catalog.primary_keys() {
            TableCodec::check_primary_key_type(plan_arena.column(*column).datatype())?;
        }

        let exists =
            table_codec.with_root_table(table_name.as_ref(), None, |key, _| self.exists(key))?;
        if exists {
            if if_not_exists {
                return Ok(None);
            }
            return Err(DatabaseError::TableExists);
        }
        self.check_name_hash(table_codec, &table_name)?;
        self.create_index_meta_from_column(table_codec, plan_arena, &mut table_catalog)?;
        let table_meta = TableMeta::empty(table_name.clone());
        table_codec.with_root_table(table_name.as_ref(), Some(&table_meta), |key, value| {
            self.set(key, value)
        })?;

        for column in table_catalog
            .columns()
            .map(|column| plan_arena.column(*column))
        {
            table_codec.with_column(&column, true, |key, value| self.set(key, value))?;
        }

        Ok(Some(table_catalog))
    }

    fn check_name_hash(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &TableName,
    ) -> Result<(), DatabaseError> {
        if table_codec.with_table_hash(table_name, |key, _| self.exists(key))? {
            return Err(DatabaseError::DuplicateSourceHash(table_name.to_string()));
        }
        table_codec.with_table_hash(table_name, |key, _| self.set(key, &[]))
    }

    fn drop_name_hash(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &TableName,
    ) -> Result<(), DatabaseError> {
        table_codec.with_table_hash(table_name, |key, _| self.remove(key))
    }

    fn drop_view(
        &mut self,
        table_codec: &mut TableCodec,
        view_name: TableName,
        if_exists: bool,
    ) -> Result<bool, DatabaseError> {
        self.drop_name_hash(table_codec, &view_name)?;
        let exists = table_codec.with_view(&view_name, |key, _| self.exists(key))?;
        if !exists {
            if if_exists {
                return Ok(false);
            } else {
                return Err(DatabaseError::ViewNotFound);
            }
        }

        table_codec.with_view(view_name.as_ref(), |key, _| self.remove(key))?;

        Ok(true)
    }

    fn drop_index(
        &mut self,
        table_codec: &mut TableCodec,
        plan_arena: &mut PlanArena,
        table_name: TableName,
        index_name: &str,
        if_exists: bool,
    ) -> Result<Option<(TableCatalog, IndexId)>, DatabaseError> {
        let table = self
            .load_table(table_codec, plan_arena, table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;
        let Some(index_meta_ref) = table
            .indexes
            .iter()
            .copied()
            .find(|index| plan_arena.index(*index).name == index_name)
        else {
            if if_exists {
                return Ok(None);
            } else {
                return Err(DatabaseError::TableNotFound);
            }
        };
        let index_meta = plan_arena.index(index_meta_ref);
        match index_meta.ty {
            IndexType::PrimaryKey { .. } => return Err(DatabaseError::InvalidIndex),
            IndexType::Unique | IndexType::Normal | IndexType::Composite => (),
        }

        let index_id = index_meta.id;
        table_codec.with_index_meta(table_name.as_ref(), index_id, None, |key, _| {
            self.remove(key)
        })?;

        table_codec.with_index_bound(table_name.as_ref(), index_id, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        self.remove_statistics_meta(table_codec, &table_name, index_id)?;

        self.load_table(table_codec, plan_arena, table_name.clone())?
            .map(|table| (table, index_id))
            .map(Some)
            .ok_or(DatabaseError::TableNotFound)
    }

    fn drop_table(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: TableName,
        if_exists: bool,
    ) -> Result<bool, DatabaseError> {
        let exists =
            table_codec.with_root_table(table_name.as_ref(), None, |key, _| self.exists(key))?;
        if !exists {
            if if_exists {
                return Ok(false);
            } else {
                return Err(DatabaseError::TableNotFound);
            }
        }
        self.drop_name_hash(table_codec, &table_name)?;
        self.drop_data(table_codec, table_name.as_ref())?;

        table_codec.with_columns_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        table_codec.with_index_meta_bound(table_name.as_ref(), |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        table_codec.with_root_table(table_name.as_ref(), None, |key, _| self.remove(key))?;

        Ok(true)
    }

    fn drop_data(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &str,
    ) -> Result<(), DatabaseError> {
        table_codec.with_tuple_bound(table_name, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        table_codec.with_all_index_bound(table_name, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        table_codec.with_statistics_bound(table_name, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })
    }

    fn view<'a>(
        &'a self,
        table_cache: &'a TableCache,
        view_cache: &'a ViewCache,
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
        view_name: TableName,
    ) -> Result<Option<&'a View>, DatabaseError> {
        let _ = (table_cache, scala_functions, table_functions);
        Ok(view_cache.get(&view_name))
    }

    fn load_view(
        &self,
        table_codec: &mut TableCodec,
        table_cache: &TableCache,
        table_arena: &TableArenaCell,
        scala_functions: &ScalaFunctions,
        table_functions: &TableFunctions,
        view_name: TableName,
    ) -> Result<Option<View>, DatabaseError> {
        table_codec.with_view(&view_name, |key, _| {
            let Some(bytes) = self.get_borrowed(key)? else {
                return Ok(None);
            };
            TableCodec::decode_view(
                bytes.as_ref(),
                (self, table_cache),
                scala_functions,
                table_functions,
                table_arena.borrow_mut(),
            )
            .map(Some)
        })
    }

    fn views<'a>(
        &'a self,
        table_codec: &mut TableCodec,
        table_cache: &'a TableCache,
        table_arena: &'a TableArenaCell,
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
    ) -> Result<ViewIter<'a, Self>, DatabaseError> {
        table_codec.with_view_bound(|min, max| {
            Ok(ViewIter {
                iter: self.range(Bound::Included(min), Bound::Included(max))?,
                transaction: self,
                table_cache,
                table_arena,
                scala_functions,
                table_functions,
            })
        })
    }

    fn table<'a>(
        &self,
        table_cache: &'a TableCache,
        table_name: TableName,
    ) -> Result<Option<&'a TableCatalog>, DatabaseError> {
        Ok(table_cache.get(&table_name))
    }

    fn load_table(
        &self,
        table_codec: &mut TableCodec,
        arena: &mut impl MetaArena,
        table_name: TableName,
    ) -> Result<Option<TableCatalog>, DatabaseError> {
        self.table_collect(table_codec, &table_name)?
            .map(|(columns, indexes)| {
                TableCatalog::reload(table_name, columns.into_iter(), indexes.into_iter(), arena)
            })
            .transpose()
    }

    fn tables<'a>(
        &'a self,
        table_codec: &mut TableCodec,
    ) -> Result<TableIter<'a, Self>, DatabaseError> {
        table_codec.with_root_table_bound(|min, max| {
            Ok(TableIter {
                iter: self.range(Bound::Included(min), Bound::Included(max))?,
            })
        })
    }

    fn save_statistics_meta(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &TableName,
        statistics_meta: StatisticsMeta,
    ) -> Result<(), DatabaseError> {
        let index_id = statistics_meta.index_id();
        let (root, buckets, cm_sketch) = statistics_meta.into_parts();
        table_codec.with_statistics_meta(
            table_name.as_ref(),
            index_id,
            Some(&root),
            |key, value| self.set(key, value),
        )?;
        let (sketch_meta, sketch_pages) =
            cm_sketch.into_storage_parts(COUNT_MIN_SKETCH_STORAGE_PAGE_LEN);
        table_codec.with_statistics_sketch_meta(
            table_name.as_ref(),
            index_id,
            Some(&sketch_meta),
            |key, value| self.set(key, value),
        )?;
        for sketch_page in sketch_pages {
            table_codec.with_statistics_sketch_page(
                table_name.as_ref(),
                index_id,
                &sketch_page,
                true,
                |key, value| self.set(key, value),
            )?;
        }

        for (ordinal, bucket) in buckets.iter().enumerate() {
            table_codec.with_statistics_bucket(
                table_name.as_ref(),
                index_id,
                ordinal as u32,
                Some(bucket),
                |key, value| self.set(key, value),
            )?;
        }

        Ok(())
    }

    fn statistics_meta(
        &self,
        table_codec: &mut TableCodec,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<Option<StatisticsMeta>, DatabaseError> {
        table_codec.with_statistics_index_bound(table_name, index_id, |min, max| {
            let mut iter = self.range(Bound::Included(min), Bound::Included(max))?;
            let mut root = None;
            let mut buckets = Vec::new();
            let mut sketch_meta = None;
            let mut sketch_pages = Vec::<CountMinSketchPage>::new();

            while let Some((key, value)) = iter.try_next()? {
                match TableCodec::decode_statistics_codec_type(key)? {
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
        })
    }

    fn remove_statistics_meta(
        &mut self,
        table_codec: &mut TableCodec,
        table_name: &TableName,
        index_id: IndexId,
    ) -> Result<(), DatabaseError> {
        table_codec.with_statistics_index_bound(table_name.as_ref(), index_id, |min, max| {
            self.remove_range(Bound::Included(min), Bound::Included(max))
        })?;

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn table_collect(
        &self,
        table_codec: &mut TableCodec,
        table_name: &TableName,
    ) -> Result<Option<(Vec<ColumnCatalog>, Vec<IndexMeta>)>, DatabaseError> {
        table_codec.with_table_bound(table_name, |table_min, table_max| {
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
                    index_metas.push(TableCodec::decode_index_meta::<Self>(value)?);
                }
            }

            Ok((!columns.is_empty()).then_some((columns, index_metas)))
        })
    }

    fn create_index_meta_from_column(
        &mut self,
        table_codec: &mut TableCodec,
        arena: &mut impl MetaArena,
        table: &mut TableCatalog,
    ) -> Result<(), DatabaseError> {
        let table_name = table.name.clone();
        let mut primary_keys = Vec::new();

        let mut i = 0;
        while i < table.columns_len() {
            let column = table.column_ref(i).unwrap();
            i += 1;
            let col = arena.column(column);
            let col_id = col.id().ok_or(DatabaseError::PrimaryKeyNotFound)?;
            let index_ty = if let Some(primary_key_index) = col.desc().primary() {
                primary_keys.push((primary_key_index, col_id));
                continue;
            } else if col.desc().is_unique() {
                IndexType::Unique
            } else {
                continue;
            };
            let index_name = format!("uk_{}_index", col.name());
            let meta_ref = table.add_index_meta(index_name, vec![col_id], index_ty, arena)?;
            let meta = arena.index(meta_ref);
            table_codec.with_index_meta(&table_name, meta.id, Some(meta), |key, value| {
                self.set(key, value)
            })?;
        }
        let primary_keys = table
            .primary_keys()
            .iter()
            .map(|(_, column)| arena.column(*column).id().unwrap())
            .collect_vec();
        let pk_index_ty = IndexType::PrimaryKey {
            is_multiple: primary_keys.len() != 1,
        };
        let meta_ref =
            table.add_index_meta("pk_index".to_string(), primary_keys, pk_index_ty, arena)?;
        let meta = arena.index(meta_ref);
        table_codec.with_index_meta(&table_name, meta.id, Some(meta), |key, value| {
            self.set(key, value)
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
    table_codec: &mut TableCodec,
    bound: &Bound<DataValue>,
    is_upper: bool,
    buffer: &'a mut Bytes,
    params: &IndexImplParams<'_, impl Transaction>,
    inner: &IndexImplEnum,
) -> Result<Bound<&'a [u8]>, DatabaseError> {
    match bound {
        Bound::Included(val) => {
            inner.bound_key(table_codec, params, val, is_upper, buffer)?;
            Ok(Bound::Included(buffer.as_slice()))
        }
        Bound::Excluded(val) => {
            inner.bound_key(table_codec, params, val, is_upper, buffer)?;
            Ok(Bound::Excluded(buffer.as_slice()))
        }
        Bound::Unbounded => Ok(Bound::Unbounded),
    }
}

trait IndexImpl<T: Transaction> {
    fn index_lookup_into(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError>;

    fn eq_to_res<'a>(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError>;

    fn bound_key(
        &self,
        table_codec: &mut TableCodec,
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
    meta_arena: &'a dyn MetaArena,
    table_name: TableName,
    deserializers: Vec<TupleValueSerializableImpl>,
    total_len: usize,
    tx: &'a T,
    cover_mapping: Option<TupleMapping>,
    with_pk: bool,
}

impl<T: Transaction> IndexImplParams<'_, T> {
    #[inline]
    pub(crate) fn index_meta(&self) -> &IndexMeta {
        self.meta_arena.index(self.index_meta)
    }

    #[inline]
    pub(crate) fn value_ty(&self) -> &LogicalType {
        &self.index_meta().value_ty
    }

    #[inline]
    fn get_tuple_by_id_into(
        &self,
        table_codec: &mut TableCodec,
        tuple_id: &TupleId,
        tuple: &mut Tuple,
    ) -> Result<bool, DatabaseError> {
        table_codec.with_tuple_unchecked(self.table_name.as_ref(), tuple_id, None, |key, _| {
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
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => {
                inner.index_lookup_into(table_codec, tuple, key, value, params)
            }
            IndexImplEnum::Unique(inner) => {
                inner.index_lookup_into(table_codec, tuple, key, value, params)
            }
            IndexImplEnum::Normal(inner) => {
                inner.index_lookup_into(table_codec, tuple, key, value, params)
            }
            IndexImplEnum::Composite(inner) => {
                inner.index_lookup_into(table_codec, tuple, key, value, params)
            }
            IndexImplEnum::Covered(inner) => {
                inner.index_lookup_into(table_codec, tuple, key, value, params)
            }
        }
    }

    fn eq_to_res<'a>(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => {
                inner.eq_to_res(table_codec, tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Unique(inner) => {
                inner.eq_to_res(table_codec, tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Normal(inner) => {
                inner.eq_to_res(table_codec, tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Composite(inner) => {
                inner.eq_to_res(table_codec, tuple, value, params, encode_min, encode_max)
            }
            IndexImplEnum::Covered(inner) => {
                inner.eq_to_res(table_codec, tuple, value, params, encode_min, encode_max)
            }
        }
    }

    fn bound_key(
        &self,
        table_codec: &mut TableCodec,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        match self {
            IndexImplEnum::PrimaryKey(inner) => {
                inner.bound_key(table_codec, params, value, is_upper, out)
            }
            IndexImplEnum::Unique(inner) => {
                inner.bound_key(table_codec, params, value, is_upper, out)
            }
            IndexImplEnum::Normal(inner) => {
                inner.bound_key(table_codec, params, value, is_upper, out)
            }
            IndexImplEnum::Composite(inner) => {
                inner.bound_key(table_codec, params, value, is_upper, out)
            }
            IndexImplEnum::Covered(inner) => {
                inner.bound_key(table_codec, params, value, is_upper, out)
            }
        }
    }
}

impl<T: Transaction> IndexImpl<T> for PrimaryKeyIndexImpl {
    fn index_lookup_into(
        &self,
        _: &mut TableCodec,
        tuple: &mut Tuple,
        key: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        let tuple_id = TableCodec::decode_tuple_key(key, &params.index_meta().pk_ty)?;
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
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        _: &mut Bytes,
        _: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let tuple_id = value.clone();
        let found = table_codec.with_tuple_unchecked(
            params.table_name.as_ref(),
            value,
            None,
            |key, _| {
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
        table_codec: &mut TableCodec,
        params: &IndexImplParams<T>,
        value: &DataValue,
        _: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        table_codec.with_tuple_unchecked(params.table_name.as_ref(), value, None, |key, _| {
            out.clear();
            out.extend_from_slice(key);
            Ok(())
        })
    }
}

#[inline(always)]
fn secondary_index_lookup<T: Transaction>(
    table_codec: &mut TableCodec,
    tuple: &mut Tuple,
    bytes: &[u8],
    params: &IndexImplParams<T>,
) -> Result<(), DatabaseError> {
    let tuple_id = TableCodec::decode_index(bytes)?;
    if params.get_tuple_by_id_into(table_codec, &tuple_id, tuple)? {
        Ok(())
    } else {
        Err(DatabaseError::TupleIdNotFound(tuple_id))
    }
}

impl<T: Transaction> IndexImpl<T> for UniqueIndexImpl {
    fn index_lookup_into(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        _: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        secondary_index_lookup(table_codec, tuple, value, params)
    }

    fn eq_to_res<'a>(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        _: &mut Bytes,
        _: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        let index = Index::new(params.index_meta().id, value, IndexType::Unique);
        let Some(bytes) =
            table_codec.with_index(params.table_name.as_ref(), &index, None, |key, _| {
                params.tx.get_borrowed(key)
            })?
        else {
            return Ok(IndexResult::Miss);
        };
        let tuple_id = TableCodec::decode_index(bytes.as_ref())?;
        if params.get_tuple_by_id_into(table_codec, &tuple_id, tuple)? {
            Ok(IndexResult::Hit)
        } else {
            Err(DatabaseError::TupleIdNotFound(tuple_id))
        }
    }

    fn bound_key(
        &self,
        table_codec: &mut TableCodec,
        params: &IndexImplParams<T>,
        value: &DataValue,
        _: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta().id, value, IndexType::Unique);

        table_codec.with_index(params.table_name.as_ref(), &index, None, |key, _| {
            out.clear();
            out.extend_from_slice(key);
            Ok(())
        })
    }
}

impl<T: Transaction> IndexImpl<T> for NormalIndexImpl {
    fn index_lookup_into(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        _: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        secondary_index_lookup(table_codec, tuple, value, params)
    }

    fn eq_to_res<'a>(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        eq_to_res_scope(
            table_codec,
            tuple,
            self,
            value,
            params,
            encode_min,
            encode_max,
        )
    }

    fn bound_key(
        &self,
        table_codec: &mut TableCodec,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta().id, value, IndexType::Normal);
        table_codec.with_index(params.table_name.as_ref(), &index, None, |key, _| {
            encode_bound_key(out, key, is_upper);
            Ok(())
        })
    }
}

impl<T: Transaction> IndexImpl<T> for CompositeIndexImpl {
    fn index_lookup_into(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        _: &[u8],
        value: &[u8],
        params: &IndexImplParams<T>,
    ) -> Result<(), DatabaseError> {
        secondary_index_lookup(table_codec, tuple, value, params)
    }

    fn eq_to_res<'a>(
        &self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        eq_to_res_scope(
            table_codec,
            tuple,
            self,
            value,
            params,
            encode_min,
            encode_max,
        )
    }

    fn bound_key(
        &self,
        table_codec: &mut TableCodec,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta().id, value, IndexType::Composite);
        table_codec.with_index(params.table_name.as_ref(), &index, None, |key, _| {
            encode_bound_key(out, key, is_upper);
            Ok(())
        })
    }
}

impl<T: Transaction> IndexImpl<T> for CoveredIndexImpl {
    fn index_lookup_into(
        &self,
        _: &mut TableCodec,
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
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
        value: &DataValue,
        params: &IndexImplParams<'a, T>,
        encode_min: &mut Bytes,
        encode_max: &mut Bytes,
    ) -> Result<IndexResult<'a, T>, DatabaseError> {
        eq_to_res_scope(
            table_codec,
            tuple,
            self,
            value,
            params,
            encode_min,
            encode_max,
        )
    }

    fn bound_key(
        &self,
        table_codec: &mut TableCodec,
        params: &IndexImplParams<T>,
        value: &DataValue,
        is_upper: bool,
        out: &mut Bytes,
    ) -> Result<(), DatabaseError> {
        let index = Index::new(params.index_meta().id, value, params.index_meta().ty);
        table_codec.with_index(params.table_name.as_ref(), &index, None, |key, _| {
            encode_bound_key(out, key, is_upper);
            Ok(())
        })
    }
}

#[inline(always)]
fn eq_to_res_scope<'a, T: Transaction + 'a>(
    table_codec: &mut TableCodec,
    tuple: &mut Tuple,
    index_impl: &impl IndexImpl<T>,
    value: &DataValue,
    params: &IndexImplParams<'a, T>,
    encode_min: &mut Bytes,
    encode_max: &mut Bytes,
) -> Result<IndexResult<'a, T>, DatabaseError> {
    let _ = tuple;
    index_impl.bound_key(table_codec, params, value, false, encode_min)?;
    index_impl.bound_key(table_codec, params, value, true, encode_max)?;

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
    fn next_tuple_into(
        &mut self,
        _: &mut TableCodec,
        tuple: &mut Tuple,
    ) -> Result<bool, DatabaseError> {
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
    fn next_tuple_into(
        &mut self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
    ) -> Result<bool, DatabaseError> {
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
                            let index_meta = self.params.index_meta();
                            let encode_min = encode_bound(
                                table_codec,
                                min,
                                false,
                                &mut self.encode_min_buffer,
                                &self.params,
                                &self.inner,
                            )?;
                            let encode_max = encode_bound(
                                table_codec,
                                max,
                                true,
                                &mut self.encode_max_buffer,
                                &self.params,
                                &self.inner,
                            )?;

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
                                table_codec,
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
                        self.inner.index_lookup_into(
                            table_codec,
                            tuple,
                            key,
                            value,
                            &self.params,
                        )?;

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
    fn next_tuple_into(
        &mut self,
        table_codec: &mut TableCodec,
        tuple: &mut Tuple,
    ) -> Result<bool, DatabaseError>;
}

pub struct TableIter<'a, T: Transaction + 'a> {
    iter: T::IterType<'a>,
}

impl<T: Transaction> TableIter<'_, T> {
    pub fn try_next(&mut self) -> Result<Option<TableMeta>, DatabaseError> {
        let Some((_, value)) = self.iter.try_next()? else {
            return Ok(None);
        };

        Ok(Some(TableCodec::decode_root_table::<T>(value)?))
    }
}

pub struct ViewIter<'a, T: Transaction + 'a> {
    iter: T::IterType<'a>,
    transaction: &'a T,
    table_cache: &'a TableCache,
    table_arena: &'a TableArenaCell,
    scala_functions: &'a ScalaFunctions,
    table_functions: &'a TableFunctions,
}

impl<T: Transaction> ViewIter<'_, T> {
    pub fn try_next(&mut self) -> Result<Option<View>, DatabaseError> {
        let Some((_, value)) = self.iter.try_next()? else {
            return Ok(None);
        };

        Ok(Some(TableCodec::decode_view(
            value,
            (self.transaction, self.table_cache),
            self.scala_functions,
            self.table_functions,
            self.table_arena.borrow_mut(),
        )?))
    }
}

#[cfg(test)]
pub(crate) fn next_tuple_for_test<I: Iter>(iter: &mut I) -> Result<Option<Tuple>, DatabaseError> {
    let mut tuple = Tuple::default();
    let mut table_codec = TableCodec::default();
    if iter.next_tuple_into(&mut table_codec, &mut tuple)? {
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
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::table_codec::TableCodec;
    use crate::storage::{
        IndexIter, InnerIter, Storage, TableCache, Transaction, TransactionIsolationLevel,
    };
    use crate::types::index::{Index, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::Bound;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn full_columns(table_cache: &TableCache) -> Vec<ColumnRef> {
        table_cache.get("t1").unwrap().columns().copied().collect()
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
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let plan_arena = PlanArena::new(&table_arena);

        let fn_assert = |transaction: &mut RocksTransaction,
                         table_cache: &TableCache,
                         plan_arena: &PlanArena|
         -> Result<(), DatabaseError> {
            let table = transaction
                .table(table_cache, "t1".to_string().into())?
                .unwrap();
            let c1_column_id = *table.get_column_id_by_name("c1").unwrap();
            let c2_column_id = *table.get_column_id_by_name("c2").unwrap();
            let c3_column_id = *table.get_column_id_by_name("c3").unwrap();

            assert_eq!(table.name.as_ref(), "t1");
            assert_eq!(table.indexes.len(), 1);

            let primary_key_index_meta = plan_arena.index(table.indexes[0]);
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
            let c1_column = plan_arena.column(*column_iter.next().unwrap());
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

            let c2_column = plan_arena.column(*column_iter.next().unwrap());
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

            let c3_column = plan_arena.column(*column_iter.next().unwrap());
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
        fn_assert(&mut transaction, &table_cache, &plan_arena)?;
        let mut reloaded_table_cache = crate::storage::TableCache::default();
        let mut table_codec = TableCodec::default();
        let table_name = "t1".to_string().into();
        let table = transaction
            .load_table(&mut table_codec, table_arena.borrow_mut(), table_name)?
            .ok_or(DatabaseError::TableNotFound)?;
        reloaded_table_cache.insert("t1".to_string().into(), table);
        let plan_arena = PlanArena::new(&table_arena);
        fn_assert(&mut transaction, &reloaded_table_cache, &plan_arena)?;

        Ok(())
    }

    #[test]
    fn test_tuple_append_delete() -> Result<(), DatabaseError> {
        let mut table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let plan_arena = PlanArena::new(&table_arena);

        let tuples = build_tuples();
        for tuple in tuples.iter().cloned() {
            transaction.append_tuple(
                &mut table_codec,
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
                &mut table_codec,
                &plan_arena,
                &table_cache,
                "t1".to_string().into(),
                (None, None),
                full_columns(&table_cache),
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

        transaction.remove_tuple(&mut table_codec, "t1", &tuples[1].values[0])?;
        {
            let mut tuple_iter = transaction.read(
                &mut table_codec,
                &plan_arena,
                &table_cache,
                "t1".to_string().into(),
                (None, None),
                full_columns(&table_cache),
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
        let mut table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let mut plan_arena = PlanArena::new(&table_arena);
        let (c2_column_id, c3_column_id) = {
            let t1_table = transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();

            (
                *t1_table.get_column_id_by_name("c2").unwrap(),
                *t1_table.get_column_id_by_name("c3").unwrap(),
            )
        };

        let (table, _) = transaction.add_index_meta(
            &mut table_codec,
            &mut plan_arena,
            &"t1".to_string().into(),
            "i1".to_string(),
            vec![c3_column_id],
            IndexType::Normal,
        )?;
        let table = table.transplant_to_table_arena(&plan_arena)?;
        table_cache.insert(table.name().clone(), table);
        let mut plan_arena = PlanArena::new(&table_arena);
        let (table, _) = transaction.add_index_meta(
            &mut table_codec,
            &mut plan_arena,
            &"t1".to_string().into(),
            "i2".to_string(),
            vec![c3_column_id, c2_column_id],
            IndexType::Composite,
        )?;
        let table = table.transplant_to_table_arena(&plan_arena)?;
        table_cache.insert(table.name().clone(), table);

        let fn_assert = |transaction: &mut RocksTransaction,
                         table_cache: &TableCache|
         -> Result<(), DatabaseError> {
            let table = transaction
                .table(table_cache, "t1".to_string().into())?
                .unwrap();
            let plan_arena = PlanArena::new(&table_arena);

            let i1_meta = plan_arena.index(table.indexes[1]);
            assert_eq!(i1_meta.id, 1);
            assert_eq!(i1_meta.column_ids, vec![c3_column_id]);
            assert_eq!(i1_meta.table_name, "t1".to_string().into());
            assert_eq!(i1_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i1_meta.name, "i1".to_string());
            assert_eq!(i1_meta.ty, IndexType::Normal);

            let i2_meta = plan_arena.index(table.indexes[2]);
            assert_eq!(i2_meta.id, 2);
            assert_eq!(i2_meta.column_ids, vec![c3_column_id, c2_column_id]);
            assert_eq!(i2_meta.table_name, "t1".to_string().into());
            assert_eq!(i2_meta.pk_ty, LogicalType::Integer);
            assert_eq!(i2_meta.name, "i2".to_string());
            assert_eq!(i2_meta.ty, IndexType::Composite);

            Ok(())
        };
        fn_assert(&mut transaction, &table_cache)?;
        let mut reloaded_table_cache = crate::storage::TableCache::default();
        let table_name = "t1".to_string().into();
        let table = transaction
            .load_table(&mut table_codec, table_arena.borrow_mut(), table_name)?
            .ok_or(DatabaseError::TableNotFound)?;
        reloaded_table_cache.insert("t1".to_string().into(), table);
        let mut plan_arena = PlanArena::new(&table_arena);
        fn_assert(&mut transaction, &reloaded_table_cache)?;
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
        match transaction.drop_index(
            &mut table_codec,
            &mut plan_arena,
            "t1".to_string().into(),
            "pk_index",
            false,
        ) {
            Err(DatabaseError::InvalidIndex) => (),
            _ => unreachable!(),
        }
        if let Some((table, _)) = transaction.drop_index(
            &mut table_codec,
            &mut plan_arena,
            "t1".to_string().into(),
            "i1",
            false,
        )? {
            let table = table.transplant_to_table_arena(&plan_arena)?;
            table_cache.insert(table.name().clone(), table);
        }
        {
            let table = transaction
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            let i2_meta = plan_arena.index(table.indexes[1]);
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
            table_cache: &'a TableCache,
            plan_arena: &'a PlanArena<'a>,
        ) -> Result<IndexIter<'a, RocksTransaction<'a>>, DatabaseError> {
            let table_name: crate::catalog::TableName = "t1".to_string().into();
            let index_meta = table_cache
                .get(&table_name)
                .and_then(|table| {
                    table
                        .indexes()
                        .copied()
                        .find(|index| plan_arena.index(*index).id == 1)
                })
                .ok_or(DatabaseError::InvalidIndex)?;
            transaction.read_by_index(
                table_cache,
                plan_arena,
                "t1".to_string().into(),
                (None, None),
                full_columns(table_cache),
                index_meta,
                vec![Range::Scope {
                    min: Bound::Unbounded,
                    max: Bound::Unbounded,
                }],
                true,
                None,
                None,
            )
        }

        let mut table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let mut table_cache = crate::storage::TableCache::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let mut plan_arena = PlanArena::new(&table_arena);
        let t1_table = transaction
            .table(&table_cache, "t1".to_string().into())?
            .unwrap();
        let c3_column_id = *t1_table.get_column_id_by_name("c3").unwrap();

        let (table, _) = transaction.add_index_meta(
            &mut table_codec,
            &mut plan_arena,
            &"t1".to_string().into(),
            "i1".to_string(),
            vec![c3_column_id],
            IndexType::Normal,
        )?;
        let table = table.transplant_to_table_arena(&plan_arena)?;
        table_cache.insert(table.name().clone(), table);
        let plan_arena = PlanArena::new(&table_arena);

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
            transaction.add_index(&mut table_codec, "t1", index, &tuple_id)?;
        }
        for tuple in tuples.iter().cloned() {
            transaction.append_tuple(
                &mut table_codec,
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
            let mut index_iter = build_index_iter(&transaction, &table_cache, &plan_arena)?;

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
        transaction.del_index(&mut table_codec, "t1", &indexes[0].1, &indexes[0].0)?;

        let mut index_iter = build_index_iter(&transaction, &table_cache, &plan_arena)?;

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
        let mut table_codec = TableCodec::default();
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut table_cache = crate::storage::TableCache::default();
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
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        let index_id = {
            let mut setup_tx = storage.transaction()?;
            build_table(&mut table_cache, &mut setup_tx, &mut plan_arena)?;
            let mut plan_arena = PlanArena::new(&table_arena);
            let table = setup_tx
                .table(&table_cache, "t1".to_string().into())?
                .unwrap();
            let c3_column_id = *table.get_column_id_by_name("c3").unwrap();
            let (table, index_id) = setup_tx.add_index_meta(
                &mut table_codec,
                &mut plan_arena,
                &"t1".to_string().into(),
                "i1".to_string(),
                vec![c3_column_id],
                IndexType::Normal,
            )?;
            let table = table.transplant_to_table_arena(&plan_arena)?;
            table_cache.insert(table.name().clone(), table);

            setup_tx.add_index(
                &mut table_codec,
                "t1",
                Index::new(index_id, &initial_tuple.values[2], IndexType::Normal),
                initial_tuple.pk.as_ref().unwrap(),
            )?;
            setup_tx.append_tuple(
                &mut table_codec,
                "t1",
                initial_tuple.clone(),
                &serializers,
                false,
            )?;
            setup_tx.commit()?;

            index_id
        };
        let reader_tx =
            storage.transaction_with_isolation(TransactionIsolationLevel::ReadCommitted)?;
        let tuple_id = {
            let mut index_iter = table_codec.with_index_bound("t1", index_id, |min, max| {
                reader_tx.range(Bound::Included(min), Bound::Included(max))
            })?;
            let (_, value) = index_iter.try_next()?.unwrap();

            TableCodec::decode_index(value)?
        };

        let before_update = table_codec.with_tuple("t1", &tuple_id, None, |key, _| {
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
            &mut table_codec,
            "t1",
            &Index::new(index_id, &initial_tuple.values[2], IndexType::Normal),
            initial_tuple.pk.as_ref().unwrap(),
        )?;
        writer_tx.add_index(
            &mut table_codec,
            "t1",
            Index::new(index_id, &updated_tuple.values[2], IndexType::Normal),
            updated_tuple.pk.as_ref().unwrap(),
        )?;
        writer_tx.append_tuple(
            &mut table_codec,
            "t1",
            updated_tuple.clone(),
            &serializers,
            true,
        )?;
        writer_tx.commit()?;

        let after_update = table_codec.with_tuple("t1", &tuple_id, None, |key, _| {
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
        let mut table_cache = crate::storage::TableCache::default();
        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);

        build_table(&mut table_cache, &mut transaction, &mut plan_arena)?;
        let mut plan_arena = PlanArena::new(&table_arena);
        let table_name: TableName = "t1".to_string().into();

        let new_column = ColumnCatalog::new(
            "c4".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        let (table, new_column_id) = transaction.add_column(
            &mut table_codec,
            &mut plan_arena,
            &table_name,
            &new_column,
            false,
        )?;
        let table = table.transplant_to_table_arena(&plan_arena)?;
        table_cache.insert(table.name().clone(), table);
        let mut plan_arena = PlanArena::new(&table_arena);
        {
            assert!(transaction
                .add_column(
                    &mut table_codec,
                    &mut plan_arena,
                    &table_name,
                    &new_column,
                    false,
                )
                .is_err());
            assert_eq!(
                new_column_id,
                transaction
                    .add_column(
                        &mut table_codec,
                        &mut plan_arena,
                        &table_name,
                        &new_column,
                        true,
                    )?
                    .1
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
            let column = table.get_column_by_name("c4").unwrap();
            assert_eq!(table_arena.borrow().column(column), &new_column);
        }
        let table =
            transaction.drop_column(&mut table_codec, &mut plan_arena, &table_name, "c4")?;
        let table = table.transplant_to_table_arena(&plan_arena)?;
        table_cache.insert(table.name().clone(), table);
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
        let scala_functions = Default::default();
        let table_functions = Default::default();

        let view_name: TableName = "v1".to_string().into();
        let mut plan_arena = PlanArena::new(&table_state.table_arena);
        let mut plan = table_state.plan_with_arena(
            "select c1, c3 from t1 inner join t2 on c1 = c3 and c1 > 1",
            &mut plan_arena,
        )?;
        let schema = plan.output_schema(&mut plan_arena).clone();
        let view = View {
            name: view_name.clone(),
            plan: Box::new(plan),
            schema,
        };
        let mut transaction = table_state.storage.transaction()?;
        let mut view_cache = table_state.view_cache.clone();
        let mut table_codec = TableCodec::default();
        let view = transaction.create_view(&mut table_codec, &plan_arena, view.clone(), true)?;
        view_cache.insert(view.name.clone(), view.clone());

        assert_eq!(
            &view,
            transaction
                .view(
                    &table_state.table_cache,
                    &view_cache,
                    &scala_functions,
                    &table_functions,
                    view_name.clone(),
                )?
                .unwrap()
        );
        assert_eq!(
            &view,
            transaction
                .view(
                    &crate::storage::TableCache::default(),
                    &view_cache,
                    &scala_functions,
                    &table_functions,
                    view_name.clone(),
                )?
                .unwrap()
        );

        if transaction.drop_view(&mut table_codec, view_name.clone(), false)? {
            view_cache.remove(&view_name);
        }
        if transaction.drop_view(&mut table_codec, view_name.clone(), true)? {
            view_cache.remove(&view_name);
        }
        assert!(transaction
            .view(
                &table_state.table_cache,
                &view_cache,
                &scala_functions,
                &table_functions,
                view_name,
            )?
            .is_none());

        Ok(())
    }
}
