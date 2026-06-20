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

use crate::catalog::view::View;
use crate::catalog::{ColumnCatalog, ColumnRelation, TableMeta};
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::optimizer::core::cm_sketch::{CountMinSketchMeta, CountMinSketchPage};
use crate::optimizer::core::histogram::Bucket;
use crate::optimizer::core::statistics_meta::StatisticsMetaRoot;
use crate::optimizer::core::top_n::ColumnTopN;
use crate::planner::MetaArena;
use crate::serdes::stable_hash::{StableHasher, TABLE_NAME_HASH_KEYS};
use crate::serdes::{ReferenceDecodeContext, ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::index::{Index, IndexId, IndexMeta, IndexType, INDEX_ID_LEN};
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, TupleMappingRef};
use crate::types::LogicalType;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::sync::LazyLock;

pub(crate) const BOUND_MIN_TAG: u8 = u8::MIN;
pub(crate) const BOUND_MAX_TAG: u8 = u8::MAX;
// Nulls Last default
pub(crate) const NULL_TAG: u8 = 1u8;
pub(crate) const NOTNULL_TAG: u8 = 0u8;
const TABLE_NAME_HASH_LEN: usize = 8;
const KEY_TYPE_TAG_LEN: usize = 1;
const KEY_BOUND_LEN: usize = 1;
const TUPLE_KEY_PREFIX_LEN: usize = TABLE_NAME_HASH_LEN + KEY_TYPE_TAG_LEN + KEY_BOUND_LEN;
const STATISTICS_BUCKET_ORD_LEN: usize = 4;

static ROOT_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| b"Root".to_vec());
static VIEW_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| b"View".to_vec());
static HASH_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| b"Hash".to_vec());
static EMPTY_REFERENCE_TABLES: LazyLock<ReferenceTables> = LazyLock::new(ReferenceTables::new);

pub type Bytes = Vec<u8>;
pub type BumpBytes<'bump> = bumpalo::collections::Vec<'bump, u8>;
type TupleValueWriter<'a> = &'a mut dyn FnMut(&Tuple, &mut Bytes) -> Result<(), DatabaseError>;

#[derive(Default)]
pub struct TableCodec {
    buffers: [Bytes; 2],
    reference_tables: ReferenceTables,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
enum CodecSlot {
    S0,
    S1,
}

impl CodecSlot {
    #[inline]
    fn index(self) -> usize {
        match self {
            CodecSlot::S0 => 0,
            CodecSlot::S1 => 1,
        }
    }
}

#[derive(Copy, Clone)]
enum CodecType {
    Column,
    IndexMeta,
    Index,
    Statistics,
    View,
    Tuple,
    Root,
    Hash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatisticsCodecType {
    Root,
    SketchMeta,
    SketchPage,
    Bucket,
    TopN,
}

impl StatisticsCodecType {
    fn tag(self) -> u8 {
        match self {
            StatisticsCodecType::Root => b'0',
            StatisticsCodecType::SketchMeta => b'1',
            StatisticsCodecType::SketchPage => b'2',
            StatisticsCodecType::Bucket => b'3',
            StatisticsCodecType::TopN => b'4',
        }
    }

    fn from_tag(tag: u8) -> Result<Self, DatabaseError> {
        match tag {
            b'0' => Ok(StatisticsCodecType::Root),
            b'1' => Ok(StatisticsCodecType::SketchMeta),
            b'2' => Ok(StatisticsCodecType::SketchPage),
            b'3' => Ok(StatisticsCodecType::Bucket),
            b'4' => Ok(StatisticsCodecType::TopN),
            _ => Err(DatabaseError::InvalidValue(format!(
                "invalid statistics codec tag: {tag}"
            ))),
        }
    }
}

impl TableCodec {
    #[inline]
    fn clear_buffers(&mut self) {
        self.buffers[CodecSlot::S0.index()].clear();
        self.buffers[CodecSlot::S1.index()].clear();
        self.reference_tables.clear();
    }

    #[inline]
    fn slots_mut(&mut self) -> (&mut Bytes, &mut Bytes, &mut ReferenceTables) {
        let [s0, s1] = &mut self.buffers;
        (s0, s1, &mut self.reference_tables)
    }

    fn hash_bytes(table_name: &str) -> [u8; 8] {
        let (key0, key1) = TABLE_NAME_HASH_KEYS;
        let mut hasher = StableHasher::new_with_keys(key0, key1);
        table_name.hash(&mut hasher);
        hasher.finish().to_le_bytes()
    }

    pub fn check_primary_key(value: &DataValue, indentation: usize) -> Result<(), DatabaseError> {
        if indentation > 1 {
            return Err(DatabaseError::PrimaryKeyTooManyLayers);
        }
        if value.is_null() {
            return Err(DatabaseError::not_null_column("primary key"));
        }

        if let DataValue::Tuple(values, _) = &value {
            for value in values {
                Self::check_primary_key(value, indentation + 1)?
            }

            return Ok(());
        } else {
            Self::check_primary_key_type(&value.logical_type())?;
        }

        Ok(())
    }

    pub fn check_primary_key_type(ty: &LogicalType) -> Result<(), DatabaseError> {
        if !matches!(
            ty,
            LogicalType::Tinyint
                | LogicalType::Smallint
                | LogicalType::Integer
                | LogicalType::Bigint
                | LogicalType::UTinyint
                | LogicalType::USmallint
                | LogicalType::UInteger
                | LogicalType::UBigint
                | LogicalType::Char(..)
                | LogicalType::Varchar(..)
                | LogicalType::Date
                | LogicalType::DateTime
                | LogicalType::Time(..)
                | LogicalType::TimeStamp(..)
        ) {
            return Err(DatabaseError::InvalidType);
        }
        Ok(())
    }

    #[inline]
    fn write_key_prefix(out: &mut Bytes, ty: CodecType, table_hash: [u8; TABLE_NAME_HASH_LEN]) {
        match ty {
            CodecType::Column => {
                out.extend_from_slice(&table_hash);
                out.push(b'0');
            }
            CodecType::IndexMeta => {
                out.extend_from_slice(&table_hash);
                out.push(b'1');
            }
            CodecType::Statistics => {
                out.extend_from_slice(&table_hash);
                out.push(b'3');
            }
            CodecType::Index => {
                out.extend_from_slice(&table_hash);
                out.push(b'7');
            }
            CodecType::Tuple => {
                out.extend_from_slice(&table_hash);
                out.push(b'8');
            }
            CodecType::Root => {
                out.extend_from_slice(ROOT_BYTES.as_slice());
                out.push(BOUND_MIN_TAG);
                out.extend_from_slice(&table_hash);
            }
            CodecType::View => {
                out.extend_from_slice(VIEW_BYTES.as_slice());
                out.push(BOUND_MIN_TAG);
                out.extend_from_slice(&table_hash);
            }
            CodecType::Hash => {
                out.extend_from_slice(HASH_BYTES.as_slice());
                out.push(BOUND_MIN_TAG);
                out.extend_from_slice(&table_hash);
            }
        }
    }

    #[inline]
    fn write_global_bound_prefix(out: &mut Bytes, prefix: &[u8], bound: u8) {
        out.extend_from_slice(prefix);
        out.push(bound);
    }

    fn with_table_hash_buffers<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(
            &mut Bytes,
            [u8; TABLE_NAME_HASH_LEN],
            &mut Bytes,
            &mut ReferenceTables,
        ) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let table_hash = Self::hash_bytes(table_name);
        let (s0, s1, reference_tables) = self.slots_mut();
        f(s0, table_hash, s1, reference_tables)
    }

    /// Key: `{TableName}{TUPLE_TAG}{BOUND_MIN_TAG}{RowID}`.
    pub fn with_tuple<R>(
        &mut self,
        table_name: &str,
        tuple_id: &TupleId,
        tuple_value: Option<(&Tuple, TupleValueWriter<'_>)>,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        Self::check_primary_key(tuple_id, 0)?;
        self.with_tuple_unchecked(table_name, tuple_id, tuple_value, f)
    }

    #[inline]
    pub(crate) fn with_tuple_unchecked<R>(
        &mut self,
        table_name: &str,
        tuple_id: &TupleId,
        tuple_value: Option<(&Tuple, TupleValueWriter<'_>)>,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, _| {
            Self::write_key_prefix(lower, CodecType::Tuple, table_hash);
            lower.push(BOUND_MIN_TAG);
            tuple_id.memcomparable_encode(lower)?;

            if let Some((tuple, serializers)) = tuple_value {
                serializers(tuple, value)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Range bounds covering all tuple keys for a table.
    pub fn with_tuple_bound<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Tuple, table_hash);
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::Tuple, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Key: `{TableName}{INDEX_META_TAG}{BOUND_MIN_TAG}{IndexID}`.
    pub fn with_index_meta<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        index_meta: Option<&IndexMeta>,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::IndexMeta, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());

            if let Some(index_meta) = index_meta {
                Self::encode_index_meta_value_into(index_meta, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Range bounds covering all index metadata for a table.
    pub fn with_index_meta_bound<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::IndexMeta, table_hash);
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::IndexMeta, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering a single secondary index.
    pub fn with_index_bound<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Index, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::Index, table_hash);
            upper.push(BOUND_MIN_TAG);
            upper.extend_from_slice(&index_id.to_le_bytes());
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering all secondary indexes for a table.
    pub fn with_all_index_bound<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Index, table_hash);
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::Index, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Non-unique index key:
    /// `{TableName}{INDEX_TAG}{BOUND_MIN_TAG}{IndexID}{BOUND_MIN_TAG}{DataValue...}{TupleId}`
    ///
    /// Unique index key:
    /// `{TableName}{INDEX_TAG}{BOUND_MIN_TAG}{IndexID}{BOUND_MIN_TAG}{DataValue}`
    pub fn with_index<R>(
        &mut self,
        table_name: &str,
        index: &Index,
        tuple_id: Option<&TupleId>,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, _| {
            Self::write_key_prefix(lower, CodecType::Index, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index.id.to_le_bytes());
            lower.push(BOUND_MIN_TAG);
            index.value.memcomparable_encode(lower)?;

            if let Some(tuple_id) = tuple_id {
                if matches!(index.ty, IndexType::Normal | IndexType::Composite) {
                    tuple_id.memcomparable_encode(lower)?;
                }
            }

            if let Some(tuple_id) = tuple_id {
                tuple_id.encode_reference_value(&mut *value)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `{TableName}{COLUMN_TAG}{BOUND_MIN_TAG}{ColumnId}`.
    pub fn with_column<R>(
        &mut self,
        col: &ColumnCatalog,
        encode_value: bool,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        if let ColumnRelation::Table {
            column_id,
            table_name,
            is_temp: false,
        } = &col.summary().relation
        {
            self.clear_buffers();
            self.with_table_hash_buffers(table_name.as_ref(), |lower, table_hash, value, refs| {
                Self::write_key_prefix(lower, CodecType::Column, table_hash);
                lower.push(BOUND_MIN_TAG);
                lower.extend_from_slice(&column_id.to_be_bytes());

                if encode_value {
                    let _ = refs.push_or_replace(table_name);
                    Self::encode_column_value_into(col, refs, value, arena)?;
                }

                f(lower.as_slice(), value.as_slice())
            })
        } else {
            Err(DatabaseError::invalid_column(
                "column does not belong to table".to_string(),
            ))
        }
    }

    /// Range bounds covering all column metadata for a table.
    pub fn with_columns_bound<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Column, table_hash);
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::Column, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds spanning a table's `Column` and `IndexMeta` metadata.
    pub fn with_table_bound<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Column, table_hash);
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::IndexMeta, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering all statistics keys for a table.
    pub fn with_statistics_bound<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);

            Self::write_key_prefix(upper, CodecType::Statistics, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering all statistics keys for one index.
    pub fn with_statistics_index_bound<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, upper, _| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());

            upper.clear();
            upper.extend_from_slice(lower);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{ROOT_TAG}`.
    pub fn with_statistics_meta<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        statistics_meta: Option<&StatisticsMetaRoot>,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::Root.tag());

            if let Some(statistics_meta) = statistics_meta {
                Self::encode_statistics_meta_value_into(statistics_meta, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{SKETCH_META_TAG}`.
    pub fn with_statistics_sketch_meta<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        sketch_meta: Option<&CountMinSketchMeta>,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::SketchMeta.tag());

            if let Some(sketch_meta) = sketch_meta {
                Self::encode_statistics_sketch_meta_value_into(sketch_meta, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{SKETCH_PAGE_TAG}{BOUND_MIN_TAG}{ROW_ID}{BOUND_MIN_TAG}{PAGE_ID}`.
    pub fn with_statistics_sketch_page<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        sketch_page: &CountMinSketchPage,
        encode_value: bool,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::SketchPage.tag());
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&(sketch_page.row_idx() as u32).to_be_bytes());
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&(sketch_page.page_idx() as u32).to_be_bytes());

            if encode_value {
                Self::encode_statistics_sketch_page_value_into(sketch_page, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{BUCKET_TAG}{BOUND_MIN_TAG}{ORDINAL}`.
    pub fn with_statistics_bucket<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        ordinal: u32,
        bucket: Option<&Bucket>,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::Bucket.tag());
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&ordinal.to_be_bytes());

            if let Some(bucket) = bucket {
                Self::encode_statistics_bucket_value_into(bucket, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{TOP_N_TAG}`.
    pub fn with_statistics_top_n<R>(
        &mut self,
        table_name: &str,
        index_id: IndexId,
        top_n: Option<&ColumnTopN>,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::TopN.tag());

            if let Some(top_n) = top_n {
                Self::encode_statistics_top_n_value_into(top_n, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `View{BOUND_MIN_TAG}{ViewNameHash}`.
    pub fn with_view<R>(
        &mut self,
        view_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(view_name, |lower, table_hash, value, _| {
            Self::write_key_prefix(lower, CodecType::View, table_hash);
            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Key: `View{BOUND_MIN_TAG}{ViewNameHash}` with encoded view payload.
    pub fn with_view_value<R, A: MetaArena>(
        &mut self,
        view_name: &str,
        view: &View,
        arena: &A,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(view_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::View, table_hash);
            Self::encode_view_value_into(view, refs, value, arena)?;
            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Range bounds covering all view definitions.
    pub fn with_view_bound<R>(
        &mut self,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        let (lower, upper, _) = self.slots_mut();
        Self::write_global_bound_prefix(lower, VIEW_BYTES.as_slice(), BOUND_MIN_TAG);
        Self::write_global_bound_prefix(upper, VIEW_BYTES.as_slice(), BOUND_MAX_TAG);

        f(lower.as_slice(), upper.as_slice())
    }

    /// Key: `Root{BOUND_MIN_TAG}{TableNameHash}`.
    pub fn with_root_table<R>(
        &mut self,
        table_name: &str,
        meta: Option<&TableMeta>,
        arena: &impl MetaArena,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, refs| {
            Self::write_key_prefix(lower, CodecType::Root, table_hash);

            if let Some(meta) = meta {
                Self::encode_root_table_value_into(meta, refs, value, arena)?;
            }

            f(lower.as_slice(), value.as_slice())
        })
    }

    /// Range bounds covering all root table metadata.
    pub fn with_root_table_bound<R>(
        &mut self,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        let (lower, upper, _) = self.slots_mut();
        Self::write_global_bound_prefix(lower, ROOT_BYTES.as_slice(), BOUND_MIN_TAG);
        Self::write_global_bound_prefix(upper, ROOT_BYTES.as_slice(), BOUND_MAX_TAG);

        f(lower.as_slice(), upper.as_slice())
    }

    /// Key: `Hash{BOUND_MIN_TAG}{TableNameHash}`.
    pub fn with_table_hash<R>(
        &mut self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.clear_buffers();
        self.with_table_hash_buffers(table_name, |lower, table_hash, value, _| {
            Self::write_key_prefix(lower, CodecType::Hash, table_hash);

            f(lower.as_slice(), value.as_slice())
        })
    }
    pub fn decode_tuple_key(bytes: &[u8], pk_ty: &LogicalType) -> Result<TupleId, DatabaseError> {
        DataValue::memcomparable_decode(&mut Cursor::new(&bytes[TUPLE_KEY_PREFIX_LEN..]), pk_ty)
    }

    #[inline]
    pub fn decode_tuple_into<I, S>(
        tuple: &mut Tuple,
        deserializers: I,
        tuple_id: Option<TupleId>,
        bytes: &[u8],
        total_len: usize,
    ) -> Result<(), DatabaseError>
    where
        I: IntoIterator<Item = S>,
        S: Borrow<TupleValueSerializableImpl>,
    {
        tuple.pk = tuple_id;
        tuple.deserialize_from_into(deserializers, bytes, total_len)
    }

    fn encode_index_meta_value_into(
        index_meta: &IndexMeta,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        index_meta.encode(value, true, reference_tables, arena)
    }

    pub fn decode_index_meta<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<IndexMeta, DatabaseError> {
        IndexMeta::decode::<T, _, _>(
            &mut Cursor::new(bytes),
            None,
            &EMPTY_REFERENCE_TABLES,
            arena,
        )
    }

    pub fn decode_index_key(
        bytes: &[u8],
        ty: &LogicalType,
        mapping: Option<TupleMappingRef<'_>>,
    ) -> Result<DataValue, DatabaseError> {
        // Hash + TypeTag + Bound Min + Index Id Len + Bound Min
        let start = TUPLE_KEY_PREFIX_LEN + INDEX_ID_LEN + KEY_BOUND_LEN;
        DataValue::memcomparable_decode_mapping(&mut Cursor::new(&bytes[start..]), ty, mapping)
    }

    pub fn decode_index(bytes: &[u8]) -> Result<TupleId, DatabaseError> {
        DataValue::decode_reference_value(&mut Cursor::new(bytes))
    }

    fn encode_column_value_into(
        col: &ColumnCatalog,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        col.encode(value, true, reference_tables, arena)
    }

    pub fn decode_column<T: Transaction, R: Read>(
        reader: &mut R,
        reference_tables: &ReferenceTables,
        arena: &mut impl MetaArena,
    ) -> Result<ColumnCatalog, DatabaseError> {
        // `TableCache` is not theoretically used in `table_collect` because `ColumnCatalog` should not depend on other Column
        ColumnCatalog::decode::<T, R, _>(reader, None, reference_tables, arena)
    }

    fn encode_statistics_meta_value_into(
        statistics_meta: &StatisticsMetaRoot,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        statistics_meta.encode(value, true, reference_tables, arena)
    }

    pub fn decode_statistics_meta<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<StatisticsMetaRoot, DatabaseError> {
        StatisticsMetaRoot::decode::<T, _, _>(
            &mut Cursor::new(bytes),
            None,
            &EMPTY_REFERENCE_TABLES,
            arena,
        )
    }

    fn encode_statistics_sketch_meta_value_into(
        sketch_meta: &CountMinSketchMeta,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        sketch_meta.encode(value, true, reference_tables, arena)
    }

    pub fn decode_statistics_sketch_meta<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<CountMinSketchMeta, DatabaseError> {
        CountMinSketchMeta::decode::<T, _, _>(
            &mut Cursor::new(bytes),
            None,
            &EMPTY_REFERENCE_TABLES,
            arena,
        )
    }

    fn encode_statistics_sketch_page_value_into(
        sketch_page: &CountMinSketchPage,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        sketch_page.encode(value, true, reference_tables, arena)
    }

    pub fn decode_statistics_sketch_page<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<CountMinSketchPage, DatabaseError> {
        CountMinSketchPage::decode::<T, _, _>(
            &mut Cursor::new(bytes),
            None,
            &EMPTY_REFERENCE_TABLES,
            arena,
        )
    }

    fn encode_statistics_bucket_value_into(
        bucket: &Bucket,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        bucket.encode(value, true, reference_tables, arena)
    }

    pub(crate) fn decode_statistics_codec_type(
        key: &[u8],
    ) -> Result<StatisticsCodecType, DatabaseError> {
        let prefix_len = TUPLE_KEY_PREFIX_LEN + INDEX_ID_LEN;
        let Some(tag) = key.get(prefix_len).copied() else {
            return Err(DatabaseError::InvalidValue(
                "statistics key is too short".to_string(),
            ));
        };

        StatisticsCodecType::from_tag(tag)
    }

    pub fn decode_statistics_bucket<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<Bucket, DatabaseError> {
        Bucket::decode::<T, _, _>(
            &mut Cursor::new(bytes),
            None,
            &EMPTY_REFERENCE_TABLES,
            arena,
        )
    }

    fn encode_statistics_top_n_value_into(
        top_n: &ColumnTopN,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        top_n.encode(value, true, reference_tables, arena)
    }

    pub fn decode_statistics_top_n<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<ColumnTopN, DatabaseError> {
        ColumnTopN::decode::<T, _, _>(
            &mut Cursor::new(bytes),
            None,
            &EMPTY_REFERENCE_TABLES,
            arena,
        )
    }

    pub fn decode_statistics_bucket_ordinal(bytes: &[u8]) -> Result<u32, DatabaseError> {
        let len = bytes.len();
        let Some(ordinal_bytes) = bytes.get(len - STATISTICS_BUCKET_ORD_LEN..) else {
            return Err(DatabaseError::InvalidValue(
                "statistics bucket key is too short".to_string(),
            ));
        };

        Ok(u32::from_be_bytes(ordinal_bytes.try_into().unwrap()))
    }

    fn encode_view_value_into(
        view: &View,
        reference_tables: &mut ReferenceTables,
        bytes: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        bytes.clear();
        bytes.resize(4, 0u8);

        let reference_tables_pos = {
            view.encode(&mut *bytes, false, reference_tables, arena)?;
            let pos = bytes.len();
            reference_tables.to_raw(&mut *bytes)?;
            pos
        };
        bytes[..4].copy_from_slice(&(reference_tables_pos as u32).to_le_bytes());

        Ok(())
    }

    pub fn decode_view<T: Transaction>(
        bytes: &[u8],
        drive: (&T, &TableCache),
        scala_functions: &ScalaFunctions,
        table_functions: &TableFunctions,
        arena: &mut impl MetaArena,
    ) -> Result<View, DatabaseError> {
        let mut cursor = Cursor::new(bytes);
        let reference_tables_pos = {
            let mut bytes = [0u8; 4];
            cursor.read_exact(&mut bytes)?;
            u32::from_le_bytes(bytes) as u64
        };
        cursor.seek(SeekFrom::Start(reference_tables_pos))?;
        let reference_tables = ReferenceTables::from_raw(&mut cursor)?;
        cursor.seek(SeekFrom::Start(4))?;

        let context =
            ReferenceDecodeContext::with_functions(Some(drive), scala_functions, table_functions);
        View::decode(&mut cursor, Some(&context), &reference_tables, arena)
    }

    fn encode_root_table_value_into(
        meta: &TableMeta,
        reference_tables: &mut ReferenceTables,
        value: &mut Bytes,
        arena: &impl MetaArena,
    ) -> Result<(), DatabaseError> {
        meta.encode(value, true, reference_tables, arena)
    }

    pub fn decode_root_table<T: Transaction>(
        bytes: &[u8],
        arena: &mut impl MetaArena,
    ) -> Result<TableMeta, DatabaseError> {
        let mut bytes = Cursor::new(bytes);

        TableMeta::decode::<T, _, _>(&mut bytes, None, &EMPTY_REFERENCE_TABLES, arena)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::catalog::view::View;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRelation, TableCatalog, TableMeta};
    use crate::errors::DatabaseError;
    use crate::iter_ext::Itertools;
    use crate::optimizer::core::histogram::{HistogramBuilder, ANALYZE_STATISTICS_RELATIVE_ERROR};
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use crate::planner::{PlanArena, TableArenaCell};
    use crate::serdes::ReferenceTables;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::table_codec::{Bytes, TableCodec};
    use crate::storage::Storage;
    use crate::types::index::{Index, IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::ColumnId;
    use crate::types::LogicalType;
    use rust_decimal::Decimal;
    use std::collections::BTreeSet;
    use std::io::Cursor;
    use std::ops::Bound;
    use std::sync::Arc;

    fn build_table_codec(table_arena: &TableArenaCell) -> TableCatalog {
        let columns = vec![
            ColumnCatalog::new(
                "c1".into(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".into(),
                false,
                ColumnDesc::new(LogicalType::Decimal(None, None), None, false, None).unwrap(),
            ),
        ];
        TableCatalog::new("t1".to_string().into(), columns, table_arena.borrow_mut()).unwrap()
    }

    #[test]
    fn test_table_codec_tuple() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let table_catalog = build_table_codec(&table_arena);
        let plan_arena = PlanArena::new(&table_arena);

        let expected = Tuple::new(
            Some(DataValue::Int32(0)),
            vec![DataValue::Int32(0), DataValue::Decimal(Decimal::new(1, 0))],
        );
        let mut bytes = Vec::new();
        expected.serialize_to(
            &[
                LogicalType::Integer.serializable(),
                LogicalType::Decimal(None, None).serializable(),
            ],
            &mut bytes,
        )?;
        let deserializers = table_catalog
            .columns()
            .map(|column| plan_arena.column(*column).datatype().serializable())
            .collect_vec();

        let mut tuple = Tuple::default();
        TableCodec::decode_tuple_into(&mut tuple, &deserializers, None, &bytes, 2)?;
        assert_eq!(
            tuple,
            Tuple::new(
                None,
                vec![DataValue::Int32(0), DataValue::Decimal(Decimal::new(1, 0))]
            )
        );

        Ok(())
    }

    #[test]
    fn test_root_catalog() {
        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let table_catalog = build_table_codec(&table_arena);
        let meta = TableMeta {
            table_name: table_catalog.name.clone(),
        };
        let bytes = table_codec
            .with_root_table(
                table_catalog.name.as_ref(),
                Some(&meta),
                table_arena.borrow(),
                |_, value| Ok::<_, DatabaseError>(value.to_vec()),
            )
            .unwrap();

        let table_meta =
            TableCodec::decode_root_table::<RocksTransaction>(&bytes, table_arena.borrow_mut())
                .unwrap();

        assert_eq!(table_meta.table_name.as_ref(), table_catalog.name.as_ref());
    }

    #[test]
    fn test_table_codec_statistics_meta() -> Result<(), DatabaseError> {
        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![1],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };
        let mut builder = HistogramBuilder::new(&index_meta, ANALYZE_STATISTICS_RELATIVE_ERROR)?;

        for value in 0..4 {
            builder.append(DataValue::Int32(value))?;
        }
        let (histogram, sketch, top_n) = builder.build(2)?;
        let (root, buckets, _, top_n) =
            StatisticsMeta::new(histogram, sketch.clone(), top_n).into_parts();

        let root_bytes =
            table_codec.with_statistics_meta("t1", 0, Some(&root), &plan_arena, |_, value| {
                Ok::<_, DatabaseError>(value.to_vec())
            })?;
        let decoded_root =
            TableCodec::decode_statistics_meta::<RocksTransaction>(&root_bytes, &mut plan_arena)?;
        assert_eq!(decoded_root.index_id(), root.index_id());
        assert_eq!(
            decoded_root.histogram_meta().values_len(),
            root.histogram_meta().values_len()
        );
        assert_eq!(
            decoded_root.histogram_meta().buckets_len(),
            root.histogram_meta().buckets_len()
        );

        let (sketch_meta, mut sketch_pages) = sketch.clone().into_storage_parts(1);
        let sketch_meta_bytes = table_codec.with_statistics_sketch_meta(
            "t1",
            0,
            Some(&sketch_meta),
            &plan_arena,
            |_, value| Ok::<_, DatabaseError>(value.to_vec()),
        )?;
        let decoded_sketch_meta = TableCodec::decode_statistics_sketch_meta::<RocksTransaction>(
            &sketch_meta_bytes,
            &mut plan_arena,
        )?;
        assert_eq!(decoded_sketch_meta.width(), sketch_meta.width());
        assert_eq!(decoded_sketch_meta.k_num(), sketch_meta.k_num());

        let first_sketch_page = sketch_pages.next().unwrap();
        let sketch_page_bytes = table_codec.with_statistics_sketch_page(
            "t1",
            0,
            &first_sketch_page,
            true,
            &plan_arena,
            |_, value| Ok::<_, DatabaseError>(value.to_vec()),
        )?;
        let decoded_sketch_page = TableCodec::decode_statistics_sketch_page::<RocksTransaction>(
            &sketch_page_bytes,
            &mut plan_arena,
        )?;
        assert_eq!(decoded_sketch_page.counters(), first_sketch_page.counters());

        let bucket0_key =
            table_codec.with_statistics_bucket("t1", 0, 0, None, &plan_arena, |key, _| {
                Ok::<_, DatabaseError>(key.to_vec())
            })?;
        let bucket1_key =
            table_codec.with_statistics_bucket("t1", 0, 1, None, &plan_arena, |key, _| {
                Ok::<_, DatabaseError>(key.to_vec())
            })?;
        assert!(bucket0_key < bucket1_key);

        let (bucket0_min, bucket0_max) =
            table_codec.with_statistics_index_bound("t1", 0, |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })?;
        assert!(bucket0_key.as_slice() >= bucket0_min.as_slice());
        assert!(bucket1_key.as_slice() <= bucket0_max.as_slice());

        let bucket_bytes = table_codec.with_statistics_bucket(
            "t1",
            0,
            0,
            Some(&buckets[0]),
            &plan_arena,
            |_, value| Ok::<_, DatabaseError>(value.to_vec()),
        )?;
        let decoded_bucket = TableCodec::decode_statistics_bucket::<RocksTransaction>(
            &bucket_bytes,
            &mut plan_arena,
        )?;
        assert_eq!(decoded_bucket, buckets[0]);
        assert_eq!(
            TableCodec::decode_statistics_bucket_ordinal(&bucket0_key)?,
            0
        );
        assert_eq!(
            TableCodec::decode_statistics_bucket_ordinal(&bucket1_key)?,
            1
        );

        let top_n_key =
            table_codec.with_statistics_top_n("t1", 0, None, &plan_arena, |key, _| {
                Ok::<_, DatabaseError>(key.to_vec())
            })?;
        assert!(top_n_key.as_slice() >= bucket0_min.as_slice());
        assert!(top_n_key.as_slice() <= bucket0_max.as_slice());

        let top_n_bytes =
            table_codec.with_statistics_top_n("t1", 0, Some(&top_n), &plan_arena, |_, value| {
                Ok::<_, DatabaseError>(value.to_vec())
            })?;
        let decoded_top_n =
            TableCodec::decode_statistics_top_n::<RocksTransaction>(&top_n_bytes, &mut plan_arena)?;
        assert_eq!(decoded_top_n, top_n);

        Ok(())
    }

    #[test]
    fn test_table_codec_index_meta() -> Result<(), DatabaseError> {
        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![1],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "index_1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };
        let bytes =
            table_codec.with_index_meta("t1", 0, Some(&index_meta), &plan_arena, |_, value| {
                Ok::<_, DatabaseError>(value.to_vec())
            })?;

        assert_eq!(
            TableCodec::decode_index_meta::<RocksTransaction>(&bytes, &mut plan_arena)?,
            index_meta
        );

        Ok(())
    }

    #[test]
    fn test_table_codec_index() -> Result<(), DatabaseError> {
        let tuple_id = DataValue::Int32(0);
        let mut bytes = Vec::new();
        tuple_id.encode_reference_value(&mut bytes)?;

        assert_eq!(TableCodec::decode_index(&bytes)?, tuple_id);

        Ok(())
    }

    #[test]
    fn test_table_codec_column() -> Result<(), DatabaseError> {
        let mut col: ColumnCatalog = ColumnCatalog::new(
            "c2".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
        );
        col.summary_mut().relation = ColumnRelation::Table {
            column_id: 1,
            table_name: "t1".to_string().into(),
            is_temp: false,
        };
        let expected_col = col.clone();

        let mut reference_tables = ReferenceTables::new();
        reference_tables.push_or_replace(&"t1".to_string().into());

        let mut table_codec = TableCodec::default();
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let bytes = table_codec
            .with_column(&col, true, &plan_arena, |_, value| {
                Ok::<_, DatabaseError>(value.to_vec())
            })
            .unwrap();
        let mut cursor = Cursor::new(bytes);
        let decode_col = TableCodec::decode_column::<RocksTransaction, _>(
            &mut cursor,
            &reference_tables,
            &mut plan_arena,
        )?;

        assert_eq!(decode_col, expected_col);

        Ok(())
    }

    #[test]
    fn test_table_codec_view() -> Result<(), DatabaseError> {
        fn normalize_explain(explain: String) -> String {
            let mut normalized = String::with_capacity(explain.len());
            let bytes = explain.as_bytes();
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i..].starts_with(b"pos: ") {
                    normalized.push_str("pos: _");
                    i += b"pos: ".len();
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                } else if bytes[i] == b'#' {
                    normalized.push_str("#_");
                    i += 1;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }
                } else {
                    normalized.push(bytes[i] as char);
                    i += 1;
                }
            }
            normalized
        }

        let mut table_codec = TableCodec::default();
        let table_state = build_t1_table()?;
        let scala_functions = Default::default();
        let table_functions = Default::default();
        let build_view = |name: &str, sql: &str| -> Result<(View, PlanArena<'_>), DatabaseError> {
            let mut plan_arena = PlanArena::new(&table_state.table_arena);
            let mut plan = table_state.plan_with_arena(sql, &mut plan_arena)?;
            let schema = plan.output_schema(&mut plan_arena).clone();
            Ok((
                View {
                    name: name.to_string().into(),
                    plan: Box::new(plan),
                    schema,
                },
                plan_arena,
            ))
        };
        // Subquery
        {
            println!("==== Subquery");
            let (view, mut plan_arena) = build_view(
                "view_subquery",
                "select * from t1 where c1 in (select c1 from t1 where c1 > 1)",
            )?;
            println!("{:#?}", view.plan);
            let transaction = table_state.storage.transaction()?;
            let mut decode_arena = PlanArena::new(&table_state.table_arena);
            let decoded = table_codec.with_view_value(
                view.name.as_ref(),
                &view,
                &plan_arena,
                |_, value| {
                    TableCodec::decode_view(
                        value,
                        (&transaction, &table_state.table_cache),
                        &scala_functions,
                        &table_functions,
                        &mut decode_arena,
                    )
                },
            )?;

            assert_eq!(
                normalize_explain(view.plan.explain(&mut plan_arena, 0)),
                normalize_explain(decoded.plan.explain(&mut decode_arena, 0))
            );
            assert_eq!(view.schema.len(), decoded.schema.len());
        }
        // No Join
        {
            println!("==== No Join");
            let (view, mut plan_arena) =
                build_view("view_filter", "select * from t1 where c1 > 1")?;
            let transaction = table_state.storage.transaction()?;
            let mut decode_arena = PlanArena::new(&table_state.table_arena);
            let decoded = table_codec.with_view_value(
                view.name.as_ref(),
                &view,
                &plan_arena,
                |_, value| {
                    TableCodec::decode_view(
                        value,
                        (&transaction, &table_state.table_cache),
                        &scala_functions,
                        &table_functions,
                        &mut decode_arena,
                    )
                },
            )?;

            assert_eq!(
                normalize_explain(view.plan.explain(&mut plan_arena, 0)),
                normalize_explain(decoded.plan.explain(&mut decode_arena, 0))
            );
            assert_eq!(view.schema.len(), decoded.schema.len());
        }
        // Join
        {
            println!("==== Join");
            let (view, mut plan_arena) =
                build_view("view_join", "select * from t1 left join t2 on c1 = c3")?;
            let transaction = table_state.storage.transaction()?;
            let mut decode_arena = PlanArena::new(&table_state.table_arena);
            let decoded = table_codec.with_view_value(
                view.name.as_ref(),
                &view,
                &plan_arena,
                |_, value| {
                    TableCodec::decode_view(
                        value,
                        (&transaction, &table_state.table_cache),
                        &scala_functions,
                        &table_functions,
                        &mut decode_arena,
                    )
                },
            )?;

            assert_eq!(
                normalize_explain(view.plan.explain(&mut plan_arena, 0)),
                normalize_explain(decoded.plan.explain(&mut decode_arena, 0))
            );
            assert_eq!(view.schema.len(), decoded.schema.len());
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_table_codec_column_bound() {
        let mut table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |col_id: usize, table_name: &str| {
            let mut table_codec = TableCodec::default();
            let mut col = ColumnCatalog::new(
                "".to_string(),
                false,
                ColumnDesc::new(LogicalType::SqlNull, None, false, None).unwrap(),
            );

            col.summary_mut().relation = ColumnRelation::Table {
                column_id: col_id as ColumnId,
                table_name: table_name.to_string().into(),
                is_temp: false,
            };

            let table_arena = TableArenaCell::default();
            let plan_arena = PlanArena::new(&table_arena);
            table_codec
                .with_column(&col, false, &plan_arena, |key, _| {
                    Ok::<_, DatabaseError>(key.to_vec())
                })
                .unwrap()
        };

        set.insert(op(0, "T0"));
        set.insert(op(1, "T0"));
        set.insert(op(2, "T0"));

        set.insert(op(0, "T1"));
        set.insert(op(1, "T1"));
        set.insert(op(2, "T1"));

        set.insert(op(0, "T2"));
        set.insert(op(0, "T2"));
        set.insert(op(0, "T2"));

        let (min, max) = table_codec
            .with_columns_bound("T1", |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })
            .unwrap();

        let vec = set
            .range::<[u8], _>((
                Bound::Included(min.as_slice()),
                Bound::Included(max.as_slice()),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(0, "T1"));
        assert_eq!(vec[1], &op(1, "T1"));
        assert_eq!(vec[2], &op(2, "T1"));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_table_codec_index_meta_bound() {
        let mut table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |index_id: usize, table_name: &str| {
            let mut table_codec = TableCodec::default();
            let index_meta = IndexMeta {
                id: index_id as u32,
                column_ids: vec![],
                table_name: table_name.to_string().into(),
                pk_ty: LogicalType::Integer,
                value_ty: LogicalType::Integer,
                name: format!("{index_id}_index"),
                ty: IndexType::PrimaryKey { is_multiple: false },
            };

            let table_arena = TableArenaCell::default();
            let plan_arena = PlanArena::new(&table_arena);
            table_codec
                .with_index_meta(table_name, index_meta.id, None, &plan_arena, |key, _| {
                    Ok::<_, DatabaseError>(key.to_vec())
                })
                .unwrap()
        };

        set.insert(op(0, "T0"));
        set.insert(op(1, "T0"));
        set.insert(op(2, "T0"));

        set.insert(op(0, "T1"));
        set.insert(op(1, "T1"));
        set.insert(op(2, "T1"));

        set.insert(op(0, "T2"));
        set.insert(op(1, "T2"));
        set.insert(op(2, "T2"));

        let (min, max) = table_codec
            .with_index_meta_bound("T1", |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })
            .unwrap();

        let vec = set
            .range::<[u8], _>((
                Bound::Included(min.as_slice()),
                Bound::Included(max.as_slice()),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(0, "T1"));
        assert_eq!(vec[1], &op(1, "T1"));
        assert_eq!(vec[2], &op(2, "T1"));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_table_codec_index_bound() {
        let mut table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let column = ColumnCatalog::new(
            "".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
        );
        let table_arena = TableArenaCell::default();
        let table_catalog = TableCatalog::new(
            "T0".to_string().into(),
            vec![column],
            table_arena.borrow_mut(),
        )
        .unwrap();

        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let mut table_codec = TableCodec::default();
            let value = Arc::new(value);
            let index = Index::new(
                index_id as u32,
                &value,
                IndexType::PrimaryKey { is_multiple: false },
            );

            table_codec
                .with_index(table_name, &index, None, |key, _| {
                    Ok::<_, DatabaseError>(key.to_vec())
                })
                .unwrap()
        };

        set.insert(op(DataValue::Int32(0), 0, &table_catalog.name));
        set.insert(op(DataValue::Int32(1), 0, &table_catalog.name));
        set.insert(op(DataValue::Int32(2), 0, &table_catalog.name));

        set.insert(op(DataValue::Int32(0), 1, &table_catalog.name));
        set.insert(op(DataValue::Int32(1), 1, &table_catalog.name));
        set.insert(op(DataValue::Int32(2), 1, &table_catalog.name));

        set.insert(op(DataValue::Int32(0), 2, &table_catalog.name));
        set.insert(op(DataValue::Int32(1), 2, &table_catalog.name));
        set.insert(op(DataValue::Int32(2), 2, &table_catalog.name));

        let (min, max) = table_codec
            .with_index_bound(&table_catalog.name, 1, |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })
            .unwrap();

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(DataValue::Int32(0), 1, &table_catalog.name));
        assert_eq!(vec[1], &op(DataValue::Int32(1), 1, &table_catalog.name));
        assert_eq!(vec[2], &op(DataValue::Int32(2), 1, &table_catalog.name));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_table_codec_index_all_bound() {
        let mut table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let mut table_codec = TableCodec::default();
            let value = Arc::new(value);
            let index = Index::new(
                index_id as u32,
                &value,
                IndexType::PrimaryKey { is_multiple: false },
            );

            table_codec
                .with_index(table_name, &index, None, |key, _| {
                    Ok::<_, DatabaseError>(key.to_vec())
                })
                .unwrap()
        };

        set.insert(op(DataValue::Int32(0), 0, "T0"));
        set.insert(op(DataValue::Int32(1), 0, "T0"));
        set.insert(op(DataValue::Int32(2), 0, "T0"));

        set.insert(op(DataValue::Int32(0), 0, "T1"));
        set.insert(op(DataValue::Int32(1), 0, "T1"));
        set.insert(op(DataValue::Int32(2), 0, "T1"));

        set.insert(op(DataValue::Int32(0), 0, "T2"));
        set.insert(op(DataValue::Int32(1), 0, "T2"));
        set.insert(op(DataValue::Int32(2), 0, "T2"));

        let (min, max) = table_codec
            .with_all_index_bound("T1", |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })
            .unwrap();

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(DataValue::Int32(0), 0, "T1"));
        assert_eq!(vec[1], &op(DataValue::Int32(1), 0, "T1"));
        assert_eq!(vec[2], &op(DataValue::Int32(2), 0, "T1"));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_table_codec_tuple_bound() {
        let mut table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |tuple_id: DataValue, table_name: &str| {
            let mut table_codec = TableCodec::default();
            table_codec
                .with_tuple(table_name, &Arc::new(tuple_id), None, |key, _| {
                    Ok::<_, DatabaseError>(key.to_vec())
                })
                .unwrap()
        };

        set.insert(op(DataValue::Int32(0), "T0"));
        set.insert(op(DataValue::Int32(1), "T0"));
        set.insert(op(DataValue::Int32(2), "T0"));

        set.insert(op(DataValue::Int32(0), "T1"));
        set.insert(op(DataValue::Int32(1), "T1"));
        set.insert(op(DataValue::Int32(2), "T1"));

        set.insert(op(DataValue::Int32(0), "T2"));
        set.insert(op(DataValue::Int32(1), "T2"));
        set.insert(op(DataValue::Int32(2), "T2"));

        let (min, max) = table_codec
            .with_tuple_bound("T1", |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })
            .unwrap();

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(DataValue::Int32(0), "T1"));
        assert_eq!(vec[1], &op(DataValue::Int32(1), "T1"));
        assert_eq!(vec[2], &op(DataValue::Int32(2), "T1"));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_root_codec_name_bound() {
        let mut table_codec = TableCodec::default();
        let mut set: BTreeSet<Bytes> = BTreeSet::new();
        let op = |table_name: &str| {
            let mut table_codec = TableCodec::default();
            let table_arena = TableArenaCell::default();
            let plan_arena = PlanArena::new(&table_arena);
            table_codec
                .with_root_table(table_name, None, &plan_arena, |key, _| {
                    Ok::<_, DatabaseError>(key.to_vec())
                })
                .unwrap()
        };

        let value_0 = Bytes::from([b'A'].as_slice());
        let value_1 = Bytes::from([b'Z'].as_slice());

        set.insert(value_0);
        set.insert(value_1);
        set.insert(op("T0"));
        set.insert(op("T1"));
        set.insert(op("T2"));

        let (min, max) = table_codec
            .with_root_table_bound(|min, max| Ok::<_, DatabaseError>((min.to_vec(), max.to_vec())))
            .unwrap();

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec[0], &op("T0"));
        assert_eq!(vec[1], &op("T2"));
        assert_eq!(vec[2], &op("T1"));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_view_codec_name_bound() {
        let mut table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |view_name: &str| {
            let mut table_codec = TableCodec::default();
            table_codec
                .with_view(view_name, |key, _| Ok::<_, DatabaseError>(key.to_vec()))
                .unwrap()
        };

        let value_0 = Bytes::from([b'A'].as_slice());
        let value_1 = Bytes::from([b'Z'].as_slice());

        set.insert(value_0);
        set.insert(value_1);

        set.insert(op("V0"));
        set.insert(op("V1"));
        set.insert(op("V2"));

        let (min, max) = table_codec
            .with_view_bound(|min, max| Ok::<_, DatabaseError>((min.to_vec(), max.to_vec())))
            .unwrap();

        let vec = set
            .range::<Bytes, (Bound<&Bytes>, Bound<&Bytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec[0], &op("V2"));
        assert_eq!(vec[1], &op("V1"));
        assert_eq!(vec[2], &op("V0"));
    }
}
