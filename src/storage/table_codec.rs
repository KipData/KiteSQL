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
use crate::catalog::{ColumnRef, ColumnRelation, TableMeta};
use crate::errors::DatabaseError;
use crate::optimizer::core::cm_sketch::{CountMinSketchMeta, CountMinSketchPage};
use crate::optimizer::core::histogram::Bucket;
use crate::optimizer::core::statistics_meta::StatisticsMetaRoot;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::index::{Index, IndexId, IndexMeta, IndexType, INDEX_ID_LEN};
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::{DataValue, TupleMappingRef};
use crate::types::LogicalType;
use bumpalo::Bump;
use siphasher::sip::SipHasher;
use std::cell::RefCell;
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

#[derive(Default)]
pub struct TableCodec {
    arena: Bump,
    key_buffer: RefCell<KeyBuffer>,
}

#[derive(Default)]
struct KeyBuffer {
    lower: Bytes,
    upper: Bytes,
    cached_table_name: String,
    cached_table_hash: [u8; TABLE_NAME_HASH_LEN],
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
}

impl StatisticsCodecType {
    fn tag(self) -> u8 {
        match self {
            StatisticsCodecType::Root => b'0',
            StatisticsCodecType::SketchMeta => b'1',
            StatisticsCodecType::SketchPage => b'2',
            StatisticsCodecType::Bucket => b'3',
        }
    }

    fn from_tag(tag: u8) -> Result<Self, DatabaseError> {
        match tag {
            b'0' => Ok(StatisticsCodecType::Root),
            b'1' => Ok(StatisticsCodecType::SketchMeta),
            b'2' => Ok(StatisticsCodecType::SketchPage),
            b'3' => Ok(StatisticsCodecType::Bucket),
            _ => Err(DatabaseError::InvalidValue(format!(
                "invalid statistics codec tag: {tag}"
            ))),
        }
    }
}

impl TableCodec {
    #[inline]
    pub fn arena(&self) -> &Bump {
        &self.arena
    }

    fn hash_bytes(table_name: &str) -> [u8; 8] {
        let mut hasher = SipHasher::new();
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
        out.clear();
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
        out.clear();
        out.extend_from_slice(prefix);
        out.push(bound);
    }

    fn with_table_hash<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&mut KeyBuffer, [u8; TABLE_NAME_HASH_LEN]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut key_buffer = self.key_buffer.borrow_mut();
        let table_hash = if key_buffer.cached_table_name != table_name {
            key_buffer.cached_table_name.clear();
            key_buffer.cached_table_name.push_str(table_name);
            key_buffer.cached_table_hash = Self::hash_bytes(table_name);
            key_buffer.cached_table_hash
        } else {
            key_buffer.cached_table_hash
        };

        f(&mut key_buffer, table_hash)
    }

    /// Key: `{TableName}{TUPLE_TAG}{BOUND_MIN_TAG}{RowID}`.
    pub fn with_tuple_key<R>(
        &self,
        table_name: &str,
        tuple_id: &TupleId,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        Self::check_primary_key(tuple_id, 0)?;

        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Tuple, table_hash);
            lower.push(BOUND_MIN_TAG);
            tuple_id.memcomparable_encode(lower)?;

            f(lower.as_slice())
        })
    }

    /// Range bounds covering all tuple keys for a table.
    pub fn with_tuple_bound<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Tuple, table_hash);
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
            Self::write_key_prefix(upper, CodecType::Tuple, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Key: `{TableName}{INDEX_META_TAG}{BOUND_MIN_TAG}{IndexID}`.
    pub fn with_index_meta_key<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::IndexMeta, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());

            f(lower.as_slice())
        })
    }

    /// Range bounds covering all index metadata for a table.
    pub fn with_index_meta_bound<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::IndexMeta, table_hash);
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
            Self::write_key_prefix(upper, CodecType::IndexMeta, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering a single secondary index.
    pub fn with_index_bound<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Index, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
            Self::write_key_prefix(upper, CodecType::Index, table_hash);
            upper.push(BOUND_MIN_TAG);
            upper.extend_from_slice(&index_id.to_le_bytes());
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering all secondary indexes for a table.
    pub fn with_all_index_bound<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Index, table_hash);
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
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
    pub fn with_index_key<R>(
        &self,
        table_name: &str,
        index: &Index,
        tuple_id: Option<&TupleId>,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
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

            f(lower.as_slice())
        })
    }

    /// Key: `{TableName}{COLUMN_TAG}{BOUND_MIN_TAG}{ColumnId}`.
    pub fn with_column_key<R>(
        &self,
        col: &ColumnRef,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        if let ColumnRelation::Table {
            column_id,
            table_name,
            is_temp: false,
        } = &col.summary().relation
        {
            self.with_table_hash(table_name, |key_buffer, table_hash| {
                let lower = &mut key_buffer.lower;
                Self::write_key_prefix(lower, CodecType::Column, table_hash);
                lower.push(BOUND_MIN_TAG);
                lower.extend_from_slice(&column_id.to_bytes());

                f(lower.as_slice())
            })
        } else {
            Err(DatabaseError::invalid_column(
                "column does not belong to table".to_string(),
            ))
        }
    }

    /// Range bounds covering all column metadata for a table.
    pub fn with_columns_bound<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Column, table_hash);
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
            Self::write_key_prefix(upper, CodecType::Column, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds spanning a table's `Column` and `IndexMeta` metadata.
    pub fn with_table_bound<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Column, table_hash);
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
            Self::write_key_prefix(upper, CodecType::IndexMeta, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering all statistics keys for a table.
    pub fn with_statistics_bound<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);

            let upper = &mut key_buffer.upper;
            Self::write_key_prefix(upper, CodecType::Statistics, table_hash);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Range bounds covering all statistics keys for one index.
    pub fn with_statistics_index_bound<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());

            let upper = &mut key_buffer.upper;
            upper.clear();
            upper.extend_from_slice(lower);
            upper.push(BOUND_MAX_TAG);

            f(lower.as_slice(), upper.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{ROOT_TAG}`.
    pub fn with_statistics_meta_key<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::Root.tag());

            f(lower.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{SKETCH_META_TAG}`.
    pub fn with_statistics_sketch_meta_key<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::SketchMeta.tag());

            f(lower.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{SKETCH_PAGE_TAG}{BOUND_MIN_TAG}{ROW_ID}{BOUND_MIN_TAG}{PAGE_ID}`.
    pub fn with_statistics_sketch_page_key<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        sketch_page: &CountMinSketchPage,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::SketchPage.tag());
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&(sketch_page.row_idx() as u32).to_be_bytes());
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&(sketch_page.page_idx() as u32).to_be_bytes());

            f(lower.as_slice())
        })
    }

    /// Key: `{TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}{BUCKET_TAG}{BOUND_MIN_TAG}{ORDINAL}`.
    pub fn with_statistics_bucket_key<R>(
        &self,
        table_name: &str,
        index_id: IndexId,
        ordinal: u32,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Statistics, table_hash);
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&index_id.to_le_bytes());
            lower.push(StatisticsCodecType::Bucket.tag());
            lower.push(BOUND_MIN_TAG);
            lower.extend_from_slice(&ordinal.to_be_bytes());

            f(lower.as_slice())
        })
    }

    /// Key: `View{BOUND_MIN_TAG}{ViewNameHash}`.
    pub fn with_view_key<R>(
        &self,
        view_name: &str,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(view_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::View, table_hash);

            f(lower.as_slice())
        })
    }

    /// Range bounds covering all view definitions.
    pub fn with_view_bound<R>(
        &self,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut key_buffer = self.key_buffer.borrow_mut();
        Self::write_global_bound_prefix(
            &mut key_buffer.lower,
            VIEW_BYTES.as_slice(),
            BOUND_MIN_TAG,
        );
        Self::write_global_bound_prefix(
            &mut key_buffer.upper,
            VIEW_BYTES.as_slice(),
            BOUND_MAX_TAG,
        );

        f(key_buffer.lower.as_slice(), key_buffer.upper.as_slice())
    }

    /// Key: `Root{BOUND_MIN_TAG}{TableNameHash}`.
    pub fn with_root_table_key<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Root, table_hash);

            f(lower.as_slice())
        })
    }

    /// Range bounds covering all root table metadata.
    pub fn with_root_table_bound<R>(
        &self,
        f: impl FnOnce(&[u8], &[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut key_buffer = self.key_buffer.borrow_mut();
        Self::write_global_bound_prefix(
            &mut key_buffer.lower,
            ROOT_BYTES.as_slice(),
            BOUND_MIN_TAG,
        );
        Self::write_global_bound_prefix(
            &mut key_buffer.upper,
            ROOT_BYTES.as_slice(),
            BOUND_MAX_TAG,
        );

        f(key_buffer.lower.as_slice(), key_buffer.upper.as_slice())
    }

    /// Key: `Hash{BOUND_MIN_TAG}{TableNameHash}`.
    pub fn with_table_hash_key<R>(
        &self,
        table_name: &str,
        f: impl FnOnce(&[u8]) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        self.with_table_hash(table_name, |key_buffer, table_hash| {
            let lower = &mut key_buffer.lower;
            Self::write_key_prefix(lower, CodecType::Hash, table_hash);

            f(lower.as_slice())
        })
    }
    pub fn decode_tuple_key(bytes: &[u8], pk_ty: &LogicalType) -> Result<TupleId, DatabaseError> {
        DataValue::memcomparable_decode(&mut Cursor::new(&bytes[TUPLE_KEY_PREFIX_LEN..]), pk_ty)
    }

    #[inline]
    pub fn decode_tuple(
        deserializers: &[TupleValueSerializableImpl],
        tuple_id: Option<TupleId>,
        bytes: &[u8],
        values_len: usize,
        total_len: usize,
    ) -> Result<Tuple, DatabaseError> {
        Tuple::deserialize_from(deserializers, tuple_id, bytes, values_len, total_len)
    }

    pub fn encode_index_meta_value(
        &self,
        index_meta: &IndexMeta,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut value_bytes = BumpBytes::new_in(&self.arena);
        index_meta.encode(&mut value_bytes, true, &mut ReferenceTables::new())?;
        Ok(value_bytes)
    }

    pub fn decode_index_meta<T: Transaction>(bytes: &[u8]) -> Result<IndexMeta, DatabaseError> {
        IndexMeta::decode::<T, _>(&mut Cursor::new(bytes), None, &EMPTY_REFERENCE_TABLES)
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
        Ok(bincode::deserialize_from(&mut Cursor::new(bytes))?)
    }

    pub fn encode_column_value(
        &self,
        col: &ColumnRef,
        reference_tables: &mut ReferenceTables,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut column_bytes = BumpBytes::new_in(&self.arena);
        col.encode(&mut column_bytes, true, reference_tables)?;
        Ok(column_bytes)
    }

    pub fn decode_column<T: Transaction, R: Read>(
        reader: &mut R,
        reference_tables: &ReferenceTables,
    ) -> Result<ColumnRef, DatabaseError> {
        // `TableCache` is not theoretically used in `table_collect` because `ColumnCatalog` should not depend on other Column
        ColumnRef::decode::<T, R>(reader, None, reference_tables)
    }

    pub fn encode_statistics_meta_value(
        &self,
        statistics_meta: &StatisticsMetaRoot,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut value = BumpBytes::new_in(&self.arena);
        statistics_meta.encode(&mut value, true, &mut ReferenceTables::new())?;
        Ok(value)
    }

    pub fn decode_statistics_meta<T: Transaction>(
        bytes: &[u8],
    ) -> Result<StatisticsMetaRoot, DatabaseError> {
        StatisticsMetaRoot::decode::<T, _>(&mut Cursor::new(bytes), None, &ReferenceTables::new())
    }

    pub fn encode_statistics_sketch_meta_value(
        &self,
        sketch_meta: &CountMinSketchMeta,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut value = BumpBytes::new_in(&self.arena);
        sketch_meta.encode(&mut value, true, &mut ReferenceTables::new())?;
        Ok(value)
    }

    pub fn decode_statistics_sketch_meta<T: Transaction>(
        bytes: &[u8],
    ) -> Result<CountMinSketchMeta, DatabaseError> {
        CountMinSketchMeta::decode::<T, _>(&mut Cursor::new(bytes), None, &ReferenceTables::new())
    }

    pub fn encode_statistics_sketch_page_value(
        &self,
        sketch_page: &CountMinSketchPage,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut value = BumpBytes::new_in(&self.arena);
        sketch_page.encode(&mut value, true, &mut ReferenceTables::new())?;
        Ok(value)
    }

    pub fn decode_statistics_sketch_page<T: Transaction>(
        bytes: &[u8],
    ) -> Result<CountMinSketchPage, DatabaseError> {
        CountMinSketchPage::decode::<T, _>(&mut Cursor::new(bytes), None, &ReferenceTables::new())
    }

    pub fn encode_statistics_bucket_value(
        &self,
        bucket: &Bucket,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut value = BumpBytes::new_in(&self.arena);
        bucket.encode(&mut value, true, &mut ReferenceTables::new())?;
        Ok(value)
    }

    pub(crate) fn decode_statistics_codec_type(
        &self,
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

    pub fn decode_statistics_bucket<T: Transaction>(bytes: &[u8]) -> Result<Bucket, DatabaseError> {
        Bucket::decode::<T, _>(&mut Cursor::new(bytes), None, &ReferenceTables::new())
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

    pub fn encode_view_value(&self, view: &View) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut reference_tables = ReferenceTables::new();
        let mut bytes = BumpBytes::new_in(&self.arena);
        bytes.resize(4, 0u8);

        let reference_tables_pos = {
            view.encode(&mut bytes, false, &mut reference_tables)?;
            let pos = bytes.len();
            reference_tables.to_raw(&mut bytes)?;
            pos
        };
        bytes[..4].copy_from_slice(&(reference_tables_pos as u32).to_le_bytes());

        Ok(bytes)
    }

    pub fn decode_view<T: Transaction>(
        bytes: &[u8],
        drive: (&T, &TableCache),
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

        View::decode(&mut cursor, Some(drive), &reference_tables)
    }

    pub fn encode_root_table_value(
        &self,
        meta: &TableMeta,
    ) -> Result<BumpBytes<'_>, DatabaseError> {
        let mut meta_bytes = BumpBytes::new_in(&self.arena);
        meta.encode(&mut meta_bytes, true, &mut ReferenceTables::new())?;
        Ok(meta_bytes)
    }

    pub fn decode_root_table<T: Transaction>(bytes: &[u8]) -> Result<TableMeta, DatabaseError> {
        let mut bytes = Cursor::new(bytes);

        TableMeta::decode::<T, _>(&mut bytes, None, &EMPTY_REFERENCE_TABLES)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::catalog::view::View;
    use crate::catalog::{
        ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, TableCatalog, TableMeta,
    };
    use crate::errors::DatabaseError;
    use crate::optimizer::core::histogram::HistogramBuilder;
    use crate::optimizer::core::statistics_meta::StatisticsMeta;
    use crate::serdes::ReferenceTables;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::table_codec::{Bytes, TableCodec};
    use crate::storage::Storage;
    use crate::types::index::{Index, IndexMeta, IndexType};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use itertools::Itertools;
    use rust_decimal::Decimal;
    use std::collections::BTreeSet;
    use std::io::Cursor;
    use std::ops::Bound;
    use std::sync::Arc;
    use ulid::Ulid;

    fn build_table_codec() -> TableCatalog {
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
        TableCatalog::new("t1".to_string().into(), columns).unwrap()
    }

    #[test]
    fn test_table_codec_tuple() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let table_catalog = build_table_codec();

        let mut tuple = Tuple::new(
            Some(DataValue::Int32(0)),
            vec![DataValue::Int32(0), DataValue::Decimal(Decimal::new(1, 0))],
        );
        let bytes = tuple.serialize_to(
            &[
                LogicalType::Integer.serializable(),
                LogicalType::Decimal(None, None).serializable(),
            ],
            table_codec.arena(),
        )?;
        let deserializers = table_catalog
            .columns()
            .map(|column| column.datatype().serializable())
            .collect_vec();

        tuple.pk = None;
        assert_eq!(
            TableCodec::decode_tuple(&deserializers, None, &bytes, deserializers.len(), 2,)?,
            tuple
        );

        Ok(())
    }

    #[test]
    fn test_root_catalog() {
        let table_codec = TableCodec::default();
        let table_catalog = build_table_codec();
        let bytes = table_codec
            .encode_root_table_value(&TableMeta {
                table_name: table_catalog.name.clone(),
            })
            .unwrap();

        let table_meta = TableCodec::decode_root_table::<RocksTransaction>(&bytes).unwrap();

        assert_eq!(table_meta.table_name.as_ref(), table_catalog.name.as_ref());
    }

    #[test]
    fn test_table_codec_statistics_meta() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![Ulid::new()],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "pk_c1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };
        let mut builder = HistogramBuilder::new(&index_meta, Some(8));

        for value in 0..4 {
            builder.append(&DataValue::Int32(value))?;
        }
        let (histogram, sketch) = builder.build(2)?;
        let (root, buckets) = StatisticsMeta::new(histogram).into_parts();

        let root_bytes = table_codec.encode_statistics_meta_value(&root)?;
        let decoded_root = TableCodec::decode_statistics_meta::<RocksTransaction>(&root_bytes)?;
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
        let sketch_meta_bytes = table_codec.encode_statistics_sketch_meta_value(&sketch_meta)?;
        let decoded_sketch_meta =
            TableCodec::decode_statistics_sketch_meta::<RocksTransaction>(&sketch_meta_bytes)?;
        assert_eq!(decoded_sketch_meta.width(), sketch_meta.width());
        assert_eq!(decoded_sketch_meta.k_num(), sketch_meta.k_num());

        let first_sketch_page = sketch_pages.next().unwrap();
        let sketch_page_bytes =
            table_codec.encode_statistics_sketch_page_value(&first_sketch_page)?;
        let decoded_sketch_page =
            TableCodec::decode_statistics_sketch_page::<RocksTransaction>(&sketch_page_bytes)?;
        assert_eq!(decoded_sketch_page.counters(), first_sketch_page.counters());

        let bucket0_key = table_codec
            .with_statistics_bucket_key("t1", 0, 0, |key| Ok::<_, DatabaseError>(key.to_vec()))?;
        let bucket1_key = table_codec
            .with_statistics_bucket_key("t1", 0, 1, |key| Ok::<_, DatabaseError>(key.to_vec()))?;
        assert!(bucket0_key < bucket1_key);

        let (bucket0_min, bucket0_max) =
            table_codec.with_statistics_index_bound("t1", 0, |min, max| {
                Ok::<_, DatabaseError>((min.to_vec(), max.to_vec()))
            })?;
        assert!(bucket0_key.as_slice() >= bucket0_min.as_slice());
        assert!(bucket1_key.as_slice() <= bucket0_max.as_slice());

        let bucket_bytes = table_codec.encode_statistics_bucket_value(&buckets[0])?;
        let decoded_bucket =
            TableCodec::decode_statistics_bucket::<RocksTransaction>(&bucket_bytes)?;
        assert_eq!(decoded_bucket, buckets[0]);
        assert_eq!(
            TableCodec::decode_statistics_bucket_ordinal(&bucket0_key)?,
            0
        );
        assert_eq!(
            TableCodec::decode_statistics_bucket_ordinal(&bucket1_key)?,
            1
        );

        Ok(())
    }

    #[test]
    fn test_table_codec_index_meta() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![Ulid::new()],
            table_name: "t1".to_string().into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "index_1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };
        let bytes = table_codec.encode_index_meta_value(&index_meta)?;

        assert_eq!(
            TableCodec::decode_index_meta::<RocksTransaction>(&bytes)?,
            index_meta
        );

        Ok(())
    }

    #[test]
    fn test_table_codec_index() -> Result<(), DatabaseError> {
        let tuple_id = DataValue::Int32(0);
        let bytes = bincode::serialize(&tuple_id)?;

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
            column_id: Ulid::new(),
            table_name: "t1".to_string().into(),
            is_temp: false,
        };
        let col = ColumnRef::from(col);

        let mut reference_tables = ReferenceTables::new();

        let table_codec = TableCodec::default();
        let bytes = table_codec
            .encode_column_value(&col, &mut reference_tables)
            .unwrap();
        let mut cursor = Cursor::new(bytes);
        let decode_col =
            TableCodec::decode_column::<RocksTransaction, _>(&mut cursor, &reference_tables)?;

        assert_eq!(decode_col, col);

        Ok(())
    }

    #[test]
    fn test_table_codec_view() -> Result<(), DatabaseError> {
        let table_codec = TableCodec::default();
        let table_state = build_t1_table()?;
        // Subquery
        {
            println!("==== Subquery");
            let plan = table_state
                .plan("select * from t1 where c1 in (select c1 from t1 where c1 > 1)")?;
            println!("{plan:#?}");
            let view = View {
                name: "view_subquery".to_string().into(),
                plan: Box::new(plan),
            };
            let bytes = table_codec.encode_view_value(&view)?;
            let transaction = table_state.storage.transaction()?;

            assert_eq!(
                view,
                TableCodec::decode_view(&bytes, (&transaction, &table_state.table_cache))?
            );
        }
        // No Join
        {
            println!("==== No Join");
            let plan = table_state.plan("select * from t1 where c1 > 1")?;
            let view = View {
                name: "view_filter".to_string().into(),
                plan: Box::new(plan),
            };
            let bytes = table_codec.encode_view_value(&view)?;
            let transaction = table_state.storage.transaction()?;

            assert_eq!(
                view,
                TableCodec::decode_view(&bytes, (&transaction, &table_state.table_cache))?
            );
        }
        // Join
        {
            println!("==== Join");
            let plan = table_state.plan("select * from t1 left join t2 on c1 = c3")?;
            let view = View {
                name: "view_join".to_string().into(),
                plan: Box::new(plan),
            };
            let bytes = table_codec.encode_view_value(&view)?;
            let transaction = table_state.storage.transaction()?;

            assert_eq!(
                view,
                TableCodec::decode_view(&bytes, (&transaction, &table_state.table_cache))?
            );
        }

        Ok(())
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_table_codec_column_bound() {
        let table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |col_id: usize, table_name: &str| {
            let mut col = ColumnCatalog::new(
                "".to_string(),
                false,
                ColumnDesc::new(LogicalType::SqlNull, None, false, None).unwrap(),
            );

            col.summary_mut().relation = ColumnRelation::Table {
                column_id: Ulid::from(col_id as u128),
                table_name: table_name.to_string().into(),
                is_temp: false,
            };

            table_codec
                .with_column_key(&ColumnRef::from(col), |key| {
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
        let table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |index_id: usize, table_name: &str| {
            let index_meta = IndexMeta {
                id: index_id as u32,
                column_ids: vec![],
                table_name: table_name.to_string().into(),
                pk_ty: LogicalType::Integer,
                value_ty: LogicalType::Integer,
                name: format!("{index_id}_index"),
                ty: IndexType::PrimaryKey { is_multiple: false },
            };

            table_codec
                .with_index_meta_key(table_name, index_meta.id, |key| {
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
        let table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let column = ColumnCatalog::new(
            "".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
        );
        let table_catalog = TableCatalog::new("T0".to_string().into(), vec![column]).unwrap();

        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let value = Arc::new(value);
            let index = Index::new(
                index_id as u32,
                &value,
                IndexType::PrimaryKey { is_multiple: false },
            );

            table_codec
                .with_index_key(table_name, &index, None, |key| {
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
        let table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let value = Arc::new(value);
            let index = Index::new(
                index_id as u32,
                &value,
                IndexType::PrimaryKey { is_multiple: false },
            );

            table_codec
                .with_index_key(table_name, &index, None, |key| {
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
        let table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |tuple_id: DataValue, table_name: &str| {
            table_codec
                .with_tuple_key(table_name, &Arc::new(tuple_id), |key| {
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
        let table_codec = TableCodec::default();
        let mut set: BTreeSet<Bytes> = BTreeSet::new();
        let op = |table_name: &str| {
            table_codec
                .with_root_table_key(table_name, |key| Ok::<_, DatabaseError>(key.to_vec()))
                .unwrap()
        };

        let mut value_0 = Bytes::new();
        value_0.push(b'A');
        let mut value_1 = Bytes::new();
        value_1.push(b'Z');

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
        assert_eq!(vec[1], &op("T1"));
        assert_eq!(vec[2], &op("T2"));
    }

    #[test]
    #[allow(clippy::mutable_key_type)]
    fn test_view_codec_name_bound() {
        let table_codec = TableCodec::default();
        let mut set = BTreeSet::new();
        let op = |view_name: &str| {
            table_codec
                .with_view_key(view_name, |key| Ok::<_, DatabaseError>(key.to_vec()))
                .unwrap()
        };

        let mut value_0 = Bytes::new();
        value_0.push(b'A');
        let mut value_1 = Bytes::new();
        value_1.push(b'Z');

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

        assert_eq!(vec[2], &op("V0"));
        assert_eq!(vec[0], &op("V1"));
        assert_eq!(vec[1], &op("V2"));
    }
}
