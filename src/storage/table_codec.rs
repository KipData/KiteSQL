use crate::catalog::view::View;
use crate::catalog::{ColumnRef, ColumnRelation, TableMeta};
use crate::errors::DatabaseError;
use crate::serdes::{ReferenceSerialization, ReferenceTables};
use crate::storage::{TableCache, Transaction};
use crate::types::index::{Index, IndexId, IndexMeta, IndexType};
use crate::types::tuple::{Schema, Tuple, TupleId};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use bumpalo::Bump;
use siphasher::sip::SipHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::LazyLock;

pub(crate) const BOUND_MIN_TAG: u8 = u8::MIN;
pub(crate) const BOUND_MAX_TAG: u8 = u8::MAX;

static ROOT_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| b"Root".to_vec());
static VIEW_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| b"View".to_vec());
static HASH_BYTES: LazyLock<Vec<u8>> = LazyLock::new(|| b"Hash".to_vec());
static EMPTY_REFERENCE_TABLES: LazyLock<ReferenceTables> = LazyLock::new(ReferenceTables::new);

pub type Bytes = Vec<u8>;
pub type BumpBytes<'bump> = bumpalo::collections::Vec<'bump, u8>;

#[derive(Default)]
pub struct TableCodec {
    arena: Bump,
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

impl TableCodec {
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
            return Err(DatabaseError::NotNull);
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
        ) {
            return Err(DatabaseError::InvalidType);
        }
        Ok(())
    }

    /// TableName + Type
    ///
    /// Tips:
    /// 1. Root & View & Hash full key = key_prefix
    /// 2. hash table name makes it 4 as a fixed length, and [prefix_extractor](https://github.com/facebook/rocksdb/wiki/Prefix-Seek#defining-a-prefix) can be enabled in rocksdb
    fn key_prefix(&self, ty: CodecType, name: &str) -> BumpBytes {
        let mut table_bytes = BumpBytes::new_in(&self.arena);
        table_bytes.extend_from_slice(Self::hash_bytes(name).as_slice());

        match ty {
            CodecType::Column => {
                table_bytes.push(b'0');
            }
            CodecType::IndexMeta => {
                table_bytes.push(b'1');
            }
            CodecType::Index => {
                table_bytes.push(b'3');
            }
            CodecType::Statistics => {
                table_bytes.push(b'4');
            }
            CodecType::Tuple => {
                table_bytes.push(b'8');
            }
            CodecType::Root => {
                let mut bytes = BumpBytes::new_in(&self.arena);

                bytes.extend_from_slice(&ROOT_BYTES);
                bytes.push(BOUND_MIN_TAG);
                bytes.extend_from_slice(&table_bytes);

                return bytes;
            }
            CodecType::View => {
                let mut bytes = BumpBytes::new_in(&self.arena);

                bytes.extend_from_slice(&VIEW_BYTES);
                bytes.push(BOUND_MIN_TAG);
                bytes.extend_from_slice(&table_bytes);

                return bytes;
            }
            CodecType::Hash => {
                let mut bytes = BumpBytes::new_in(&self.arena);

                bytes.extend_from_slice(&HASH_BYTES);
                bytes.push(BOUND_MIN_TAG);
                bytes.append(&mut table_bytes);
                bytes.extend_from_slice(&table_bytes);

                return bytes;
            }
        }

        table_bytes
    }

    pub fn tuple_bound(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = self.key_prefix(CodecType::Tuple, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn index_meta_bound(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = self.key_prefix(CodecType::IndexMeta, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn index_bound(
        &self,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        let op = |bound_id| -> Result<BumpBytes, DatabaseError> {
            let mut key_prefix = self.key_prefix(CodecType::Index, table_name);

            key_prefix.write_all(&[BOUND_MIN_TAG])?;
            key_prefix.write_all(&index_id.to_le_bytes()[..])?;
            key_prefix.write_all(&[bound_id])?;
            Ok(key_prefix)
        };

        Ok((op(BOUND_MIN_TAG)?, op(BOUND_MAX_TAG)?))
    }

    pub fn all_index_bound(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = self.key_prefix(CodecType::Index, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn root_table_bound(&self) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = BumpBytes::new_in(&self.arena);

            key_prefix.extend_from_slice(&ROOT_BYTES);
            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn table_bound(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        let mut column_prefix = self.key_prefix(CodecType::Column, table_name);
        column_prefix.push(BOUND_MIN_TAG);

        let mut index_prefix = self.key_prefix(CodecType::IndexMeta, table_name);
        index_prefix.push(BOUND_MAX_TAG);

        (column_prefix, index_prefix)
    }

    pub fn columns_bound(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = self.key_prefix(CodecType::Column, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn statistics_bound(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = self.key_prefix(CodecType::Statistics, table_name);

            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    pub fn view_bound(&self) -> (BumpBytes, BumpBytes) {
        let op = |bound_id| {
            let mut key_prefix = BumpBytes::new_in(&self.arena);

            key_prefix.extend_from_slice(&VIEW_BYTES);
            key_prefix.push(bound_id);
            key_prefix
        };

        (op(BOUND_MIN_TAG), op(BOUND_MAX_TAG))
    }

    /// Key: {TableName}{TUPLE_TAG}{BOUND_MIN_TAG}{RowID}(Sorted)
    /// Value: Tuple
    pub fn encode_tuple(
        &self,
        table_name: &str,
        tuple: &mut Tuple,
        types: &[LogicalType],
    ) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        let tuple_id = tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound)?;
        let key = self.encode_tuple_key(table_name, tuple_id)?;

        Ok((key, tuple.serialize_to(types, &self.arena)?))
    }

    pub fn encode_tuple_key(
        &self,
        table_name: &str,
        tuple_id: &TupleId,
    ) -> Result<BumpBytes, DatabaseError> {
        Self::check_primary_key(tuple_id, 0)?;

        let mut key_prefix = self.key_prefix(CodecType::Tuple, table_name);
        key_prefix.push(BOUND_MIN_TAG);

        tuple_id.memcomparable_encode(&mut key_prefix)?;

        Ok(key_prefix)
    }

    #[inline]
    pub fn decode_tuple(
        table_types: &[LogicalType],
        pk_indices: &[usize],
        projections: &[usize],
        schema: &Schema,
        bytes: &[u8],
        with_pk: bool,
    ) -> Result<Tuple, DatabaseError> {
        Tuple::deserialize_from(table_types, pk_indices, projections, schema, bytes, with_pk)
    }

    pub fn encode_index_meta_key(
        &self,
        table_name: &str,
        index_id: IndexId,
    ) -> Result<BumpBytes, DatabaseError> {
        let mut key_prefix = self.key_prefix(CodecType::IndexMeta, table_name);

        key_prefix.write_all(&[BOUND_MIN_TAG])?;
        key_prefix.write_all(&index_id.to_le_bytes()[..])?;
        Ok(key_prefix)
    }

    /// Key: {TableName}{INDEX_META_TAG}{BOUND_MIN_TAG}{IndexID}
    /// Value: IndexMeta
    pub fn encode_index_meta(
        &self,
        table_name: &str,
        index_meta: &IndexMeta,
    ) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        let key_bytes = self.encode_index_meta_key(table_name, index_meta.id)?;

        let mut value_bytes = BumpBytes::new_in(&self.arena);
        index_meta.encode(&mut value_bytes, true, &mut ReferenceTables::new())?;

        Ok((key_bytes, value_bytes))
    }

    pub fn decode_index_meta<T: Transaction>(bytes: &[u8]) -> Result<IndexMeta, DatabaseError> {
        IndexMeta::decode::<T, _>(&mut Cursor::new(bytes), None, &EMPTY_REFERENCE_TABLES)
    }

    /// NonUnique Index:
    /// Key: {TableName}{INDEX_TAG}{BOUND_MIN_TAG}{IndexID}{BOUND_MIN_TAG}{DataValue1}{BOUND_MIN_TAG}{DataValue2} .. {TupleId}
    /// Value: TupleID
    ///
    /// Unique Index:
    /// Key: {TableName}{INDEX_TAG}{BOUND_MIN_TAG}{IndexID}{BOUND_MIN_TAG}{DataValue}
    /// Value: TupleID
    ///
    /// Tips: The unique index has only one ColumnID and one corresponding DataValue,
    /// so it can be positioned directly.
    pub fn encode_index(
        &self,
        name: &str,
        index: &Index,
        tuple_id: &TupleId,
    ) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        let key = self.encode_index_key(name, index, Some(tuple_id))?;
        let mut bytes = BumpBytes::new_in(&self.arena);

        bincode::serialize_into(&mut bytes, tuple_id)?;

        Ok((key, bytes))
    }

    pub fn encode_index_bound_key(
        &self,
        name: &str,
        index: &Index,
        is_upper: bool,
    ) -> Result<BumpBytes, DatabaseError> {
        let mut key_prefix = self.key_prefix(CodecType::Index, name);
        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.extend_from_slice(&index.id.to_le_bytes());
        key_prefix.push(BOUND_MIN_TAG);

        index.value.memcomparable_encode(&mut key_prefix)?;
        if is_upper {
            key_prefix.push(BOUND_MAX_TAG)
        }

        Ok(key_prefix)
    }

    pub fn encode_index_key(
        &self,
        name: &str,
        index: &Index,
        tuple_id: Option<&TupleId>,
    ) -> Result<BumpBytes, DatabaseError> {
        let mut key_prefix = self.encode_index_bound_key(name, index, false)?;

        if let Some(tuple_id) = tuple_id {
            if matches!(index.ty, IndexType::Normal | IndexType::Composite) {
                tuple_id.memcomparable_encode(&mut key_prefix)?;
            }
        }
        Ok(key_prefix)
    }

    pub fn decode_index(bytes: &[u8]) -> Result<TupleId, DatabaseError> {
        Ok(bincode::deserialize_from(&mut Cursor::new(bytes))?)
    }

    /// Key: {TableName}{COLUMN_TAG}{BOUND_MIN_TAG}{ColumnId}
    /// Value: ColumnCatalog
    ///
    /// Tips: the `0` for bound range
    pub fn encode_column(
        &self,
        col: &ColumnRef,
        reference_tables: &mut ReferenceTables,
    ) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        if let ColumnRelation::Table {
            column_id,
            table_name,
            is_temp: false,
        } = &col.summary().relation
        {
            let mut key_prefix = self.key_prefix(CodecType::Column, table_name);

            key_prefix.write_all(&[BOUND_MIN_TAG])?;
            key_prefix.write_all(&column_id.to_bytes()[..])?;

            let mut column_bytes = BumpBytes::new_in(&self.arena);
            col.encode(&mut column_bytes, true, reference_tables)?;

            Ok((key_prefix, column_bytes))
        } else {
            Err(DatabaseError::InvalidColumn(
                "column does not belong to table".to_string(),
            ))
        }
    }

    pub fn decode_column<T: Transaction, R: Read>(
        reader: &mut R,
        reference_tables: &ReferenceTables,
    ) -> Result<ColumnRef, DatabaseError> {
        // `TableCache` is not theoretically used in `table_collect` because `ColumnCatalog` should not depend on other Column
        ColumnRef::decode::<T, R>(reader, None, reference_tables)
    }

    /// Key: {TableName}{STATISTICS_TAG}{BOUND_MIN_TAG}{INDEX_ID}
    /// Value: StatisticsMeta Path
    pub fn encode_statistics_path(
        &self,
        table_name: &str,
        index_id: IndexId,
        path: String,
    ) -> (BumpBytes, BumpBytes) {
        let key = self.encode_statistics_path_key(table_name, index_id);

        let mut value = BumpBytes::new_in(&self.arena);
        value.extend_from_slice(path.as_bytes());

        (key, value)
    }

    pub fn encode_statistics_path_key(&self, table_name: &str, index_id: IndexId) -> BumpBytes {
        let mut key_prefix = self.key_prefix(CodecType::Statistics, table_name);

        key_prefix.push(BOUND_MIN_TAG);
        key_prefix.extend(index_id.to_le_bytes());
        key_prefix
    }

    pub fn decode_statistics_path(bytes: &[u8]) -> Result<String, DatabaseError> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }

    /// Key: View{BOUND_MIN_TAG}{ViewName}
    /// Value: View
    pub fn encode_view(&self, view: &View) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        let key = self.encode_view_key(&view.name);

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

        Ok((key, bytes))
    }

    pub fn encode_view_key(&self, view_name: &str) -> BumpBytes {
        self.key_prefix(CodecType::View, view_name)
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

    /// Key: Root{BOUND_MIN_TAG}{TableName}
    /// Value: TableMeta
    pub fn encode_root_table(
        &self,
        meta: &TableMeta,
    ) -> Result<(BumpBytes, BumpBytes), DatabaseError> {
        let key = self.encode_root_table_key(&meta.table_name);

        let mut meta_bytes = BumpBytes::new_in(&self.arena);
        meta.encode(&mut meta_bytes, true, &mut ReferenceTables::new())?;
        Ok((key, meta_bytes))
    }

    pub fn encode_root_table_key(&self, table_name: &str) -> BumpBytes {
        self.key_prefix(CodecType::Root, table_name)
    }

    pub fn decode_root_table<T: Transaction>(bytes: &[u8]) -> Result<TableMeta, DatabaseError> {
        let mut bytes = Cursor::new(bytes);

        TableMeta::decode::<T, _>(&mut bytes, None, &EMPTY_REFERENCE_TABLES)
    }

    pub fn encode_table_hash_key(&self, table_name: &str) -> BumpBytes {
        self.key_prefix(CodecType::Hash, table_name)
    }

    pub fn encode_table_hash(&self, table_name: &str) -> (BumpBytes, BumpBytes) {
        (
            self.key_prefix(CodecType::Hash, table_name),
            BumpBytes::new_in(&self.arena),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::catalog::view::View;
    use crate::catalog::{
        ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, TableCatalog, TableMeta,
    };
    use crate::errors::DatabaseError;
    use crate::serdes::ReferenceTables;
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::table_codec::{BumpBytes, TableCodec};
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
        TableCatalog::new(Arc::new("t1".to_string()), columns).unwrap()
    }

    #[test]
    fn test_table_codec_tuple() -> Result<(), DatabaseError> {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let table_catalog = build_table_codec();

        let mut tuple = Tuple::new(
            Some(DataValue::Int32(0)),
            vec![DataValue::Int32(0), DataValue::Decimal(Decimal::new(1, 0))],
        );
        let (_, bytes) = table_codec.encode_tuple(
            &table_catalog.name,
            &mut tuple,
            &[LogicalType::Integer, LogicalType::Decimal(None, None)],
        )?;
        let schema = table_catalog.schema_ref();
        let pk_indices = table_catalog.primary_keys_indices();

        tuple.pk = None;
        assert_eq!(
            TableCodec::decode_tuple(
                &table_catalog.types(),
                pk_indices,
                &[0, 1],
                schema,
                &bytes,
                false
            )?,
            tuple
        );

        Ok(())
    }

    #[test]
    fn test_root_catalog() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let table_catalog = build_table_codec();
        let (_, bytes) = table_codec
            .encode_root_table(&TableMeta {
                table_name: table_catalog.name.clone(),
            })
            .unwrap();

        let table_meta = TableCodec::decode_root_table::<RocksTransaction>(&bytes).unwrap();

        assert_eq!(table_meta.table_name.as_str(), table_catalog.name.as_str());
    }

    #[test]
    fn test_table_codec_statistics_meta_path() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let path = String::from("./lol");
        let (_, bytes) = table_codec.encode_statistics_path("t1", 0, path.clone());
        let decode_path = TableCodec::decode_statistics_path(&bytes).unwrap();

        assert_eq!(path, decode_path);
    }

    #[test]
    fn test_table_codec_index_meta() -> Result<(), DatabaseError> {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let index_meta = IndexMeta {
            id: 0,
            column_ids: vec![Ulid::new()],
            table_name: Arc::new("T1".to_string()),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "index_1".to_string(),
            ty: IndexType::PrimaryKey { is_multiple: false },
        };
        let (_, bytes) = table_codec.encode_index_meta(&"T1".to_string(), &index_meta)?;

        assert_eq!(
            TableCodec::decode_index_meta::<RocksTransaction>(&bytes)?,
            index_meta
        );

        Ok(())
    }

    #[test]
    fn test_table_codec_index() -> Result<(), DatabaseError> {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let table_catalog = build_table_codec();
        let value = Arc::new(DataValue::Int32(0));
        let index = Index::new(0, &value, IndexType::PrimaryKey { is_multiple: false });
        let tuple_id = DataValue::Int32(0);
        let (_, bytes) = table_codec.encode_index(&table_catalog.name, &index, &tuple_id)?;

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
            table_name: Arc::new("t1".to_string()),
            is_temp: false,
        };
        let col = ColumnRef::from(col);

        let mut reference_tables = ReferenceTables::new();

        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let (_, bytes) = table_codec
            .encode_column(&col, &mut reference_tables)
            .unwrap();
        let mut cursor = Cursor::new(bytes);
        let decode_col =
            TableCodec::decode_column::<RocksTransaction, _>(&mut cursor, &reference_tables)?;

        assert_eq!(decode_col, col);

        Ok(())
    }

    #[test]
    fn test_table_codec_view() -> Result<(), DatabaseError> {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let table_state = build_t1_table()?;
        // Subquery
        {
            println!("==== Subquery");
            let plan = table_state
                .plan("select * from t1 where c1 in (select c1 from t1 where c1 > 1)")?;
            println!("{:#?}", plan);
            let view = View {
                name: Arc::new("view_subquery".to_string()),
                plan: Box::new(plan),
            };
            let (_, bytes) = table_codec.encode_view(&view)?;
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
                name: Arc::new("view_filter".to_string()),
                plan: Box::new(plan),
            };
            let (_, bytes) = table_codec.encode_view(&view)?;
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
                name: Arc::new("view_join".to_string()),
                plan: Box::new(plan),
            };
            let (_, bytes) = table_codec.encode_view(&view)?;
            let transaction = table_state.storage.transaction()?;

            assert_eq!(
                view,
                TableCodec::decode_view(&bytes, (&transaction, &table_state.table_cache))?
            );
        }

        Ok(())
    }

    #[test]
    fn test_table_codec_column_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set = BTreeSet::new();
        let op = |col_id: usize, table_name: &str| {
            let mut col = ColumnCatalog::new(
                "".to_string(),
                false,
                ColumnDesc::new(LogicalType::SqlNull, None, false, None).unwrap(),
            );

            col.summary_mut().relation = ColumnRelation::Table {
                column_id: Ulid::from(col_id as u128),
                table_name: Arc::new(table_name.to_string()),
                is_temp: false,
            };

            let (key, _) = table_codec
                .encode_column(&ColumnRef::from(col), &mut ReferenceTables::new())
                .unwrap();
            key
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

        let (min, max) = table_codec.columns_bound(&Arc::new("T1".to_string()));

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(0, "T1"));
        assert_eq!(vec[1], &op(1, "T1"));
        assert_eq!(vec[2], &op(2, "T1"));
    }

    #[test]
    fn test_table_codec_index_meta_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set = BTreeSet::new();
        let op = |index_id: usize, table_name: &str| {
            let index_meta = IndexMeta {
                id: index_id as u32,
                column_ids: vec![],
                table_name: Arc::new(table_name.to_string()),
                pk_ty: LogicalType::Integer,
                value_ty: LogicalType::Integer,
                name: format!("{}_index", index_id),
                ty: IndexType::PrimaryKey { is_multiple: false },
            };

            let (key, _) = table_codec
                .encode_index_meta(&table_name.to_string(), &index_meta)
                .unwrap();
            key
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

        let (min, max) = table_codec.index_meta_bound(&"T1".to_string());

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec.len(), 3);

        assert_eq!(vec[0], &op(0, "T1"));
        assert_eq!(vec[1], &op(1, "T1"));
        assert_eq!(vec[2], &op(2, "T1"));
    }

    #[test]
    fn test_table_codec_index_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set = BTreeSet::new();
        let column = ColumnCatalog::new(
            "".to_string(),
            false,
            ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
        );
        let table_catalog = TableCatalog::new(Arc::new("T0".to_string()), vec![column]).unwrap();

        let op = |value: DataValue, index_id: usize, table_name: &String| {
            let value = Arc::new(value);
            let index = Index::new(
                index_id as u32,
                &value,
                IndexType::PrimaryKey { is_multiple: false },
            );

            table_codec
                .encode_index_key(table_name, &index, None)
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

        println!("{:#?}", set);

        let (min, max) = table_codec.index_bound(&table_catalog.name, 1).unwrap();

        println!("{:?}", min);
        println!("{:?}", max);

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
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
    fn test_table_codec_index_all_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set = BTreeSet::new();
        let op = |value: DataValue, index_id: usize, table_name: &str| {
            let value = Arc::new(value);
            let index = Index::new(
                index_id as u32,
                &value,
                IndexType::PrimaryKey { is_multiple: false },
            );

            table_codec
                .encode_index_key(&table_name.to_string(), &index, None)
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

        let (min, max) = table_codec.all_index_bound(&"T1".to_string());

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
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
    fn test_table_codec_tuple_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set = BTreeSet::new();
        let op = |tuple_id: DataValue, table_name: &str| {
            table_codec
                .encode_tuple_key(&table_name.to_string(), &Arc::new(tuple_id))
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

        let (min, max) = table_codec.tuple_bound(&"T1".to_string());

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
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
    fn test_root_codec_name_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set: BTreeSet<BumpBytes> = BTreeSet::new();
        let op = |table_name: &str| table_codec.encode_root_table_key(table_name);

        let mut value_0 = BumpBytes::new_in(&table_codec.arena);
        value_0.push(b'A');
        let mut value_1 = BumpBytes::new_in(&table_codec.arena);
        value_1.push(b'Z');

        set.insert(value_0);
        set.insert(value_1);
        set.insert(op("T0"));
        set.insert(op("T1"));
        set.insert(op("T2"));

        let (min, max) = table_codec.root_table_bound();

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec[0], &op("T0"));
        assert_eq!(vec[1], &op("T1"));
        assert_eq!(vec[2], &op("T2"));
    }

    #[test]
    fn test_view_codec_name_bound() {
        let table_codec = TableCodec {
            arena: Default::default(),
        };
        let mut set = BTreeSet::new();
        let op = |view_name: &str| table_codec.encode_view_key(view_name);

        let mut value_0 = BumpBytes::new_in(&table_codec.arena);
        value_0.push(b'A');
        let mut value_1 = BumpBytes::new_in(&table_codec.arena);
        value_1.push(b'Z');

        set.insert(value_0);
        set.insert(value_1);

        set.insert(op("V0"));
        set.insert(op("V1"));
        set.insert(op("V2"));

        let (min, max) = table_codec.view_bound();

        let vec = set
            .range::<BumpBytes, (Bound<&BumpBytes>, Bound<&BumpBytes>)>((
                Bound::Included(&min),
                Bound::Included(&max),
            ))
            .collect_vec();

        assert_eq!(vec[2], &op("V0"));
        assert_eq!(vec[0], &op("V1"));
        assert_eq!(vec[1], &op("V2"));
    }
}
