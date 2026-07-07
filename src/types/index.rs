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

use crate::catalog::{TableCatalog, TableName};
use crate::errors::DatabaseError;
use crate::expression::range_detacher::Range;
use crate::expression::ScalarExpression;
use crate::planner::operator::SortOption;
use crate::planner::PlanArena;
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::value::DataValue;
use crate::types::{ColumnId, LogicalType};
use kite_sql_serde_macros::ReferenceSerialization;
use std::collections::Bound;
use std::fmt;
use std::fmt::Formatter;

pub type IndexId = u32;

pub const INDEX_ID_LEN: usize = 4;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct IndexMetaRef {
    pos: usize,
}

impl IndexMetaRef {
    pub(crate) fn new(pos: usize) -> Self {
        Self { pos }
    }

    pub(crate) fn pos(self) -> usize {
        self.pos
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ReferenceSerialization)]
pub enum IndexType {
    PrimaryKey { is_multiple: bool },
    Unique,
    Normal,
    Composite,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ReferenceSerialization)]
pub enum RuntimeIndexProbe {
    Eq(DataValue),
    Scope {
        min: Bound<DataValue>,
        max: Bound<DataValue>,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ReferenceSerialization)]
pub enum IndexLookup {
    Static(Range),
    Probe,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ReferenceSerialization)]
pub struct IndexInfo {
    pub(crate) meta: IndexMetaRef,
    pub(crate) sort_option: SortOption,
    pub(crate) lookup: Option<IndexLookup>,
    pub(crate) residual_predicate: Option<ScalarExpression>,
    pub(crate) covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
    pub(crate) cover_mapping: Option<Vec<usize>>,
    pub(crate) sort_elimination_hint: Option<IndexOrderHint>,
    pub(crate) stream_distinct_hint: Option<IndexOrderHint>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ReferenceSerialization)]
pub struct IndexOrderHint {
    cover_num: usize,
}

impl IndexOrderHint {
    pub(crate) fn new(cover_num: usize) -> Self {
        Self { cover_num }
    }

    pub(crate) fn cover_num(self) -> usize {
        self.cover_num
    }

    pub(crate) fn merge_cover_num(&mut self, cover_num: usize) {
        self.cover_num = self.cover_num.max(cover_num);
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ReferenceSerialization)]
pub struct IndexMeta {
    pub id: IndexId,
    pub column_ids: Vec<ColumnId>,
    pub table_name: TableName,
    pub pk_ty: LogicalType,
    pub value_ty: LogicalType,
    pub name: String,
    pub ty: IndexType,
}

impl IndexMeta {
    pub(crate) fn column_exprs(
        &self,
        table: &TableCatalog,
        arena: &PlanArena,
    ) -> Result<Vec<ScalarExpression>, DatabaseError> {
        let mut exprs = Vec::with_capacity(self.column_ids.len());

        for column_id in self.column_ids.iter() {
            if let Some((position, column_ref)) = table
                .columns()
                .copied()
                .enumerate()
                .find(|(_, column)| arena.column(*column).id() == Some(*column_id))
            {
                exprs.push(ScalarExpression::column_expr(column_ref, position));
            } else {
                return Err(DatabaseError::column_not_found(column_id.to_string()));
            }
        }
        Ok(exprs)
    }
}

#[derive(Debug, Clone)]
pub struct Index<'a> {
    pub id: IndexId,
    pub value: &'a DataValue,
    pub ty: IndexType,
}

impl<'a> Index<'a> {
    pub fn new(id: IndexId, value: &'a DataValue, ty: IndexType) -> Self {
        Index { id, value, ty }
    }
}

impl fmt::Display for IndexInfo {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.meta)?;
        write!(f, " => ")?;

        if let Some(lookup) = &self.lookup {
            match lookup {
                IndexLookup::Static(range) => write!(f, "{range}")?,
                IndexLookup::Probe => write!(f, "Probe ?")?,
            }
        } else {
            write!(f, "EMPTY")?;
        }
        if self.covered_deserializers.is_some() {
            write!(f, " Covered")?;
        }

        Ok(())
    }
}

impl fmt::Display for IndexMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for IndexMetaRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "#{}", self.pos)
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog};
    use crate::planner::{PlanArena, TableArena, TableArenaCell};
    use crate::serdes::{ReferenceSerialization, ReferenceTables};
    use crate::storage::rocksdb::RocksTransaction;
    use std::fmt::Debug;
    use std::io::{Cursor, Seek, SeekFrom};

    fn index_info(lookup: Option<IndexLookup>) -> IndexInfo {
        IndexInfo {
            meta: IndexMetaRef::new(7),
            sort_option: SortOption::None,
            lookup,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_distinct_hint: None,
        }
    }

    fn index_meta() -> IndexMeta {
        IndexMeta {
            id: 1,
            column_ids: vec![10],
            table_name: "t".into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "idx_t".to_string(),
            ty: IndexType::Normal,
        }
    }

    fn roundtrip_with_arena<T>(
        value: T,
        encode_arena: &TableArena,
        decode_arena: &mut TableArena,
    ) -> Result<T, DatabaseError>
    where
        T: ReferenceSerialization + Debug + PartialEq,
    {
        let mut cursor = Cursor::new(Vec::new());
        let mut reference_tables = ReferenceTables::new();

        value.encode(&mut cursor, false, &mut reference_tables, encode_arena)?;
        cursor.seek(SeekFrom::Start(0))?;

        T::decode::<RocksTransaction, _, _>(&mut cursor, None, &reference_tables, decode_arena)
    }

    #[test]
    fn test_index_helpers_and_display() {
        let meta_ref = IndexMetaRef::new(3);
        assert_eq!(meta_ref.pos(), 3);
        assert_eq!(meta_ref.to_string(), "#3");

        let mut hint = IndexOrderHint::new(2);
        assert_eq!(hint.cover_num(), 2);
        hint.merge_cover_num(5);
        assert_eq!(hint.cover_num(), 5);
        hint.merge_cover_num(3);
        assert_eq!(hint.cover_num(), 5);

        let meta = index_meta();
        assert_eq!(meta.to_string(), "idx_t");

        assert_eq!(index_info(None).to_string(), "#7 => EMPTY");
        assert_eq!(
            index_info(Some(IndexLookup::Probe)).to_string(),
            "#7 => Probe ?"
        );
        let mut info = index_info(Some(IndexLookup::Static(Range::Eq(DataValue::Int32(1)))));
        info.covered_deserializers = Some(vec![LogicalType::Integer.serializable()]);
        assert_eq!(info.to_string(), "#7 => 1 Covered");

        let value = DataValue::Int32(1);
        let index = Index::new(9, &value, IndexType::Unique);
        assert_eq!(index.id, 9);
        assert_eq!(index.value, &value);
        assert_eq!(index.ty, IndexType::Unique);
    }

    #[test]
    fn test_index_types_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(IndexType::PrimaryKey { is_multiple: false });
        set.insert(IndexType::PrimaryKey { is_multiple: true });
        set.insert(IndexType::Unique);
        set.insert(IndexType::Normal);
        set.insert(IndexType::Composite);
        assert!(set.contains(&IndexType::Unique));
        assert!(!set.insert(IndexType::Normal));

        let mut probes = HashSet::new();
        probes.insert(RuntimeIndexProbe::Eq(DataValue::Int32(1)));
        probes.insert(RuntimeIndexProbe::Scope {
            min: Bound::Included(DataValue::Int32(1)),
            max: Bound::Excluded(DataValue::Int32(2)),
        });
        assert!(probes.contains(&RuntimeIndexProbe::Eq(DataValue::Int32(1))));

        let mut lookups = HashSet::new();
        lookups.insert(IndexLookup::Static(Range::Eq(DataValue::Int32(1))));
        lookups.insert(IndexLookup::Probe);
        assert!(lookups.contains(&IndexLookup::Probe));

        let mut hints = HashSet::new();
        hints.insert(IndexOrderHint::new(1));
        assert!(hints.contains(&IndexOrderHint::new(1)));

        let mut metas = HashSet::new();
        metas.insert(index_meta());
        assert!(metas.contains(&index_meta()));

        let mut infos = HashSet::new();
        infos.insert(index_info(Some(IndexLookup::Probe)));
        assert!(infos.contains(&index_info(Some(IndexLookup::Probe))));
    }

    #[test]
    fn test_index_serialization_roundtrips() -> Result<(), DatabaseError> {
        let mut encode_arena = TableArena::default();
        let mut decode_arena = TableArena::default();

        for ty in [
            IndexType::PrimaryKey { is_multiple: false },
            IndexType::PrimaryKey { is_multiple: true },
            IndexType::Unique,
            IndexType::Normal,
            IndexType::Composite,
        ] {
            assert_eq!(
                roundtrip_with_arena(ty, &encode_arena, &mut decode_arena)?,
                ty
            );
        }

        for probe in [
            RuntimeIndexProbe::Eq(DataValue::Int32(1)),
            RuntimeIndexProbe::Scope {
                min: Bound::Included(DataValue::Int32(1)),
                max: Bound::Excluded(DataValue::Int32(2)),
            },
        ] {
            assert_eq!(
                roundtrip_with_arena(probe.clone(), &encode_arena, &mut decode_arena)?,
                probe
            );
        }

        for lookup in [
            IndexLookup::Static(Range::Eq(DataValue::Int32(1))),
            IndexLookup::Probe,
        ] {
            assert_eq!(
                roundtrip_with_arena(lookup.clone(), &encode_arena, &mut decode_arena)?,
                lookup
            );
        }

        assert_eq!(
            roundtrip_with_arena(IndexOrderHint::new(3), &encode_arena, &mut decode_arena)?,
            IndexOrderHint::new(3)
        );
        assert_eq!(
            roundtrip_with_arena(index_meta(), &encode_arena, &mut decode_arena)?,
            index_meta()
        );

        let meta = encode_arena.alloc_index(index_meta());
        let info = IndexInfo {
            meta,
            sort_option: SortOption::None,
            lookup: Some(IndexLookup::Probe),
            residual_predicate: None,
            covered_deserializers: Some(vec![LogicalType::Integer.serializable()]),
            cover_mapping: Some(vec![0]),
            sort_elimination_hint: Some(IndexOrderHint::new(1)),
            stream_distinct_hint: Some(IndexOrderHint::new(1)),
        };

        assert_eq!(
            roundtrip_with_arena(info.clone(), &encode_arena, &mut decode_arena)?,
            info
        );

        Ok(())
    }

    #[test]
    fn test_index_meta_column_exprs() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id_col = ColumnCatalog::new(
            "id".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, Some(0), false, None)?,
        );
        let name_col = ColumnCatalog::new(
            "name".to_string(),
            true,
            ColumnDesc::new(
                LogicalType::Varchar(None, crate::types::CharLengthUnits::Characters),
                None,
                false,
                None,
            )?,
        );
        let table = TableCatalog::new("t".into(), vec![id_col, name_col], &mut arena)?;
        let id_column = arena.column(*table.columns().next().unwrap()).id().unwrap();
        let meta = IndexMeta {
            id: 1,
            column_ids: vec![id_column],
            table_name: "t".into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "idx_id".to_string(),
            ty: IndexType::Normal,
        };

        assert_eq!(meta.column_exprs(&table, &arena)?.len(), 1);

        let missing = IndexMeta {
            column_ids: vec![u64::MAX],
            ..meta
        };
        assert!(matches!(
            missing.column_exprs(&table, &arena),
            Err(DatabaseError::ColumnNotFound { .. })
        ));

        Ok(())
    }
}
// GRCOV_EXCL_STOP
