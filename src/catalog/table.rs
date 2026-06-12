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

use crate::catalog::{ColumnCatalog, ColumnRef, ColumnRelation};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::{MetaArena, PlanArena};
use crate::types::index::{IndexMeta, IndexMetaRef, IndexType};
use crate::types::tuple::Schema;
use crate::types::{ColumnId, LogicalType};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::{slice, vec};
use ulid::Generator;

pub type TableName = Arc<str>;
pub type PrimaryKeyIndices = Arc<Vec<usize>>;

#[derive(Debug, Clone, PartialEq)]
pub struct TableCatalog {
    pub(crate) name: TableName,
    /// Mapping from column names to column ids
    column_idxs: BTreeMap<String, (ColumnId, usize)>,
    columns: BTreeMap<ColumnId, usize>,
    column_refs: Vec<ColumnRef>,
    pub(crate) indexes: Vec<IndexMetaRef>,

    primary_keys: Vec<(usize, ColumnRef)>,
    primary_key_indices: PrimaryKeyIndices,
    primary_key_type: LogicalType,
}

pub(crate) struct DmlTableSnapshot {
    pub(crate) columns: Schema,
    pub(crate) primary_key_indices: PrimaryKeyIndices,
    pub(crate) columns_len: usize,
    pub(crate) index_metas: Vec<(IndexMetaRef, Vec<ScalarExpression>)>,
}

//TODO: can add some like Table description and other information as attributes
#[derive(Debug, Clone, PartialEq, ReferenceSerialization)]
pub struct TableMeta {
    pub(crate) table_name: TableName,
}

impl TableCatalog {
    pub(crate) fn name(&self) -> &TableName {
        &self.name
    }

    pub(crate) fn get_unique_index(&self, col_id: &ColumnId) -> Option<&IndexMetaRef> {
        self.indexes
            .iter()
            .find(|meta| matches!(meta.ty, IndexType::Unique) && &meta.column_ids[0] == col_id)
    }

    pub(crate) fn get_column_by_id(&self, id: &ColumnId) -> Option<ColumnRef> {
        self.columns.get(id).map(|i| self.column_refs[*i])
    }

    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<&ColumnId> {
        self.column_idxs.get(name).map(|(id, _)| id)
    }

    pub(crate) fn get_column_by_name(&self, name: &str) -> Option<ColumnRef> {
        self.column_idxs
            .get(name)
            .map(|(_, i)| self.column_refs[*i])
    }

    #[allow(dead_code)]
    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn columns(&self) -> slice::Iter<'_, ColumnRef> {
        self.column_refs.iter()
    }

    pub(crate) fn column_ref(&self, index: usize) -> Option<ColumnRef> {
        self.column_refs.get(index).copied()
    }

    pub(crate) fn indexes(&self) -> slice::Iter<'_, IndexMetaRef> {
        self.indexes.iter()
    }

    pub(crate) fn columns_len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn primary_keys(&self) -> &[(usize, ColumnRef)] {
        &self.primary_keys
    }

    pub(crate) fn primary_keys_type(&self) -> &LogicalType {
        &self.primary_key_type
    }

    pub(crate) fn primary_keys_indices(&self) -> &PrimaryKeyIndices {
        &self.primary_key_indices
    }

    pub(crate) fn dml_snapshot(
        &self,
        arena: &mut PlanArena,
    ) -> Result<DmlTableSnapshot, DatabaseError> {
        let index_metas = self
            .indexes()
            .map(|index_meta| Ok((index_meta.clone(), index_meta.column_exprs(self, arena)?)))
            .collect::<Result<Vec<_>, DatabaseError>>()?;

        Ok(DmlTableSnapshot {
            columns: self.column_refs.clone(),
            primary_key_indices: self.primary_key_indices.clone(),
            columns_len: self.columns_len(),
            index_metas,
        })
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(
        &mut self,
        mut col: ColumnCatalog,
        generator: &mut Generator,
        arena: &mut impl MetaArena,
    ) -> Result<ColumnId, DatabaseError> {
        if self.column_idxs.contains_key(col.name()) {
            return Err(DatabaseError::DuplicateColumn(col.name().to_string()));
        }
        let max_existing_id = self.columns.keys().max().copied();
        let mut col_id = generator.generate().unwrap();
        while max_existing_id.is_some_and(|max_id| col_id <= max_id) {
            col_id = generator.generate().unwrap();
        }

        col.summary_mut().relation = ColumnRelation::Table {
            column_id: col_id,
            table_name: self.name.clone(),
            is_temp: false,
        };

        self.column_idxs
            .insert(col.name().to_string(), (col_id, self.column_refs.len()));
        self.columns.insert(col_id, self.column_refs.len());
        let column_ref = arena.alloc_column(col);
        self.column_refs.push(column_ref);

        Ok(col_id)
    }

    pub(crate) fn add_index_meta(
        &mut self,
        name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
        arena: &impl MetaArena,
    ) -> Result<&IndexMeta, DatabaseError> {
        for index in self.indexes.iter() {
            if index.name == name {
                return Err(DatabaseError::DuplicateIndex(name));
            }
        }

        let index_id = self.indexes.last().map(|index| index.id + 1).unwrap_or(0);
        let pk_ty = self.primary_key_type.clone();

        let mut val_tys = Vec::with_capacity(column_ids.len());
        for column_id in column_ids.iter() {
            let val_ty = self
                .get_column_by_id(column_id)
                .map(|column| arena.column(column))
                .ok_or_else(|| DatabaseError::column_not_found(column_id.to_string()))?
                .datatype()
                .clone();
            val_tys.push(val_ty)
        }
        let value_ty = if val_tys.len() == 1 {
            val_tys.pop().unwrap()
        } else {
            LogicalType::Tuple(val_tys)
        };

        let index = IndexMeta {
            id: index_id,
            column_ids,
            table_name: self.name.clone(),
            pk_ty,
            value_ty,
            name,
            ty,
        };
        self.indexes.push(Arc::new(index));
        Ok(self.indexes.last().unwrap())
    }

    pub fn new(
        name: TableName,
        columns: Vec<ColumnCatalog>,
        arena: &mut impl MetaArena,
    ) -> Result<TableCatalog, DatabaseError> {
        if columns.is_empty() {
            return Err(DatabaseError::ColumnsEmpty);
        }
        let mut table_catalog = TableCatalog {
            name,
            column_idxs: BTreeMap::new(),
            columns: BTreeMap::new(),
            column_refs: vec![],
            indexes: vec![],
            primary_keys: vec![],
            primary_key_indices: Default::default(),
            primary_key_type: LogicalType::SqlNull,
        };
        let mut generator = Generator::new();
        for col_catalog in columns.into_iter() {
            let _ = table_catalog
                .add_column(col_catalog, &mut generator, arena)
                .unwrap();
        }
        let (primary_keys, primary_key_indices) =
            Self::build_primary_keys(&table_catalog.column_refs, arena);

        table_catalog.primary_key_type = Self::build_primary_key_type(&primary_keys, arena);
        table_catalog.primary_keys = primary_keys;
        table_catalog.primary_key_indices = primary_key_indices;

        Ok(table_catalog)
    }

    fn build_primary_key_type(
        primary_keys: &[(usize, ColumnRef)],
        arena: &impl MetaArena,
    ) -> LogicalType {
        if primary_keys.len() == 1 {
            arena.column(primary_keys[0].1).datatype().clone()
        } else {
            LogicalType::Tuple(
                primary_keys
                    .iter()
                    .map(|(_, column)| arena.column(*column).datatype().clone())
                    .collect_vec(),
            )
        }
    }

    pub(crate) fn reload<I>(
        name: TableName,
        column_catalogs: I,
        indexes: Vec<IndexMetaRef>,
        arena: &mut impl MetaArena,
    ) -> Result<TableCatalog, DatabaseError>
    where
        I: Iterator<Item = ColumnCatalog>,
    {
        let (lower_bound, _) = column_catalogs.size_hint();
        let mut column_idxs = BTreeMap::new();
        let mut columns = BTreeMap::new();
        let mut column_refs = Vec::with_capacity(lower_bound);

        for (i, column_catalog) in column_catalogs.enumerate() {
            let column_id = column_catalog.id().ok_or(DatabaseError::invalid_column(
                "column does not belong to table".to_string(),
            ))?;

            column_idxs.insert(column_catalog.name().to_string(), (column_id, i));
            columns.insert(column_id, i);
            column_refs.push(arena.alloc_column(column_catalog));
        }
        let (primary_keys, primary_key_indices) = Self::build_primary_keys(&column_refs, arena);
        let primary_key_type = Self::build_primary_key_type(&primary_keys, arena);

        Ok(TableCatalog {
            name,
            column_idxs,
            columns,
            column_refs,
            indexes,
            primary_keys,
            primary_key_indices,
            primary_key_type,
        })
    }

    pub(crate) fn transplant_to_table_arena(
        &self,
        source_arena: &PlanArena,
    ) -> Result<TableCatalog, DatabaseError> {
        let column_catalogs = self
            .columns()
            .map(|column| source_arena.column(*column).clone())
            .collect_vec();

        Self::reload(
            self.name.clone(),
            column_catalogs.into_iter(),
            self.indexes.clone(),
            source_arena.table_arena_cell().borrow_mut(),
        )
    }

    fn build_primary_keys(
        columns: &[ColumnRef],
        arena: &impl MetaArena,
    ) -> (Vec<(usize, ColumnRef)>, PrimaryKeyIndices) {
        let mut primary_keys = Vec::new();
        let mut primary_key_indices = Vec::new();

        for (_, (i, column)) in columns
            .iter()
            .enumerate()
            .filter_map(|(i, column)| {
                arena
                    .column(*column)
                    .desc()
                    .primary()
                    .map(|p_i| (p_i, (i, *column)))
            })
            .sorted_by_key(|(p_i, _)| *p_i)
        {
            primary_key_indices.push(i);
            primary_keys.push((i, column));
        }

        (primary_keys, Arc::new(primary_key_indices))
    }
}

impl TableMeta {
    pub(crate) fn empty(table_name: TableName) -> Self {
        TableMeta { table_name }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::ColumnDesc;
    use crate::planner::TableArenaCell;
    use crate::types::LogicalType;
    use ulid::Generator;

    fn build_table_catalog(
        name: &str,
        columns: Vec<ColumnCatalog>,
    ) -> (TableArenaCell, TableCatalog) {
        let table_arena = TableArenaCell::default();
        let table_catalog =
            TableCatalog::new(name.to_string().into(), columns, table_arena.borrow_mut()).unwrap();
        (table_arena, table_catalog)
    }

    #[test]
    // | a (Int32) | b (Bool) |
    // |-----------|----------|
    // | 1         | true     |
    // | 2         | false    |
    fn test_table_catalog() {
        let col0 = ColumnCatalog::new(
            "a".into(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        );
        let col1 = ColumnCatalog::new(
            "b".into(),
            false,
            ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
        );
        let col_catalogs = vec![col0, col1];
        let (table_arena, table_catalog) = build_table_catalog("test", col_catalogs);

        assert!(table_catalog.contains_column("a"));
        assert!(table_catalog.contains_column("b"));
        assert!(!table_catalog.contains_column("c"));

        let col_a_id = table_catalog.get_column_id_by_name("a").unwrap();
        let col_b_id = table_catalog.get_column_id_by_name("b").unwrap();
        assert!(col_a_id < col_b_id);

        let column_catalog = table_arena
            .borrow()
            .column(table_catalog.get_column_by_id(col_a_id).unwrap());
        assert_eq!(column_catalog.name(), "a");
        assert_eq!(*column_catalog.datatype(), LogicalType::Integer,);

        let column_catalog = table_arena
            .borrow()
            .column(table_catalog.get_column_by_id(col_b_id).unwrap());
        assert_eq!(column_catalog.name(), "b");
        assert_eq!(*column_catalog.datatype(), LogicalType::Boolean,);
    }

    #[test]
    fn test_add_column_generates_id_after_existing_columns() {
        for _ in 0..256 {
            let (table_arena, mut table_catalog) = build_table_catalog(
                "test",
                vec![
                    ColumnCatalog::new(
                        "id".into(),
                        false,
                        ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
                    ),
                    ColumnCatalog::new(
                        "name".into(),
                        true,
                        ColumnDesc::new(
                            LogicalType::Varchar(None, crate::types::CharLengthUnits::Characters),
                            None,
                            false,
                            None,
                        )
                        .unwrap(),
                    ),
                ],
            );
            let max_existing_id = table_catalog
                .columns()
                .filter_map(|column| table_arena.borrow().column(*column).id())
                .max()
                .unwrap();
            let mut generator = Generator::new();
            let new_id = table_catalog
                .add_column(
                    ColumnCatalog::new(
                        "age".into(),
                        true,
                        ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
                    ),
                    &mut generator,
                    table_arena.borrow_mut(),
                )
                .unwrap();

            assert!(new_id > max_existing_id);
        }
    }
}
