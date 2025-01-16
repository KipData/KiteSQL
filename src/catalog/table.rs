use crate::catalog::{ColumnCatalog, ColumnRef, ColumnRelation};
use crate::errors::DatabaseError;
use crate::types::index::{IndexMeta, IndexMetaRef, IndexType};
use crate::types::tuple::SchemaRef;
use crate::types::{ColumnId, LogicalType};
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::{slice, vec};
use ulid::Generator;

pub type TableName = Arc<String>;
pub type PrimaryKeyIndices = Arc<Vec<usize>>;

#[derive(Debug, Clone, PartialEq)]
pub struct TableCatalog {
    pub(crate) name: TableName,
    /// Mapping from column names to column ids
    column_idxs: BTreeMap<String, (ColumnId, usize)>,
    columns: BTreeMap<ColumnId, usize>,
    pub(crate) indexes: Vec<IndexMetaRef>,

    schema_ref: SchemaRef,
    primary_keys: Vec<(usize, ColumnRef)>,
    primary_key_indices: PrimaryKeyIndices,
    primary_key_type: Option<LogicalType>,
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

    #[allow(dead_code)]
    pub(crate) fn get_column_by_id(&self, id: &ColumnId) -> Option<&ColumnRef> {
        self.columns.get(id).map(|i| &self.schema_ref[*i])
    }

    #[cfg(test)]
    pub(crate) fn get_column_id_by_name(&self, name: &str) -> Option<&ColumnId> {
        self.column_idxs.get(name).map(|(id, _)| id)
    }

    pub(crate) fn get_column_by_name(&self, name: &str) -> Option<&ColumnRef> {
        self.column_idxs
            .get(name)
            .map(|(_, i)| &self.schema_ref[*i])
    }

    #[allow(dead_code)]
    pub(crate) fn contains_column(&self, name: &str) -> bool {
        self.column_idxs.contains_key(name)
    }

    pub(crate) fn columns(&self) -> slice::Iter<'_, ColumnRef> {
        self.schema_ref.iter()
    }

    pub(crate) fn indexes(&self) -> slice::Iter<'_, IndexMetaRef> {
        self.indexes.iter()
    }

    pub fn schema_ref(&self) -> &SchemaRef {
        &self.schema_ref
    }

    pub(crate) fn columns_len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) fn primary_keys(&self) -> &[(usize, ColumnRef)] {
        &self.primary_keys
    }

    pub(crate) fn primary_keys_indices(&self) -> &PrimaryKeyIndices {
        &self.primary_key_indices
    }

    pub(crate) fn types(&self) -> Vec<LogicalType> {
        self.columns()
            .map(|column| column.datatype().clone())
            .collect_vec()
    }

    /// Add a column to the table catalog.
    pub(crate) fn add_column(
        &mut self,
        mut col: ColumnCatalog,
        generator: &mut Generator,
    ) -> Result<ColumnId, DatabaseError> {
        if self.column_idxs.contains_key(col.name()) {
            return Err(DatabaseError::DuplicateColumn(col.name().to_string()));
        }
        let col_id = generator.generate().unwrap();

        col.summary_mut().relation = ColumnRelation::Table {
            column_id: col_id,
            table_name: self.name.clone(),
            is_temp: false,
        };

        self.column_idxs
            .insert(col.name().to_string(), (col_id, self.schema_ref.len()));
        self.columns.insert(col_id, self.schema_ref.len());

        let mut schema = Vec::clone(&self.schema_ref);
        schema.push(ColumnRef::from(col));
        self.schema_ref = Arc::new(schema);

        Ok(col_id)
    }

    pub(crate) fn add_index_meta(
        &mut self,
        name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<&IndexMeta, DatabaseError> {
        for index in self.indexes.iter() {
            if index.name == name {
                return Err(DatabaseError::DuplicateIndex(name));
            }
        }

        let index_id = self.indexes.last().map(|index| index.id + 1).unwrap_or(0);
        let pk_ty = self
            .primary_key_type
            .get_or_insert_with(|| {
                let primary_keys = &self.primary_keys;

                if primary_keys.len() == 1 {
                    primary_keys[0].1.datatype().clone()
                } else {
                    LogicalType::Tuple(
                        primary_keys
                            .iter()
                            .map(|(_, column)| column.datatype().clone())
                            .collect_vec(),
                    )
                }
            })
            .clone();

        let mut val_tys = Vec::with_capacity(column_ids.len());
        for column_id in column_ids.iter() {
            let val_ty = self
                .get_column_by_id(column_id)
                .ok_or_else(|| DatabaseError::ColumnNotFound(column_id.to_string()))?
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
    ) -> Result<TableCatalog, DatabaseError> {
        if columns.is_empty() {
            return Err(DatabaseError::ColumnsEmpty);
        }
        let mut table_catalog = TableCatalog {
            name,
            column_idxs: BTreeMap::new(),
            columns: BTreeMap::new(),
            indexes: vec![],
            schema_ref: Arc::new(vec![]),
            primary_keys: vec![],
            primary_key_indices: Default::default(),
            primary_key_type: None,
        };
        let mut generator = Generator::new();
        for col_catalog in columns.into_iter() {
            let _ = table_catalog
                .add_column(col_catalog, &mut generator)
                .unwrap();
        }
        let (primary_keys, primary_key_indices) =
            Self::build_primary_keys(&table_catalog.schema_ref);

        table_catalog.primary_keys = primary_keys;
        table_catalog.primary_key_indices = primary_key_indices;

        Ok(table_catalog)
    }

    pub(crate) fn reload(
        name: TableName,
        column_refs: Vec<ColumnRef>,
        indexes: Vec<IndexMetaRef>,
    ) -> Result<TableCatalog, DatabaseError> {
        let mut column_idxs = BTreeMap::new();
        let mut columns = BTreeMap::new();

        for (i, column_ref) in column_refs.iter().enumerate() {
            let column_id = column_ref.id().ok_or(DatabaseError::InvalidColumn(
                "column does not belong to table".to_string(),
            ))?;

            column_idxs.insert(column_ref.name().to_string(), (column_id, i));
            columns.insert(column_id, i);
        }
        let schema_ref = Arc::new(column_refs.clone());
        let (primary_keys, primary_key_indices) = Self::build_primary_keys(&schema_ref);

        Ok(TableCatalog {
            name,
            column_idxs,
            columns,
            indexes,
            schema_ref,
            primary_keys,
            primary_key_indices,
            primary_key_type: None,
        })
    }

    fn build_primary_keys(
        schema_ref: &Arc<Vec<ColumnRef>>,
    ) -> (Vec<(usize, ColumnRef)>, PrimaryKeyIndices) {
        let mut primary_keys = Vec::new();
        let mut primary_key_indices = Vec::new();

        for (_, (i, column)) in schema_ref
            .iter()
            .enumerate()
            .filter_map(|(i, column)| {
                column
                    .desc()
                    .primary()
                    .map(|p_i| (p_i, (i, column.clone())))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDesc;
    use crate::types::LogicalType;

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
        let table_catalog = TableCatalog::new(Arc::new("test".to_string()), col_catalogs).unwrap();

        assert_eq!(table_catalog.contains_column(&"a".to_string()), true);
        assert_eq!(table_catalog.contains_column(&"b".to_string()), true);
        assert_eq!(table_catalog.contains_column(&"c".to_string()), false);

        let col_a_id = table_catalog
            .get_column_id_by_name(&"a".to_string())
            .unwrap();
        let col_b_id = table_catalog
            .get_column_id_by_name(&"b".to_string())
            .unwrap();
        assert!(col_a_id < col_b_id);

        let column_catalog = table_catalog.get_column_by_id(&col_a_id).unwrap();
        assert_eq!(column_catalog.name(), "a");
        assert_eq!(*column_catalog.datatype(), LogicalType::Integer,);

        let column_catalog = table_catalog.get_column_by_id(&col_b_id).unwrap();
        assert_eq!(column_catalog.name(), "b");
        assert_eq!(*column_catalog.datatype(), LogicalType::Boolean,);
    }
}
