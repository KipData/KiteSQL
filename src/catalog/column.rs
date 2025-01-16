use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;
use crate::types::{ColumnId, LogicalType};
use kite_sql_serde_macros::ReferenceSerialization;
use sqlparser::ast::CharLengthUnits;
use std::hash::Hash;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnRef(pub Arc<ColumnCatalog>);

impl Deref for ColumnRef {
    type Target = ColumnCatalog;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl From<ColumnCatalog> for ColumnRef {
    fn from(c: ColumnCatalog) -> Self {
        ColumnRef(Arc::new(c))
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, ReferenceSerialization)]
pub struct ColumnCatalog {
    summary: ColumnSummary,
    nullable: bool,
    desc: ColumnDesc,
    in_join: bool,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum ColumnRelation {
    None,
    Table {
        column_id: ColumnId,
        table_name: TableName,
        is_temp: bool,
    },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, ReferenceSerialization)]
pub struct ColumnSummary {
    pub name: String,
    pub relation: ColumnRelation,
}

impl ColumnRef {
    pub(crate) fn nullable_for_join(&self, nullable: bool) -> Option<ColumnRef> {
        if self.nullable == nullable {
            return None;
        }
        let mut temp = ColumnCatalog::clone(self);
        temp.nullable = nullable;
        temp.in_join = true;
        Some(ColumnRef::from(temp))
    }
}

impl ColumnCatalog {
    pub fn new(column_name: String, nullable: bool, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                name: column_name,
                relation: ColumnRelation::None,
            },
            nullable,
            desc: column_desc,
            in_join: false,
        }
    }

    pub(crate) fn direct_new(
        summary: ColumnSummary,
        nullable: bool,
        column_desc: ColumnDesc,
        in_join: bool,
    ) -> ColumnCatalog {
        ColumnCatalog {
            summary,
            nullable,
            desc: column_desc,
            in_join,
        }
    }

    pub(crate) fn new_dummy(column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                name: column_name,
                relation: ColumnRelation::None,
            },
            nullable: true,
            // SAFETY: default expr must not be [`ScalarExpression::ColumnRef`]
            desc: ColumnDesc::new(
                LogicalType::Varchar(None, CharLengthUnits::Characters),
                None,
                false,
                None,
            )
            .unwrap(),
            in_join: false,
        }
    }

    pub(crate) fn summary(&self) -> &ColumnSummary {
        &self.summary
    }

    pub fn summary_mut(&mut self) -> &mut ColumnSummary {
        &mut self.summary
    }

    pub fn id(&self) -> Option<ColumnId> {
        match &self.summary.relation {
            ColumnRelation::None => None,
            ColumnRelation::Table { column_id, .. } => Some(*column_id),
        }
    }

    pub fn name(&self) -> &str {
        &self.summary.name
    }

    pub fn full_name(&self) -> String {
        if let Some(table_name) = self.table_name() {
            return format!("{}.{}", table_name, self.name());
        }
        self.name().to_string()
    }

    pub fn table_name(&self) -> Option<&TableName> {
        match &self.summary.relation {
            ColumnRelation::None => None,
            ColumnRelation::Table { table_name, .. } => Some(table_name),
        }
    }

    pub fn set_name(&mut self, name: String) {
        self.summary.name = name;
    }

    pub fn set_ref_table(&mut self, table_name: TableName, column_id: ColumnId, is_temp: bool) {
        self.summary.relation = ColumnRelation::Table {
            column_id,
            table_name,
            is_temp,
        };
    }

    pub fn in_join(&self) -> bool {
        self.in_join
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn datatype(&self) -> &LogicalType {
        &self.desc.column_datatype
    }

    pub(crate) fn default_value(&self) -> Result<Option<DataValue>, DatabaseError> {
        self.desc
            .default
            .as_ref()
            .map(|expr| expr.eval(None))
            .transpose()
    }

    pub(crate) fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub(crate) fn desc_mut(&mut self) -> &mut ColumnDesc {
        &mut self.desc
    }
}

/// The descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub struct ColumnDesc {
    pub(crate) column_datatype: LogicalType,
    primary: Option<usize>,
    is_unique: bool,
    pub(crate) default: Option<ScalarExpression>,
}

impl ColumnDesc {
    pub fn new(
        column_datatype: LogicalType,
        primary: Option<usize>,
        is_unique: bool,
        default: Option<ScalarExpression>,
    ) -> Result<ColumnDesc, DatabaseError> {
        if let Some(expr) = &default {
            if expr.has_table_ref_column() {
                return Err(DatabaseError::DefaultNotColumnRef);
            }
        }

        Ok(ColumnDesc {
            column_datatype,
            primary,
            is_unique,
            default,
        })
    }

    pub(crate) fn primary(&self) -> Option<usize> {
        self.primary
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.primary.is_some()
    }

    pub(crate) fn set_primary(&mut self, is_primary: Option<usize>) {
        self.primary = is_primary
    }

    pub(crate) fn is_unique(&self) -> bool {
        self.is_unique
    }

    pub(crate) fn set_unique(&mut self, is_unique: bool) {
        self.is_unique = is_unique
    }
}
