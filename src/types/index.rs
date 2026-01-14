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
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::value::DataValue;
use crate::types::{ColumnId, LogicalType};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

pub type IndexId = u32;
pub type IndexMetaRef = Arc<IndexMeta>;

pub const INDEX_ID_LEN: usize = 4;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ReferenceSerialization)]
pub enum IndexType {
    PrimaryKey { is_multiple: bool },
    Unique,
    Normal,
    Composite,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash, ReferenceSerialization)]
pub struct IndexInfo {
    pub(crate) meta: IndexMetaRef,
    pub(crate) range: Option<Range>,
    pub(crate) covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
    pub(crate) cover_mapping: Option<Vec<usize>>,
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
    ) -> Result<Vec<ScalarExpression>, DatabaseError> {
        let mut exprs = Vec::with_capacity(self.column_ids.len());

        for column_id in self.column_ids.iter() {
            if let Some(column) = table.get_column_by_id(column_id) {
                exprs.push(ScalarExpression::column_expr(column.clone()));
            } else {
                return Err(DatabaseError::ColumnNotFound(column_id.to_string()));
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

        if let Some(range) = &self.range {
            write!(f, "{range}")?;
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
