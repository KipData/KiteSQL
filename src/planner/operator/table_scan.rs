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

use super::{Operator, SortOption};
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::planner::operator::sort::SortField;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Bounds;
use crate::types::index::IndexInfo;
use crate::types::ColumnId;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct TableScanOperator {
    pub(crate) table_name: TableName,
    pub(crate) primary_keys: Vec<ColumnId>,
    #[rustfmt::skip]
    pub(crate) columns: BTreeMap::<usize, ColumnRef>,
    // Support push down limit.
    pub(crate) limit: Bounds,

    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub(crate) index_infos: Vec<IndexInfo>,
    pub(crate) with_pk: bool,
}

impl TableScanOperator {
    pub fn build(
        table_name: TableName,
        table_catalog: &TableCatalog,
        with_pk: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let primary_keys = table_catalog
            .primary_keys()
            .iter()
            .filter_map(|(_, column)| column.id())
            .collect_vec();
        // Fill all Columns in TableCatalog by default
        let columns = table_catalog
            .columns()
            .enumerate()
            .map(|(i, column)| (i, column.clone()))
            .collect();
        let mut index_infos = Vec::with_capacity(table_catalog.indexes.len());

        for index_meta in table_catalog.indexes.iter() {
            let mut sort_fields = Vec::with_capacity(index_meta.column_ids.len());
            for col_id in &index_meta.column_ids {
                let column = table_catalog.get_column_by_id(col_id).ok_or_else(|| {
                    DatabaseError::ColumnNotFound(format!("index column id: {col_id} not found"))
                })?;
                sort_fields.push(SortField {
                    expr: ScalarExpression::column_expr(column.clone()),
                    asc: true,
                    nulls_first: true,
                })
            }

            index_infos.push(IndexInfo {
                meta: index_meta.clone(),
                sort_option: SortOption::OrderBy {
                    fields: sort_fields,
                    ignore_prefix_len: 0,
                },
                range: None,
                covered_deserializers: None,
                cover_mapping: None,
                sort_elimination_hint: None,
                stream_distinct_hint: None,
            });
        }

        Ok(LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                index_infos,
                table_name,
                primary_keys,
                columns,
                limit: (None, None),
                with_pk,
            }),
            Childrens::None,
        ))
    }
}

impl fmt::Display for TableScanOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let projection_columns = self
            .columns
            .values()
            .map(|column| column.name().to_string())
            .join(", ");
        let (offset, limit) = self.limit;

        write!(
            f,
            "TableScan {} -> [{}]",
            self.table_name, projection_columns
        )?;
        if let Some(limit) = limit {
            write!(f, ", Limit: {limit}")?;
        }
        if let Some(offset) = offset {
            write!(f, ", Offset: {offset}")?;
        }

        Ok(())
    }
}
