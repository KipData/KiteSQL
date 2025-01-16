use super::Operator;
use crate::catalog::{ColumnRef, TableCatalog, TableName};
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Bounds;
use crate::types::index::IndexInfo;
use crate::types::ColumnId;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct TableScanOperator {
    pub(crate) table_name: TableName,
    pub(crate) primary_keys: Vec<ColumnId>,
    pub(crate) columns: Vec<(usize, ColumnRef)>,
    // Support push down limit.
    pub(crate) limit: Bounds,

    // Support push down predicate.
    // If pre_where is simple predicate, for example:  a > 1 then can calculate directly when read data.
    pub(crate) index_infos: Vec<IndexInfo>,
}

impl TableScanOperator {
    pub fn build(table_name: TableName, table_catalog: &TableCatalog) -> LogicalPlan {
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
            .collect_vec();
        let index_infos = table_catalog
            .indexes
            .iter()
            .map(|meta| IndexInfo {
                meta: meta.clone(),
                range: None,
            })
            .collect_vec();

        LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                index_infos,
                table_name,
                primary_keys,
                columns,
                limit: (None, None),
            }),
            Childrens::None,
        )
    }
}

impl fmt::Display for TableScanOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let projection_columns = self
            .columns
            .iter()
            .map(|(_, column)| column.name().to_string())
            .join(", ");
        let (offset, limit) = self.limit;

        write!(
            f,
            "TableScan {} -> [{}]",
            self.table_name, projection_columns
        )?;
        if let Some(limit) = limit {
            write!(f, ", Limit: {}", limit)?;
        }
        if let Some(offset) = offset {
            write!(f, ", Offset: {}", offset)?;
        }

        Ok(())
    }
}
