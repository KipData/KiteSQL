use crate::catalog::TableName;
use crate::types::index::IndexMetaRef;
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct AnalyzeOperator {
    pub table_name: TableName,
    pub index_metas: Vec<IndexMetaRef>,
}
