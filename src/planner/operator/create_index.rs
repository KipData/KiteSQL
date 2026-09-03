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

use crate::catalog::{ColumnRef, TableName};
use crate::planner::{fmt_explain_list, Explain, PlanArena};
use crate::types::index::IndexType;
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CreateIndexOperator {
    pub table_name: TableName,
    /// List of columns of the index
    pub columns: Vec<ColumnRef>,
    pub index_name: String,
    pub if_not_exists: bool,
    pub ty: IndexType,
}

impl Explain for CreateIndexOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Create Index On {} -> [", self.table_name)?;
        fmt_explain_list(&self.columns, ", ", arena, f)?;
        write!(f, "], If Not Exists: {}", self.if_not_exists)
    }
}
