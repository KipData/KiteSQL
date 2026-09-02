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

use crate::catalog::TableName;
use crate::planner::{Explain, ExprRef, PlanArena};
use crate::types::LogicalType;
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum DefaultChange {
    NoChange,
    Set(ExprRef),
    Drop,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum NotNullChange {
    NoChange,
    Set,
    Drop,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct ChangeColumnOperator {
    pub table_name: TableName,
    pub old_column_name: String,
    pub new_column_name: String,
    pub data_type: LogicalType,
    pub default_change: DefaultChange,
    pub not_null_change: NotNullChange,
}

impl Explain for ChangeColumnOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Change {} -> {}.{} ({}, ",
            self.old_column_name, self.table_name, self.new_column_name, self.data_type
        )?;
        match &self.default_change {
            DefaultChange::NoChange => f.write_str("NoChange")?,
            DefaultChange::Set(expr) => write!(f, "Set({})", expr.explain(arena))?,
            DefaultChange::Drop => f.write_str("Drop")?,
        }
        write!(f, ", {:?})", self.not_null_change)
    }
}
