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
use crate::expression::ScalarExpression;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct UpdateOperator {
    pub table_name: TableName,
    pub value_exprs: Vec<(ColumnRef, ScalarExpression)>,
}

impl fmt::Display for UpdateOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let values = self
            .value_exprs
            .iter()
            .map(|(column, expr)| format!("{} -> {}", column.full_name(), expr))
            .join(", ");
        write!(f, "Update {} set {}", self.table_name, values)?;

        Ok(())
    }
}
