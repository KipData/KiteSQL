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

use crate::planner::MetaArena;
use crate::planner::{fmt_explain_list, Explain, ExprRef};
use crate::types::tuple::Schema;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt::{self, Formatter};

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct ValuesOperator {
    pub(crate) rows: Vec<ExprRef>,
    pub(crate) row_count: usize,
    pub schema_ref: Schema,
}

impl ValuesOperator {
    pub fn new(rows: Vec<ExprRef>, row_count: usize, schema_ref: Schema) -> Self {
        assert_eq!(
            Some(rows.len()),
            row_count.checked_mul(schema_ref.len()),
            "VALUES row width must match its schema"
        );
        Self {
            rows,
            row_count,
            schema_ref,
        }
    }
}

impl Explain for ValuesOperator {
    fn fmt(&self, arena: &(dyn MetaArena + '_), f: &mut Formatter) -> fmt::Result {
        f.write_str("Values ")?;
        let width = self.schema_ref.len();
        for i in 0..self.row_count {
            let row = &self.rows[i * width..(i + 1) * width];
            if i != 0 {
                f.write_str(", ")?;
            }
            f.write_str("[")?;
            fmt_explain_list(row, ", ", arena, f)?;
            f.write_str("]")?;
        }
        write!(f, ", RowsLen: {}", self.row_count)
    }
}
