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

use crate::catalog::table::DmlTable;
use crate::planner::{fmt_table, Explain, ExprRef, MetaArena};
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct InsertOperator {
    pub(crate) table: DmlTable,
    pub is_overwrite: bool,
    pub on_duplicate_key_update: Option<OnDuplicateKeyUpdate>,
}

/// The `*_rows` fields are the affected rows reported per input row, resolved from
/// [`SqlDialect::upsert_affected_rows`](crate::dialect::SqlDialect::upsert_affected_rows).
#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct OnDuplicateKeyUpdate {
    /// Values are evaluated against the existing row (`0..n`) followed by the proposed row
    /// (`n..2n`).
    pub assignments: Vec<(usize, ExprRef)>,
    pub inserted_rows: u64,
    pub updated_rows: u64,
    pub unchanged_rows: u64,
}

impl Explain for InsertOperator {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str("Insert ")?;
        fmt_table(self.table.table_id, arena, f)?;
        write!(f, ", Is Overwrite: {}", self.is_overwrite)?;
        if self.on_duplicate_key_update.is_some() {
            f.write_str(", On Duplicate Key Update: enabled")?;
        }
        Ok(())
    }
}
