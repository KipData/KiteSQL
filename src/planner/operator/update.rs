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
use crate::planner::MetaArena;
use crate::planner::{fmt_table, Explain, ExprRef};
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct UpdateOperator {
    pub(crate) table: DmlTable,
    pub value_exprs: Vec<(usize, ExprRef)>,
}

impl Explain for UpdateOperator {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str("Update ")?;
        fmt_table(self.table.table_id, arena, f)?;
        f.write_str(" set ")?;
        for (index, (position, expr)) in self.value_exprs.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            let column = self.table.columns[*position];
            write!(f, "{} -> {}", column.explain(arena), expr.explain(arena))?;
        }
        Ok(())
    }
}
