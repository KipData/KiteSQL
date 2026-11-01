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

use crate::binder::copy::ExtSource;
use crate::catalog::TableId;
use crate::planner::MetaArena;
use crate::planner::{fmt_explain_list, fmt_table, Explain};
use crate::types::tuple::Schema;
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CopyFromFileOperator {
    pub table_id: TableId,
    pub source: ExtSource,
    pub schema_ref: Schema,
}

impl Explain for CopyFromFileOperator {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "Copy {} -> ", self.source.path.display())?;
        fmt_table(self.table_id, arena, f)?;
        f.write_str(" [")?;
        fmt_explain_list(&self.schema_ref, ", ", arena, f)?;
        f.write_str("]")
    }
}
