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

use crate::planner::{fmt_explain_list, Explain, ExprRef, PlanArena};
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct SortField {
    pub expr: ExprRef,
    pub asc: bool,
    pub nulls_first: bool,
}

impl SortField {
    pub fn new(expr: ExprRef, asc: bool, nulls_first: bool) -> Self {
        SortField {
            expr,
            asc,
            nulls_first,
        }
    }

    pub fn asc(mut self) -> Self {
        self.asc = true;
        self
    }

    pub fn desc(mut self) -> Self {
        self.asc = false;
        self
    }

    pub fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }

    pub fn nulls_last(mut self) -> Self {
        self.nulls_first = false;
        self
    }
}

impl From<ExprRef> for SortField {
    fn from(expr: ExprRef) -> Self {
        SortField::new(expr, true, false)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct SortOperator {
    pub sort_fields: Vec<SortField>,
}

impl Explain for SortOperator {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Sort By ")?;
        fmt_explain_list(&self.sort_fields, ", ", arena, f)
    }
}

impl Explain for SortField {
    fn fmt(&self, arena: &PlanArena<'_>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let direction = if self.asc { "Asc" } else { "Desc" };
        let nulls = if self.nulls_first {
            "Nulls First"
        } else {
            "Nulls Last"
        };
        write!(f, "{} {direction} {nulls}", self.expr.explain(arena))
    }
}
