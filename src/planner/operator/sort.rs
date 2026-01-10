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

use crate::expression::ScalarExpression;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct SortField {
    pub expr: ScalarExpression,
    pub asc: bool,
    pub nulls_first: bool,
}

impl SortField {
    pub fn new(expr: ScalarExpression, asc: bool, nulls_first: bool) -> Self {
        SortField {
            expr,
            asc,
            nulls_first,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct SortOperator {
    pub sort_fields: Vec<SortField>,
    /// Support push down limit to sort plan.
    pub limit: Option<usize>,
}

impl fmt::Display for SortOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let sort_fields = self
            .sort_fields
            .iter()
            .map(|sort_field| format!("{sort_field}"))
            .join(", ");
        write!(f, "Sort By {sort_fields}")?;

        if let Some(limit) = self.limit {
            write!(f, ", Limit {limit}")?;
        }

        Ok(())
    }
}

impl fmt::Display for SortField {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.asc {
            write!(f, " Asc")?;
        } else {
            write!(f, " Desc")?;
        }
        if self.nulls_first {
            write!(f, " Nulls First")?;
        } else {
            write!(f, " Nulls Last")?;
        }

        Ok(())
    }
}
