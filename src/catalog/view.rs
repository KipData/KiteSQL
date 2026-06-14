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
use crate::planner::{LogicalPlan, MetaArena};
use crate::types::tuple::Schema;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, Clone, Hash, Eq, PartialEq, ReferenceSerialization)]
pub struct View {
    pub name: TableName,
    pub plan: Box<LogicalPlan>,
    pub schema: Schema,
}

impl View {
    pub(crate) fn visit_column_refs<A, F>(&self, arena: &mut A, f: &mut F)
    where
        A: MetaArena,
        F: FnMut(&ColumnRef) + ?Sized,
    {
        for column in &self.schema {
            f(column);
        }
        self.plan.visit_column_refs(arena, f);
    }
}

impl fmt::Display for View {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "View {}", self.name)?;

        Ok(())
    }
}
