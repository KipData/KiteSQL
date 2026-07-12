// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::catalog::ColumnRef;
use crate::expression::window::WindowFunction;
use crate::expression::ScalarExpression;
use crate::iter_ext::Itertools;
use crate::planner::operator::sort::SortField;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct WindowOperator {
    pub partition_by: Vec<ScalarExpression>,
    pub order_by: Vec<SortField>,
    pub functions: Vec<WindowFunction>,
    pub output_columns: Vec<ColumnRef>,
}

impl fmt::Display for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Window [{}]",
            self.functions
                .iter()
                .map(|expr| format!("{expr:?}"))
                .join(", ")
        )?;
        if !self.partition_by.is_empty() || !self.order_by.is_empty() {
            write!(f, " ->")?;
        }
        if !self.partition_by.is_empty() {
            write!(
                f,
                " Partition By [{}]",
                self.partition_by.iter().map(ToString::to_string).join(", ")
            )?;
        }
        if !self.order_by.is_empty() {
            write!(
                f,
                " Order By [{}]",
                self.order_by.iter().map(ToString::to_string).join(", ")
            )?;
        }
        Ok(())
    }
}
