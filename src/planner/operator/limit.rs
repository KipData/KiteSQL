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

use super::Operator;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct LimitOperator {
    pub offset: Option<usize>,
    pub limit: Option<usize>,
}

impl LimitOperator {
    pub fn build(
        offset: Option<usize>,
        limit: Option<usize>,
        children: LogicalPlan,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Limit(LimitOperator { offset, limit }),
            Childrens::Only(Box::new(children)),
        )
    }
}

impl fmt::Display for LimitOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Some(limit) = self.limit {
            write!(f, "Limit {limit}")?;
        }
        if self.limit.is_some() && self.offset.is_some() {
            write!(f, ", ")?;
        }
        if let Some(offset) = self.offset {
            write!(f, "Offset {offset}")?;
        }

        Ok(())
    }
}
