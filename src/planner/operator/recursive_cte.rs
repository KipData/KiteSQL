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

use crate::iter_ext::Itertools;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::types::tuple::Schema;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct RecursiveCteOperator {
    pub schema_ref: Schema,
}

impl RecursiveCteOperator {
    pub fn build(schema_ref: Schema, anchor: LogicalPlan, recursive: LogicalPlan) -> LogicalPlan {
        LogicalPlan::new(
            Operator::RecursiveCte(Self { schema_ref }),
            Childrens::Twins {
                left: Box::new(anchor),
                right: Box::new(recursive),
            },
        )
    }
}

impl fmt::Display for RecursiveCteOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Recursive CTE: [{}]", self.schema_ref.iter().join(", "))
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct RecursiveScanOperator {
    pub schema_ref: Schema,
}

impl fmt::Display for RecursiveScanOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Recursive Scan: [{}]", self.schema_ref.iter().join(", "))
    }
}
