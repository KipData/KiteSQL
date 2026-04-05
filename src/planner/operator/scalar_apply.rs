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
pub struct ScalarApplyOperator;

impl ScalarApplyOperator {
    pub fn build(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
        LogicalPlan::new(
            Operator::ScalarApply(ScalarApplyOperator),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        )
    }
}

impl fmt::Display for ScalarApplyOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "ScalarApply")
    }
}
