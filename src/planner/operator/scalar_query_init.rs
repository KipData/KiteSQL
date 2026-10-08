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
use crate::expression::ScalarExpression;
use crate::planner::{Childrens, Explain, ExprRef, LogicalPlan, MetaArena, ScalarQueryRef};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct ScalarQueryInitOperator {
    pub value: ExprRef,
    pub param_bindings: Vec<(ScalarQueryRef, ExprRef)>,
}

impl ScalarQueryInitOperator {
    pub fn build(
        left: LogicalPlan,
        right: LogicalPlan,
        value: ExprRef,
        param_bindings: Vec<(ScalarQueryRef, ExprRef)>,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::ScalarQueryInit(Self {
                value,
                param_bindings,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        )
    }

    pub(crate) fn reference(&self, arena: &dyn MetaArena) -> ScalarQueryRef {
        match arena.expression(self.value) {
            ScalarExpression::InitValue { id, .. } | ScalarExpression::OuterValue { id, .. } => *id,
            _ => unreachable!("scalar initializer requires a value marker"),
        }
    }
}

impl Explain for ScalarQueryInitOperator {
    fn fmt(&self, arena: &dyn MetaArena, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScalarQueryInit {}", self.value.explain(arena))
    }
}
