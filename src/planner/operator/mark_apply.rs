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
use crate::catalog::ColumnRef;
use crate::expression::ScalarExpression;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum MarkApplyKind {
    Exists,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct MarkApplyOperator {
    pub kind: MarkApplyKind,
    predicates: Vec<ScalarExpression>,
    output_column: ColumnRef,
}

impl MarkApplyOperator {
    pub fn new_exists(output_column: ColumnRef, predicates: Vec<ScalarExpression>) -> Self {
        Self {
            kind: MarkApplyKind::Exists,
            predicates,
            output_column,
        }
    }

    pub fn build_exists(
        left: LogicalPlan,
        right: LogicalPlan,
        output_column: ColumnRef,
        predicates: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::MarkApply(MarkApplyOperator::new_exists(output_column, predicates)),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        )
    }

    pub fn predicates(&self) -> &[ScalarExpression] {
        &self.predicates
    }

    pub fn predicates_mut(&mut self) -> &mut Vec<ScalarExpression> {
        &mut self.predicates
    }

    pub fn output_column(&self) -> &ColumnRef {
        &self.output_column
    }
}

impl fmt::Display for MarkApplyOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.kind {
            MarkApplyKind::Exists => write!(f, "MarkExistsApply"),
        }
    }
}
