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

use crate::errors::DatabaseError;
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::join::{JoinCondition, JoinOperator};
use crate::planner::operator::{Operator, PhysicalOption};
use crate::storage::Transaction;
use std::sync::LazyLock;

static JOIN_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Join(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct JoinImplementation;

impl MatchPattern for JoinImplementation {
    fn pattern(&self) -> &Pattern {
        &JOIN_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for JoinImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        _: &StatisticMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        let mut physical_option = PhysicalOption::NestLoopJoin;

        if let Operator::Join(JoinOperator {
            on: JoinCondition::On { on, .. },
            ..
        }) = op
        {
            if !on.is_empty() {
                physical_option = PhysicalOption::HashJoin;
            }
        }
        group_expr.append_expr(Expression {
            op: physical_option,
            cost: None,
        });
        Ok(())
    }
}
