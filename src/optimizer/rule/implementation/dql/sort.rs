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
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::storage::Transaction;
use std::sync::LazyLock;

static SORT_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::Sort(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct SortImplementation;

impl MatchPattern for SortImplementation {
    fn pattern(&self) -> &Pattern {
        &SORT_PATTERN
    }
}

impl<T: Transaction> ImplementationRule<T> for SortImplementation {
    fn to_expression(
        &self,
        op: &Operator,
        _: &StatisticMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        if let Operator::Sort(op) = op {
            group_expr.append_expr(Expression {
                op: PhysicalOption::new(
                    PlanImpl::Sort,
                    SortOption::OrderBy {
                        fields: op.sort_fields.clone(),
                        ignore_prefix_len: 0,
                    },
                ),
                cost: None,
            });
        }

        Ok(())
    }
}
