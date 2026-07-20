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
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{BestPhysicalOption, ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption, SortOption};
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

impl ImplementationRule for JoinImplementation {
    fn update_best_option(
        &self,
        op: &Operator,
        _: &crate::planner::PlanArena,
        _: &StatisticMetaLoader<'_>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        let Operator::Join(operator) = op else {
            unreachable!()
        };
        let physical_option = PhysicalOption::new(operator.plan_impl(), SortOption::None);
        crate::optimizer::core::rule::keep_best_physical_option(
            best_physical_option,
            physical_option,
            None,
        );
        Ok(())
    }
}
