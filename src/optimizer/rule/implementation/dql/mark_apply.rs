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
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::{keep_best_physical_option, BestPhysicalOption};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::mark_apply::MarkApplyKind;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, RowCountOption, SortOption};
use crate::planner::{LogicalPlan, PlanArena};
use std::sync::LazyLock;

static MARK_APPLY_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::MarkApply(_)),
});

#[derive(Clone)]
pub struct MarkApplyImplementation;

impl MatchPattern for MarkApplyImplementation {
    fn pattern(&self) -> &Pattern {
        &MARK_APPLY_PATTERN
    }
}

impl ImplementationRule for MarkApplyImplementation {
    fn update_best_option(
        &self,
        plan: &LogicalPlan,
        _: &PlanArena,
        _: &StatisticMetaLoader<'_>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        let Operator::MarkApply(op) = &plan.operator else {
            return Ok(());
        };
        // A mark adds one value to each left row; INNER JOIN may duplicate rows.
        let rows = if matches!(op.kind, MarkApplyKind::InnerJoin) {
            RowCountOption::Unknown
        } else {
            RowCountOption::Follow
        };
        keep_best_physical_option(
            best_physical_option,
            PhysicalOption::new(PlanImpl::MarkApply, SortOption::Follow, rows),
            None,
        );
        Ok(())
    }
}
