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
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use std::cmp::Ordering;

pub type BestPhysicalOption = Option<(PhysicalOption, Option<usize>)>;

// TODO: Use indexing and other methods for matching optimization to avoid traversal
pub trait MatchPattern {
    fn pattern(&self) -> &Pattern;
}

pub struct NormalizationContext {
    runtime_param_count: usize,
}

impl NormalizationContext {
    pub fn new() -> Self {
        Self {
            runtime_param_count: 0,
        }
    }

    pub fn alloc_runtime_param(&mut self) -> usize {
        let param = self.runtime_param_count;
        self.runtime_param_count += 1;
        param
    }

    pub fn runtime_param_count(&self) -> usize {
        self.runtime_param_count
    }
}

impl Default for NormalizationContext {
    fn default() -> Self {
        Self::new()
    }
}

pub trait NormalizationRule {
    /// Returns true when the plan tree is modified.
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        ctx: &mut NormalizationContext,
    ) -> Result<bool, DatabaseError>;
}

fn compare_costs(candidate_cost: Option<usize>, best_cost: Option<usize>) -> Ordering {
    match (candidate_cost, best_cost) {
        (Some(candidate_cost), Some(best_cost)) => candidate_cost.cmp(&best_cost),
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (None, None) => Ordering::Equal,
    }
}

pub fn keep_best_physical_option(
    best_physical_option: &mut BestPhysicalOption,
    option: PhysicalOption,
    cost: Option<usize>,
) {
    let should_replace = match best_physical_option.as_ref() {
        Some((_, best_cost)) => compare_costs(cost, *best_cost).is_lt(),
        None => true,
    };

    if should_replace {
        *best_physical_option = Some((option, cost));
    }
}

pub trait ImplementationRule<T: Transaction>: MatchPattern {
    fn update_best_option(
        &self,
        op: &Operator,
        loader: &StatisticMetaLoader<T>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError>;
}
