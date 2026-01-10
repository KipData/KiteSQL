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

use crate::optimizer::rule::normalization::NormalizationRuleImpl;

/// A batch of rules.
#[derive(Clone)]
pub struct HepBatch {
    #[allow(dead_code)]
    pub name: String,
    pub strategy: HepBatchStrategy,
    pub rules: Vec<NormalizationRuleImpl>,
}

impl HepBatch {
    pub fn new(
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        Self {
            name,
            strategy,
            rules,
        }
    }
}

#[derive(Clone, Copy)]
pub enum HepBatchStrategy {
    /// An execution_ap strategy for rules that indicates the maximum number of executions. If the
    /// execution_ap reaches fix point (i.e. converge) before maxIterations, it will stop.
    ///
    /// Fix Point means that plan tree not changed after applying all rules.
    MaxTimes(usize),
    #[allow(dead_code)]
    LoopIfApplied,
}

impl HepBatchStrategy {
    pub fn once_topdown() -> Self {
        HepBatchStrategy::MaxTimes(1)
    }

    pub fn fix_point_topdown(max_iteration: usize) -> Self {
        HepBatchStrategy::MaxTimes(max_iteration)
    }

    #[allow(dead_code)]
    pub fn loop_if_applied() -> Self {
        HepBatchStrategy::LoopIfApplied
    }
}
