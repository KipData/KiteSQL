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

use crate::optimizer::rule::normalization::{
    NormalizationPassKind, NormalizationRuleImpl, NormalizationRuleRootTag, WholeTreePassKind,
};
use crate::planner::operator::Operator;
use std::array;

/// A batch of rules.
#[derive(Clone)]
pub struct HepBatch {
    #[allow(dead_code)]
    pub name: String,
    pub strategy: HepBatchStrategy,
    pub steps: Vec<HepBatchStep>,
}

#[derive(Clone)]
pub enum HepBatchStep {
    WholeTree(HepWholeTreePass),
    LocalRewrite(HepLocalRewriteBatch),
}

#[derive(Clone)]
pub struct HepWholeTreePass {
    pub kind: WholeTreePassKind,
    pub rules: Vec<NormalizationRuleImpl>,
}

#[derive(Clone)]
pub struct HepLocalRewriteBatch {
    pub rules: Vec<NormalizationRuleImpl>,
    groups: [Vec<usize>; NormalizationRuleRootTag::COUNT],
}

impl HepLocalRewriteBatch {
    fn new(rules: Vec<NormalizationRuleImpl>) -> Self {
        let mut groups = array::from_fn(|_| Vec::new());
        for (idx, rule) in rules.iter().enumerate() {
            groups[rule.root_tag() as usize].push(idx);
        }
        Self { rules, groups }
    }

    fn push(&mut self, rule: NormalizationRuleImpl) {
        let idx = self.rules.len();
        self.groups[rule.root_tag() as usize].push(idx);
        self.rules.push(rule);
    }

    pub fn len(&self) -> usize {
        self.rules.len()
    }

    pub fn next_matching_rule_index_from(
        &self,
        operator: &Operator,
        start_idx: usize,
    ) -> Option<usize> {
        let any_group = &self.groups[NormalizationRuleRootTag::Any as usize];
        let any_idx = any_group.iter().copied().find(|idx| *idx >= start_idx);

        let specific_idx = NormalizationRuleRootTag::from_operator(operator).and_then(|tag| {
            self.groups[tag as usize]
                .iter()
                .copied()
                .find(|idx| *idx >= start_idx)
        });

        match (any_idx, specific_idx) {
            (Some(any_idx), Some(specific_idx)) => Some(any_idx.min(specific_idx)),
            (Some(any_idx), None) => Some(any_idx),
            (None, Some(specific_idx)) => Some(specific_idx),
            (None, None) => None,
        }
    }
}

impl HepBatch {
    pub fn new(
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        let mut steps = Vec::new();

        for rule in rules {
            match rule.pass_kind() {
                NormalizationPassKind::WholeTreePass(kind) => match steps.last_mut() {
                    Some(HepBatchStep::WholeTree(pass)) if pass.kind == kind => {
                        pass.rules.push(rule);
                    }
                    _ => steps.push(HepBatchStep::WholeTree(HepWholeTreePass {
                        kind,
                        rules: vec![rule],
                    })),
                },
                NormalizationPassKind::LocalRewrite => match steps.last_mut() {
                    Some(HepBatchStep::LocalRewrite(local_rules)) => local_rules.push(rule),
                    _ => steps.push(HepBatchStep::LocalRewrite(HepLocalRewriteBatch::new(vec![
                        rule,
                    ]))),
                },
            }
        }

        Self {
            name,
            strategy,
            steps,
        }
    }
}

#[derive(Clone, Copy)]
pub enum HepMatchOrder {
    TopDown,
    BottomUp,
}

#[derive(Clone, Copy)]
pub enum HepBatchStrategy {
    /// Each local rule may succeed once across the tree.
    Once(HepMatchOrder),
    /// Stop at a fix point or after the configured number of full-tree iterations.
    MaxTimes {
        max_iteration: usize,
        order: HepMatchOrder,
    },
    #[allow(dead_code)]
    LoopIfApplied(HepMatchOrder),
}

impl HepBatchStrategy {
    pub fn once_topdown() -> Self {
        Self::Once(HepMatchOrder::TopDown)
    }

    pub fn once_bottomup() -> Self {
        Self::Once(HepMatchOrder::BottomUp)
    }

    pub fn fix_point_topdown(max_iteration: usize) -> Self {
        Self::MaxTimes {
            max_iteration,
            order: HepMatchOrder::TopDown,
        }
    }

    pub fn fix_point_bottomup(max_iteration: usize) -> Self {
        Self::MaxTimes {
            max_iteration,
            order: HepMatchOrder::BottomUp,
        }
    }

    #[allow(dead_code)]
    pub fn loop_if_applied() -> Self {
        Self::LoopIfApplied(HepMatchOrder::TopDown)
    }
}
