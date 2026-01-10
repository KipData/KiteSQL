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
use crate::optimizer::core::memo::Memo;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::heuristic::batch::{HepBatch, HepBatchStrategy};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::heuristic::matcher::HepMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use std::ops::Not;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    pub graph: HepGraph,
    implementations: Vec<ImplementationRuleImpl>,
}

impl HepOptimizer {
    pub fn new(root: LogicalPlan) -> Self {
        Self {
            batches: vec![],
            graph: HepGraph::new(root),
            implementations: vec![],
        }
    }

    pub fn batch(
        mut self,
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        self.batches.push(HepBatch::new(name, strategy, rules));
        self
    }

    pub fn implementations(mut self, implementations: Vec<ImplementationRuleImpl>) -> Self {
        self.implementations = implementations;
        self
    }

    pub fn find_best<T: Transaction>(
        mut self,
        loader: Option<&StatisticMetaLoader<'_, T>>,
    ) -> Result<LogicalPlan, DatabaseError> {
        for ref batch in self.batches {
            match batch.strategy {
                HepBatchStrategy::MaxTimes(max_iteration) => {
                    for _ in 0..max_iteration {
                        if !Self::apply_batch(&mut self.graph, batch)? {
                            break;
                        }
                    }
                }
                HepBatchStrategy::LoopIfApplied => {
                    while Self::apply_batch(&mut self.graph, batch)? {}
                }
            }
        }
        let memo = loader
            .and_then(|loader| {
                self.implementations
                    .is_empty()
                    .not()
                    .then(|| Memo::new(&self.graph, loader, &self.implementations))
            })
            .transpose()?;

        self.graph
            .into_plan(memo.as_ref())
            .ok_or(DatabaseError::EmptyPlan)
    }

    fn apply_batch(
        graph: *mut HepGraph,
        HepBatch { rules, .. }: &HepBatch,
    ) -> Result<bool, DatabaseError> {
        let before_version = unsafe { &*graph }.version;

        for rule in rules {
            // SAFETY: after successfully modifying the graph, the iterator is no longer used.
            for node_id in unsafe { &*graph }.nodes_iter(None) {
                if Self::apply_rule(unsafe { &mut *graph }, rule, node_id)? {
                    break;
                }
            }
        }

        Ok(before_version != unsafe { &*graph }.version)
    }

    fn apply_rule(
        graph: &mut HepGraph,
        rule: &NormalizationRuleImpl,
        node_id: HepNodeId,
    ) -> Result<bool, DatabaseError> {
        let before_version = graph.version;

        if HepMatcher::new(rule.pattern(), node_id, graph).match_opt_expr() {
            rule.apply(node_id, graph)?;
        }

        Ok(before_version != graph.version)
    }
}
