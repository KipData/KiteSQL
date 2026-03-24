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
use crate::expression::ScalarExpression;
use crate::optimizer::core::memo::Memo;
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::heuristic::batch::{
    HepBatch, HepBatchStep, HepBatchStrategy, HepWholeTreePass,
};
use crate::optimizer::heuristic::matcher::PlanMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::{
    annotate_sort_preserving_indexes, annotate_stream_distinct_indexes,
    bind_expression_position_current, constant_calculation_current, evaluator_bind_current,
    NormalizationRuleImpl, WholeTreePassKind,
};
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use std::ops::Not;

pub struct HepOptimizer<'a> {
    before_batches: &'a [HepBatch],
    after_batches: &'a [HepBatch],
    implementations: &'a [ImplementationRuleImpl],
    plan: LogicalPlan,
}

impl<'a> HepOptimizer<'a> {
    pub fn new(
        plan: LogicalPlan,
        before_batches: &'a [HepBatch],
        after_batches: &'a [HepBatch],
        implementations: &'a [ImplementationRuleImpl],
    ) -> Self {
        Self {
            before_batches,
            after_batches,
            implementations,
            plan,
        }
    }

    pub fn find_best<T: Transaction>(
        mut self,
        loader: Option<&StatisticMetaLoader<'_, T>>,
    ) -> Result<LogicalPlan, DatabaseError> {
        Self::apply_batches(&mut self.plan, self.before_batches)?;
        annotate_sort_preserving_indexes(&mut self.plan);
        annotate_stream_distinct_indexes(&mut self.plan);

        if let Some(loader) = loader {
            if self.implementations.is_empty().not() {
                let memo = Memo::new(&self.plan, loader, self.implementations)?;
                Memo::annotate_plan(&memo, &mut self.plan);
            }
        }
        Self::apply_batches(&mut self.plan, self.after_batches)?;

        Ok(self.plan)
    }

    #[inline]
    fn apply_batches(plan: &mut LogicalPlan, batches: &[HepBatch]) -> Result<(), DatabaseError> {
        for batch in batches {
            match batch.strategy {
                HepBatchStrategy::MaxTimes(max_iteration) => {
                    for _ in 0..max_iteration {
                        if !Self::apply_batch(plan, batch)? {
                            break;
                        }
                    }
                }
                HepBatchStrategy::LoopIfApplied => while Self::apply_batch(plan, batch)? {},
            }
        }
        Ok(())
    }

    #[inline]
    fn apply_batch(plan: &mut LogicalPlan, batch: &HepBatch) -> Result<bool, DatabaseError> {
        let mut applied = false;
        for step in &batch.steps {
            match step {
                HepBatchStep::WholeTree(pass) => {
                    if Self::apply_whole_tree_pass(plan, pass)? {
                        plan.reset_output_schema_cache_recursive();
                        applied = true;
                    }
                }
                HepBatchStep::LocalRewrite(rules) => {
                    for rule in rules {
                        if Self::apply_rule(plan, rule)? {
                            applied = true;
                        }
                    }
                }
            }
        }
        Ok(applied)
    }

    fn apply_whole_tree_pass(
        plan: &mut LogicalPlan,
        pass: &HepWholeTreePass,
    ) -> Result<bool, DatabaseError> {
        match pass.kind {
            WholeTreePassKind::ColumnPruning => {
                let mut applied = false;
                for rule in &pass.rules {
                    applied |= rule.apply(plan)?;
                }
                Ok(applied)
            }
            WholeTreePassKind::ExpressionRewrite => {
                let has_constant_calculation = pass
                    .rules
                    .iter()
                    .any(|rule| matches!(rule, NormalizationRuleImpl::ConstantCalculation));
                let has_bind_expression_position = pass
                    .rules
                    .iter()
                    .any(|rule| matches!(rule, NormalizationRuleImpl::BindExpressionPosition));
                let has_evaluator_bind = pass
                    .rules
                    .iter()
                    .any(|rule| matches!(rule, NormalizationRuleImpl::EvaluatorBind));

                Self::apply_expression_rewrite_pass(
                    plan,
                    has_constant_calculation,
                    has_bind_expression_position,
                    has_evaluator_bind,
                )?;
                Ok(true)
            }
        }
    }

    fn apply_expression_rewrite_pass(
        plan: &mut LogicalPlan,
        has_constant_calculation: bool,
        has_bind_expression_position: bool,
        has_evaluator_bind: bool,
    ) -> Result<(), DatabaseError> {
        let mut output_exprs = Vec::new();
        Self::apply_expression_rewrite_pass_inner(
            plan,
            &mut output_exprs,
            has_constant_calculation,
            has_bind_expression_position,
            has_evaluator_bind,
        )
    }

    fn apply_expression_rewrite_pass_inner(
        plan: &mut LogicalPlan,
        output_exprs: &mut Vec<ScalarExpression>,
        has_constant_calculation: bool,
        has_bind_expression_position: bool,
        has_evaluator_bind: bool,
    ) -> Result<(), DatabaseError> {
        let mut left_len = 0;
        match plan.childrens.as_mut() {
            Childrens::Only(child) => {
                Self::apply_expression_rewrite_pass_inner(
                    child,
                    output_exprs,
                    has_constant_calculation,
                    has_bind_expression_position,
                    has_evaluator_bind,
                )?;
            }
            Childrens::Twins { left, right } => {
                Self::apply_expression_rewrite_pass_inner(
                    left,
                    output_exprs,
                    has_constant_calculation,
                    has_bind_expression_position,
                    has_evaluator_bind,
                )?;
                if matches!(
                    plan.operator,
                    Operator::Join(_) | Operator::Union(_) | Operator::Except(_)
                ) {
                    let mut second_output_exprs = Vec::new();
                    Self::apply_expression_rewrite_pass_inner(
                        right,
                        &mut second_output_exprs,
                        has_constant_calculation,
                        has_bind_expression_position,
                        has_evaluator_bind,
                    )?;
                    left_len = output_exprs.len();
                    output_exprs.append(&mut second_output_exprs);
                }
            }
            Childrens::None => {}
        }

        if has_constant_calculation {
            constant_calculation_current(plan)?;
        }
        if has_bind_expression_position {
            bind_expression_position_current(output_exprs, plan, left_len)?;
        } else if let Some(exprs) = plan.operator.output_exprs() {
            *output_exprs = exprs;
        }
        if has_evaluator_bind {
            evaluator_bind_current(plan)?;
        }

        Ok(())
    }

    fn apply_rule(
        plan: &mut LogicalPlan,
        rule: &NormalizationRuleImpl,
    ) -> Result<bool, DatabaseError> {
        if PlanMatcher::new(rule.pattern(), plan).match_opt_expr() && rule.apply(plan)? {
            plan.reset_output_schema_cache_recursive();
            return Ok(true);
        }

        match plan.childrens.as_mut() {
            Childrens::Only(child) => {
                if Self::apply_rule(child, rule)? {
                    plan.reset_output_schema_cache();
                    return Ok(true);
                }
                Ok(false)
            }
            Childrens::Twins { left, right } => {
                if Self::apply_rule(left, rule)? {
                    plan.reset_output_schema_cache();
                    return Ok(true);
                }
                if Self::apply_rule(right, rule)? {
                    plan.reset_output_schema_cache();
                    return Ok(true);
                }
                Ok(false)
            }
            Childrens::None => Ok(false),
        }
    }
}

#[derive(Clone, Default)]
pub struct HepOptimizerPipeline {
    before_batches: Vec<HepBatch>,
    after_batches: Vec<HepBatch>,
    implementations: Vec<ImplementationRuleImpl>,
}

impl HepOptimizerPipeline {
    pub fn builder() -> HepOptimizerPipelineBuilder {
        HepOptimizerPipelineBuilder {
            before_batches: vec![],
            after_batches: vec![],
            implementations: vec![],
        }
    }

    pub fn new(
        before_batches: Vec<HepBatch>,
        after_batches: Vec<HepBatch>,
        implementations: Vec<ImplementationRuleImpl>,
    ) -> Self {
        Self {
            before_batches,
            after_batches,
            implementations,
        }
    }

    pub fn instantiate(&self, plan: LogicalPlan) -> HepOptimizer<'_> {
        HepOptimizer::new(
            plan,
            &self.before_batches,
            &self.after_batches,
            &self.implementations,
        )
    }
}

pub struct HepOptimizerPipelineBuilder {
    before_batches: Vec<HepBatch>,
    after_batches: Vec<HepBatch>,
    implementations: Vec<ImplementationRuleImpl>,
}

impl HepOptimizerPipelineBuilder {
    pub fn before_batch(
        mut self,
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        self.before_batches
            .push(HepBatch::new(name, strategy, rules));
        self
    }

    pub fn after_batch(
        mut self,
        name: String,
        strategy: HepBatchStrategy,
        rules: Vec<NormalizationRuleImpl>,
    ) -> Self {
        self.after_batches
            .push(HepBatch::new(name, strategy, rules));
        self
    }

    pub fn implementations(mut self, implementations: Vec<ImplementationRuleImpl>) -> Self {
        self.implementations = implementations;
        self
    }

    pub fn build(self) -> HepOptimizerPipeline {
        HepOptimizerPipeline::new(
            self.before_batches,
            self.after_batches,
            self.implementations,
        )
    }
}
