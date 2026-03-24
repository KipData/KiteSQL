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
use crate::optimizer::core::rule::{
    BestPhysicalOption, ImplementationRule, MatchPattern, NormalizationRule,
};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::heuristic::batch::{
    HepBatch, HepBatchStep, HepBatchStrategy, HepWholeTreePass,
};
use crate::optimizer::rule::implementation::{ImplementationRuleImpl, ImplementationRuleRootTag};
use crate::optimizer::rule::normalization::{
    apply_scan_order_hint, constant_calculation_current, distinct_sort_fields,
    evaluator_bind_current, NormalizationRuleImpl, OrderHintKind, WholeTreePassKind,
};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use std::array;
use std::ops::Not;

pub struct HepOptimizer<'a> {
    before_batches: &'a [HepBatch],
    after_batches: &'a [HepBatch],
    implementation_index: &'a ImplementationRuleIndex,
    plan: LogicalPlan,
}

impl<'a> HepOptimizer<'a> {
    fn new(
        plan: LogicalPlan,
        before_batches: &'a [HepBatch],
        after_batches: &'a [HepBatch],
        implementation_index: &'a ImplementationRuleIndex,
    ) -> Self {
        Self {
            before_batches,
            after_batches,
            implementation_index,
            plan,
        }
    }

    pub fn find_best<T: Transaction>(
        mut self,
        loader: Option<&StatisticMetaLoader<'_, T>>,
    ) -> Result<LogicalPlan, DatabaseError> {
        Self::apply_batches(&mut self.plan, self.before_batches)?;

        if let Some(loader) = loader {
            if self.implementation_index.is_empty().not() {
                let mut inherited_sort_hints = Vec::new();
                let mut inherited_stream_distinct_hints = Vec::new();
                Self::annotate_hints_and_physical_options(
                    &mut self.plan,
                    loader,
                    self.implementation_index,
                    &mut inherited_sort_hints,
                    &mut inherited_stream_distinct_hints,
                    0,
                    0,
                )?;
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
                    if Self::apply_local_rules(plan, rules)? {
                        applied = true;
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
                let has_evaluator_bind = pass
                    .rules
                    .iter()
                    .any(|rule| matches!(rule, NormalizationRuleImpl::EvaluatorBind));

                Self::apply_expression_rewrite_pass(
                    plan,
                    has_constant_calculation,
                    has_evaluator_bind,
                )?;
                Ok(true)
            }
        }
    }

    fn apply_expression_rewrite_pass(
        plan: &mut LogicalPlan,
        has_constant_calculation: bool,
        has_evaluator_bind: bool,
    ) -> Result<(), DatabaseError> {
        Self::apply_expression_rewrite_pass_inner(
            plan,
            has_constant_calculation,
            has_evaluator_bind,
        )
    }

    fn apply_expression_rewrite_pass_inner(
        plan: &mut LogicalPlan,
        has_constant_calculation: bool,
        has_evaluator_bind: bool,
    ) -> Result<(), DatabaseError> {
        match plan.childrens.as_mut() {
            Childrens::Only(child) => {
                Self::apply_expression_rewrite_pass_inner(
                    child,
                    has_constant_calculation,
                    has_evaluator_bind,
                )?;
            }
            Childrens::Twins { left, right } => {
                Self::apply_expression_rewrite_pass_inner(
                    left,
                    has_constant_calculation,
                    has_evaluator_bind,
                )?;
                Self::apply_expression_rewrite_pass_inner(
                    right,
                    has_constant_calculation,
                    has_evaluator_bind,
                )?;
            }
            Childrens::None => {}
        }

        if has_constant_calculation {
            constant_calculation_current(plan)?;
        }
        if has_evaluator_bind {
            evaluator_bind_current(plan)?;
        }

        Ok(())
    }

    fn annotate_hints_and_physical_options<'plan, T: Transaction>(
        plan: &'plan mut LogicalPlan,
        loader: &StatisticMetaLoader<'_, T>,
        implementation_index: &ImplementationRuleIndex,
        inherited_sort_hints: &mut Vec<SortHint<'plan>>,
        inherited_stream_distinct_hints: &mut Vec<SortHint<'plan>>,
        sort_hint_start: usize,
        stream_distinct_hint_start: usize,
    ) -> Result<(), DatabaseError> {
        if let Operator::TableScan(scan_op) = &mut plan.operator {
            for required in inherited_sort_hints[sort_hint_start..].iter() {
                apply_scan_order_hint(scan_op, required.as_slice(), OrderHintKind::SortElimination);
            }
            for required in inherited_stream_distinct_hints[stream_distinct_hint_start..].iter() {
                apply_scan_order_hint(scan_op, required.as_slice(), OrderHintKind::StreamDistinct);
            }
        }

        let mut best_physical_option: BestPhysicalOption = None;
        for rule in implementation_index.for_matching_operator(&plan.operator) {
            rule.update_best_option(&plan.operator, loader, &mut best_physical_option)?;
        }
        if let Some((option, _)) = best_physical_option {
            plan.physical_option = Some(option);
        }

        match (&plan.operator, plan.childrens.as_mut()) {
            (operator, Childrens::Only(child)) => {
                let propagate_hints = matches!(
                    operator,
                    Operator::Filter(_)
                        | Operator::Project(_)
                        | Operator::Limit(_)
                        | Operator::TopK(_)
                        | Operator::Sort(_)
                );

                let child_sort_hint_start = if propagate_hints {
                    sort_hint_start
                } else {
                    inherited_sort_hints.len()
                };
                let old_sort_hint_len = inherited_sort_hints.len();
                if let Operator::Sort(op) = operator {
                    inherited_sort_hints.push(SortHint::Borrowed(&op.sort_fields));
                }

                let child_stream_distinct_hint_start = if propagate_hints {
                    stream_distinct_hint_start
                } else {
                    inherited_stream_distinct_hints.len()
                };
                let old_stream_distinct_hint_len = inherited_stream_distinct_hints.len();

                if let Operator::Aggregate(op) = operator {
                    if op.is_distinct && op.agg_calls.is_empty() && !op.groupby_exprs.is_empty() {
                        inherited_stream_distinct_hints
                            .push(SortHint::Owned(distinct_sort_fields(&op.groupby_exprs)));
                    }
                }

                Self::annotate_hints_and_physical_options(
                    child,
                    loader,
                    implementation_index,
                    inherited_sort_hints,
                    inherited_stream_distinct_hints,
                    child_sort_hint_start,
                    child_stream_distinct_hint_start,
                )?;

                inherited_sort_hints.truncate(old_sort_hint_len);
                inherited_stream_distinct_hints.truncate(old_stream_distinct_hint_len);
            }
            (_, Childrens::Twins { left, right }) => {
                let left_sort_hint_start = inherited_sort_hints.len();
                let left_stream_distinct_hint_start = inherited_stream_distinct_hints.len();
                Self::annotate_hints_and_physical_options(
                    left,
                    loader,
                    implementation_index,
                    inherited_sort_hints,
                    inherited_stream_distinct_hints,
                    left_sort_hint_start,
                    left_stream_distinct_hint_start,
                )?;
                let right_sort_hint_start = inherited_sort_hints.len();
                let right_stream_distinct_hint_start = inherited_stream_distinct_hints.len();
                Self::annotate_hints_and_physical_options(
                    right,
                    loader,
                    implementation_index,
                    inherited_sort_hints,
                    inherited_stream_distinct_hints,
                    right_sort_hint_start,
                    right_stream_distinct_hint_start,
                )?;
            }
            (_, Childrens::None) => {}
        }

        Ok(())
    }

    fn apply_local_rules(
        plan: &mut LogicalPlan,
        rules: &[NormalizationRuleImpl],
    ) -> Result<bool, DatabaseError> {
        let mut applied_rules = vec![false; rules.len()];
        Self::apply_local_rules_inner(plan, rules, &mut applied_rules)
    }

    fn apply_local_rules_inner(
        plan: &mut LogicalPlan,
        rules: &[NormalizationRuleImpl],
        applied_rules: &mut [bool],
    ) -> Result<bool, DatabaseError> {
        let mut applied = false;

        for (idx, rule) in rules.iter().enumerate() {
            if applied_rules[idx] || !rule.root_tag().matches(&plan.operator) {
                continue;
            }
            if rule.apply(plan)? {
                plan.reset_output_schema_cache_recursive();
                applied_rules[idx] = true;
                applied = true;
            }
        }

        match plan.childrens.as_mut() {
            Childrens::Only(child) => {
                let child_applied = Self::apply_local_rules_inner(child, rules, applied_rules)?;
                applied |= child_applied;
            }
            Childrens::Twins { left, right } => {
                let left_applied = Self::apply_local_rules_inner(left, rules, applied_rules)?;
                let right_applied = Self::apply_local_rules_inner(right, rules, applied_rules)?;
                applied |= left_applied || right_applied;
            }
            Childrens::None => {}
        }

        if applied {
            plan.reset_output_schema_cache();
        }

        Ok(applied)
    }
}

enum SortHint<'a> {
    Borrowed(&'a [SortField]),
    Owned(Vec<SortField>),
}

impl SortHint<'_> {
    fn as_slice(&self) -> &[SortField] {
        match self {
            SortHint::Borrowed(fields) => fields,
            SortHint::Owned(fields) => fields.as_slice(),
        }
    }
}

#[derive(Clone, Default)]
struct ImplementationRuleIndex {
    groups: [Vec<ImplementationRuleImpl>; ImplementationRuleRootTag::COUNT],
}

impl ImplementationRuleIndex {
    fn new(implementations: Vec<ImplementationRuleImpl>) -> Self {
        let mut groups = array::from_fn(|_| Vec::new());
        for implementation in implementations {
            groups[implementation.root_tag() as usize].push(implementation);
        }
        Self { groups }
    }

    fn is_empty(&self) -> bool {
        self.groups.iter().all(Vec::is_empty)
    }

    fn for_matching_operator<'b>(
        &'b self,
        operator: &'b Operator,
    ) -> impl Iterator<Item = &'b ImplementationRuleImpl> + 'b {
        ImplementationRuleRootTag::from_operator(operator)
            .into_iter()
            .flat_map(move |tag| self.groups[tag as usize].iter())
            .filter(move |rule| (rule.pattern().predicate)(operator))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::{Binder, BinderContext};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::expression::ScalarExpression;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::implementation::ImplementationRuleImpl;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::sort::SortField;
    use crate::planner::operator::{PhysicalOption, PlanImpl, SortOption};
    use crate::storage::{Storage, Transaction};
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_find_best_selects_cheapest_scan() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        database
            .run("create table t1 (c1 int primary key, c2 int)")?
            .done()?;
        database
            .run("create table t2 (c3 int primary key, c4 int)")?
            .done()?;

        for i in 0..1000 {
            database
                .run(format!("insert into t1 values({}, {})", i, i + 1).as_str())?
                .done()?;
        }
        database.run("analyze table t1")?.done()?;

        let transaction = database.storage.transaction()?;
        let c1_column = transaction
            .table(database.state.table_cache(), "t1".to_string().into())?
            .unwrap()
            .get_column_by_name("c1")
            .unwrap();
        let sort_fields = vec![SortField::new(
            ScalarExpression::column_expr(c1_column.clone(), 0),
            true,
            false,
        )];
        let scala_functions = Default::default();
        let table_functions = Default::default();
        let mut binder = Binder::new(
            BinderContext::new(
                database.state.table_cache(),
                database.state.view_cache(),
                &transaction,
                &scala_functions,
                &table_functions,
                Arc::new(AtomicUsize::new(0)),
            ),
            &[],
            None,
        );
        let stmt = crate::parser::parse_sql(
            "select c1, c3 from t1 inner join t2 on c1 = c3 where (c1 > 40 or c1 = 2) and c3 > 22",
        )?;
        let plan = binder.bind(&stmt[0])?;
        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "Simplify Filter".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::SimplifyFilter],
            )
            .before_batch(
                "Predicate Pushdown".to_string(),
                HepBatchStrategy::fix_point_topdown(10),
                vec![
                    NormalizationRuleImpl::PushPredicateThroughJoin,
                    NormalizationRuleImpl::PushJoinPredicateIntoScan,
                    NormalizationRuleImpl::PushPredicateIntoScan,
                ],
            )
            .implementations(vec![
                ImplementationRuleImpl::Projection,
                ImplementationRuleImpl::Filter,
                ImplementationRuleImpl::HashJoin,
                ImplementationRuleImpl::SeqScan,
                ImplementationRuleImpl::IndexScan,
            ])
            .build();

        let best_plan = pipeline
            .instantiate(plan)
            .find_best(Some(&transaction.meta_loader(database.state.meta_cache())))?;

        assert_eq!(
            best_plan
                .childrens
                .pop_only()
                .childrens
                .pop_twins()
                .0
                .childrens
                .pop_only()
                .physical_option,
            Some(PhysicalOption::new(
                PlanImpl::IndexScan(Box::new(IndexInfo {
                    meta: Arc::new(IndexMeta {
                        id: 0,
                        column_ids: vec![c1_column.id().unwrap()],
                        table_name: "t1".to_string().into(),
                        pk_ty: LogicalType::Integer,
                        value_ty: LogicalType::Integer,
                        name: "pk_index".to_string(),
                        ty: IndexType::PrimaryKey { is_multiple: false },
                    }),
                    sort_option: SortOption::OrderBy {
                        fields: sort_fields.clone(),
                        ignore_prefix_len: 0,
                    },
                    range: Some(Range::SortedRanges(vec![
                        Range::Eq(DataValue::Int32(2)),
                        Range::Scope {
                            min: Bound::Excluded(DataValue::Int32(40)),
                            max: Bound::Unbounded,
                        }
                    ])),
                    covered_deserializers: None,
                    cover_mapping: None,
                    sort_elimination_hint: None,
                    stream_distinct_hint: None,
                })),
                SortOption::OrderBy {
                    fields: sort_fields,
                    ignore_prefix_len: 0,
                }
            ))
        );

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct HepOptimizerPipeline {
    before_batches: Vec<HepBatch>,
    after_batches: Vec<HepBatch>,
    implementation_index: ImplementationRuleIndex,
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
            implementation_index: ImplementationRuleIndex::new(implementations),
        }
    }

    pub fn instantiate(&self, plan: LogicalPlan) -> HepOptimizer<'_> {
        HepOptimizer::new(
            plan,
            &self.before_batches,
            &self.after_batches,
            &self.implementation_index,
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
