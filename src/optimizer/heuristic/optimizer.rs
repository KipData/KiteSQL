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
    HepBatch, HepBatchStep, HepBatchStrategy, HepLocalRewriteBatch, HepWholeTreePass,
};
use crate::optimizer::rule::implementation::{ImplementationRuleImpl, ImplementationRuleRootTag};
use crate::optimizer::rule::normalization::{
    apply_annotated_post_rules, apply_scan_order_hint, constant_calculation_current,
    evaluator_bind_current, NormalizationRuleImpl, OrderHintKind, ScanOrderHint, WholeTreePassKind,
};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use std::array;
use std::ops::Not;

type ScanHintApplier<'a> = dyn Fn(&mut TableScanOperator) + 'a;

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
                let apply_no_sort_hints = |_scan_op: &mut TableScanOperator| {};
                let apply_no_stream_distinct_hints = |_scan_op: &mut TableScanOperator| {};
                Self::annotate_hints_and_physical_options(
                    &mut self.plan,
                    loader,
                    self.implementation_index,
                    &apply_no_sort_hints,
                    &apply_no_stream_distinct_hints,
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
        inherited_sort_hints: &'plan ScanHintApplier<'plan>,
        inherited_stream_distinct_hints: &'plan ScanHintApplier<'plan>,
    ) -> Result<(), DatabaseError> {
        if let Operator::TableScan(scan_op) = &mut plan.operator {
            inherited_sort_hints(scan_op);
            inherited_stream_distinct_hints(scan_op);
        }

        {
            let LogicalPlan {
                operator,
                physical_option,
                ..
            } = plan;
            if let Some(option) = implementation_index.direct_physical_option(operator) {
                *physical_option = Some(option);
            } else {
                let mut best_physical_option: BestPhysicalOption = None;
                for rule in implementation_index.for_matching_operator(operator) {
                    rule.update_best_option(operator, loader, &mut best_physical_option)?;
                }
                if let Some((option, _)) = best_physical_option {
                    *physical_option = Some(option);
                }
            }
        }

        {
            let LogicalPlan {
                operator,
                childrens,
                ..
            } = plan;
            Self::with_child_sort_hints(operator, inherited_sort_hints, |child_sort_hints| {
                Self::with_child_stream_distinct_hints(
                    operator,
                    inherited_stream_distinct_hints,
                    |child_stream_distinct_hints| match &mut **childrens {
                        Childrens::Only(child) => Self::annotate_hints_and_physical_options(
                            child,
                            loader,
                            implementation_index,
                            child_sort_hints,
                            child_stream_distinct_hints,
                        ),
                        Childrens::Twins { left, right } => {
                            Self::annotate_hints_and_physical_options(
                                left,
                                loader,
                                implementation_index,
                                child_sort_hints,
                                child_stream_distinct_hints,
                            )?;
                            Self::annotate_hints_and_physical_options(
                                right,
                                loader,
                                implementation_index,
                                child_sort_hints,
                                child_stream_distinct_hints,
                            )
                        }
                        Childrens::None => Ok(()),
                    },
                )
            })?;
        }

        apply_annotated_post_rules(plan)?;

        Ok(())
    }

    fn with_child_sort_hints<'plan, R>(
        operator: &'plan Operator,
        inherited_sort_hints: &'plan ScanHintApplier<'plan>,
        f: impl for<'b> FnOnce(&'b ScanHintApplier<'plan>) -> R,
    ) -> R {
        let propagate_hints = matches!(
            operator,
            Operator::Filter(_)
                | Operator::Project(_)
                | Operator::Limit(_)
                | Operator::TopK(_)
                | Operator::Sort(_)
        );

        match operator {
            Operator::Sort(op) => {
                let child_sort_hints = |scan_op: &mut TableScanOperator| {
                    inherited_sort_hints(scan_op);
                    apply_scan_order_hint(
                        scan_op,
                        ScanOrderHint::sort_fields(&op.sort_fields),
                        OrderHintKind::SortElimination,
                    );
                };
                f(&child_sort_hints)
            }
            _ if propagate_hints => f(inherited_sort_hints),
            _ => {
                let no_sort_hints = |_scan_op: &mut TableScanOperator| {};
                f(&no_sort_hints)
            }
        }
    }

    fn with_child_stream_distinct_hints<'plan, R>(
        operator: &'plan Operator,
        inherited_stream_distinct_hints: &'plan ScanHintApplier<'plan>,
        f: impl for<'b> FnOnce(&'b ScanHintApplier<'plan>) -> R,
    ) -> R {
        let propagate_hints = matches!(
            operator,
            Operator::Filter(_)
                | Operator::Project(_)
                | Operator::Limit(_)
                | Operator::TopK(_)
                | Operator::Sort(_)
        );

        match operator {
            Operator::Aggregate(op)
                if op.is_distinct && op.agg_calls.is_empty() && !op.groupby_exprs.is_empty() =>
            {
                let child_stream_distinct_hints = |scan_op: &mut TableScanOperator| {
                    apply_scan_order_hint(
                        scan_op,
                        ScanOrderHint::distinct_groupby(&op.groupby_exprs),
                        OrderHintKind::StreamDistinct,
                    );
                };
                f(&child_stream_distinct_hints)
            }
            _ if propagate_hints => f(inherited_stream_distinct_hints),
            _ => {
                let no_stream_distinct_hints = |_scan_op: &mut TableScanOperator| {};
                f(&no_stream_distinct_hints)
            }
        }
    }

    fn apply_local_rules(
        plan: &mut LogicalPlan,
        rules: &HepLocalRewriteBatch,
    ) -> Result<bool, DatabaseError> {
        let mut applied_rules = vec![false; rules.len()];
        Self::apply_local_rules_inner(plan, rules, &mut applied_rules)
    }

    fn apply_local_rules_inner(
        plan: &mut LogicalPlan,
        rules: &HepLocalRewriteBatch,
        applied_rules: &mut [bool],
    ) -> Result<bool, DatabaseError> {
        let mut applied = false;
        let mut next_rule_idx = 0;

        while let Some(idx) = rules.next_matching_rule_index_from(&plan.operator, next_rule_idx) {
            let rule = rules.rules[idx];
            next_rule_idx = idx + 1;
            if applied_rules[idx] {
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

    fn contains(&self, implementation: ImplementationRuleImpl) -> bool {
        self.groups[implementation.root_tag() as usize].contains(&implementation)
    }

    fn direct_physical_option(&self, operator: &Operator) -> Option<PhysicalOption> {
        match operator {
            Operator::Aggregate(op)
                if !op.groupby_exprs.is_empty()
                    && self.contains(ImplementationRuleImpl::GroupByAggregate) =>
            {
                Some(PhysicalOption::new(
                    PlanImpl::HashAggregate,
                    SortOption::None,
                ))
            }
            Operator::Aggregate(op)
                if op.groupby_exprs.is_empty()
                    && self.contains(ImplementationRuleImpl::SimpleAggregate) =>
            {
                Some(PhysicalOption::new(
                    PlanImpl::SimpleAggregate,
                    SortOption::None,
                ))
            }
            Operator::Dummy if self.contains(ImplementationRuleImpl::Dummy) => {
                Some(PhysicalOption::new(PlanImpl::Dummy, SortOption::None))
            }
            Operator::Filter(_) if self.contains(ImplementationRuleImpl::Filter) => {
                Some(PhysicalOption::new(PlanImpl::Filter, SortOption::Follow))
            }
            Operator::Join(join_op) if self.contains(ImplementationRuleImpl::HashJoin) => {
                let plan = match &join_op.on {
                    JoinCondition::On { on, .. } if !on.is_empty() => PlanImpl::HashJoin,
                    _ => PlanImpl::NestLoopJoin,
                };
                Some(PhysicalOption::new(plan, SortOption::None))
            }
            Operator::Limit(_) if self.contains(ImplementationRuleImpl::Limit) => {
                Some(PhysicalOption::new(PlanImpl::Limit, SortOption::Follow))
            }
            Operator::MarkApply(_) if self.contains(ImplementationRuleImpl::MarkApply) => {
                Some(PhysicalOption::new(PlanImpl::MarkApply, SortOption::Follow))
            }
            Operator::Project(_) if self.contains(ImplementationRuleImpl::Projection) => {
                Some(PhysicalOption::new(PlanImpl::Project, SortOption::Follow))
            }
            Operator::ScalarApply(_) if self.contains(ImplementationRuleImpl::ScalarApply) => {
                Some(PhysicalOption::new(PlanImpl::ScalarApply, SortOption::Follow))
            }
            Operator::ScalarSubquery(_)
                if self.contains(ImplementationRuleImpl::ScalarSubquery) =>
            {
                Some(PhysicalOption::new(
                    PlanImpl::ScalarSubquery,
                    SortOption::Follow,
                ))
            }
            Operator::FunctionScan(_) if self.contains(ImplementationRuleImpl::FunctionScan) => {
                Some(PhysicalOption::new(
                    PlanImpl::FunctionScan,
                    SortOption::None,
                ))
            }
            Operator::Sort(op) if self.contains(ImplementationRuleImpl::Sort) => {
                Some(PhysicalOption::new(
                    PlanImpl::Sort,
                    SortOption::OrderBy {
                        fields: op.sort_fields.clone(),
                        ignore_prefix_len: 0,
                    },
                ))
            }
            Operator::TopK(op) if self.contains(ImplementationRuleImpl::TopK) => {
                Some(PhysicalOption::new(
                    PlanImpl::TopK,
                    SortOption::OrderBy {
                        fields: op.sort_fields.clone(),
                        ignore_prefix_len: 0,
                    },
                ))
            }
            Operator::Values(_) if self.contains(ImplementationRuleImpl::Values) => {
                Some(PhysicalOption::new(PlanImpl::Values, SortOption::None))
            }
            Operator::Analyze(_) if self.contains(ImplementationRuleImpl::Analyze) => {
                Some(PhysicalOption::new(PlanImpl::Analyze, SortOption::None))
            }
            Operator::CopyFromFile(_) if self.contains(ImplementationRuleImpl::CopyFromFile) => {
                Some(PhysicalOption::new(
                    PlanImpl::CopyFromFile,
                    SortOption::None,
                ))
            }
            Operator::CopyToFile(_) if self.contains(ImplementationRuleImpl::CopyToFile) => {
                Some(PhysicalOption::new(PlanImpl::CopyToFile, SortOption::None))
            }
            Operator::Delete(_) if self.contains(ImplementationRuleImpl::Delete) => {
                Some(PhysicalOption::new(PlanImpl::Delete, SortOption::None))
            }
            Operator::Insert(_) if self.contains(ImplementationRuleImpl::Insert) => {
                Some(PhysicalOption::new(PlanImpl::Insert, SortOption::None))
            }
            Operator::Update(_) if self.contains(ImplementationRuleImpl::Update) => {
                Some(PhysicalOption::new(PlanImpl::Update, SortOption::None))
            }
            Operator::AddColumn(_) if self.contains(ImplementationRuleImpl::AddColumn) => {
                Some(PhysicalOption::new(PlanImpl::AddColumn, SortOption::None))
            }
            Operator::ChangeColumn(_) if self.contains(ImplementationRuleImpl::ChangeColumn) => {
                Some(PhysicalOption::new(
                    PlanImpl::ChangeColumn,
                    SortOption::None,
                ))
            }
            Operator::CreateTable(_) if self.contains(ImplementationRuleImpl::CreateTable) => {
                Some(PhysicalOption::new(PlanImpl::CreateTable, SortOption::None))
            }
            Operator::DropColumn(_) if self.contains(ImplementationRuleImpl::DropColumn) => {
                Some(PhysicalOption::new(PlanImpl::DropColumn, SortOption::None))
            }
            Operator::DropTable(_) if self.contains(ImplementationRuleImpl::DropTable) => {
                Some(PhysicalOption::new(PlanImpl::DropTable, SortOption::None))
            }
            Operator::Truncate(_) if self.contains(ImplementationRuleImpl::Truncate) => {
                Some(PhysicalOption::new(PlanImpl::Truncate, SortOption::None))
            }
            _ => None,
        }
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
    use crate::db::DataBaseBuilder;
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
