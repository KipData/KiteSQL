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
use crate::optimizer::core::pattern::PatternMatcher;
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::heuristic::matcher::PlanMatcher;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::planner::operator::PhysicalOption;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Expression {
    pub(crate) op: PhysicalOption,
    pub(crate) cost: Option<usize>,
    // TODO: output rows
}

#[derive(Debug, Clone)]
pub struct GroupExpression {
    exprs: Vec<Expression>,
}

impl GroupExpression {
    pub(crate) fn append_expr(&mut self, expr: Expression) {
        self.exprs.push(expr);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct NodePath(Vec<usize>);

impl NodePath {
    fn root() -> Self {
        Self(Vec::new())
    }

    fn child(&self, idx: usize) -> Self {
        let mut path = self.0.clone();
        path.push(idx);
        Self(path)
    }
}

#[derive(Debug)]
pub struct Memo {
    groups: HashMap<NodePath, GroupExpression>,
}

impl Memo {
    pub(crate) fn new<T: Transaction>(
        plan: &LogicalPlan,
        loader: &StatisticMetaLoader<'_, T>,
        implementations: &[ImplementationRuleImpl],
    ) -> Result<Self, DatabaseError> {
        let mut groups = HashMap::new();
        Self::collect(plan, NodePath::root(), loader, implementations, &mut groups)?;
        Ok(Memo { groups })
    }

    fn collect<T: Transaction>(
        plan: &LogicalPlan,
        path: NodePath,
        loader: &StatisticMetaLoader<'_, T>,
        implementations: &[ImplementationRuleImpl],
        groups: &mut HashMap<NodePath, GroupExpression>,
    ) -> Result<(), DatabaseError> {
        for rule in implementations {
            if PlanMatcher::new(rule.pattern(), plan).match_opt_expr() {
                let group_expr = groups
                    .entry(path.clone())
                    .or_insert_with(|| GroupExpression { exprs: vec![] });
                rule.to_expression(&plan.operator, loader, group_expr)?;
            }
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => {
                Self::collect(child, path.child(0), loader, implementations, groups)?;
            }
            Childrens::Twins { left, right } => {
                Self::collect(left, path.child(0), loader, implementations, groups)?;
                Self::collect(right, path.child(1), loader, implementations, groups)?;
            }
            Childrens::None => {}
        }

        Ok(())
    }

    pub(crate) fn annotate_plan(&self, plan: &mut LogicalPlan) {
        Self::annotate(plan, &NodePath::root(), self);
    }

    fn annotate(plan: &mut LogicalPlan, path: &NodePath, memo: &Memo) {
        if let Some(option) = memo.cheapest_physical_option(path) {
            plan.physical_option = Some(option);
        }

        match plan.childrens.as_mut() {
            Childrens::Only(child) => Self::annotate(child, &path.child(0), memo),
            Childrens::Twins { left, right } => {
                Self::annotate(left, &path.child(0), memo);
                Self::annotate(right, &path.child(1), memo);
            }
            Childrens::None => {}
        }
    }

    pub(crate) fn cheapest_physical_option(&self, path: &NodePath) -> Option<PhysicalOption> {
        self.groups.get(path).and_then(|exprs| {
            exprs
                .exprs
                .iter()
                .min_by(|expr_1, expr_2| match (expr_1.cost, expr_2.cost) {
                    (Some(cost_1), Some(cost_2)) => cost_1.cmp(&cost_2),
                    (None, Some(_)) => Ordering::Greater,
                    (Some(_), None) => Ordering::Less,
                    (None, None) => Ordering::Equal,
                })
                .map(|expr| expr.op.clone())
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::NodePath;
    use crate::binder::{Binder, BinderContext};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::errors::DatabaseError;
    use crate::expression::range_detacher::Range;
    use crate::expression::ScalarExpression;
    use crate::optimizer::core::memo::Memo;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::implementation::ImplementationRuleImpl;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::sort::SortField;
    use crate::planner::operator::{PhysicalOption, PlanImpl, SortOption};
    use crate::storage::rocksdb::RocksTransaction;
    use crate::storage::{Storage, Transaction};
    use crate::types::index::{IndexInfo, IndexMeta, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::ops::Bound;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use tempfile::TempDir;

    // Tips: This test may occasionally encounter errors; you can repeat the test multiple times.
    #[test]
    fn test_build_memo() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = DataBaseBuilder::path(temp_dir.path()).build()?;
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
            ScalarExpression::column_expr(c1_column.clone()),
            true,
            true,
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
        // where: c1 => 2, (40, +inf)
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
            .build();
        let mut best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;
        let rules = vec![
            ImplementationRuleImpl::Projection,
            ImplementationRuleImpl::Filter,
            ImplementationRuleImpl::HashJoin,
            ImplementationRuleImpl::SeqScan,
            ImplementationRuleImpl::IndexScan,
        ];

        let memo = Memo::new(
            &best_plan,
            &transaction.meta_loader(database.state.meta_cache()),
            &rules,
        )?;
        Memo::annotate_plan(&memo, &mut best_plan);
        let exprs = memo
            .groups
            .get(&NodePath(vec![0, 0, 0]))
            .expect("missing group");

        assert_eq!(exprs.exprs.len(), 2);
        assert_eq!(exprs.exprs[0].cost, Some(1000));
        assert_eq!(
            exprs.exprs[0].op,
            PhysicalOption::new(PlanImpl::SeqScan, SortOption::None)
        );
        assert!(exprs.exprs[1].cost.unwrap() >= 960);
        assert!(matches!(
            exprs.exprs[1].op,
            PhysicalOption {
                plan: PlanImpl::IndexScan(..),
                ..
            }
        ));
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
                PlanImpl::IndexScan(IndexInfo {
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
                }),
                SortOption::OrderBy {
                    fields: sort_fields,
                    ignore_prefix_len: 0,
                }
            ))
        );

        Ok(())
    }
}
