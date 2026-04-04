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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ExecutorNode};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use itertools::Itertools;

pub struct StreamDistinctExecutor {
    groupby_exprs: Vec<ScalarExpression>,
    input: ExecId,
    last_keys: Option<Vec<DataValue>>,
    scratch: Tuple,
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for StreamDistinctExecutor {
    type Input = (AggregateOperator, LogicalPlan);

    fn into_executor(
        (op, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        let input = build_read(arena, input, cache, transaction);
        arena.push(ExecNode::StreamDistinct(StreamDistinctExecutor {
            groupby_exprs: op.groupby_exprs,
            input,
            last_keys: None,
            scratch: Tuple::default(),
        }))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        loop {
            if !arena.next_tuple(self.input)? {
                arena.finish();
                return Ok(());
            }
            std::mem::swap(&mut self.scratch, arena.result_tuple_mut());
            let tuple = &self.scratch;
            let group_keys = self
                .groupby_exprs
                .iter()
                .map(|expr| expr.eval(Some(tuple)))
                .try_collect()?;

            if self.last_keys.as_ref() != Some(&group_keys) {
                self.last_keys = Some(group_keys.clone());
                let output = arena.result_tuple_mut();
                output.pk.clone_from(&tuple.pk);
                output.values = group_keys;
                arena.resume();
                return Ok(());
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::aggregate::stream_distinct::StreamDistinctExecutor;
    use crate::execution::{execute_input, try_collect};
    use crate::expression::ScalarExpression;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[allow(clippy::type_complexity)]
    fn build_test_storage() -> Result<
        (
            Arc<TableCache>,
            Arc<ViewCache>,
            Arc<StatisticsMetaCache>,
            TempDir,
            RocksStorage,
        ),
        DatabaseError,
    > {
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;

        Ok((table_cache, view_cache, meta_cache, temp_dir, storage))
    }

    fn optimize_exprs(plan: LogicalPlan) -> Result<LogicalPlan, DatabaseError> {
        HepOptimizerPipeline::builder()
            .before_batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::EvaluatorBind],
            )
            .build()
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)
    }

    #[test]
    fn stream_distinct_single_column_sorted() -> Result<(), DatabaseError> {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let schema_ref = Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
            "c1".to_string(),
            true,
            desc,
        ))]);

        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
                    vec![DataValue::Int32(1)],
                    vec![DataValue::Int32(1)],
                    vec![DataValue::Int32(2)],
                    vec![DataValue::Int32(2)],
                    vec![DataValue::Int32(3)],
                ],
                schema_ref: schema_ref.clone(),
            }),
            Childrens::None,
        );
        let agg = AggregateOperator {
            groupby_exprs: vec![ScalarExpression::column_expr(schema_ref[0].clone(), 0)],
            agg_calls: vec![],
            is_distinct: true,
        };
        let plan = LogicalPlan::new(Operator::Aggregate(agg), Childrens::Only(Box::new(input)));
        let plan = optimize_exprs(plan)?;
        let Operator::Aggregate(agg) = plan.operator else {
            unreachable!()
        };

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, StreamDistinctExecutor>(
            (agg, plan.childrens.pop_only()),
            (&table_cache, &view_cache, &meta_cache),
            &mut transaction,
        ))?;

        let actual = tuples
            .into_iter()
            .flat_map(|tuple| tuple.values)
            .flat_map(|value| value.i32())
            .collect_vec();
        assert_eq!(actual, vec![1, 2, 3]);

        Ok(())
    }

    #[test]
    fn stream_distinct_multi_column_sorted() -> Result<(), DatabaseError> {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let schema_ref = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c2".to_string(), true, desc)),
        ]);

        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
                    vec![DataValue::Int32(1), DataValue::Int32(1)],
                    vec![DataValue::Int32(1), DataValue::Int32(1)],
                    vec![DataValue::Int32(1), DataValue::Int32(2)],
                    vec![DataValue::Int32(2), DataValue::Int32(1)],
                    vec![DataValue::Int32(2), DataValue::Int32(1)],
                ],
                schema_ref: schema_ref.clone(),
            }),
            Childrens::None,
        );
        let agg = AggregateOperator {
            groupby_exprs: vec![
                ScalarExpression::column_expr(schema_ref[0].clone(), 0),
                ScalarExpression::column_expr(schema_ref[1].clone(), 1),
            ],
            agg_calls: vec![],
            is_distinct: true,
        };
        let plan = LogicalPlan::new(Operator::Aggregate(agg), Childrens::Only(Box::new(input)));
        let plan = optimize_exprs(plan)?;
        let Operator::Aggregate(agg) = plan.operator else {
            unreachable!()
        };

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, StreamDistinctExecutor>(
            (agg, plan.childrens.pop_only()),
            (&table_cache, &view_cache, &meta_cache),
            &mut transaction,
        ))?;

        let actual = tuples.into_iter().map(|tuple| tuple.values).collect_vec();
        assert_eq!(
            actual,
            vec![
                vec![DataValue::Int32(1), DataValue::Int32(1)],
                vec![DataValue::Int32(1), DataValue::Int32(2)],
                vec![DataValue::Int32(2), DataValue::Int32(1)],
            ]
        );

        Ok(())
    }
}
