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
use crate::execution::dql::aggregate::{create_accumulators, Accumulator};
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::SchemaRef;
use crate::types::value::DataValue;
use ahash::{HashMap, HashMapExt};
use itertools::Itertools;
use std::collections::hash_map::{Entry, IntoIter as HashMapIntoIter};

pub struct HashAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    groupby_exprs: Vec<ScalarExpression>,
    input_schema: SchemaRef,
    input_plan: Option<LogicalPlan>,
    input: ExecId,
    output: Option<HashMapIntoIter<Vec<DataValue>, Vec<Box<dyn Accumulator>>>>,
}

impl From<(AggregateOperator, LogicalPlan)> for HashAggExecutor {
    fn from(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
                ..
            },
            mut input,
        ): (AggregateOperator, LogicalPlan),
    ) -> Self {
        HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input_schema: input.output_schema().clone(),
            input_plan: Some(input),
            input: 0,
            output: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for HashAggExecutor {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.input = build_read(
            arena,
            self.input_plan
                .take()
                .expect("hash aggregate input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::HashAgg(self))
    }
}

impl HashAggExecutor {
    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        id: ExecId,
    ) -> Result<(), DatabaseError> {
        let _ = id;
        if self.output.is_none() {
            let mut group_hash_accs: HashMap<Vec<DataValue>, Vec<Box<dyn Accumulator>>> =
                HashMap::new();

            while arena.next_tuple(self.input)? {
                let tuple = arena.result_tuple();
                let mut values = Vec::with_capacity(self.agg_calls.len());

                for expr in &self.agg_calls {
                    if let ScalarExpression::AggCall { args, .. } = expr {
                        if args.len() > 1 {
                            return Err(DatabaseError::UnsupportedStmt(
                                "currently aggregate functions only support a single Column as a parameter"
                                    .to_string(),
                            ));
                        }
                        values.push(args[0].eval(Some((tuple, &self.input_schema)))?);
                    } else {
                        unreachable!()
                    }
                }

                let group_keys = self
                    .groupby_exprs
                    .iter()
                    .map(|expr| expr.eval(Some((tuple, &self.input_schema))))
                    .try_collect()?;

                let entry = match group_hash_accs.entry(group_keys) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => entry.insert(create_accumulators(&self.agg_calls)?),
                };
                for (acc, value) in entry.iter_mut().zip_eq(values.iter()) {
                    acc.update_value(value)?;
                }
            }

            self.output = Some(group_hash_accs.into_iter());
        }

        let Some((group_keys, accs)) = self.output.as_mut().and_then(Iterator::next) else {
            arena.finish();
            return Ok(());
        };

        let output = arena.result_tuple_mut();
        output.pk = None;
        output.values = accs
            .iter()
            .map(|acc| acc.evaluate())
            .chain(group_keys.into_iter().map(Ok))
            .try_collect()?;
        arena.resume();
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::aggregate::hash_agg::HashAggExecutor;
    use crate::execution::dql::test::build_integers;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use itertools::Itertools;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_hash_agg() -> Result<(), DatabaseError> {
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path()).unwrap();
        let mut transaction = storage.transaction()?;
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;

        let t1_schema = Arc::new(vec![
            ColumnRef::from(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c2".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c3".to_string(), true, desc.clone())),
        ]);

        let input = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
                rows: vec![
                    vec![
                        DataValue::Int32(0),
                        DataValue::Int32(2),
                        DataValue::Int32(4),
                    ],
                    vec![
                        DataValue::Int32(1),
                        DataValue::Int32(3),
                        DataValue::Int32(5),
                    ],
                    vec![
                        DataValue::Int32(0),
                        DataValue::Int32(1),
                        DataValue::Int32(2),
                    ],
                    vec![
                        DataValue::Int32(1),
                        DataValue::Int32(2),
                        DataValue::Int32(3),
                    ],
                ],
                schema_ref: t1_schema.clone(),
            }),
            childrens: Box::new(Childrens::None),
            physical_option: None,
            _output_schema_ref: None,
        };
        let plan = LogicalPlan::new(
            Operator::Aggregate(AggregateOperator {
                groupby_exprs: vec![ScalarExpression::column_expr(t1_schema[0].clone(), 0)],
                agg_calls: vec![ScalarExpression::AggCall {
                    distinct: false,
                    kind: AggKind::Sum,
                    args: vec![ScalarExpression::column_expr(t1_schema[1].clone(), 1)],
                    ty: LogicalType::Integer,
                }],
                is_distinct: false,
            }),
            Childrens::Only(Box::new(input)),
        );

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::EvaluatorBind],
            )
            .build();
        let plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

        let Operator::Aggregate(op) = plan.operator else {
            unreachable!()
        };
        let tuples = try_collect(
            HashAggExecutor::from((op, plan.childrens.pop_only()))
                .execute((&table_cache, &view_cache, &meta_cache), &mut transaction),
        )?;

        assert_eq!(tuples.len(), 2);

        let vec_values = tuples.into_iter().map(|tuple| tuple.values).collect_vec();

        assert!(vec_values.contains(&build_integers(vec![Some(3), Some(0)])));
        assert!(vec_values.contains(&build_integers(vec![Some(5), Some(1)])));

        Ok(())
    }
}
