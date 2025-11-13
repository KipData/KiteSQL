use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::{create_accumulators, Accumulator};
use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::{HashMap, HashMapExt};
use itertools::Itertools;
use std::collections::hash_map::Entry;

pub struct HashAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    groupby_exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(AggregateOperator, LogicalPlan)> for HashAggExecutor {
    fn from(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
                ..
            },
            input,
        ): (AggregateOperator, LogicalPlan),
    ) -> Self {
        HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for HashAggExecutor {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let HashAggExecutor {
                agg_calls,
                groupby_exprs,
                mut input,
            } = self;

            let schema_ref = input.output_schema().clone();
            let mut group_hash_accs: HashMap<Vec<DataValue>, Vec<Box<dyn Accumulator>>> =
                HashMap::new();

            let mut executor = build_read(input, cache, transaction);

            for result in executor.by_ref() {
                let tuple = throw!(co, result);
                let mut values = Vec::with_capacity(agg_calls.len());

                for expr in agg_calls.iter() {
                    if let ScalarExpression::AggCall { args, .. } = expr {
                        if args.len() > 1 {
                            throw!(co, Err(DatabaseError::UnsupportedStmt(
                                "currently aggregate functions only support a single Column as a parameter"
                                    .to_string()
                            )))
                        }
                        values.push(throw!(co, args[0].eval(Some((&tuple, &schema_ref)))));
                    } else {
                        unreachable!()
                    }
                }
                let group_keys: Vec<DataValue> = throw!(
                    co,
                    groupby_exprs
                        .iter()
                        .map(|expr| expr.eval(Some((&tuple, &schema_ref))))
                        .try_collect()
                );

                let entry = match group_hash_accs.entry(group_keys) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        entry.insert(throw!(co, create_accumulators(&agg_calls)))
                    }
                };
                for (acc, value) in entry.iter_mut().zip_eq(values.iter()) {
                    throw!(co, acc.update_value(value));
                }
            }

            for (group_keys, accs) in group_hash_accs {
                // Tips: Accumulator First
                let values: Vec<DataValue> = throw!(
                    co,
                    accs.iter()
                        .map(|acc| acc.evaluate())
                        .chain(group_keys.into_iter().map(Ok))
                        .try_collect()
                );
                co.yield_(Ok(Tuple::new(None, values))).await;
            }
        })
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::aggregate::hash_agg::HashAggExecutor;
    use crate::execution::dql::test::build_integers;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
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
                groupby_exprs: vec![ScalarExpression::column_expr(t1_schema[0].clone())],
                agg_calls: vec![ScalarExpression::AggCall {
                    distinct: false,
                    kind: AggKind::Sum,
                    args: vec![ScalarExpression::column_expr(t1_schema[1].clone())],
                    ty: LogicalType::Integer,
                }],
                is_distinct: false,
            }),
            Childrens::Only(Box::new(input)),
        );

        let plan = HepOptimizer::new(plan)
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::BindExpressionPosition,
                    // TIPS: This rule is necessary
                    NormalizationRuleImpl::EvaluatorBind,
                ],
            )
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
