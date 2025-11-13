use crate::execution::dql::aggregate::create_accumulators;
use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use itertools::Itertools;
pub struct SimpleAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(AggregateOperator, LogicalPlan)> for SimpleAggExecutor {
    fn from(
        (AggregateOperator { agg_calls, .. }, input): (AggregateOperator, LogicalPlan),
    ) -> Self {
        SimpleAggExecutor { agg_calls, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SimpleAggExecutor {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let SimpleAggExecutor {
                agg_calls,
                mut input,
            } = self;

            let mut accs = throw!(co, create_accumulators(&agg_calls));
            let schema = input.output_schema().clone();

            let mut executor = build_read(input, cache, transaction);

            for tuple in executor.by_ref() {
                let tuple = throw!(co, tuple);

                let values: Vec<DataValue> = throw!(
                    co,
                    agg_calls
                        .iter()
                        .map(|expr| match expr {
                            ScalarExpression::AggCall { args, .. } => {
                                args[0].eval(Some((&tuple, &schema)))
                            }
                            _ => unreachable!(),
                        })
                        .try_collect()
                );

                for (acc, value) in accs.iter_mut().zip_eq(values.iter()) {
                    throw!(co, acc.update_value(value));
                }
            }
            let values: Vec<DataValue> =
                throw!(co, accs.into_iter().map(|acc| acc.evaluate()).try_collect());

            co.yield_(Ok(Tuple::new(None, values))).await;
        })
    }
}
