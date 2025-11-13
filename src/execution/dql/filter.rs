use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
pub struct Filter {
    predicate: ScalarExpression,
    input: LogicalPlan,
}

impl From<(FilterOperator, LogicalPlan)> for Filter {
    fn from((FilterOperator { predicate, .. }, input): (FilterOperator, LogicalPlan)) -> Self {
        Filter { predicate, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Filter {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Filter {
                predicate,
                mut input,
            } = self;

            let schema = input.output_schema().clone();

            let executor = build_read(input, cache, transaction);

            for tuple in executor {
                let tuple = throw!(co, tuple);

                if throw!(
                    co,
                    throw!(co, predicate.eval(Some((&tuple, &schema)))).is_true()
                ) {
                    co.yield_(Ok(tuple)).await;
                }
            }
        })
    }
}
