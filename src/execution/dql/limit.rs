use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::planner::operator::limit::LimitOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
pub struct Limit {
    offset: Option<usize>,
    limit: Option<usize>,
    input: LogicalPlan,
}

impl From<(LimitOperator, LogicalPlan)> for Limit {
    fn from((LimitOperator { offset, limit }, input): (LimitOperator, LogicalPlan)) -> Self {
        Limit {
            offset,
            limit,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Limit {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let Limit {
                offset,
                limit,
                input,
            } = self;

            if limit.is_some() && limit.unwrap() == 0 {
                return;
            }

            let offset_val = offset.unwrap_or(0);
            let offset_limit = offset_val.saturating_add(limit.unwrap_or(usize::MAX)) - 1;

            let mut i = 0;
            let executor = build_read(input, cache, transaction);

            for tuple in executor {
                i += 1;
                if i - 1 < offset_val {
                    continue;
                } else if i - 1 > offset_limit {
                    break;
                }

                co.yield_(tuple).await;
            }
        })
    }
}
