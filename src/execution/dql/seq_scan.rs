use crate::execution::{Executor, ReadExecutor};
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{Iter, StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;

pub(crate) struct SeqScan {
    op: TableScanOperator,
}

impl From<TableScanOperator> for SeqScan {
    fn from(op: TableScanOperator) -> Self {
        SeqScan { op }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for SeqScan {
    fn execute(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let TableScanOperator {
                    table_name,
                    columns,
                    limit,
                    with_pk,
                    ..
                } = self.op;

                let mut iter = throw!(unsafe { &mut (*transaction) }.read(
                    table_cache,
                    table_name,
                    limit,
                    columns,
                    with_pk
                ));

                while let Some(tuple) = throw!(iter.next_tuple()) {
                    yield Ok(tuple);
                }
            },
        )
    }
}
