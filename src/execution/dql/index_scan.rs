use crate::execution::{Executor, ReadExecutor};
use crate::expression::range_detacher::Range;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{Iter, StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::IndexMetaRef;

pub(crate) struct IndexScan {
    op: TableScanOperator,
    index_by: IndexMetaRef,
    ranges: Vec<Range>,
}

impl From<(TableScanOperator, IndexMetaRef, Range)> for IndexScan {
    fn from((op, index_by, range): (TableScanOperator, IndexMetaRef, Range)) -> Self {
        let ranges = match range {
            Range::SortedRanges(ranges) => ranges,
            range => vec![range],
        };

        IndexScan {
            op,
            index_by,
            ranges,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for IndexScan {
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

                let mut iter = throw!(unsafe { &(*transaction) }.read_by_index(
                    table_cache,
                    table_name,
                    limit,
                    columns,
                    self.index_by,
                    self.ranges,
                    with_pk,
                ));

                while let Some(tuple) = throw!(iter.next_tuple()) {
                    yield Ok(tuple);
                }
            },
        )
    }
}
