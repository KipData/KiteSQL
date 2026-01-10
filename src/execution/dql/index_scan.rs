use crate::execution::{spawn_executor, Executor, ReadExecutor};
use crate::expression::range_detacher::Range;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::storage::{Iter, StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::IndexMetaRef;
use crate::types::serialize::TupleValueSerializableImpl;

pub(crate) struct IndexScan {
    op: TableScanOperator,
    index_by: IndexMetaRef,
    ranges: Vec<Range>,
    covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
}

impl
    From<(
        TableScanOperator,
        IndexMetaRef,
        Range,
        Option<Vec<TupleValueSerializableImpl>>,
    )> for IndexScan
{
    fn from(
        (op, index_by, range, covered_deserializers): (
            TableScanOperator,
            IndexMetaRef,
            Range,
            Option<Vec<TupleValueSerializableImpl>>,
        ),
    ) -> Self {
        let ranges = match range {
            Range::SortedRanges(ranges) => ranges,
            range => vec![range],
        };

        IndexScan {
            op,
            index_by,
            ranges,
            covered_deserializers,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for IndexScan {
    fn execute(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let TableScanOperator {
                table_name,
                columns,
                limit,
                with_pk,
                ..
            } = self.op;

            let mut iter = throw!(
                co,
                unsafe { &(*transaction) }.read_by_index(
                    table_cache,
                    table_name,
                    limit,
                    columns,
                    self.index_by,
                    self.ranges,
                    with_pk,
                    self.covered_deserializers,
                )
            );

            while let Some(tuple) = throw!(co, iter.next_tuple()) {
                co.yield_(Ok(tuple)).await;
            }
        })
    }
}
