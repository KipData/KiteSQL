use crate::execution::volcano::{BoxedExecutor, Executor};
use crate::execution::ExecutorError;
use crate::expression::simplify::ConstantBinary;
use crate::planner::operator::scan::ScanOperator;
use crate::storage::{Iter, Transaction};
use crate::types::index::IndexMetaRef;
use crate::types::tuple::Tuple;
use futures_async_stream::try_stream;
use std::cell::RefCell;

pub(crate) struct IndexScan {
    op: ScanOperator,
    index_by: IndexMetaRef,
    binaries: Vec<ConstantBinary>,
}

impl From<(ScanOperator, IndexMetaRef, Vec<ConstantBinary>)> for IndexScan {
    fn from((op, index_by, binaries): (ScanOperator, IndexMetaRef, Vec<ConstantBinary>)) -> Self {
        IndexScan {
            op,
            index_by,
            binaries,
        }
    }
}

impl<T: Transaction> Executor<T> for IndexScan {
    fn execute(self, transaction: &RefCell<T>) -> BoxedExecutor {
        unsafe { self._execute(transaction.as_ptr().as_ref().unwrap()) }
    }
}

impl IndexScan {
    #[try_stream(boxed, ok = Tuple, error = ExecutorError)]
    pub async fn _execute<T: Transaction>(self, transaction: &T) {
        let ScanOperator {
            table_name,
            columns,
            limit,
            ..
        } = self.op;
        let mut iter =
            transaction.read_by_index(table_name, limit, columns, self.index_by, self.binaries)?;

        while let Some(tuple) = iter.next_tuple()? {
            yield tuple;
        }
    }
}
