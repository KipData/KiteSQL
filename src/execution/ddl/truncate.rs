use crate::execution::{spawn_executor, Executor, WriteExecutor};
use crate::planner::operator::truncate::TruncateOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct Truncate {
    op: TruncateOperator,
}

impl From<TruncateOperator> for Truncate {
    fn from(op: TruncateOperator) -> Self {
        Truncate { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Truncate {
    fn execute_mut(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let TruncateOperator { table_name } = self.op;

            throw!(co, unsafe { &mut (*transaction) }.drop_data(&table_name));

            co.yield_(Ok(TupleBuilder::build_result(format!("{table_name}"))))
                .await;
        })
    }
}
