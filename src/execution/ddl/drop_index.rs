use crate::execution::{Executor, WriteExecutor};
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct DropIndex {
    op: DropIndexOperator,
}

impl From<DropIndexOperator> for DropIndex {
    fn from(op: DropIndexOperator) -> Self {
        Self { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for DropIndex {
    fn execute_mut(
        self,
        (table_cache, _, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let DropIndexOperator {
                    table_name,
                    index_name,
                    if_exists,
                } = self.op;

                throw!(unsafe { &mut (*transaction) }.drop_index(
                    table_cache,
                    table_name,
                    &index_name,
                    if_exists
                ));

                yield Ok(TupleBuilder::build_result(index_name.to_string()));
            },
        )
    }
}
