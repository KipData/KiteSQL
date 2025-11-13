use crate::execution::{spawn_executor, Executor, WriteExecutor};
use crate::planner::operator::create_view::CreateViewOperator;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple_builder::TupleBuilder;

pub struct CreateView {
    op: CreateViewOperator,
}

impl From<CreateViewOperator> for CreateView {
    fn from(op: CreateViewOperator) -> Self {
        CreateView { op }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateView {
    fn execute_mut(
        self,
        (_, view_cache, _): (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let CreateViewOperator { view, or_replace } = self.op;

            let result_tuple = TupleBuilder::build_result(format!("{}", view.name));
            throw!(
                co,
                unsafe { &mut (*transaction) }.create_view(view_cache, view, or_replace)
            );

            co.yield_(Ok(result_tuple)).await;
        })
    }
}
