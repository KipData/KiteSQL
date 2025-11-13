use crate::execution::{spawn_executor, Executor, ReadExecutor};
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::types::tuple::Tuple;

pub struct Dummy {}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Dummy {
    fn execute(
        self,
        _: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        _: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            co.yield_(Ok(Tuple::new(None, Vec::new()))).await;
        })
    }
}
