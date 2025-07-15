use crate::execution::dql::projection::Projection;
use crate::execution::DatabaseError;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct CreateIndex {
    op: CreateIndexOperator,
    input: LogicalPlan,
}

impl From<(CreateIndexOperator, LogicalPlan)> for CreateIndex {
    fn from((op, input): (CreateIndexOperator, LogicalPlan)) -> Self {
        Self { op, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for CreateIndex {
    fn execute_mut(
        mut self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let CreateIndexOperator {
                    table_name,
                    index_name,
                    columns,
                    if_not_exists,
                    ty,
                } = self.op;

                let (column_ids, column_exprs): (Vec<ColumnId>, Vec<ScalarExpression>) = columns
                    .into_iter()
                    .filter_map(|column| {
                        column
                            .id()
                            .map(|id| (id, ScalarExpression::ColumnRef(column)))
                    })
                    .unzip();
                let schema = self.input.output_schema().clone();
                let index_id = match unsafe { &mut (*transaction) }.add_index_meta(
                    cache.0,
                    &table_name,
                    index_name,
                    column_ids,
                    ty,
                ) {
                    Ok(index_id) => index_id,
                    Err(DatabaseError::DuplicateIndex(index_name)) => {
                        if if_not_exists {
                            return;
                        } else {
                            throw!(Err(DatabaseError::DuplicateIndex(index_name)))
                        }
                    }
                    err => throw!(err),
                };
                let mut coroutine = build_read(self.input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);

                    let Some(value) = DataValue::values_to_tuple(throw!(Projection::projection(
                        &tuple,
                        &column_exprs,
                        &schema
                    ))) else {
                        continue;
                    };
                    let tuple_id = if let Some(tuple_id) = tuple.pk.as_ref() {
                        tuple_id
                    } else {
                        continue;
                    };
                    let index = Index::new(index_id, &value, ty);
                    throw!(unsafe { &mut (*transaction) }.add_index(
                        table_name.as_str(),
                        index,
                        tuple_id
                    ));
                }
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}
