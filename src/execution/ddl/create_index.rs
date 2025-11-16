use crate::execution::dql::projection::Projection;
use crate::execution::DatabaseError;
use crate::execution::{build_read, spawn_executor, Executor, WriteExecutor};
use crate::expression::{BindPosition, ScalarExpression};
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use crate::types::ColumnId;
use std::borrow::Cow;

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
        spawn_executor(move |co| async move {
            let CreateIndexOperator {
                table_name,
                index_name,
                columns,
                if_not_exists,
                ty,
            } = self.op;

            let (column_ids, mut column_exprs): (Vec<ColumnId>, Vec<ScalarExpression>) = columns
                .into_iter()
                .filter_map(|column| {
                    column
                        .id()
                        .map(|id| (id, ScalarExpression::column_expr(column)))
                })
                .unzip();
            let schema = self.input.output_schema().clone();
            throw!(
                co,
                BindPosition::bind_exprs(
                    column_exprs.iter_mut(),
                    || schema.iter().map(Cow::Borrowed),
                    |a, b| a == b
                )
            );
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
                        throw!(co, Err(DatabaseError::DuplicateIndex(index_name)))
                    }
                }
                err => throw!(co, err),
            };
            let mut coroutine = build_read(self.input, cache, transaction);

            for tuple in coroutine.by_ref() {
                let tuple: Tuple = throw!(co, tuple);

                let Some(value) = DataValue::values_to_tuple(throw!(
                    co,
                    Projection::projection(&tuple, &column_exprs, &schema)
                )) else {
                    continue;
                };
                let tuple_id = if let Some(tuple_id) = tuple.pk.as_ref() {
                    tuple_id
                } else {
                    continue;
                };
                let index = Index::new(index_id, &value, ty);
                throw!(
                    co,
                    unsafe { &mut (*transaction) }.add_index(table_name.as_ref(), index, tuple_id)
                );
            }
            co.yield_(Ok(TupleBuilder::build_result("1".to_string())))
                .await;
        })
    }
}
