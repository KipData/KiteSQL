use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::{Index, IndexId, IndexType};
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::collections::HashMap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Delete {
    table_name: TableName,
    input: LogicalPlan,
}

impl From<(DeleteOperator, LogicalPlan)> for Delete {
    fn from((DeleteOperator { table_name, .. }, input): (DeleteOperator, LogicalPlan)) -> Self {
        Delete { table_name, input }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Delete {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Delete {
                    table_name,
                    mut input,
                } = self;

                let schema = input.output_schema().clone();
                let table = throw!(throw!(
                    unsafe { &mut (*transaction) }.table(cache.0, table_name.clone())
                )
                .ok_or(DatabaseError::TableNotFound));
                let mut indexes: HashMap<IndexId, Value> = HashMap::new();

                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);

                    for index_meta in table.indexes() {
                        if let Some(Value { exprs, values, .. }) = indexes.get_mut(&index_meta.id) {
                            let Some(data_value) = DataValue::values_to_tuple(throw!(
                                Projection::projection(&tuple, exprs, &schema)
                            )) else {
                                continue;
                            };
                            values.push(data_value);
                        } else {
                            let mut values = Vec::with_capacity(table.indexes().len());
                            let exprs = throw!(index_meta.column_exprs(table));
                            let Some(data_value) = DataValue::values_to_tuple(throw!(
                                Projection::projection(&tuple, &exprs, &schema)
                            )) else {
                                continue;
                            };
                            values.push(data_value);

                            indexes.insert(
                                index_meta.id,
                                Value {
                                    exprs,
                                    values,
                                    index_ty: index_meta.ty,
                                },
                            );
                        }
                    }
                    if let Some(tuple_id) = &tuple.pk {
                        for (
                            index_id,
                            Value {
                                values, index_ty, ..
                            },
                        ) in indexes.iter_mut()
                        {
                            for value in values {
                                throw!(unsafe { &mut (*transaction) }.del_index(
                                    &table_name,
                                    &Index::new(*index_id, value, *index_ty),
                                    tuple_id,
                                ));
                            }
                        }

                        throw!(unsafe { &mut (*transaction) }.remove_tuple(&table_name, tuple_id));
                    }
                }
                drop(coroutine);
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}

struct Value {
    exprs: Vec<ScalarExpression>,
    values: Vec<DataValue>,
    index_ty: IndexType,
}
