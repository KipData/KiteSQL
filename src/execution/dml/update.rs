use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, WriteExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::index::Index;
use crate::types::tuple::types;
use crate::types::tuple::Tuple;
use crate::types::tuple_builder::TupleBuilder;
use crate::types::value::DataValue;
use std::collections::HashMap;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Update {
    table_name: TableName,
    value_exprs: Vec<(ColumnRef, ScalarExpression)>,
    input: LogicalPlan,
}

impl From<(UpdateOperator, LogicalPlan)> for Update {
    fn from(
        (
            UpdateOperator {
                table_name,
                value_exprs,
            },
            input,
        ): (UpdateOperator, LogicalPlan),
    ) -> Self {
        Update {
            table_name,
            value_exprs,
            input,
        }
    }
}

impl<'a, T: Transaction + 'a> WriteExecutor<'a, T> for Update {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Update {
                    table_name,
                    value_exprs,
                    mut input,
                } = self;

                let mut exprs_map = HashMap::with_capacity(value_exprs.len());
                for (column, expr) in value_exprs {
                    exprs_map.insert(column.id(), expr);
                }

                let input_schema = input.output_schema().clone();
                let types = types(&input_schema);

                if let Some(table_catalog) =
                    throw!(unsafe { &mut (*transaction) }.table(cache.0, table_name.clone()))
                        .cloned()
                {
                    let mut index_metas = Vec::new();
                    for index_meta in table_catalog.indexes() {
                        let exprs = throw!(index_meta.column_exprs(&table_catalog));
                        index_metas.push((index_meta, exprs));
                    }

                    let mut coroutine = build_read(input, cache, transaction);

                    while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                        let mut tuple: Tuple = throw!(tuple);

                        let mut is_overwrite = true;

                        let old_pk =
                            throw!(tuple.pk.clone().ok_or(DatabaseError::PrimaryKeyNotFound));
                        for (index_meta, exprs) in index_metas.iter() {
                            let values =
                                throw!(Projection::projection(&tuple, exprs, &input_schema));
                            let Some(value) = DataValue::values_to_tuple(values) else {
                                continue;
                            };
                            let index = Index::new(index_meta.id, &value, index_meta.ty);
                            throw!(unsafe { &mut (*transaction) }.del_index(
                                &table_name,
                                &index,
                                &old_pk
                            ));
                        }
                        for (i, column) in input_schema.iter().enumerate() {
                            if let Some(expr) = exprs_map.get(&column.id()) {
                                tuple.values[i] = throw!(expr.eval(Some((&tuple, &input_schema))));
                            }
                        }

                        tuple.pk = Some(Tuple::primary_projection(
                            table_catalog.primary_keys_indices(),
                            &tuple.values,
                        ));
                        let new_pk =
                            throw!(tuple.pk.as_ref().ok_or(DatabaseError::PrimaryKeyNotFound));

                        if new_pk != &old_pk {
                            throw!(
                                unsafe { &mut (*transaction) }.remove_tuple(&table_name, &old_pk)
                            );
                            is_overwrite = false;
                        }
                        for (index_meta, exprs) in index_metas.iter() {
                            let values =
                                throw!(Projection::projection(&tuple, exprs, &input_schema));
                            let Some(value) = DataValue::values_to_tuple(values) else {
                                continue;
                            };
                            let index = Index::new(index_meta.id, &value, index_meta.ty);
                            throw!(unsafe { &mut (*transaction) }.add_index(
                                &table_name,
                                index,
                                new_pk
                            ));
                        }

                        throw!(unsafe { &mut (*transaction) }.append_tuple(
                            &table_name,
                            tuple,
                            &types,
                            is_overwrite
                        ));
                    }
                    drop(coroutine);
                }
                yield Ok(TupleBuilder::build_result("1".to_string()));
            },
        )
    }
}
