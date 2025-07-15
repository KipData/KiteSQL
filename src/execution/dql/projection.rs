use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct Projection {
    exprs: Vec<ScalarExpression>,
    input: LogicalPlan,
}

impl From<(ProjectOperator, LogicalPlan)> for Projection {
    fn from((ProjectOperator { exprs }, input): (ProjectOperator, LogicalPlan)) -> Self {
        Projection { exprs, input }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Projection {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let Projection { exprs, mut input } = self;
                let schema = input.output_schema().clone();
                let mut coroutine = build_read(input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple = throw!(tuple);
                    let values = throw!(Self::projection(&tuple, &exprs, &schema));
                    yield Ok(Tuple::new(tuple.pk, values));
                }
            },
        )
    }
}

impl Projection {
    pub fn projection(
        tuple: &Tuple,
        exprs: &[ScalarExpression],
        schema: &[ColumnRef],
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let mut values = Vec::with_capacity(exprs.len());

        for expr in exprs.iter() {
            values.push(expr.eval(Some((tuple, schema)))?);
        }
        Ok(values)
    }
}
