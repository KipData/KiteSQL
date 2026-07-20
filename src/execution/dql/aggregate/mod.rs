// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod avg;
mod count;
pub mod hash_agg;
mod min_max;
pub mod simple_agg;
pub mod stream_agg;
pub mod stream_distinct;
mod sum;

use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::avg::AvgAccumulator;
use crate::execution::dql::aggregate::count::{CountAccumulator, DistinctCountAccumulator};
use crate::execution::dql::aggregate::min_max::MinMaxAccumulator;
use crate::execution::dql::aggregate::sum::{DistinctSumAccumulator, SumAccumulator};
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::iter_ext::Itertools;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::borrow::Cow;

/// Tips: Idea for sqlrs
/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator {
    /// updates the accumulator's state from a vector of arrays.
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError>;

    /// evaluates its result based on its current state.
    fn evaluate(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn result(&self) -> &DataValue;

    fn result_owned(self: Box<Self>) -> DataValue;
}

pub(crate) fn create_accumulator(
    kind: AggKind,
    ty: &crate::types::LogicalType,
    distinct: bool,
) -> Result<Box<dyn Accumulator>, DatabaseError> {
    Ok(match (kind, distinct) {
        (AggKind::Count, false) => Box::new(CountAccumulator::new()),
        (AggKind::Count, true) => Box::new(DistinctCountAccumulator::new()),
        (AggKind::Sum, false) => Box::new(SumAccumulator::new(Cow::Borrowed(ty))?),
        (AggKind::Sum, true) => Box::new(DistinctSumAccumulator::new(ty)?),
        (AggKind::Min, _) => Box::new(MinMaxAccumulator::new(false)),
        (AggKind::Max, _) => Box::new(MinMaxAccumulator::new(true)),
        (AggKind::Avg, _) => Box::new(AvgAccumulator::new()),
    })
}

#[inline]
pub(crate) fn create_accumulators(
    exprs: &[ScalarExpression],
) -> Result<Vec<Box<dyn Accumulator>>, DatabaseError> {
    exprs
        .iter()
        .map(|expr| {
            let ScalarExpression::AggCall {
                kind, ty, distinct, ..
            } = expr
            else {
                unreachable!("create_accumulators called with non-aggregate expression {expr}")
            };
            create_accumulator(*kind, ty, *distinct)
        })
        .try_collect()
}

pub(crate) fn update_accumulators(
    accs: &mut [Box<dyn Accumulator>],
    agg_calls: &[ScalarExpression],
    tuple: &Tuple,
) -> Result<(), DatabaseError> {
    for (acc, expr) in accs.iter_mut().zip(agg_calls.iter()) {
        let ScalarExpression::AggCall { args, .. } = expr else {
            unreachable!()
        };
        if args.len() > 1 {
            return Err(DatabaseError::UnsupportedStmt(
                "currently aggregate functions only support a single Column as a parameter"
                    .to_string(),
            ));
        }
        let value = args[0].eval(Some(tuple))?;
        acc.update_value(&value)?;
    }
    Ok(())
}

pub(crate) fn write_aggregate_output(
    output: &mut Tuple,
    accs: Vec<Box<dyn Accumulator>>,
    group_keys: Vec<DataValue>,
) -> Result<(), DatabaseError> {
    output.pk = None;
    output.values.clear();
    output.values.reserve(accs.len() + group_keys.len());
    for mut acc in accs {
        acc.evaluate()?;
        output.values.push(acc.result_owned());
    }
    output.values.extend(group_keys);
    Ok(())
}
