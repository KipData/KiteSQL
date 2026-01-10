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
mod sum;

use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::avg::AvgAccumulator;
use crate::execution::dql::aggregate::count::{CountAccumulator, DistinctCountAccumulator};
use crate::execution::dql::aggregate::min_max::MinMaxAccumulator;
use crate::execution::dql::aggregate::sum::{DistinctSumAccumulator, SumAccumulator};
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;
use itertools::Itertools;

/// Tips: Idea for sqlrs
/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator: Send + Sync {
    /// updates the accumulator's state from a vector of arrays.
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<DataValue, DatabaseError>;
}

fn create_accumulator(expr: &ScalarExpression) -> Result<Box<dyn Accumulator>, DatabaseError> {
    if let ScalarExpression::AggCall {
        kind, ty, distinct, ..
    } = expr
    {
        Ok(match (kind, distinct) {
            (AggKind::Count, false) => Box::new(CountAccumulator::new()),
            (AggKind::Count, true) => Box::new(DistinctCountAccumulator::new()),
            (AggKind::Sum, false) => Box::new(SumAccumulator::new(ty)?),
            (AggKind::Sum, true) => Box::new(DistinctSumAccumulator::new(ty)?),
            (AggKind::Min, _) => Box::new(MinMaxAccumulator::new(false)),
            (AggKind::Max, _) => Box::new(MinMaxAccumulator::new(true)),
            (AggKind::Avg, _) => Box::new(AvgAccumulator::new()),
        })
    } else {
        unreachable!(
            "create_accumulator called with non-aggregate expression {}",
            expr
        );
    }
}

pub(crate) fn create_accumulators(
    exprs: &[ScalarExpression],
) -> Result<Vec<Box<dyn Accumulator>>, DatabaseError> {
    exprs.iter().map(create_accumulator).try_collect()
}
