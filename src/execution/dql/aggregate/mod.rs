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
mod group_concat;
pub mod hash_agg;
mod min_max;
pub mod simple_agg;
pub mod stream_agg;
pub mod stream_distinct;
mod sum;
mod total;

use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::avg::AvgAccumulator;
use crate::execution::dql::aggregate::count::{CountAccumulator, DistinctCountAccumulator};
use crate::execution::dql::aggregate::group_concat::GroupConcatAccumulator;
use crate::execution::dql::aggregate::min_max::MinMaxAccumulator;
use crate::execution::dql::aggregate::sum::{DistinctSumAccumulator, SumAccumulator};
use crate::execution::dql::aggregate::total::TotalAccumulator;
use crate::expression::agg::AggKind;
use crate::expression::ScalarExpression;
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::tuple::{Tuple, TupleLike};
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

    fn take_result(&mut self) -> DataValue;

    fn reset(&mut self);
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
        (AggKind::Avg, distinct) => Box::new(AvgAccumulator::new(distinct)),
        (AggKind::Total, distinct) => Box::new(TotalAccumulator::new(distinct)),
        (AggKind::GroupConcat, distinct) => Box::new(GroupConcatAccumulator::new(distinct)),
    })
}

#[inline]
pub(crate) fn push_accumulators(
    accs: &mut Vec<Box<dyn Accumulator>>,
    exprs: &[ExprRef],
    arena: &(dyn MetaArena + '_),
) -> Result<(), DatabaseError> {
    for expr in exprs {
        let ScalarExpression::AggCall {
            kind, ty, distinct, ..
        } = arena.expression(*expr)
        else {
            unreachable!("push_accumulators called with non-aggregate expression {expr}")
        };
        accs.push(create_accumulator(*kind, ty, *distinct)?);
    }
    Ok(())
}

pub(crate) fn prepare_accumulators(
    accs: &mut Vec<Box<dyn Accumulator>>,
    exprs: &[ExprRef],
    arena: &(dyn MetaArena + '_),
) -> Result<(), DatabaseError> {
    if accs.is_empty() {
        return push_accumulators(accs, exprs, arena);
    }
    for acc in accs {
        acc.reset();
    }
    Ok(())
}

pub(crate) fn update_accumulators<T: TupleLike + ?Sized>(
    accs: &mut [Box<dyn Accumulator>],
    agg_calls: &[ExprRef],
    tuple: &T,
    arena: &(dyn MetaArena + '_),
) -> Result<(), DatabaseError> {
    for (acc, expr) in accs.iter_mut().zip(agg_calls.iter()) {
        let value = aggregate_arg(*expr, tuple, arena)?;
        acc.update_value(&value)?;
    }
    Ok(())
}

pub(crate) fn aggregate_arg<'a, T: TupleLike + ?Sized>(
    agg_call: ExprRef,
    tuple: &'a T,
    arena: &'a (dyn MetaArena + '_),
) -> Result<Cow<'a, DataValue>, DatabaseError> {
    let ScalarExpression::AggCall { args, .. } = arena.expression(agg_call) else {
        unreachable!()
    };
    if args.len() > 1 {
        return Err(DatabaseError::UnsupportedStmt(
            "currently aggregate functions only support a single Column as a parameter".to_string(),
        ));
    }
    arena.expression(args[0]).eval(arena, Some(tuple))
}

pub(crate) fn write_aggregate_output(
    output: &mut Tuple,
    accs: &mut [Box<dyn Accumulator>],
    group_keys: impl IntoIterator<Item = DataValue>,
) -> Result<(), DatabaseError> {
    let group_keys = group_keys.into_iter();
    output.pk = None;
    output.values.clear();
    output.values.reserve(accs.len() + group_keys.size_hint().0);
    for acc in accs {
        acc.evaluate()?;
        output.values.push(acc.take_result());
    }
    output.values.extend(group_keys);
    Ok(())
}
