// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::{create_accumulator, Accumulator};
use crate::expression::agg::AggKind;
use crate::expression::window::WindowFunctionKind;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::ops::Range;

pub(super) trait WindowFunction {
    fn reset(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn evaluate(
        &mut self,
        rows: &mut [Tuple],
        peer: Range<usize>,
        peer_index: usize,
        output_position: usize,
    ) -> Result<(), DatabaseError>;
}

struct RowNumber;

impl WindowFunction for RowNumber {
    fn evaluate(
        &mut self,
        rows: &mut [Tuple],
        peer: Range<usize>,
        _peer_index: usize,
        output_position: usize,
    ) -> Result<(), DatabaseError> {
        let start = peer.start;
        for (offset, row) in rows[peer].iter_mut().enumerate() {
            row.values[output_position] = DataValue::Int64((start + offset + 1) as i64);
        }
        Ok(())
    }
}

struct Rank {
    dense: bool,
}

impl WindowFunction for Rank {
    fn evaluate(
        &mut self,
        rows: &mut [Tuple],
        peer: Range<usize>,
        peer_index: usize,
        output_position: usize,
    ) -> Result<(), DatabaseError> {
        let rank = if self.dense {
            peer_index + 1
        } else {
            peer.start + 1
        };
        for row in &mut rows[peer] {
            row.values[output_position] = DataValue::Int64(rank as i64);
        }
        Ok(())
    }
}

struct Aggregate {
    kind: AggKind,
    ty: LogicalType,
    arg: ScalarExpression,
    accumulator: Option<Box<dyn Accumulator>>,
}

impl WindowFunction for Aggregate {
    fn reset(&mut self) -> Result<(), DatabaseError> {
        self.accumulator = Some(create_accumulator(self.kind, &self.ty, false)?);
        Ok(())
    }

    fn evaluate(
        &mut self,
        rows: &mut [Tuple],
        peer: Range<usize>,
        _peer_index: usize,
        output_position: usize,
    ) -> Result<(), DatabaseError> {
        let Some(accumulator) = self.accumulator.as_mut() else {
            unreachable!()
        };
        for row in &rows[peer.clone()] {
            accumulator.update_value(&self.arg.eval(Some(row))?)?;
        }
        accumulator.evaluate()?;
        let result = accumulator.result();
        for row in &mut rows[peer] {
            row.values[output_position] = result.clone();
        }
        Ok(())
    }
}

pub(super) fn new(
    kind: WindowFunctionKind,
    args: Vec<ScalarExpression>,
    ty: LogicalType,
) -> Box<dyn WindowFunction> {
    match kind {
        WindowFunctionKind::RowNumber => Box::new(RowNumber),
        WindowFunctionKind::Rank => Box::new(Rank { dense: false }),
        WindowFunctionKind::DenseRank => Box::new(Rank { dense: true }),
        WindowFunctionKind::Aggregate(kind) => {
            let Some(arg) = args.into_iter().next() else {
                unreachable!()
            };
            Box::new(Aggregate {
                kind,
                ty,
                arg,
                accumulator: None,
            })
        }
    }
}
