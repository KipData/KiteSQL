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

use crate::errors::DatabaseError;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::expression::window::WindowFunctionKind;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::window::WindowOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::mem;

mod function;

use function::WindowFunction;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Retention {
    Row,
    Peer,
    Partition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    Partition,
    Peer,
}

#[derive(Default)]
struct WindowState {
    buffered: Vec<(usize, Tuple)>,
    pending: Option<(Tuple, Boundary)>,
    sort_values: Vec<DataValue>,
    started: bool,
    partition_rows: usize,
    peer_start: usize,
    peer_index: usize,
}

impl WindowState {
    fn begin_partition(&mut self) {
        self.partition_rows = 0;
        self.peer_start = 0;
        self.peer_index = 0;
    }

    fn begin_peer(&mut self) {
        self.peer_start = self.partition_rows;
        self.peer_index += 1;
    }
}

pub struct Window {
    state: WindowState,
    retention: Retention,
    sort_fields: Vec<SortField>,
    partition_by_len: usize,
    functions: Vec<Box<dyn WindowFunction>>,
    input_exhausted: bool,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for Window {
    type Input = (WindowOperator, LogicalPlan);

    fn into_executor(
        (operator, input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        let WindowOperator {
            sort_fields,
            partition_by_len,
            functions: window_functions,
            ..
        } = operator;
        let has_aggregate = window_functions
            .iter()
            .any(|function| matches!(function.kind, WindowFunctionKind::Aggregate(_)));
        let retention = if !has_aggregate {
            Retention::Row
        } else if sort_fields.len() == partition_by_len {
            Retention::Partition
        } else {
            Retention::Peer
        };
        let functions = window_functions
            .into_iter()
            .map(|function| function::new(function.kind, function.args, function.ty))
            .collect();
        arena.push(ExecNode::Window(Window {
            state: WindowState::default(),
            retention,
            sort_fields,
            partition_by_len,
            functions,
            input_exhausted: false,
            input,
        }))
    }
}

impl Window {
    fn update_keys(&mut self, tuple: &Tuple) -> Result<Option<Boundary>, DatabaseError> {
        let mut boundary = (!self.state.started).then_some(Boundary::Partition);
        for (index, field) in self.sort_fields.iter().enumerate() {
            let value = field.expr.eval(Some(tuple))?;
            if self.state.started && self.state.sort_values[index] != value {
                if index < self.partition_by_len {
                    boundary = Some(Boundary::Partition);
                } else if boundary.is_none() {
                    boundary = Some(Boundary::Peer);
                }
                self.state.sort_values[index] = value;
            } else if !self.state.started {
                self.state.sort_values.push(value);
            }
        }
        self.state.started = true;
        Ok(boundary)
    }

    fn reset_functions(&mut self) -> Result<(), DatabaseError> {
        for function in &mut self.functions {
            function.reset()?;
        }
        Ok(())
    }

    fn eval_functions(&mut self) -> Result<(), DatabaseError> {
        if self.state.buffered.is_empty() {
            return Ok(());
        }
        let output_offset = self.state.buffered[0].1.values.len();
        for (_, row) in &mut self.state.buffered {
            row.values
                .resize(output_offset + self.functions.len(), DataValue::Null);
        }
        let len = self.state.buffered.len();
        for (slot, function) in self.functions.iter_mut().enumerate() {
            function.evaluate(
                &mut self.state.buffered,
                0..len,
                self.state.peer_start,
                self.state.peer_index,
                output_offset + slot,
            )?;
        }
        self.state.buffered.reverse();
        Ok(())
    }

    fn eval(&mut self, tuple: Tuple, boundary: Option<Boundary>) -> Result<bool, DatabaseError> {
        let boundary = match boundary {
            Some(boundary) => Some(boundary),
            None => self.update_keys(&tuple)?,
        };
        if let Some(boundary) = boundary {
            let reached_boundary = boundary == Boundary::Partition
                || boundary == Boundary::Peer && self.retention == Retention::Peer;
            if reached_boundary && !self.state.buffered.is_empty() {
                self.state.pending = Some((tuple, boundary));
                self.eval_functions()?;
                return Ok(true);
            }
        }

        match boundary {
            Some(Boundary::Partition) => {
                self.state.begin_partition();
                self.reset_functions()?;
            }
            Some(Boundary::Peer) => self.state.begin_peer(),
            None => {}
        }

        let row_index = self.state.partition_rows;
        self.state.partition_rows += 1;
        self.state.buffered.push((row_index, tuple));
        if self.retention == Retention::Row {
            self.eval_functions()?;
            return Ok(true);
        }
        Ok(false)
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Window {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        let mut output_ready = true;
        loop {
            if output_ready {
                if let Some((_, tuple)) = self.state.buffered.pop() {
                    arena.produce_tuple(tuple);
                    return Ok(());
                }
            }
            if self.input_exhausted {
                arena.finish();
                return Ok(());
            }

            let (tuple, boundary) = if let Some((tuple, boundary)) = self.state.pending.take() {
                (tuple, Some(boundary))
            } else if arena.next_tuple(self.input, plan_arena)? {
                (mem::take(arena.result_tuple_mut()), None)
            } else {
                self.eval_functions()?;
                self.input_exhausted = true;
                output_ready = true;
                continue;
            };

            output_ready = self.eval(tuple, boundary)?;
        }
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::ColumnRef;
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::types::LogicalType;

    fn column(position: usize) -> ScalarExpression {
        ScalarExpression::column_expr(ColumnRef::new(position + 1), position)
    }

    fn window(
        retention: Retention,
        sort_fields: Vec<SortField>,
        partition_by_len: usize,
        functions: Vec<Box<dyn WindowFunction>>,
    ) -> Window {
        Window {
            state: WindowState::default(),
            retention,
            sort_fields,
            partition_by_len,
            functions,
            input_exhausted: false,
            input: 0,
        }
    }

    fn tuple(values: &[i32]) -> Tuple {
        Tuple::new(None, values.iter().copied().map(DataValue::from).collect())
    }

    #[test]
    fn row_materialization_streams_rows() -> Result<(), DatabaseError> {
        let mut window = window(
            Retention::Row,
            vec![column(0).asc(), column(1).asc()],
            1,
            vec![
                function::new(
                    WindowFunctionKind::RowNumber,
                    Vec::new(),
                    LogicalType::Bigint,
                ),
                function::new(WindowFunctionKind::Rank, Vec::new(), LogicalType::Bigint),
            ],
        );
        for (value, expected) in [(10, [1_i64, 1]), (10, [2, 1]), (20, [3, 3])] {
            window.eval(tuple(&[1, value]), None)?;
            let row = window.state.buffered.pop().unwrap().1;
            assert_eq!(row.values[2..], expected.map(DataValue::from));
        }
        Ok(())
    }

    #[test]
    fn peer_materialization_waits_for_peer_boundary() -> Result<(), DatabaseError> {
        let mut window = window(
            Retention::Peer,
            vec![column(0).asc(), column(1).asc()],
            1,
            vec![function::new(
                WindowFunctionKind::Aggregate(AggKind::Sum),
                vec![column(1)],
                LogicalType::Integer,
            )],
        );
        window.eval(tuple(&[1, 10]), None)?;
        window.eval(tuple(&[1, 10]), None)?;
        window.eval(tuple(&[1, 20]), None)?;
        assert_eq!(window.state.buffered.len(), 2);
        assert!(window.state.pending.is_some());
        Ok(())
    }

    #[test]
    fn partition_materialization_waits_for_partition_boundary() -> Result<(), DatabaseError> {
        let mut window = window(
            Retention::Partition,
            vec![column(0).asc()],
            1,
            vec![function::new(
                WindowFunctionKind::Aggregate(AggKind::Sum),
                vec![column(1)],
                LogicalType::Integer,
            )],
        );
        window.eval(tuple(&[1, 3]), None)?;
        window.eval(tuple(&[1, 7]), None)?;
        window.eval(tuple(&[2, 5]), None)?;
        assert_eq!(window.state.buffered.len(), 2);
        assert!(window.state.pending.is_some());
        Ok(())
    }
}
// GRCOV_EXCL_STOP
