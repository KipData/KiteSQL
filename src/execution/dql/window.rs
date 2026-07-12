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
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::window::WindowOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use std::mem;

mod function;

use function::WindowFunction;

pub struct Window {
    partition_by: Vec<ScalarExpression>,
    order_by: Vec<SortField>,
    functions: Vec<Box<dyn WindowFunction>>,
    input: ExecId,
    pending: Option<Tuple>,
    rows: Vec<Tuple>,
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
            partition_by,
            order_by,
            functions: window_functions,
            ..
        } = operator;
        let mut functions = Vec::with_capacity(window_functions.len());
        for function in window_functions {
            let crate::expression::window::WindowFunction { kind, args, ty } = function;
            functions.push(function::new(kind, args, ty));
        }
        arena.push(ExecNode::Window(Window {
            partition_by,
            order_by,
            functions,
            input,
            pending: None,
            rows: Vec::new(),
        }))
    }
}

fn evaluate_partition(
    rows: &mut [Tuple],
    order_by: &[SortField],
    functions: &mut [Box<dyn WindowFunction>],
) -> Result<(), DatabaseError> {
    let Some(first) = rows.first() else {
        return Ok(());
    };
    let output_offset = first.values.len();
    for row in rows.iter_mut() {
        row.values
            .resize(output_offset + functions.len(), DataValue::Null);
    }
    for function in functions.iter_mut() {
        function.reset()?;
    }
    let mut peer_start = 0;
    let mut peer_index = 0;
    while peer_start < rows.len() {
        let mut peer_end = peer_start + 1;
        'peer: while peer_end < rows.len() {
            // TODO: Cache evaluated order keys to avoid recalculating the previous row.
            for field in order_by {
                if field.expr.eval(Some(&rows[peer_end - 1]))?
                    != field.expr.eval(Some(&rows[peer_end]))?
                {
                    break 'peer;
                }
            }
            peer_end += 1;
        }
        for (slot, function) in functions.iter_mut().enumerate() {
            function.evaluate(rows, peer_start..peer_end, peer_index, output_offset + slot)?;
        }
        peer_start = peer_end;
        peer_index += 1;
    }
    Ok(())
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for Window {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            if let Some(tuple) = self.rows.pop() {
                arena.produce_tuple(tuple);
                return Ok(());
            }

            let first = if let Some(tuple) = self.pending.take() {
                tuple
            } else if arena.next_tuple(self.input, plan_arena)? {
                mem::take(arena.result_tuple_mut())
            } else {
                arena.finish();
                return Ok(());
            };
            self.rows.push(first);

            while arena.next_tuple(self.input, plan_arena)? {
                let tuple = mem::take(arena.result_tuple_mut());
                let mut same_partition = true;
                // TODO: Cache evaluated partition keys to avoid recalculating the previous row.
                for expr in &self.partition_by {
                    if expr.eval(self.rows.last())? != expr.eval(Some(&tuple))? {
                        same_partition = false;
                        break;
                    }
                }
                if same_partition {
                    self.rows.push(tuple);
                } else {
                    self.pending = Some(tuple);
                    break;
                }
            }

            evaluate_partition(&mut self.rows, &self.order_by, &mut self.functions)?;
            self.rows.reverse();
        }
    }
}
