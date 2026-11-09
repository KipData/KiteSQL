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

use crate::dual::dual;
use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::{
    aggregate_arg, prepare_accumulators, update_accumulators, write_aggregate_output, Accumulator,
};
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::value::DataValue;
use std::borrow::Cow;
use std::mem;

// The optimizer selects this executor only when equal group keys are contiguous in the input.
pub struct StreamAggExecutor<'a> {
    agg_calls: &'a [ExprRef],
    groupby_exprs: &'a [ExprRef],
    // The keys of the open group, followed by the keys of the row being read.
    group_keys: Vec<DataValue>,
    in_group: bool,
    accs: Vec<Box<dyn Accumulator>>,
    carried: bool,
    carried_args: Vec<DataValue>,
    input: ExecNode<'a>,
}

impl<'a> ReadExecutor<'a> for StreamAggExecutor<'a> {
    type Input = (&'a AggregateOperator, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
                ..
            },
            input,
        ): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let input = build_read(arena, plan_arena, input);
        ExecNode::StreamAgg(Box::new(StreamAggExecutor {
            agg_calls,
            groupby_exprs,
            group_keys: Vec::new(),
            in_group: false,
            accs: Vec::new(),
            carried: false,
            carried_args: Vec::new(),
            input,
        }))
    }
}

#[dual]
impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for StreamAggExecutor<'a> {
    async fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.group_keys.clear();
        self.in_group = false;
        self.carried = false;
        self.carried_args.clear();
        self.input.reset(arena, plan_arena).await
    }

    async fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let len = self.groupby_exprs.len();
        if mem::take(&mut self.carried) {
            prepare_accumulators(&mut self.accs, self.agg_calls, plan_arena)?;
            for (acc, arg) in self.accs.iter_mut().zip(&self.carried_args) {
                acc.update_value(arg)?;
            }
            arena.bytes_pool().recycle(&mut self.carried_args);
        }
        loop {
            if !self.input.next_tuple(arena, plan_arena).await? {
                if !mem::take(&mut self.in_group) {
                    arena.finish();
                    return Ok(());
                }
                write_aggregate_output(
                    arena.output_tuple_mut(),
                    &mut self.accs,
                    self.group_keys.drain(..),
                )?;
                arena.resume();
                return Ok(());
            }

            let tuple = arena.result_tuple();
            for expr in self.groupby_exprs {
                let key = plan_arena.expression(*expr).eval(plan_arena, Some(tuple))?;
                // SAFETY: the keys live until `reset` clears them, before the next execution.
                self.group_keys.push(unsafe { DataValue::own_in_txn(key) });
            }

            if !self.in_group {
                self.in_group = true;
                prepare_accumulators(&mut self.accs, self.agg_calls, plan_arena)?;
                update_accumulators(&mut self.accs, self.agg_calls, tuple, plan_arena)?;
            } else if self.group_keys[..len] == self.group_keys[len..] {
                self.group_keys.truncate(len);
                update_accumulators(&mut self.accs, self.agg_calls, tuple, plan_arena)?;
            } else {
                let (tuple, pool) = arena.result_tuple_and_pool();
                for expr in self.agg_calls {
                    let arg = match aggregate_arg(*expr, tuple, plan_arena)? {
                        Cow::Owned(arg) => arg,
                        Cow::Borrowed(arg) => pool.clone_value(arg),
                    };
                    self.carried_args.push(arg);
                }
                self.carried = true;
                // Draining the closed group's keys moves the row's keys to the front.
                write_aggregate_output(
                    arena.output_tuple_mut(),
                    &mut self.accs,
                    self.group_keys.drain(..len),
                )?;
                arena.resume();
                return Ok(());
            }
        }
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
#[crate::dual::dual]
mod tests {
    use super::StreamAggExecutor;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::{execute_input, try_collect};
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::test::PlanArenaTestExt;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    async fn aggregates_sorted_groups() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let columns = ["group", "value"]
            .map(|name| {
                plan_arena.alloc_column(ColumnCatalog::new(name.to_string(), true, desc.clone()))
            })
            .to_vec();
        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                plan_arena.alloc_expression_rows(&[
                    vec![1.into(), 10.into()],
                    vec![1.into(), 20.into()],
                    vec![2.into(), 5.into()],
                    vec![2.into(), DataValue::Null],
                    vec![2.into(), 7.into()],
                ]),
                5,
                columns.clone(),
            )),
            Childrens::None,
        );
        let group = plan_arena.alloc_expression(ScalarExpression::column_expr(columns[0], 0));
        let value = plan_arena.alloc_expression(ScalarExpression::column_expr(columns[1], 1));
        let sum = plan_arena.alloc_expression(ScalarExpression::AggCall {
            distinct: false,
            kind: AggKind::Sum,
            args: vec![value],
            ty: LogicalType::Integer,
        });
        let count = plan_arena.alloc_expression(ScalarExpression::AggCall {
            distinct: false,
            kind: AggKind::Count,
            args: vec![value],
            ty: LogicalType::Integer,
        });
        let operator = AggregateOperator {
            groupby_exprs: vec![group],
            agg_calls: vec![sum, count],
            is_distinct: false,
            force_spill: false,
        };
        let table_cache = crate::storage::TableCache::default();
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;

        let rows = try_collect(execute_input::<_, StreamAggExecutor>(
            (&operator, &input),
            &table_cache,
            plan_arena,
            &transaction,
        ))
        .await?;

        assert_eq!(
            rows.into_iter()
                .map(|mut row| row.take_values())
                .collect::<Vec<_>>(),
            vec![
                vec![30.into(), 2.into(), 1.into()],
                vec![12.into(), 2.into(), 2.into()],
            ]
        );
        Ok(())
    }

    #[test]
    async fn empty_input_returns_no_groups() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let column = plan_arena.alloc_column(ColumnCatalog::new(
            "group".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(Vec::new(), 0, vec![column])),
            Childrens::None,
        );
        let operator = AggregateOperator {
            groupby_exprs: vec![
                plan_arena.alloc_expression(ScalarExpression::column_expr(column, 0))
            ],
            agg_calls: Vec::new(),
            is_distinct: false,
            force_spill: false,
        };
        let table_cache = crate::storage::TableCache::default();
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;

        let rows = try_collect(execute_input::<_, StreamAggExecutor>(
            (&operator, &input),
            &table_cache,
            plan_arena,
            &transaction,
        ))
        .await?;

        assert!(rows.is_empty());
        Ok(())
    }
}
// GRCOV_EXCL_STOP
