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
use crate::execution::dql::aggregate::{
    create_accumulators, update_accumulators, write_aggregate_output, Accumulator,
};
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::value::DataValue;
use std::mem;

// The optimizer selects this executor only when equal group keys are contiguous in the input.
pub struct StreamAggExecutor {
    agg_calls: Vec<ScalarExpression>,
    groupby_exprs: Vec<ScalarExpression>,
    group_keys: Option<Vec<DataValue>>,
    accs: Vec<Box<dyn Accumulator>>,
    input: ExecId,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for StreamAggExecutor {
    type Input = (AggregateOperator, LogicalPlan);

    fn into_executor(
        (
            AggregateOperator {
                agg_calls,
                groupby_exprs,
                ..
            },
            input,
        ): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        arena.push(ExecNode::StreamAgg(StreamAggExecutor {
            agg_calls,
            groupby_exprs,
            group_keys: None,
            accs: Vec::new(),
            input,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for StreamAggExecutor {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            if !arena.next_tuple(self.input, plan_arena)? {
                let Some(group_keys) = self.group_keys.take() else {
                    arena.finish();
                    return Ok(());
                };
                write_aggregate_output(
                    arena.result_tuple_mut(),
                    mem::take(&mut self.accs),
                    group_keys,
                )?;
                arena.resume();
                return Ok(());
            }

            let tuple = arena.result_tuple();
            let mut group_keys = Vec::with_capacity(self.groupby_exprs.len());
            for expr in &self.groupby_exprs {
                group_keys.push(expr.eval(Some(tuple))?);
            }

            match &mut self.group_keys {
                None => {
                    self.accs = create_accumulators(&self.agg_calls)?;
                    update_accumulators(&mut self.accs, &self.agg_calls, tuple)?;
                    self.group_keys = Some(group_keys);
                }
                Some(current_keys) if current_keys == &group_keys => {
                    update_accumulators(&mut self.accs, &self.agg_calls, tuple)?;
                }
                Some(current_keys) => {
                    let mut next_accs = create_accumulators(&self.agg_calls)?;
                    update_accumulators(&mut next_accs, &self.agg_calls, tuple)?;
                    mem::swap(current_keys, &mut group_keys);
                    let current_accs = mem::replace(&mut self.accs, next_accs);
                    write_aggregate_output(arena.result_tuple_mut(), current_accs, group_keys)?;
                    arena.resume();
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::StreamAggExecutor;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::{empty_context, execute_input, try_collect};
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn aggregates_sorted_groups() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let columns = ["group", "value"]
            .map(|name| {
                plan_arena.alloc_column(ColumnCatalog::new(name.to_string(), true, desc.clone()))
            })
            .to_vec();
        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
                    vec![1.into(), 10.into()],
                    vec![1.into(), 20.into()],
                    vec![2.into(), 5.into()],
                    vec![2.into(), DataValue::Null],
                    vec![2.into(), 7.into()],
                ],
                schema_ref: columns.clone(),
            }),
            Childrens::None,
        );
        let value = ScalarExpression::column_expr(columns[1], 1);
        let operator = AggregateOperator {
            groupby_exprs: vec![ScalarExpression::column_expr(columns[0], 0)],
            agg_calls: vec![
                ScalarExpression::AggCall {
                    distinct: false,
                    kind: AggKind::Sum,
                    args: vec![value.clone()],
                    ty: LogicalType::Integer,
                },
                ScalarExpression::AggCall {
                    distinct: false,
                    kind: AggKind::Count,
                    args: vec![value],
                    ty: LogicalType::Integer,
                },
            ],
            is_distinct: false,
            force_spill: false,
        };
        let table_cache = crate::storage::TableCache::default();
        let view_cache = crate::storage::ViewCache::default();
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;

        let rows = try_collect(execute_input::<_, StreamAggExecutor>(
            (operator, input),
            empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        ))?;

        assert_eq!(
            rows.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![
                vec![30.into(), 2.into(), 1.into()],
                vec![12.into(), 2.into(), 2.into()],
            ]
        );
        Ok(())
    }

    #[test]
    fn empty_input_returns_no_groups() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let column = plan_arena.alloc_column(ColumnCatalog::new(
            "group".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: Vec::new(),
                schema_ref: vec![column],
            }),
            Childrens::None,
        );
        let operator = AggregateOperator {
            groupby_exprs: vec![ScalarExpression::column_expr(column, 0)],
            agg_calls: Vec::new(),
            is_distinct: false,
            force_spill: false,
        };
        let table_cache = crate::storage::TableCache::default();
        let view_cache = crate::storage::ViewCache::default();
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;

        let rows = try_collect(execute_input::<_, StreamAggExecutor>(
            (operator, input),
            empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        ))?;

        assert!(rows.is_empty());
        Ok(())
    }
}
