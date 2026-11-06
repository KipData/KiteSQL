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
use crate::execution::dql::aggregate::{
    push_accumulators, update_accumulators, write_aggregate_output, Accumulator,
};
use crate::execution::hash::FxBuildHasher;
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::value::DataValue;
use hashbrown::HashTable;
use std::hash::BuildHasher;
use std::mem;

pub struct HashAggExecutor<'a> {
    agg_calls: &'a [ExprRef],
    groupby_exprs: &'a [ExprRef],
    input: ExecNode<'a>,
    // Every group's keys and accumulators back to back, in the order the groups appear:
    // `groupby_exprs.len()` keys and `agg_calls.len()` accumulators per group.
    keys: Vec<DataValue>,
    accs: Vec<Box<dyn Accumulator>>,
    // Group indexes by key hash, which locate each group's keys and accumulators. Kept across
    // executions for its capacity.
    groups: HashTable<usize>,
    // The next group to output, once every input row is folded.
    output: Option<usize>,
}

impl<'a> ReadExecutor<'a> for HashAggExecutor<'a> {
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
        ExecNode::HashAgg(Box::new(HashAggExecutor {
            agg_calls,
            groupby_exprs,
            input,
            keys: Vec::new(),
            accs: Vec::new(),
            groups: HashTable::new(),
            output: None,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for HashAggExecutor<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.keys.clear();
        self.accs.clear();
        self.groups.clear();
        self.output = None;
        self.input.reset(arena, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let len = self.groupby_exprs.len();
        let acc_len = self.agg_calls.len();
        if self.output.is_none() {
            let hasher = FxBuildHasher::default();
            let groups = &mut self.groups;

            while self.input.next_tuple(arena, plan_arena)? {
                let tuple = arena.result_tuple();
                // Evaluate the key in place, and drop it again if its group exists.
                let start = self.keys.len();
                for expr in self.groupby_exprs {
                    let key = plan_arena.expression(*expr).eval(plan_arena, Some(tuple))?;
                    // SAFETY: the keys live until `reset` clears them, before the next execution.
                    self.keys.push(unsafe { DataValue::own_in_txn(key) });
                }

                let keys = &self.keys;
                let hash = hasher.hash_one(&keys[start..]);
                let group = groups
                    .find(hash, |&group| keys[group * len..][..len] == keys[start..])
                    .copied();
                let group = match group {
                    Some(group) => {
                        self.keys.truncate(start);
                        group
                    }
                    None => {
                        let group = groups.len();
                        groups.insert_unique(hash, group, |&group| {
                            hasher.hash_one(&keys[group * len..][..len])
                        });
                        push_accumulators(&mut self.accs, self.agg_calls, plan_arena)?;
                        group
                    }
                };
                update_accumulators(
                    &mut self.accs[group * acc_len..][..acc_len],
                    self.agg_calls,
                    tuple,
                    plan_arena,
                )?;
            }

            self.output = Some(0);
        }

        // Groups are numbered as they appear, so they are output in that order.
        let Some(group) = self.output.filter(|group| *group < self.groups.len()) else {
            arena.finish();
            return Ok(());
        };
        self.output = Some(group + 1);
        let group_keys = self.keys[group * len..][..len]
            .iter_mut()
            .map(|value| mem::replace(value, DataValue::Null));

        write_aggregate_output(
            arena.output_tuple_mut(),
            &mut self.accs[group * acc_len..][..acc_len],
            group_keys,
        )?;
        arena.resume();
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::dql::aggregate::hash_agg::HashAggExecutor;
    use crate::execution::dql::test::build_integers;
    use crate::execution::{execute_input, try_collect};
    use crate::expression::agg::AggKind;
    use crate::expression::ScalarExpression;
    use crate::iter_ext::Itertools;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::aggregate::AggregateOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::test::PlanArenaTestExt;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use tempfile::TempDir;

    #[test]
    fn test_hash_agg() -> Result<(), DatabaseError> {
        let table_cache = crate::storage::TableCache::default();

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path()).unwrap();
        let transaction = storage.transaction()?;
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);

        let t1_schema = vec![
            plan_arena.alloc_column(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            plan_arena.alloc_column(ColumnCatalog::new("c2".to_string(), true, desc.clone())),
            plan_arena.alloc_column(ColumnCatalog::new("c3".to_string(), true, desc.clone())),
        ];

        let input = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                plan_arena.alloc_expression_rows(&[
                    vec![
                        DataValue::Int32(0),
                        DataValue::Int32(2),
                        DataValue::Int32(4),
                    ],
                    vec![
                        DataValue::Int32(1),
                        DataValue::Int32(3),
                        DataValue::Int32(5),
                    ],
                    vec![
                        DataValue::Int32(0),
                        DataValue::Int32(1),
                        DataValue::Int32(2),
                    ],
                    vec![
                        DataValue::Int32(1),
                        DataValue::Int32(2),
                        DataValue::Int32(3),
                    ],
                ]),
                4,
                t1_schema.clone(),
            )),
            Childrens::None,
        );
        let groupby_expr =
            plan_arena.alloc_expression(ScalarExpression::column_expr(t1_schema[0], 0));
        let agg_arg = plan_arena.alloc_expression(ScalarExpression::column_expr(t1_schema[1], 1));
        let agg_call = plan_arena.alloc_expression(ScalarExpression::AggCall {
            distinct: false,
            kind: AggKind::Sum,
            args: vec![agg_arg],
            ty: LogicalType::Integer,
        });
        let plan = LogicalPlan::new(
            Operator::Aggregate(AggregateOperator {
                groupby_exprs: vec![groupby_expr],
                agg_calls: vec![agg_call],
                is_distinct: false,
                force_spill: false,
            }),
            Childrens::Only(Box::new(input)),
        );

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "Expression Remapper",
                HepBatchStrategy::once_topdown(),
                &[NormalizationRuleImpl::EvaluatorBind],
            )
            .build();
        let plan = pipeline
            .instantiate(plan)
            .find_best(None, &mut plan_arena)?;

        let Operator::Aggregate(op) = &plan.operator else {
            unreachable!()
        };
        let tuples = try_collect(execute_input::<_, HashAggExecutor>(
            (op, plan.childrens.iter().next().unwrap()),
            &table_cache,
            plan_arena,
            &transaction,
        ))?;

        assert_eq!(tuples.len(), 2);

        let vec_values = tuples
            .into_iter()
            .map(|mut tuple| tuple.take_values())
            .collect_vec();

        assert!(vec_values.contains(&build_integers(vec![Some(3), Some(0)])));
        assert!(vec_values.contains(&build_integers(vec![Some(5), Some(1)])));

        Ok(())
    }
}
// GRCOV_EXCL_STOP
