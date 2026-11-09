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
use crate::execution::dql::join::hash::full_join::FullJoinState;
use crate::execution::dql::join::hash::inner_join::InnerJoinState;
use crate::execution::dql::join::hash::left_join::LeftJoinState;
use crate::execution::dql::join::hash::right_join::RightJoinState;
use crate::execution::dql::join::hash::{
    BuildRow, JoinProbeState, JoinProbeStateImpl, LeftDropState, ProbeState,
};
use crate::execution::hash::FxBuildHasher;
use crate::execution::{
    build_read, ExecBuilder, ExecContext, ExecNode, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::ExprRef;
use crate::planner::LogicalPlan;
use crate::planner::{ExecMetaArena, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::bitset::BitSet;
use crate::types::tuple::TupleImpl;
use crate::types::value::DataValue;
use hashbrown::HashTable;
use std::hash::BuildHasher;

pub struct HashJoin<'a> {
    state: HashJoinState,
    ty: JoinType,
    on_left_keys: Vec<ExprRef>,
    on_right_keys: Vec<ExprRef>,
    filter: Option<ExprRef>,
    left_schema_len: usize,
    right_schema_len: usize,
    left_input: ExecNode<'a>,
    right_input: ExecNode<'a>,
    init_error: Option<DatabaseError>,
}

/// A distinct build key and the chain of its rows in `BuildTable::rows`.
struct BuildKey {
    // Where the key starts in `BuildTable::keys`.
    offset: usize,
    first_row: usize,
    last_row: usize,
}

/// The build side: each distinct key once in `keys`, `on_left_keys.len()` values per key.
struct BuildTable {
    hasher: FxBuildHasher,
    table: HashTable<BuildKey>,
    keys: Vec<DataValue>,
    rows: Vec<BuildRow>,
}

impl BuildTable {
    fn first_row(&self, key: &[DataValue]) -> Option<usize> {
        let keys = &self.keys;
        self.table
            .find(self.hasher.hash_one(key), |build_key| {
                &keys[build_key.offset..][..key.len()] == key
            })
            .map(|build_key| build_key.first_row)
    }
}

enum HashJoinState {
    Build,
    Probe {
        build_table: Box<BuildTable>,
        join_impl: JoinProbeStateImpl,
        probe_buf: Vec<DataValue>,
        probe_state: Option<ProbeState>,
    },
    LeftDrop {
        join_impl: JoinProbeStateImpl,
        left_drop: LeftDropState,
    },
    End,
}

impl<'a> HashJoin<'a> {
    fn new(
        JoinOperator { on, join_type, .. }: &JoinOperator,
        left_schema_len: usize,
        right_schema_len: usize,
        left_input: ExecNode<'a>,
        right_input: ExecNode<'a>,
    ) -> Self {
        let ((on_left_keys, on_right_keys), filter_expr) = match on {
            JoinCondition::On { on, filter } => (on.iter().copied().unzip(), *filter),
            JoinCondition::None => ((vec![], vec![]), None),
        };
        let join_type = *join_type;

        let init_error = if join_type == JoinType::Cross {
            Some(DatabaseError::UnsupportedStmt(
                "Cross join should not be executed by HashJoin".to_string(),
            ))
        } else if on_left_keys.is_empty() || on_right_keys.is_empty() {
            Some(DatabaseError::UnsupportedStmt(
                "`NestLoopJoin` should be used when there is no equivalent condition".to_string(),
            ))
        } else {
            None
        };

        HashJoin {
            state: HashJoinState::Build,
            ty: join_type,
            on_left_keys,
            on_right_keys,
            filter: filter_expr,
            left_schema_len,
            right_schema_len,
            left_input,
            right_input,
            init_error,
        }
    }
}

#[maybe_async]
impl<'a> HashJoin<'a> {
    fn eval_keys(
        on_keys: &[ExprRef],
        tuple: &TupleImpl,
        buf: &mut Vec<DataValue>,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        for expr in on_keys {
            let key = plan_arena.expression(*expr).eval(plan_arena, Some(tuple))?;
            // SAFETY: the build table and probe keys are dropped when `reset` returns to
            // `HashJoinState::Build`, before the next execution.
            buf.push(unsafe { DataValue::own_in_txn(key) });
        }
        Ok(())
    }

    async fn initialize_build<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if !matches!(self.state, HashJoinState::Build) {
            return Ok(());
        }

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left tuples.
        let hasher = FxBuildHasher::default();
        let mut table: HashTable<BuildKey> = HashTable::new();
        let mut keys = Vec::new();
        let mut rows: Vec<BuildRow> = Vec::new();
        let len = self.on_left_keys.len();

        while self.left_input.next_tuple(arena, plan_arena).await? {
            let tuple = arena.take_tuple();
            let row = rows.len();
            // Evaluate the key in place, and drop it again if the key is already there.
            let start = keys.len();
            Self::eval_keys(&self.on_left_keys, &tuple, &mut keys, plan_arena)?;
            let hash = hasher.hash_one(&keys[start..]);
            let found = table.find_mut(hash, |build_key| {
                keys[build_key.offset..][..len] == keys[start..]
            });

            if let Some(build_key) = found {
                rows[build_key.last_row].next = Some(row);
                build_key.last_row = row;
                keys.truncate(start);
            } else {
                let build_key = BuildKey {
                    offset: start,
                    first_row: row,
                    last_row: row,
                };
                table.insert_unique(hash, build_key, |build_key| {
                    hasher.hash_one(&keys[build_key.offset..][..len])
                });
            }
            rows.push(BuildRow { tuple, next: None });
        }
        let build_count = rows.len();

        self.state = HashJoinState::Probe {
            join_impl: Self::create_join_impl(
                self.ty,
                self.left_schema_len,
                self.right_schema_len,
                build_count,
            ),
            probe_buf: Vec::with_capacity(self.on_right_keys.len()),
            build_table: Box::new(BuildTable {
                hasher,
                table,
                keys,
                rows,
            }),
            probe_state: None,
        };
        Ok(())
    }

    fn create_join_impl(
        ty: JoinType,
        left_schema_len: usize,
        right_schema_len: usize,
        build_count: usize,
    ) -> JoinProbeStateImpl {
        match ty {
            JoinType::Inner => JoinProbeStateImpl::Inner(InnerJoinState),
            JoinType::LeftOuter => JoinProbeStateImpl::Left(LeftJoinState {
                left_schema_len,
                right_schema_len,
                bits: BitSet::with_capacity(build_count),
            }),
            JoinType::RightOuter => JoinProbeStateImpl::Right(RightJoinState { left_schema_len }),
            JoinType::Full => JoinProbeStateImpl::Full(FullJoinState {
                left_schema_len,
                right_schema_len,
                bits: BitSet::with_capacity(build_count),
            }),
            JoinType::Cross => unreachable!(),
        }
    }
}

impl<'a> ReadExecutor<'a> for HashJoin<'a> {
    type Input = (&'a JoinOperator, &'a LogicalPlan, &'a LogicalPlan);

    fn into_executor<B: ExecBuilder>(
        (op, left_plan, right_plan): Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let left_schema_len = left_plan.read_schema().len();
        let right_schema_len = right_plan.read_schema().len();
        let left_input = build_read(arena, plan_arena, left_plan);
        let right_input = build_read(arena, plan_arena, right_plan);
        let executor = HashJoin::new(
            op,
            left_schema_len,
            right_schema_len,
            left_input,
            right_input,
        );
        ExecNode::HashJoin(Box::new(executor))
    }
}

#[maybe_async]
impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for HashJoin<'a> {
    async fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.state = HashJoinState::Build;
        self.left_input.reset(arena, plan_arena).await?;
        self.right_input.reset(arena, plan_arena).await
    }

    async fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        if let Some(err) = self.init_error.take() {
            return Err(err);
        }

        self.initialize_build(arena, plan_arena).await?;
        let mut state = std::mem::replace(&mut self.state, HashJoinState::End);

        loop {
            match state {
                HashJoinState::Build => unreachable!("hash join must be initialized before probe"),
                HashJoinState::Probe {
                    build_table,
                    mut join_impl,
                    mut probe_buf,
                    mut probe_state,
                } => {
                    let probe_finished = loop {
                        if probe_state.is_none() {
                            if !self.right_input.next_tuple(arena, plan_arena).await? {
                                break true;
                            }
                            let tuple = arena.take_tuple();
                            probe_buf.clear();
                            Self::eval_keys(
                                &self.on_right_keys,
                                &tuple,
                                &mut probe_buf,
                                plan_arena,
                            )?;
                            // A null key matches nothing.
                            let next_row = if probe_buf.iter().any(DataValue::is_null) {
                                None
                            } else {
                                build_table.first_row(&probe_buf)
                            };
                            probe_state = Some(ProbeState {
                                probe_tuple: tuple,
                                next_row,
                                produced: false,
                                finished: false,
                                emitted_unmatched: false,
                            });
                        }

                        let Some(probe) = probe_state.as_mut() else {
                            continue;
                        };

                        if let Some(tuple) = join_impl.probe_next(
                            probe,
                            &build_table.rows,
                            self.filter.as_ref(),
                            plan_arena,
                        )? {
                            if probe.finished {
                                probe_state = None;
                            }
                            self.state = HashJoinState::Probe {
                                build_table,
                                join_impl,
                                probe_buf,
                                probe_state,
                            };
                            arena.produce(tuple);
                            return Ok(());
                        }

                        if probe.finished {
                            probe_state = None;
                        }
                    };

                    debug_assert!(probe_finished);
                    state = HashJoinState::LeftDrop {
                        join_impl,
                        left_drop: LeftDropState {
                            rows: build_table.rows.into_iter().enumerate(),
                        },
                    };
                }
                HashJoinState::LeftDrop {
                    mut join_impl,
                    mut left_drop,
                } => {
                    if let Some(tuple) = join_impl.left_drop_next(
                        &mut left_drop,
                        self.filter.as_ref(),
                        plan_arena,
                    )? {
                        self.state = HashJoinState::LeftDrop {
                            join_impl,
                            left_drop,
                        };
                        arena.produce(tuple);
                        return Ok(());
                    }
                    state = HashJoinState::End;
                }
                HashJoinState::End => {
                    self.state = HashJoinState::End;
                    arena.finish();
                    return Ok(());
                }
            }
        }
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
#[maybe_async]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::execution::dql::join::hash_join::HashJoin;
    use crate::execution::dql::test::build_integers;
    use crate::execution::try_collect;
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::test::PlanArenaTestExt;
    use crate::planner::{Childrens, ExprRef, LogicalPlan};
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn optimize_exprs(
        plan: LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        HepOptimizerPipeline::builder()
            .before_batch(
                "Expression Remapper",
                HepBatchStrategy::once_topdown(),
                &[NormalizationRuleImpl::EvaluatorBind],
            )
            .build()
            .instantiate(plan)
            .find_best(None, arena)
    }

    fn build_join_values(
        arena: &mut crate::planner::PlanArena,
    ) -> (Vec<(ExprRef, ExprRef)>, LogicalPlan, LogicalPlan) {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap();

        let t1_columns = vec![
            arena.alloc_column(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            arena.alloc_column(ColumnCatalog::new("c2".to_string(), true, desc.clone())),
            arena.alloc_column(ColumnCatalog::new("c3".to_string(), true, desc.clone())),
        ];

        let t2_columns = vec![
            arena.alloc_column(ColumnCatalog::new("c4".to_string(), true, desc.clone())),
            arena.alloc_column(ColumnCatalog::new("c5".to_string(), true, desc.clone())),
            arena.alloc_column(ColumnCatalog::new("c6".to_string(), true, desc.clone())),
        ];

        let left_key = arena.alloc_expression(ScalarExpression::column_expr(t1_columns[0], 0));
        let right_key = arena.alloc_expression(ScalarExpression::column_expr(t2_columns[0], 0));
        let on_keys = vec![(left_key, right_key)];

        let values_t1 = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                arena.alloc_expression_rows(&[
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
                        DataValue::Int32(3),
                        DataValue::Int32(5),
                        DataValue::Int32(7),
                    ],
                ]),
                3,
                t1_columns,
            )),
            Childrens::None,
        );

        let values_t2 = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                arena.alloc_expression_rows(&[
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
                        DataValue::Int32(4),
                        DataValue::Int32(6),
                        DataValue::Int32(8),
                    ],
                    vec![
                        DataValue::Int32(1),
                        DataValue::Int32(1),
                        DataValue::Int32(1),
                    ],
                ]),
                4,
                t2_columns,
            )),
            Childrens::None,
        );

        (on_keys, values_t1, values_t2)
    }

    #[test]
    async fn test_inner_join() -> Result<(), DatabaseError> {
        let storage = crate::storage::new_test_storage().await;
        let transaction = storage.transaction()?;
        let table_cache = crate::storage::TableCache::default();
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let (keys, left, right) = build_join_values(&mut plan_arena);

        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: None,
                },
                join_type: JoinType::Inner,
                force_nested_loop: false,
                limit_pushed: false,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan, &mut plan_arena)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (mut left, mut right) = plan.childrens.pop_twins();
        left.populate_output_schema_recursive(&mut plan_arena);
        right.populate_output_schema_recursive(&mut plan_arena);
        let executor = crate::execution::execute_input::<_, HashJoin>(
            (&op, &left, &right),
            &table_cache,
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor).await?;

        assert_eq!(tuples.len(), 3);

        assert_eq!(
            tuples[0],
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1],
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2],
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );

        Ok(())
    }

    #[test]
    async fn test_left_join() -> Result<(), DatabaseError> {
        let storage = crate::storage::new_test_storage().await;
        let transaction = storage.transaction()?;
        let table_cache = crate::storage::TableCache::default();
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let (keys, left, right) = build_join_values(&mut plan_arena);

        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: None,
                },
                join_type: JoinType::LeftOuter,
                force_nested_loop: false,
                limit_pushed: false,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan, &mut plan_arena)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (mut left, mut right) = plan.childrens.pop_twins();
        left.populate_output_schema_recursive(&mut plan_arena);
        right.populate_output_schema_recursive(&mut plan_arena);
        {
            let tuples = try_collect(crate::execution::execute_input::<_, HashJoin>(
                (&op, &left, &right),
                &table_cache,
                plan_arena,
                &transaction,
            ))
            .await?;

            assert_eq!(tuples.len(), 4);

            assert_eq!(
                tuples[0],
                build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
            );
            assert_eq!(
                tuples[1],
                build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
            );
            assert_eq!(
                tuples[2],
                build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
            );
            assert_eq!(
                tuples[3],
                build_integers(vec![Some(3), Some(5), Some(7), None, None, None])
            );
        }

        Ok(())
    }

    #[test]
    async fn test_right_join() -> Result<(), DatabaseError> {
        let storage = crate::storage::new_test_storage().await;
        let transaction = storage.transaction()?;
        let table_cache = crate::storage::TableCache::default();
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let (keys, left, right) = build_join_values(&mut plan_arena);

        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: None,
                },
                join_type: JoinType::RightOuter,
                force_nested_loop: false,
                limit_pushed: false,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan, &mut plan_arena)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (mut left, mut right) = plan.childrens.pop_twins();
        left.populate_output_schema_recursive(&mut plan_arena);
        right.populate_output_schema_recursive(&mut plan_arena);
        let executor = crate::execution::execute_input::<_, HashJoin>(
            (&op, &left, &right),
            &table_cache,
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor).await?;

        assert_eq!(tuples.len(), 4);

        assert_eq!(
            tuples[0],
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1],
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2],
            build_integers(vec![None, None, None, Some(4), Some(6), Some(8)])
        );
        assert_eq!(
            tuples[3],
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );

        Ok(())
    }

    #[test]
    async fn test_right_join_filter_only_left_columns() -> Result<(), DatabaseError> {
        let storage = crate::storage::new_test_storage().await;
        let transaction = storage.transaction()?;
        let table_cache = crate::storage::TableCache::default();
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);

        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let left_columns = vec![
            plan_arena.alloc_column(ColumnCatalog::new("k".to_string(), true, desc.clone())),
            plan_arena.alloc_column(ColumnCatalog::new("v".to_string(), true, desc.clone())),
        ];
        let right_columns =
            vec![plan_arena.alloc_column(ColumnCatalog::new("rk".to_string(), true, desc.clone()))];

        let left_key =
            plan_arena.alloc_expression(ScalarExpression::column_expr(left_columns[0], 0));
        let right_key =
            plan_arena.alloc_expression(ScalarExpression::column_expr(right_columns[0], 0));
        let on_keys = vec![(left_key, right_key)];
        let filter_left =
            plan_arena.alloc_expression(ScalarExpression::column_expr(left_columns[1], 1));
        let filter_right =
            plan_arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let filter_expr = plan_arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Gt,
            left_expr: filter_left,
            right_expr: filter_right,
            evaluator: None,
            ty: LogicalType::Boolean,
        });

        let left = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                plan_arena.alloc_expression_rows(&[
                    vec![DataValue::Int32(2), DataValue::Int32(0)],
                    vec![DataValue::Int32(2), DataValue::Int32(5)],
                ]),
                2,
                left_columns,
            )),
            Childrens::None,
        );
        let right = LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                plan_arena.alloc_expression_rows(&[vec![DataValue::Int32(2)]]),
                1,
                right_columns,
            )),
            Childrens::None,
        );

        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: on_keys,
                    filter: Some(filter_expr),
                },
                join_type: JoinType::RightOuter,
                force_nested_loop: false,
                limit_pushed: false,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );

        let plan = optimize_exprs(plan, &mut plan_arena)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (mut left, mut right) = plan.childrens.pop_twins();
        left.populate_output_schema_recursive(&mut plan_arena);
        right.populate_output_schema_recursive(&mut plan_arena);
        let executor = crate::execution::execute_input::<_, HashJoin>(
            (&op, &left, &right),
            &table_cache,
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor).await?;

        assert_eq!(tuples.len(), 1);
        assert_eq!(
            tuples[0],
            vec![
                DataValue::Int32(2),
                DataValue::Int32(5),
                DataValue::Int32(2)
            ]
        );

        Ok(())
    }

    #[test]
    async fn test_full_join() -> Result<(), DatabaseError> {
        let storage = crate::storage::new_test_storage().await;
        let transaction = storage.transaction()?;
        let table_cache = crate::storage::TableCache::default();
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let (keys, left, right) = build_join_values(&mut plan_arena);

        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: None,
                },
                join_type: JoinType::Full,
                force_nested_loop: false,
                limit_pushed: false,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan, &mut plan_arena)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (mut left, mut right) = plan.childrens.pop_twins();
        left.populate_output_schema_recursive(&mut plan_arena);
        right.populate_output_schema_recursive(&mut plan_arena);
        let executor = crate::execution::execute_input::<_, HashJoin>(
            (&op, &left, &right),
            &table_cache,
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor).await?;

        assert_eq!(tuples.len(), 5);

        assert_eq!(
            tuples[0],
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1],
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2],
            build_integers(vec![None, None, None, Some(4), Some(6), Some(8)])
        );
        assert_eq!(
            tuples[3],
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            tuples[4],
            build_integers(vec![Some(3), Some(5), Some(7), None, None, None])
        );

        Ok(())
    }
}
// GRCOV_EXCL_STOP
