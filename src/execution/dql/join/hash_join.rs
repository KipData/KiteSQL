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
    JoinProbeState, JoinProbeStateImpl, LeftDropState, ProbeState,
};
use crate::execution::dql::join::RowBitmap;
use crate::execution::dql::sort::BumpVec;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use bumpalo::Bump;
use std::collections::HashMap;
use std::mem::{self, transmute};

pub struct HashJoin {
    state: HashJoinState,
    ty: JoinType,
    on_left_keys: Vec<ScalarExpression>,
    on_right_keys: Vec<ScalarExpression>,
    filter: Option<ScalarExpression>,
    left_schema_len: usize,
    right_schema_len: usize,
    left_input_plan: LogicalPlan,
    right_input_plan: LogicalPlan,
    left_input: ExecId,
    right_input: ExecId,
    bump: Box<Bump>,
    init_error: Option<DatabaseError>,
}

enum HashJoinState {
    Build,
    Probe {
        build_map: HashMap<BumpVec<'static, DataValue>, BuildState>,
        join_impl: JoinProbeStateImpl,
        probe_buf: BumpVec<'static, DataValue>,
        probe_state: Option<ProbeState>,
    },
    LeftDrop {
        join_impl: JoinProbeStateImpl,
        left_drop: LeftDropState,
    },
    End,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for HashJoin {
    fn from(
        (JoinOperator { on, join_type }, left_input, right_input): (
            JoinOperator,
            LogicalPlan,
            LogicalPlan,
        ),
    ) -> Self {
        let ((on_left_keys, on_right_keys), filter_expr) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => ((vec![], vec![]), None),
        };

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
            left_schema_len: 0,
            right_schema_len: 0,
            left_input_plan: left_input,
            right_input_plan: right_input,
            left_input: 0,
            right_input: 0,
            bump: Box::<Bump>::default(),
            init_error,
        }
    }
}

impl HashJoin {
    #[allow(clippy::mutable_key_type)]
    fn own_bump_vec(buf: BumpVec<'_, DataValue>) -> BumpVec<'static, DataValue> {
        unsafe { transmute::<BumpVec<'_, DataValue>, BumpVec<'static, DataValue>>(buf) }
    }

    #[allow(clippy::mutable_key_type)]
    fn own_build_map(
        build_map: HashMap<BumpVec<'_, DataValue>, BuildState>,
    ) -> HashMap<BumpVec<'static, DataValue>, BuildState> {
        unsafe {
            transmute::<
                HashMap<BumpVec<'_, DataValue>, BuildState>,
                HashMap<BumpVec<'static, DataValue>, BuildState>,
            >(build_map)
        }
    }

    fn eval_keys(
        on_keys: &[ScalarExpression],
        tuple: &Tuple,
        build_buf: &mut BumpVec<'_, DataValue>,
    ) -> Result<(), DatabaseError> {
        build_buf.clear();
        for expr in on_keys {
            build_buf.push(expr.eval(Some(tuple))?);
        }
        Ok(())
    }

    fn initialize_build<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if !matches!(self.state, HashJoinState::Build) {
            return Ok(());
        }

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left tuples.
        #[allow(clippy::mutable_key_type)]
        let mut build_map = HashMap::new();
        let mut build_buf = BumpVec::with_capacity_in(self.on_left_keys.len(), &self.bump);
        let mut build_count = 0usize;

        while arena.next_tuple(self.left_input, plan_arena)? {
            let tuple = mem::take(arena.result_tuple_mut());
            Self::eval_keys(&self.on_left_keys, &tuple, &mut build_buf)?;

            match build_map.get_mut(&build_buf) {
                None => {
                    build_map.insert(
                        Self::own_bump_vec(build_buf.clone()),
                        BuildState {
                            tuples: vec![(build_count, tuple)],
                            ..Default::default()
                        },
                    );
                }
                Some(BuildState { tuples, .. }) => tuples.push((build_count, tuple)),
            }
            build_count += 1;
        }

        self.state = HashJoinState::Probe {
            join_impl: Self::create_join_impl(
                self.ty,
                self.left_schema_len,
                self.right_schema_len,
                build_count,
            ),
            probe_buf: Self::own_bump_vec(BumpVec::with_capacity_in(
                self.on_right_keys.len(),
                &self.bump,
            )),
            build_map: Self::own_build_map(build_map),
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
                bits: RowBitmap::with_capacity(build_count),
            }),
            JoinType::RightOuter => JoinProbeStateImpl::Right(RightJoinState { left_schema_len }),
            JoinType::Full => JoinProbeStateImpl::Full(FullJoinState {
                left_schema_len,
                right_schema_len,
                bits: RowBitmap::with_capacity(build_count),
            }),
            JoinType::Cross => unreachable!(),
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct BuildState {
    pub(crate) tuples: Vec<(usize, Tuple)>,
    pub(crate) is_used: bool,
    pub(crate) has_filted: bool,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for HashJoin {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut executor = input;
        let left_schema_len = executor.left_input_plan.output_schema(plan_arena).len();
        let right_schema_len = executor.right_input_plan.output_schema(plan_arena).len();
        executor.left_schema_len = left_schema_len;
        executor.right_schema_len = right_schema_len;
        executor.left_input = build_read(
            arena,
            plan_arena,
            executor.left_input_plan.take(),
            cache,
            transaction,
        );
        executor.right_input = build_read(
            arena,
            plan_arena,
            executor.right_input_plan.take(),
            cache,
            transaction,
        );
        arena.push(ExecNode::HashJoin(executor))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for HashJoin {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if let Some(err) = self.init_error.take() {
            return Err(err);
        }

        self.initialize_build(arena, plan_arena)?;
        let mut state = std::mem::replace(&mut self.state, HashJoinState::End);

        loop {
            match state {
                HashJoinState::Build => unreachable!("hash join must be initialized before probe"),
                HashJoinState::Probe {
                    mut build_map,
                    mut join_impl,
                    mut probe_buf,
                    mut probe_state,
                } => {
                    let probe_finished = loop {
                        if probe_state.is_none() {
                            if !arena.next_tuple(self.right_input, plan_arena)? {
                                break true;
                            }
                            let tuple = mem::take(arena.result_tuple_mut());
                            Self::eval_keys(&self.on_right_keys, &tuple, &mut probe_buf)?;
                            probe_state = Some(ProbeState {
                                is_keys_has_null: probe_buf.iter().any(DataValue::is_null),
                                probe_tuple: tuple,
                                index: 0,
                                has_filtered: false,
                                produced: false,
                                finished: false,
                                emitted_unmatched: false,
                            });
                        }

                        let Some(probe) = probe_state.as_mut() else {
                            continue;
                        };
                        let build_state = if probe.is_keys_has_null {
                            None
                        } else {
                            build_map.get_mut(&probe_buf)
                        };

                        if let Some(tuple) =
                            join_impl.probe_next(probe, build_state, self.filter.as_ref())?
                        {
                            if probe.finished {
                                probe_state = None;
                            }
                            self.state = HashJoinState::Probe {
                                build_map,
                                join_impl,
                                probe_buf,
                                probe_state,
                            };
                            arena.produce_tuple(tuple);
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
                            states: build_map.into_iter(),
                            current: None,
                        },
                    };
                }
                HashJoinState::LeftDrop {
                    mut join_impl,
                    mut left_drop,
                } => {
                    if let Some(tuple) =
                        join_impl.left_drop_next(&mut left_drop, self.filter.as_ref())?
                    {
                        self.state = HashJoinState::LeftDrop {
                            join_impl,
                            left_drop,
                        };
                        arena.produce_tuple(tuple);
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

#[cfg(all(test, not(target_arch = "wasm32")))]
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
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use tempfile::TempDir;

    fn optimize_exprs(
        plan: LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        HepOptimizerPipeline::builder()
            .before_batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::EvaluatorBind],
            )
            .build()
            .instantiate(plan)
            .find_best(None, arena)
    }

    fn build_join_values(
        arena: &mut crate::planner::PlanArena,
    ) -> (
        Vec<(ScalarExpression, ScalarExpression)>,
        LogicalPlan,
        LogicalPlan,
    ) {
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

        let on_keys = vec![(
            ScalarExpression::column_expr(t1_columns[0], 0),
            ScalarExpression::column_expr(t2_columns[0], 0),
        )];

        let values_t1 = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
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
                ],
                schema_ref: t1_columns,
            }),
            Childrens::None,
        );

        let values_t2 = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
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
                ],
                schema_ref: t2_columns,
            }),
            Childrens::None,
        );

        (on_keys, values_t1, values_t2)
    }

    #[test]
    fn test_inner_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let view_cache = crate::storage::ViewCache::default();
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
        let (left, right) = plan.childrens.pop_twins();
        let executor = crate::execution::execute(
            HashJoin::from((op, left, right)),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor)?;

        assert_eq!(tuples.len(), 3);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );

        Ok(())
    }

    #[test]
    fn test_left_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let view_cache = crate::storage::ViewCache::default();
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
        let (left, right) = plan.childrens.pop_twins();
        {
            let executor = HashJoin::from((op.clone(), left.clone(), right.clone()));
            let tuples = try_collect(crate::execution::execute(
                executor,
                crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
                plan_arena,
                &transaction,
            ))?;

            assert_eq!(tuples.len(), 4);

            assert_eq!(
                tuples[0].values,
                build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
            );
            assert_eq!(
                tuples[1].values,
                build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
            );
            assert_eq!(
                tuples[2].values,
                build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
            );
            assert_eq!(
                tuples[3].values,
                build_integers(vec![Some(3), Some(5), Some(7), None, None, None])
            );
        }

        Ok(())
    }

    #[test]
    fn test_right_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let view_cache = crate::storage::ViewCache::default();
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
        let (left, right) = plan.childrens.pop_twins();
        let executor = crate::execution::execute(
            HashJoin::from((op, left, right)),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor)?;

        assert_eq!(tuples.len(), 4);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![None, None, None, Some(4), Some(6), Some(8)])
        );
        assert_eq!(
            tuples[3].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );

        Ok(())
    }

    #[test]
    fn test_right_join_filter_only_left_columns() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let view_cache = crate::storage::ViewCache::default();
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

        let on_keys = vec![(
            ScalarExpression::column_expr(left_columns[0], 0),
            ScalarExpression::column_expr(right_columns[0], 0),
        )];
        let filter_expr = ScalarExpression::Binary {
            op: BinaryOperator::Gt,
            left_expr: Box::new(ScalarExpression::column_expr(left_columns[1], 1)),
            right_expr: Box::new(ScalarExpression::Constant(DataValue::Int32(1))),
            evaluator: None,
            ty: LogicalType::Boolean,
        };

        let left = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![
                    vec![DataValue::Int32(2), DataValue::Int32(0)],
                    vec![DataValue::Int32(2), DataValue::Int32(5)],
                ],
                schema_ref: left_columns,
            }),
            Childrens::None,
        );
        let right = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![vec![DataValue::Int32(2)]],
                schema_ref: right_columns,
            }),
            Childrens::None,
        );

        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: on_keys,
                    filter: Some(filter_expr),
                },
                join_type: JoinType::RightOuter,
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
        let (left, right) = plan.childrens.pop_twins();
        let executor = crate::execution::execute(
            HashJoin::from((op, left, right)),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor)?;

        assert_eq!(tuples.len(), 1);
        assert_eq!(
            tuples[0].values,
            vec![
                DataValue::Int32(2),
                DataValue::Int32(5),
                DataValue::Int32(2)
            ]
        );

        Ok(())
    }

    #[test]
    fn test_full_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let view_cache = crate::storage::ViewCache::default();
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
        let (left, right) = plan.childrens.pop_twins();
        let executor = crate::execution::execute(
            HashJoin::from((op, left, right)),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        );
        let tuples = try_collect(executor)?;

        assert_eq!(tuples.len(), 5);

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)])
        );
        assert_eq!(
            tuples[1].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)])
        );
        assert_eq!(
            tuples[2].values,
            build_integers(vec![None, None, None, Some(4), Some(6), Some(8)])
        );
        assert_eq!(
            tuples[3].values,
            build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(1), Some(1)])
        );
        assert_eq!(
            tuples[4].values,
            build_integers(vec![Some(3), Some(5), Some(7), None, None, None])
        );

        Ok(())
    }
}
