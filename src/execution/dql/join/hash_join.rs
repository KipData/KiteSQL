use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::dql::join::hash::full_join::FullJoinState;
use crate::execution::dql::join::hash::inner_join::InnerJoinState;
use crate::execution::dql::join::hash::left_anti_join::LeftAntiJoinState;
use crate::execution::dql::join::hash::left_join::LeftJoinState;
use crate::execution::dql::join::hash::left_semi_join::LeftSemiJoinState;
use crate::execution::dql::join::hash::right_join::RightJoinState;
use crate::execution::dql::join::hash::{
    FilterArgs, JoinProbeState, JoinProbeStateImpl, ProbeArgs,
};
use crate::execution::dql::join::joins_nullable;
use crate::execution::dql::sort::BumpVec;
use crate::execution::{build_read, spawn_executor, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use ahash::{HashMap, HashMapExt};
use bumpalo::Bump;
use fixedbitset::FixedBitSet;
use std::sync::Arc;

pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
    left_input: LogicalPlan,
    right_input: LogicalPlan,
    bump: Bump,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for HashJoin {
    fn from(
        (JoinOperator { on, join_type, .. }, left_input, right_input): (
            JoinOperator,
            LogicalPlan,
            LogicalPlan,
        ),
    ) -> Self {
        HashJoin {
            on,
            ty: join_type,
            left_input,
            right_input,
            bump: Default::default(),
        }
    }
}

impl HashJoin {
    fn eval_keys(
        on_keys: &[ScalarExpression],
        tuple: &Tuple,
        schema: &[ColumnRef],
        build_buf: &mut BumpVec<DataValue>,
    ) -> Result<(), DatabaseError> {
        build_buf.clear();
        for expr in on_keys {
            build_buf.push(expr.eval(Some((tuple, schema)))?);
        }
        Ok(())
    }
}

#[derive(Default, Debug)]
pub(crate) struct BuildState {
    pub(crate) tuples: Vec<(usize, Tuple)>,
    pub(crate) is_used: bool,
    pub(crate) has_filted: bool,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for HashJoin {
    #[allow(clippy::mutable_key_type)]
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        spawn_executor(move |co| async move {
            let HashJoin {
                on,
                ty,
                mut left_input,
                mut right_input,
                mut bump,
            } = self;

            if ty == JoinType::Cross {
                unreachable!("Cross join should not be in HashJoinExecutor");
            }
            let ((on_left_keys, on_right_keys), filter): (
                (Vec<ScalarExpression>, Vec<ScalarExpression>),
                _,
            ) = match on {
                JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
                JoinCondition::None => unreachable!("HashJoin must has on condition"),
            };
            if on_left_keys.is_empty() || on_right_keys.is_empty() {
                throw!(
                    co,
                    Err(DatabaseError::UnsupportedStmt(
                        "`NestLoopJoin` should be used when there is no equivalent condition"
                            .to_string()
                    ))
                )
            }
            debug_assert!(!on_left_keys.is_empty());
            debug_assert!(!on_right_keys.is_empty());

            let fn_process = |schema: &mut [ColumnRef], force_nullable| {
                for column in schema.iter_mut() {
                    if let Some(new_column) = column.nullable_for_join(force_nullable) {
                        *column = new_column;
                    }
                }
            };

            let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);

            let mut full_schema_ref = Vec::clone(left_input.output_schema());
            let left_schema_len = full_schema_ref.len();

            fn_process(&mut full_schema_ref, left_force_nullable);
            full_schema_ref.extend_from_slice(right_input.output_schema());
            fn_process(
                &mut full_schema_ref[left_schema_len..],
                right_force_nullable,
            );
            let right_schema_len = full_schema_ref.len() - left_schema_len;
            let full_schema_ref = Arc::new(full_schema_ref);

            // build phase:
            // 1.construct hashtable, one hash key may contains multiple rows indices.
            // 2.merged all left tuples.
            let mut coroutine = build_read(left_input, cache, transaction);
            let mut build_map = HashMap::new();
            let bump_ptr: *mut Bump = &mut bump;
            let build_map_ptr: *mut HashMap<BumpVec<DataValue>, BuildState> = &mut build_map;

            let mut buf_row =
                BumpVec::with_capacity_in(on_left_keys.len(), unsafe { &mut (*bump_ptr) });

            let mut build_count = 0;
            for tuple in coroutine.by_ref() {
                let tuple: Tuple = throw!(co, tuple);
                throw!(
                    co,
                    Self::eval_keys(
                        &on_left_keys,
                        &tuple,
                        &full_schema_ref[0..left_schema_len],
                        &mut buf_row,
                    )
                );

                let build_map_ref = unsafe { &mut (*build_map_ptr) };
                match build_map_ref.get_mut(&buf_row) {
                    None => {
                        build_map_ref.insert(
                            buf_row.clone(),
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
            let mut join_impl =
                Self::create_join_impl(self.ty, left_schema_len, right_schema_len, build_count);
            let mut filter_arg = filter.map(|expr| FilterArgs {
                full_schema: full_schema_ref.clone(),
                filter_expr: expr,
            });
            let filter_arg_ptr: *mut Option<FilterArgs> = &mut filter_arg;

            // probe phase
            let mut coroutine = build_read(right_input, cache, transaction);

            for tuple in coroutine.by_ref() {
                let tuple: Tuple = throw!(co, tuple);

                throw!(
                    co,
                    Self::eval_keys(
                        &on_right_keys,
                        &tuple,
                        &full_schema_ref[left_schema_len..],
                        &mut buf_row
                    )
                );
                let build_value = unsafe { (*build_map_ptr).get_mut(&buf_row) };

                let probe_args = ProbeArgs {
                    is_keys_has_null: buf_row.iter().any(|value| value.is_null()),
                    probe_tuple: tuple,
                    build_state: build_value,
                };
                let executor =
                    join_impl.probe(probe_args, unsafe { &mut (*filter_arg_ptr) }.as_ref());
                for tuple in executor {
                    co.yield_(tuple).await;
                }
            }
            if let Some(executor) =
                join_impl.left_drop(build_map, unsafe { &mut (*filter_arg_ptr) }.as_ref())
            {
                for tuple in executor {
                    co.yield_(tuple).await;
                }
            }
        })
    }
}

impl HashJoin {
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
                bits: FixedBitSet::with_capacity(build_count),
            }),
            JoinType::LeftSemi => JoinProbeStateImpl::LeftSemi(LeftSemiJoinState {
                bits: FixedBitSet::with_capacity(build_count),
            }),
            JoinType::LeftAnti => JoinProbeStateImpl::LeftAnti(LeftAntiJoinState {
                right_schema_len,
                inner: LeftSemiJoinState {
                    bits: FixedBitSet::with_capacity(build_count),
                },
            }),
            JoinType::RightOuter => JoinProbeStateImpl::Right(RightJoinState { left_schema_len }),
            JoinType::Full => JoinProbeStateImpl::Full(FullJoinState {
                left_schema_len,
                right_schema_len,
                bits: FixedBitSet::with_capacity(build_count),
            }),
            JoinType::Cross => unreachable!(),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::execution::dql::join::hash_join::HashJoin;
    use crate::execution::dql::test::build_integers;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::expression::ScalarExpression;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::table_codec::BumpBytes;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use bumpalo::Bump;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_join_values() -> (
        Vec<(ScalarExpression, ScalarExpression)>,
        LogicalPlan,
        LogicalPlan,
    ) {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap();

        let t1_columns = vec![
            ColumnRef::from(ColumnCatalog::new("c1".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c2".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c3".to_string(), true, desc.clone())),
        ];

        let t2_columns = vec![
            ColumnRef::from(ColumnCatalog::new("c4".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c5".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("c6".to_string(), true, desc.clone())),
        ];

        let on_keys = vec![(
            ScalarExpression::column_expr(t1_columns[0].clone()),
            ScalarExpression::column_expr(t2_columns[0].clone()),
        )];

        let values_t1 = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
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
                schema_ref: Arc::new(t1_columns),
            }),
            childrens: Box::new(Childrens::None),
            physical_option: None,
            _output_schema_ref: None,
        };

        let values_t2 = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
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
                schema_ref: Arc::new(t2_columns),
            }),
            childrens: Box::new(Childrens::None),
            physical_option: None,
            _output_schema_ref: None,
        };

        (on_keys, values_t1, values_t2)
    }

    #[test]
    fn test_inner_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right) = build_join_values();

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
        let plan = HepOptimizer::new(plan)
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::BindExpressionPosition,
                    // TIPS: This rule is necessary
                    NormalizationRuleImpl::EvaluatorBind,
                ],
            )
            .find_best::<RocksTransaction>(None)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = HashJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
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
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right) = build_join_values();

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
        let plan = HepOptimizer::new(plan)
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::BindExpressionPosition,
                    // TIPS: This rule is necessary
                    NormalizationRuleImpl::EvaluatorBind,
                ],
            )
            .find_best::<RocksTransaction>(None)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        //Outer
        {
            let executor = HashJoin::from((op.clone(), left.clone(), right.clone()));
            let tuples = try_collect(
                executor.execute((&table_cache, &view_cache, &meta_cache), &mut transaction),
            )?;

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
        // Semi
        {
            let mut executor = HashJoin::from((op.clone(), left.clone(), right.clone()));
            executor.ty = JoinType::LeftSemi;
            let mut tuples = try_collect(
                executor.execute((&table_cache, &view_cache, &meta_cache), &mut transaction),
            )?;

            let arena = Bump::new();
            assert_eq!(tuples.len(), 2);
            tuples.sort_by_key(|tuple| {
                let mut bytes = BumpBytes::new_in(&arena);
                tuple.values[0].memcomparable_encode(&mut bytes).unwrap();
                bytes
            });

            assert_eq!(
                tuples[0].values,
                build_integers(vec![Some(0), Some(2), Some(4)])
            );
            assert_eq!(
                tuples[1].values,
                build_integers(vec![Some(1), Some(3), Some(5)])
            );
        }
        // Anti
        {
            let mut executor = HashJoin::from((op, left, right));
            executor.ty = JoinType::LeftAnti;
            let tuples = try_collect(
                executor.execute((&table_cache, &view_cache, &meta_cache), &mut transaction),
            )?;

            assert_eq!(tuples.len(), 1);
            assert_eq!(
                tuples[0].values,
                build_integers(vec![Some(3), Some(5), Some(7)])
            );
        }

        Ok(())
    }

    #[test]
    fn test_right_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right) = build_join_values();

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
        let plan = HepOptimizer::new(plan)
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::BindExpressionPosition,
                    // TIPS: This rule is necessary
                    NormalizationRuleImpl::EvaluatorBind,
                ],
            )
            .find_best::<RocksTransaction>(None)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = HashJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
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
    fn test_full_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right) = build_join_values();

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
        let plan = HepOptimizer::new(plan)
            .batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![
                    NormalizationRuleImpl::BindExpressionPosition,
                    // TIPS: This rule is necessary
                    NormalizationRuleImpl::EvaluatorBind,
                ],
            )
            .find_best::<RocksTransaction>(None)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = HashJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
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
