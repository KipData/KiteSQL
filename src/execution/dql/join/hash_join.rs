use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::dql::join::joins_nullable;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, Tuple};
use crate::types::value::{DataValue, NULL_VALUE};
use ahash::{HashMap, HashMapExt};
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;

pub struct HashJoin {
    on: JoinCondition,
    ty: JoinType,
    left_input: LogicalPlan,
    right_input: LogicalPlan,
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
        }
    }
}

impl HashJoin {
    fn eval_keys(
        on_keys: &[ScalarExpression],
        tuple: &Tuple,
        schema: &[ColumnRef],
    ) -> Result<Vec<DataValue>, DatabaseError> {
        let mut values = Vec::with_capacity(on_keys.len());

        for expr in on_keys {
            values.push(expr.eval(Some((tuple, schema)))?);
        }
        Ok(values)
    }

    pub(crate) fn filter(
        mut tuple: Tuple,
        schema: &Schema,
        filter: &Option<ScalarExpression>,
        join_ty: &JoinType,
        left_schema_len: usize,
    ) -> Result<Option<Tuple>, DatabaseError> {
        if let (Some(expr), false) = (filter, matches!(join_ty, JoinType::Full | JoinType::Cross)) {
            match &expr.eval(Some((&tuple, schema)))? {
                DataValue::Boolean(false) | DataValue::Null => {
                    let full_schema_len = schema.len();

                    match join_ty {
                        JoinType::LeftOuter => {
                            for i in left_schema_len..full_schema_len {
                                tuple.values[i] = NULL_VALUE.clone();
                            }
                        }
                        JoinType::RightOuter => {
                            for i in 0..left_schema_len {
                                tuple.values[i] = NULL_VALUE.clone();
                            }
                        }
                        _ => return Ok(None),
                    }
                }
                DataValue::Boolean(true) => (),
                _ => return Err(DatabaseError::InvalidType),
            }
        }

        Ok(Some(tuple))
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for HashJoin {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let HashJoin {
                    on,
                    ty,
                    mut left_input,
                    mut right_input,
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
                    throw!(Err(DatabaseError::UnsupportedStmt(
                        "`NestLoopJoin` should be used when there is no equivalent condition"
                            .to_string()
                    )))
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

                // build phase:
                // 1.construct hashtable, one hash key may contains multiple rows indices.
                // 2.merged all left tuples.
                let mut coroutine = build_read(left_input, cache, transaction);
                let mut build_map = HashMap::new();
                let build_map_ptr: *mut HashMap<Vec<DataValue>, (Vec<Tuple>, bool, bool)> =
                    &mut build_map;

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);
                    let values = throw!(Self::eval_keys(
                        &on_left_keys,
                        &tuple,
                        &full_schema_ref[0..left_schema_len]
                    ));

                    unsafe {
                        (*build_map_ptr)
                            .entry(values)
                            .or_insert_with(|| (Vec::new(), false, false))
                            .0
                            .push(tuple);
                    }
                }

                // probe phase
                let mut coroutine = build_read(right_input, cache, transaction);

                while let CoroutineState::Yielded(tuple) = Pin::new(&mut coroutine).resume(()) {
                    let tuple: Tuple = throw!(tuple);

                    let right_cols_len = tuple.values.len();
                    let values = throw!(Self::eval_keys(
                        &on_right_keys,
                        &tuple,
                        &full_schema_ref[left_schema_len..]
                    ));
                    let has_null = values.iter().any(|value| value.is_null());
                    let build_value = unsafe { (*build_map_ptr).get_mut(&values) };
                    drop(values);

                    if let (false, Some((tuples, is_used, is_filtered))) = (has_null, build_value) {
                        let mut bits_option = None;
                        *is_used = true;

                        match ty {
                            JoinType::LeftSemi => {
                                if *is_filtered {
                                    continue;
                                } else {
                                    bits_option = Some(FixedBitSet::with_capacity(tuples.len()));
                                }
                            }
                            JoinType::LeftAnti => continue,
                            _ => (),
                        }
                        for (i, Tuple { values, pk }) in tuples.iter().enumerate() {
                            let full_values = values
                                .iter()
                                .chain(tuple.values.iter())
                                .cloned()
                                .collect_vec();
                            let tuple = Tuple::new(pk.clone(), full_values);
                            if let Some(tuple) = throw!(Self::filter(
                                tuple,
                                &full_schema_ref,
                                &filter,
                                &ty,
                                left_schema_len
                            )) {
                                if let Some(bits) = bits_option.as_mut() {
                                    bits.insert(i);
                                } else {
                                    yield Ok(tuple);
                                }
                            }
                        }
                        if let Some(bits) = bits_option {
                            let mut cnt = 0;
                            tuples.retain(|_| {
                                let res = bits.contains(cnt);
                                cnt += 1;
                                res
                            });
                            *is_filtered = true
                        }
                    } else if matches!(ty, JoinType::RightOuter | JoinType::Full) {
                        let empty_len = full_schema_ref.len() - right_cols_len;
                        let values = (0..empty_len)
                            .map(|_| NULL_VALUE.clone())
                            .chain(tuple.values)
                            .collect_vec();
                        let tuple = Tuple::new(tuple.pk, values);
                        if let Some(tuple) = throw!(Self::filter(
                            tuple,
                            &full_schema_ref,
                            &filter,
                            &ty,
                            left_schema_len
                        )) {
                            yield Ok(tuple);
                        }
                    }
                }

                // left drop
                match ty {
                    JoinType::LeftOuter | JoinType::Full => {
                        for (_, (left_tuples, is_used, _)) in build_map {
                            if is_used {
                                continue;
                            }
                            for mut tuple in left_tuples {
                                while tuple.values.len() != full_schema_ref.len() {
                                    tuple.values.push(NULL_VALUE.clone());
                                }
                                yield Ok(tuple);
                            }
                        }
                    }
                    JoinType::LeftSemi | JoinType::LeftAnti => {
                        let is_left_semi = matches!(ty, JoinType::LeftSemi);

                        for (_, (left_tuples, mut is_used, is_filtered)) in build_map {
                            if is_left_semi {
                                is_used = !is_used;
                            }
                            if is_used {
                                continue;
                            }
                            if is_filtered {
                                for tuple in left_tuples {
                                    yield Ok(tuple);
                                }
                                continue;
                            }
                            for tuple in left_tuples {
                                if let Some(tuple) = throw!(Self::filter(
                                    tuple,
                                    &full_schema_ref,
                                    &filter,
                                    &ty,
                                    left_schema_len
                                )) {
                                    yield Ok(tuple);
                                }
                            }
                        }
                    }
                    _ => (),
                }
            },
        )
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
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
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
            ScalarExpression::ColumnRef(t1_columns[0].clone()),
            ScalarExpression::ColumnRef(t2_columns[0].clone()),
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

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Inner,
        };
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

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::LeftOuter,
        };
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

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::RightOuter,
        };
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

        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Full,
        };
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
