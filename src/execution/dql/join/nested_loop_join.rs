//! Defines the nested loop join executor, it supports [`JoinType::Inner`], [`JoinType::LeftOuter`],
//! [`JoinType::LeftSemi`], [`JoinType::LeftAnti`], [`JoinType::RightOuter`], [`JoinType::Cross`], [`JoinType::Full`].

use super::joins_nullable;
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, Executor, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::throw;
use crate::types::tuple::{Schema, SchemaRef, Tuple};
use crate::types::value::{DataValue, NULL_VALUE};
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use std::ops::Coroutine;
use std::ops::CoroutineState;
use std::pin::Pin;
use std::sync::Arc;

/// Equivalent condition
struct EqualCondition {
    on_left_keys: Vec<ScalarExpression>,
    on_right_keys: Vec<ScalarExpression>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
}

impl EqualCondition {
    /// Constructs a new `EqualCondition`
    /// If the `on_left_keys` and `on_right_keys` are empty, it means no equivalent condition
    /// Note: `on_left_keys` and `on_right_keys` are either all empty or none of them.
    fn new(
        on_left_keys: Vec<ScalarExpression>,
        on_right_keys: Vec<ScalarExpression>,
        left_schema: Arc<Schema>,
        right_schema: Arc<Schema>,
    ) -> EqualCondition {
        if !on_left_keys.is_empty() && on_left_keys.len() != on_right_keys.len() {
            unreachable!("Unexpected join on condition.")
        }
        EqualCondition {
            on_left_keys,
            on_right_keys,
            left_schema,
            right_schema,
        }
    }

    /// Compare left tuple and right tuple on equivalent condition
    /// `left_tuple` must be from the [`NestedLoopJoin::left_input`]
    /// `right_tuple` must be from the [`NestedLoopJoin::right_input`]
    fn equals(&self, left_tuple: &Tuple, right_tuple: &Tuple) -> Result<bool, DatabaseError> {
        if self.on_left_keys.is_empty() {
            return Ok(true);
        }
        let left_values =
            Projection::projection(left_tuple, &self.on_left_keys, &self.left_schema)?;
        let right_values =
            Projection::projection(right_tuple, &self.on_right_keys, &self.right_schema)?;

        Ok(left_values == right_values)
    }
}

/// NestedLoopJoin using nested loop join algorithm to execute a join operation.
/// One input will be selected to be the inner table and the other will be the outer
/// | JoinType                       |  Inner-table   |   Outer-table  |
/// |--------------------------------|----------------|----------------|
/// | Inner/Left/LeftSemi/LeftAnti   |    right       |      left      |
/// |--------------------------------|----------------|----------------|
/// | Right/RightSemi/RightAnti/Full |    left        |      right     |
/// |--------------------------------|----------------|----------------|
/// | Full                           |    left        |      right     |
pub struct NestedLoopJoin {
    left_input: LogicalPlan,
    right_input: LogicalPlan,
    output_schema_ref: SchemaRef,
    ty: JoinType,
    filter: Option<ScalarExpression>,
    eq_cond: EqualCondition,
}

impl From<(JoinOperator, LogicalPlan, LogicalPlan)> for NestedLoopJoin {
    fn from(
        (JoinOperator { on, join_type, .. }, left_input, right_input): (
            JoinOperator,
            LogicalPlan,
            LogicalPlan,
        ),
    ) -> Self {
        let ((mut on_left_keys, mut on_right_keys), filter) = match on {
            JoinCondition::On { on, filter } => (on.into_iter().unzip(), filter),
            JoinCondition::None => ((vec![], vec![]), None),
        };

        let (mut left_input, mut right_input) = (left_input, right_input);
        let mut left_schema = left_input.output_schema().clone();
        let mut right_schema = right_input.output_schema().clone();
        let output_schema_ref = Self::merge_schema(&left_schema, &right_schema, join_type);

        if matches!(join_type, JoinType::RightOuter) {
            std::mem::swap(&mut left_input, &mut right_input);
            std::mem::swap(&mut on_left_keys, &mut on_right_keys);
            std::mem::swap(&mut left_schema, &mut right_schema);
        }

        let eq_cond = EqualCondition::new(
            on_left_keys,
            on_right_keys,
            left_schema.clone(),
            right_schema.clone(),
        );

        NestedLoopJoin {
            ty: join_type,
            left_input,
            right_input,
            output_schema_ref,
            filter,
            eq_cond,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for NestedLoopJoin {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a> {
        Box::new(
            #[coroutine]
            move || {
                let NestedLoopJoin {
                    ty,
                    left_input,
                    right_input,
                    output_schema_ref,
                    filter,
                    eq_cond,
                    ..
                } = self;

                let right_schema_len = eq_cond.right_schema.len();
                let mut left_coroutine = build_read(left_input, cache, transaction);
                let mut bitmap: Option<FixedBitSet> = None;
                let mut first_matches = Vec::new();

                while let CoroutineState::Yielded(left_tuple) =
                    Pin::new(&mut left_coroutine).resume(())
                {
                    let left_tuple: Tuple = throw!(left_tuple);
                    let mut has_matched = false;

                    let mut right_coroutine = build_read(right_input.clone(), cache, transaction);
                    let mut right_idx = 0;

                    while let CoroutineState::Yielded(right_tuple) =
                        Pin::new(&mut right_coroutine).resume(())
                    {
                        let right_tuple: Tuple = throw!(right_tuple);

                        let tuple = match (
                            filter.as_ref(),
                            throw!(eq_cond.equals(&left_tuple, &right_tuple)),
                        ) {
                            (None, true) if matches!(ty, JoinType::RightOuter) => {
                                has_matched = true;
                                Self::emit_tuple(&right_tuple, &left_tuple, ty, true)
                            }
                            (None, true) => {
                                has_matched = true;
                                Self::emit_tuple(&left_tuple, &right_tuple, ty, true)
                            }
                            (Some(filter), true) => {
                                let new_tuple = Self::merge_tuple(&left_tuple, &right_tuple, &ty);
                                let value =
                                    throw!(filter.eval(Some((&new_tuple, &output_schema_ref))));
                                match &value {
                                    DataValue::Boolean(true) => {
                                        let tuple = match ty {
                                            JoinType::LeftAnti => None,
                                            JoinType::LeftSemi if has_matched => None,
                                            JoinType::RightOuter => Self::emit_tuple(
                                                &right_tuple,
                                                &left_tuple,
                                                ty,
                                                true,
                                            ),
                                            _ => Self::emit_tuple(
                                                &left_tuple,
                                                &right_tuple,
                                                ty,
                                                true,
                                            ),
                                        };
                                        has_matched = true;
                                        tuple
                                    }
                                    DataValue::Boolean(false) | DataValue::Null => None,
                                    _ => {
                                        yield Err(DatabaseError::InvalidType);
                                        return;
                                    }
                                }
                            }
                            _ => None,
                        };

                        if let Some(tuple) = tuple {
                            yield Ok(tuple);
                            if matches!(ty, JoinType::LeftSemi) {
                                break;
                            }
                            if let Some(bits) = bitmap.as_mut() {
                                bits.insert(right_idx);
                            } else if matches!(ty, JoinType::Full) {
                                first_matches.push(right_idx);
                            }
                        }
                        if matches!(ty, JoinType::LeftAnti) && has_matched {
                            break;
                        }
                        right_idx += 1;
                    }

                    if matches!(self.ty, JoinType::Full) && bitmap.is_none() {
                        bitmap = Some(FixedBitSet::with_capacity(right_idx));
                    }

                    // handle no matched tuple case
                    let tuple = match ty {
                        JoinType::LeftAnti if !has_matched => Some(left_tuple.clone()),
                        JoinType::LeftOuter
                        | JoinType::LeftSemi
                        | JoinType::RightOuter
                        | JoinType::Full
                            if !has_matched =>
                        {
                            let right_tuple =
                                Tuple::new(None, vec![NULL_VALUE.clone(); right_schema_len]);
                            if matches!(ty, JoinType::RightOuter) {
                                Self::emit_tuple(&right_tuple, &left_tuple, ty, false)
                            } else {
                                Self::emit_tuple(&left_tuple, &right_tuple, ty, false)
                            }
                        }
                        _ => None,
                    };
                    if let Some(tuple) = tuple {
                        yield Ok(tuple)
                    }
                }

                if matches!(ty, JoinType::Full) {
                    for idx in first_matches.into_iter() {
                        bitmap.as_mut().unwrap().insert(idx);
                    }

                    let mut right_coroutine = build_read(right_input.clone(), cache, transaction);
                    let mut idx = 0;
                    while let CoroutineState::Yielded(right_tuple) =
                        Pin::new(&mut right_coroutine).resume(())
                    {
                        if !bitmap.as_ref().unwrap().contains(idx) {
                            let mut right_tuple: Tuple = throw!(right_tuple);
                            let mut values = vec![NULL_VALUE.clone(); right_schema_len];
                            values.append(&mut right_tuple.values);

                            yield Ok(Tuple::new(right_tuple.pk, values))
                        }
                        idx += 1;
                    }
                }
            },
        )
    }
}

impl NestedLoopJoin {
    /// Emit a tuple according to the join type.
    ///
    /// `left_tuple`: left tuple to be included.
    /// `right_tuple` right tuple to be included.
    /// `ty`: the type of join
    /// `is_match`: whether [`NestedLoopJoin::left_input`] and [`NestedLoopJoin::right_input`] are matched
    fn emit_tuple(
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        ty: JoinType,
        is_matched: bool,
    ) -> Option<Tuple> {
        let left_len = left_tuple.values.len();
        let mut values = left_tuple
            .values
            .iter()
            .cloned()
            .chain(right_tuple.values.clone())
            .collect_vec();
        match ty {
            JoinType::Inner | JoinType::Cross | JoinType::LeftSemi if !is_matched => values.clear(),
            JoinType::LeftOuter | JoinType::Full if !is_matched => {
                values
                    .iter_mut()
                    .skip(left_len)
                    .for_each(|v| *v = NULL_VALUE.clone());
            }
            JoinType::RightOuter if !is_matched => {
                (0..left_len).for_each(|i| {
                    values[i] = NULL_VALUE.clone();
                });
            }
            JoinType::LeftSemi => values.truncate(left_len),
            JoinType::LeftAnti => {
                if is_matched {
                    values.clear();
                } else {
                    values.truncate(left_len);
                }
            }
            _ => (),
        };

        if values.is_empty() {
            return None;
        }

        Some(Tuple::new(
            left_tuple.pk.as_ref().or(right_tuple.pk.as_ref()).cloned(),
            values,
        ))
    }

    /// Merge the two tuples.
    /// `left_tuple` must be from the `NestedLoopJoin.left_input`
    /// `right_tuple` must be from the `NestedLoopJoin.right_input`
    fn merge_tuple(left_tuple: &Tuple, right_tuple: &Tuple, ty: &JoinType) -> Tuple {
        let pk = left_tuple.pk.as_ref().or(right_tuple.pk.as_ref()).cloned();
        match ty {
            JoinType::RightOuter => Tuple::new(
                pk,
                right_tuple
                    .values
                    .iter()
                    .chain(left_tuple.values.iter())
                    .cloned()
                    .collect_vec(),
            ),
            _ => Tuple::new(
                pk,
                left_tuple
                    .values
                    .iter()
                    .chain(right_tuple.values.iter())
                    .cloned()
                    .collect_vec(),
            ),
        }
    }

    fn merge_schema(
        left_schema: &[ColumnRef],
        right_schema: &[ColumnRef],
        ty: JoinType,
    ) -> Arc<Vec<ColumnRef>> {
        let (left_force_nullable, right_force_nullable) = joins_nullable(&ty);

        let mut join_schema = vec![];
        for column in left_schema.iter() {
            join_schema.push(
                column
                    .nullable_for_join(left_force_nullable)
                    .unwrap_or_else(|| column.clone()),
            );
        }
        for column in right_schema.iter() {
            join_schema.push(
                column
                    .nullable_for_join(right_force_nullable)
                    .unwrap_or_else(|| column.clone()),
            );
        }
        Arc::new(join_schema)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::dql::test::build_integers;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::Childrens;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::Storage;
    use crate::types::evaluator::int32::Int32GtBinaryEvaluator;
    use crate::types::evaluator::BinaryEvaluatorBox;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use std::collections::HashSet;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_join_values(
        eq: bool,
    ) -> (
        Vec<(ScalarExpression, ScalarExpression)>,
        LogicalPlan,
        LogicalPlan,
        ScalarExpression,
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

        let on_keys = if eq {
            vec![(
                ScalarExpression::ColumnRef(t1_columns[1].clone()),
                ScalarExpression::ColumnRef(t2_columns[1].clone()),
            )]
        } else {
            vec![]
        };

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
                        DataValue::Int32(2),
                        DataValue::Int32(5),
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

        let filter = ScalarExpression::Binary {
            op: crate::expression::BinaryOperator::Gt,
            left_expr: Box::new(ScalarExpression::ColumnRef(ColumnRef::from(
                ColumnCatalog::new("c1".to_owned(), true, desc.clone()),
            ))),
            right_expr: Box::new(ScalarExpression::ColumnRef(ColumnRef::from(
                ColumnCatalog::new("c4".to_owned(), true, desc.clone()),
            ))),
            evaluator: Some(BinaryEvaluatorBox(Arc::new(Int32GtBinaryEvaluator))),
            ty: LogicalType::Boolean,
        };

        (on_keys, values_t1, values_t2, filter)
    }

    fn valid_result(expected: &mut HashSet<Vec<DataValue>>, actual: &[Tuple]) {
        assert_eq!(actual.len(), expected.len());

        for tuple in actual {
            let values = tuple
                .values
                .iter()
                .map(|v| {
                    if matches!(v, DataValue::Null) {
                        DataValue::Null
                    } else {
                        v.clone()
                    }
                })
                .collect_vec();
            assert!(expected.remove(&values));
        }

        assert!(expected.is_empty());
    }

    #[test]
    fn test_nested_inner_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::Inner,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(1);
        let tuple = build_integers(vec![Some(1), Some(2), Some(5), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_left_out_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::LeftOuter,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), None, None, None])
        );

        let mut expected_set = HashSet::with_capacity(4);
        let tuple = build_integers(vec![Some(0), Some(2), Some(4), None, None, None]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![Some(1), Some(2), Some(5), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);

        let tuple = build_integers(vec![Some(1), Some(3), Some(5), None, None, None]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![Some(3), Some(5), Some(7), None, None, None]);
        expected_set.insert(tuple);

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_cross_join_with_on() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::Cross,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(1);

        let tuple = build_integers(vec![Some(1), Some(2), Some(5), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_cross_join_without_filter() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, _) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Cross,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(3);

        let tuple = build_integers(vec![Some(0), Some(2), Some(4), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![Some(1), Some(2), Some(5), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![Some(1), Some(3), Some(5), Some(1), Some(3), Some(5)]);
        expected_set.insert(tuple);

        valid_result(&mut expected_set, &tuples);
        Ok(())
    }

    #[test]
    fn test_nested_cross_join_without_on() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, _) = build_join_values(false);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: None,
            },
            join_type: JoinType::Cross,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        assert_eq!(tuples.len(), 16);

        Ok(())
    }

    #[test]
    fn test_nested_left_semi_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::LeftSemi,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(1);
        expected_set.insert(build_integers(vec![Some(1), Some(2), Some(5)]));

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_left_anti_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::LeftAnti,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(3);
        expected_set.insert(build_integers(vec![Some(0), Some(2), Some(4)]));
        expected_set.insert(build_integers(vec![Some(1), Some(3), Some(5)]));
        expected_set.insert(build_integers(vec![Some(3), Some(5), Some(7)]));

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_right_out_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::RightOuter,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(4);
        let tuple = build_integers(vec![Some(1), Some(2), Some(5), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![None, None, None, Some(1), Some(3), Some(5)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![None, None, None, Some(1), Some(1), Some(1)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![None, None, None, Some(4), Some(6), Some(8)]);
        expected_set.insert(tuple);

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_full_join() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let (keys, left, right, filter) = build_join_values(true);
        let op = JoinOperator {
            on: JoinCondition::On {
                on: keys,
                filter: Some(filter),
            },
            join_type: JoinType::Full,
        };
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), None, None, None])
        );

        let mut expected_set = HashSet::with_capacity(7);
        let tuple = build_integers(vec![Some(0), Some(2), Some(4), None, None, None]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![Some(1), Some(2), Some(5), Some(0), Some(2), Some(4)]);
        expected_set.insert(tuple);

        let tuple = build_integers(vec![Some(1), Some(3), Some(5), None, None, None]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![Some(3), Some(5), Some(7), None, None, None]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![None, None, None, Some(1), Some(3), Some(5)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![None, None, None, Some(4), Some(6), Some(8)]);
        expected_set.insert(tuple);
        let tuple = build_integers(vec![None, None, None, Some(1), Some(1), Some(1)]);
        expected_set.insert(tuple);

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }
}
