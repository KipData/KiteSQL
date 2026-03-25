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

//! Defines the nested loop join executor, it supports [`JoinType::Inner`], [`JoinType::LeftOuter`],
//! [`JoinType::LeftSemi`], [`JoinType::LeftAnti`], [`JoinType::RightOuter`], [`JoinType::Cross`], [`JoinType::Full`].

use super::joins_nullable;
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::execution::dql::projection::Projection;
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::expression::ScalarExpression;
use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::{Schema, SchemaRef, Tuple};
use crate::types::value::DataValue;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
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
    left_input_plan: Option<LogicalPlan>,
    right_input_plan: LogicalPlan,
    output_schema_ref: SchemaRef,
    ty: JoinType,
    filter: Option<ScalarExpression>,
    eq_cond: EqualCondition,
    left_input: ExecId,
    state: NestedLoopJoinState,
}

enum NestedLoopJoinState {
    PullLeft {
        right_bitmap: Option<FixedBitSet>,
    },
    ScanRight {
        active_left: ActiveLeftState,
        right_bitmap: Option<FixedBitSet>,
    },
    EmitRightUnmatched {
        right_input: ExecId,
        right_bitmap: FixedBitSet,
        right_emit_index: usize,
    },
    End,
}

struct ActiveLeftState {
    left_tuple: Tuple,
    right_input: ExecId,
    right_index: usize,
    has_matched: bool,
    first_matches: Vec<usize>,
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
            left_input_plan: Some(left_input),
            right_input_plan: right_input,
            output_schema_ref,
            ty: join_type,
            filter,
            eq_cond,
            left_input: 0,
            state: NestedLoopJoinState::PullLeft { right_bitmap: None },
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for NestedLoopJoin {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.left_input = build_read(
            arena,
            self.left_input_plan
                .take()
                .expect("nested loop join left input plan initialized"),
            cache,
            transaction,
        );
        arena.push(ExecNode::NestedLoopJoin(self))
    }
}

impl NestedLoopJoin {
    fn build_right_input<'a, T: Transaction + 'a>(&self, arena: &mut ExecArena<'a, T>) -> ExecId {
        let cache = (arena.table_cache(), arena.view_cache(), arena.meta_cache());
        let transaction = arena.transaction_mut() as *mut T;
        build_read(arena, self.right_input_plan.clone(), cache, transaction)
    }

    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<Option<Tuple>, DatabaseError> {
        let mut state = std::mem::replace(&mut self.state, NestedLoopJoinState::End);

        loop {
            match state {
                NestedLoopJoinState::PullLeft { right_bitmap } => {
                    let Some(left_tuple) = arena.next_tuple(self.left_input)? else {
                        if matches!(self.ty, JoinType::Full) {
                            state = NestedLoopJoinState::EmitRightUnmatched {
                                right_input: self.build_right_input(arena),
                                right_bitmap: right_bitmap.unwrap_or_default(),
                                right_emit_index: 0,
                            };
                            continue;
                        }
                        self.state = NestedLoopJoinState::End;
                        return Ok(None);
                    };

                    state = NestedLoopJoinState::ScanRight {
                        active_left: ActiveLeftState {
                            left_tuple,
                            right_input: self.build_right_input(arena),
                            right_index: 0,
                            has_matched: false,
                            first_matches: Vec::new(),
                        },
                        right_bitmap,
                    };
                }
                NestedLoopJoinState::ScanRight {
                    mut active_left,
                    mut right_bitmap,
                } => {
                    while let Some(right_tuple) = arena.next_tuple(active_left.right_input)? {
                        let idx = active_left.right_index;
                        active_left.right_index += 1;

                        let tuple = match (
                            self.filter.as_ref(),
                            self.eq_cond.equals(&active_left.left_tuple, &right_tuple)?,
                        ) {
                            (None, true) if matches!(self.ty, JoinType::RightOuter) => {
                                active_left.has_matched = true;
                                Self::emit_tuple(
                                    &right_tuple,
                                    &active_left.left_tuple,
                                    self.ty,
                                    true,
                                )
                            }
                            (None, true) => {
                                active_left.has_matched = true;
                                Self::emit_tuple(
                                    &active_left.left_tuple,
                                    &right_tuple,
                                    self.ty,
                                    true,
                                )
                            }
                            (Some(filter), true) => {
                                let new_tuple = Self::merge_tuple(
                                    &active_left.left_tuple,
                                    &right_tuple,
                                    &self.ty,
                                );
                                let value =
                                    filter.eval(Some((&new_tuple, &self.output_schema_ref)))?;
                                match &value {
                                    DataValue::Boolean(true) => {
                                        let tuple = match self.ty {
                                            JoinType::LeftAnti => None,
                                            JoinType::LeftSemi if active_left.has_matched => None,
                                            JoinType::RightOuter => Self::emit_tuple(
                                                &right_tuple,
                                                &active_left.left_tuple,
                                                self.ty,
                                                true,
                                            ),
                                            _ => Self::emit_tuple(
                                                &active_left.left_tuple,
                                                &right_tuple,
                                                self.ty,
                                                true,
                                            ),
                                        };
                                        active_left.has_matched = true;
                                        tuple
                                    }
                                    DataValue::Boolean(false) | DataValue::Null => None,
                                    _ => return Err(DatabaseError::InvalidType),
                                }
                            }
                            _ => None,
                        };

                        if let Some(tuple) = tuple {
                            if matches!(self.ty, JoinType::Full) {
                                if let Some(bits) = right_bitmap.as_mut() {
                                    bits.insert(idx);
                                } else {
                                    active_left.first_matches.push(idx);
                                }
                            }

                            self.state = if matches!(self.ty, JoinType::LeftSemi) {
                                NestedLoopJoinState::PullLeft { right_bitmap }
                            } else {
                                NestedLoopJoinState::ScanRight {
                                    active_left,
                                    right_bitmap,
                                }
                            };
                            return Ok(Some(tuple));
                        }

                        if matches!(self.ty, JoinType::LeftAnti) && active_left.has_matched {
                            break;
                        }
                    }

                    if matches!(self.ty, JoinType::Full) {
                        if let Some(bits) = right_bitmap.as_mut() {
                            for idx in active_left.first_matches {
                                bits.insert(idx);
                            }
                        } else {
                            let mut bits = FixedBitSet::with_capacity(active_left.right_index);
                            for idx in active_left.first_matches {
                                bits.insert(idx);
                            }
                            right_bitmap = Some(bits);
                        }
                    }
                    let right_schema_len = self.eq_cond.right_schema.len();
                    let tuple = match self.ty {
                        JoinType::LeftAnti if !active_left.has_matched => {
                            Some(active_left.left_tuple)
                        }
                        JoinType::LeftOuter
                        | JoinType::LeftSemi
                        | JoinType::RightOuter
                        | JoinType::Full
                            if !active_left.has_matched =>
                        {
                            let right_tuple =
                                Tuple::new(None, vec![DataValue::Null; right_schema_len]);
                            if matches!(self.ty, JoinType::RightOuter) {
                                Self::emit_tuple(
                                    &right_tuple,
                                    &active_left.left_tuple,
                                    self.ty,
                                    false,
                                )
                            } else {
                                Self::emit_tuple(
                                    &active_left.left_tuple,
                                    &right_tuple,
                                    self.ty,
                                    false,
                                )
                            }
                        }
                        _ => None,
                    };

                    self.state = NestedLoopJoinState::PullLeft { right_bitmap };
                    if let Some(tuple) = tuple {
                        return Ok(Some(tuple));
                    }
                    state = std::mem::replace(&mut self.state, NestedLoopJoinState::End);
                }
                NestedLoopJoinState::EmitRightUnmatched {
                    right_input,
                    right_bitmap,
                    mut right_emit_index,
                } => {
                    while let Some(mut right_tuple) = arena.next_tuple(right_input)? {
                        let idx = right_emit_index;
                        right_emit_index += 1;

                        if !right_bitmap.contains(idx) {
                            let mut values = vec![DataValue::Null; self.eq_cond.left_schema.len()];
                            values.append(&mut right_tuple.values);
                            self.state = NestedLoopJoinState::EmitRightUnmatched {
                                right_input,
                                right_bitmap,
                                right_emit_index,
                            };
                            return Ok(Some(Tuple::new(right_tuple.pk, values)));
                        }
                    }

                    self.state = NestedLoopJoinState::End;
                    return Ok(None);
                }
                NestedLoopJoinState::End => {
                    self.state = NestedLoopJoinState::End;
                    return Ok(None);
                }
            }
        }
    }

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
                    .for_each(|v| *v = DataValue::Null);
            }
            JoinType::RightOuter if !is_matched => {
                (0..left_len).for_each(|i| {
                    values[i] = DataValue::Null;
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::db::{DataBaseBuilder, ResultIter};
    use crate::execution::dql::test::build_integers;
    use crate::execution::{try_collect, ReadExecutor};
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::Childrens;
    use crate::storage::rocksdb::{RocksStorage, RocksTransaction};
    use crate::storage::Storage;
    use crate::types::evaluator::int32::Int32GtBinaryEvaluator;
    use crate::types::evaluator::BinaryEvaluatorBox;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use std::collections::HashSet;
    use std::hash::RandomState;
    use tempfile::TempDir;

    fn optimize_exprs(plan: LogicalPlan) -> Result<LogicalPlan, DatabaseError> {
        HepOptimizerPipeline::builder()
            .before_batch(
                "Expression Remapper".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::EvaluatorBind],
            )
            .build()
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)
    }

    fn tuple_to_strings(tuple: &Tuple) -> Vec<Option<String>> {
        tuple
            .values
            .iter()
            .map(|value| match value {
                DataValue::Null => None,
                DataValue::Utf8 { value, .. } => Some(value.clone()),
                other => Some(other.to_string()),
            })
            .collect()
    }

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
                ScalarExpression::column_expr(t1_columns[1].clone(), 1),
                ScalarExpression::column_expr(t2_columns[1].clone(), 1),
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
            left_expr: Box::new(ScalarExpression::column_expr(
                ColumnRef::from(ColumnCatalog::new("c1".to_owned(), true, desc.clone())),
                0,
            )),
            right_expr: Box::new(ScalarExpression::column_expr(
                ColumnRef::from(ColumnCatalog::new("c4".to_owned(), true, desc.clone())),
                3,
            )),
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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::Inner,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(1);
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(2),
            Some(5),
            Some(0),
            Some(2),
            Some(4),
        ]));

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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::LeftOuter,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), None, None, None])
        );

        let mut expected_set = HashSet::with_capacity(4);
        expected_set.insert(build_integers(vec![
            Some(0),
            Some(2),
            Some(4),
            None,
            None,
            None,
        ]));
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(2),
            Some(5),
            Some(0),
            Some(2),
            Some(4),
        ]));
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(3),
            Some(5),
            None,
            None,
            None,
        ]));
        expected_set.insert(build_integers(vec![
            Some(3),
            Some(5),
            Some(7),
            None,
            None,
            None,
        ]));

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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::Cross,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(1);
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(2),
            Some(5),
            Some(0),
            Some(2),
            Some(4),
        ]));

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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: None,
                },
                join_type: JoinType::Cross,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(3);
        expected_set.insert(build_integers(vec![
            Some(0),
            Some(2),
            Some(4),
            Some(0),
            Some(2),
            Some(4),
        ]));
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(2),
            Some(5),
            Some(0),
            Some(2),
            Some(4),
        ]));
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(3),
            Some(5),
            Some(1),
            Some(3),
            Some(5),
        ]));

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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: None,
                },
                join_type: JoinType::Cross,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::LeftSemi,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::LeftAnti,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::RightOuter,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        let mut expected_set = HashSet::with_capacity(4);
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(2),
            Some(5),
            Some(0),
            Some(2),
            Some(4),
        ]));
        expected_set.insert(build_integers(vec![
            None,
            None,
            None,
            Some(1),
            Some(3),
            Some(5),
        ]));
        expected_set.insert(build_integers(vec![
            None,
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
        ]));
        expected_set.insert(build_integers(vec![
            None,
            None,
            None,
            Some(4),
            Some(6),
            Some(8),
        ]));

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
        let plan = LogicalPlan::new(
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: keys,
                    filter: Some(filter),
                },
                join_type: JoinType::Full,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        );
        let plan = optimize_exprs(plan)?;
        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
        let tuples = try_collect(executor)?;

        assert_eq!(
            tuples[0].values,
            build_integers(vec![Some(0), Some(2), Some(4), None, None, None])
        );

        let mut expected_set = HashSet::with_capacity(7);
        expected_set.insert(build_integers(vec![
            Some(0),
            Some(2),
            Some(4),
            None,
            None,
            None,
        ]));
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(2),
            Some(5),
            Some(0),
            Some(2),
            Some(4),
        ]));
        expected_set.insert(build_integers(vec![
            Some(1),
            Some(3),
            Some(5),
            None,
            None,
            None,
        ]));
        expected_set.insert(build_integers(vec![
            Some(3),
            Some(5),
            Some(7),
            None,
            None,
            None,
        ]));
        expected_set.insert(build_integers(vec![
            None,
            None,
            None,
            Some(1),
            Some(3),
            Some(5),
        ]));
        expected_set.insert(build_integers(vec![
            None,
            None,
            None,
            Some(4),
            Some(6),
            Some(8),
        ]));
        expected_set.insert(build_integers(vec![
            None,
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
        ]));

        valid_result(&mut expected_set, &tuples);

        Ok(())
    }

    #[test]
    fn test_nested_right_join_filter_only_left_columns() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let mut transaction = storage.transaction()?;
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None)?;
        let left_columns = vec![
            ColumnRef::from(ColumnCatalog::new("k".to_string(), true, desc.clone())),
            ColumnRef::from(ColumnCatalog::new("v".to_string(), true, desc.clone())),
        ];
        let right_columns = vec![ColumnRef::from(ColumnCatalog::new(
            "rk".to_string(),
            true,
            desc.clone(),
        ))];

        let on_keys = vec![(
            ScalarExpression::column_expr(left_columns[0].clone(), 0),
            ScalarExpression::column_expr(right_columns[0].clone(), 0),
        )];
        let filter_expr = ScalarExpression::Binary {
            op: crate::expression::BinaryOperator::Gt,
            left_expr: Box::new(ScalarExpression::column_expr(left_columns[1].clone(), 1)),
            right_expr: Box::new(ScalarExpression::Constant(DataValue::Int32(1))),
            evaluator: None,
            ty: LogicalType::Boolean,
        };

        let left = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
                rows: vec![
                    vec![DataValue::Int32(2), DataValue::Int32(0)],
                    vec![DataValue::Int32(2), DataValue::Int32(5)],
                ],
                schema_ref: Arc::new(left_columns),
            }),
            childrens: Box::new(Childrens::None),
            physical_option: None,
            _output_schema_ref: None,
        };
        let right = LogicalPlan {
            operator: Operator::Values(ValuesOperator {
                rows: vec![vec![DataValue::Int32(2)]],
                schema_ref: Arc::new(right_columns),
            }),
            childrens: Box::new(Childrens::None),
            physical_option: None,
            _output_schema_ref: None,
        };

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
        let plan = optimize_exprs(plan)?;

        let Operator::Join(op) = plan.operator else {
            unreachable!()
        };
        let (left, right) = plan.childrens.pop_twins();
        let executor = NestedLoopJoin::from((op, left, right))
            .execute((&table_cache, &view_cache, &meta_cache), &mut transaction);
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
    fn test_right_join_using_keeps_left_visible_column_binding() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db = DataBaseBuilder::path(temp_dir.path()).build_in_memory()?;

        let setup_sql = [
            "DROP TABLE IF EXISTS str1",
            "DROP TABLE IF EXISTS str2",
            "CREATE TABLE str1 (aid INT PRIMARY KEY, a INT, s VARCHAR)",
            "CREATE TABLE str2 (bid INT PRIMARY KEY, a INT, s VARCHAR)",
            "INSERT INTO str1 VALUES (0, 1, 'a'), (1, 2, 'A'), (2, 3, 'c'), (3, 4, 'D')",
            "INSERT INTO str2 VALUES (0, 1, 'A'), (1, 2, 'B'), (2, 3, 'C'), (3, 4, 'E')",
        ];

        for sql in setup_sql {
            db.run(sql)?.done()?;
        }

        let mut iter = db.run(
            "SELECT s, str1.s, str2.s \
             FROM str1 RIGHT OUTER JOIN str2 USING(s) \
             ORDER BY str2.s",
        )?;
        let mut actual = Vec::new();

        for row in iter.by_ref() {
            let tuple = row?;
            actual.push(tuple_to_strings(&tuple));
        }
        iter.done()?;

        assert_eq!(
            actual,
            vec![
                vec![
                    Some("A".to_string()),
                    Some("A".to_string()),
                    Some("A".to_string())
                ],
                vec![None, None, Some("B".to_string())],
                vec![None, None, Some("C".to_string())],
                vec![None, None, Some("E".to_string())],
            ]
        );

        Ok(())
    }
}
