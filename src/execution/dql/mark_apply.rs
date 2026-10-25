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
#[cfg(feature = "spill")]
use crate::execution::spill::SpillVec;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier};
use crate::planner::{ExecMetaArena, ExprRef, LogicalPlan, MetaArena};
use crate::storage::Transaction;
use crate::types::index::RuntimeIndexProbe;
use crate::types::tuple::{SplitTupleRef, Tuple};
use crate::types::value::DataValue;

#[derive(PartialEq, Eq)]
enum QuantifiedPredicateOutcome {
    True,
    False,
    Null,
    Skip,
}

#[cfg(feature = "spill")]
type MaterializedRows = SpillVec<'static, Tuple>;
#[cfg(not(feature = "spill"))]
type MaterializedRows = Vec<Tuple>;

enum RightInput<'a> {
    Rebuild {
        plan: &'a LogicalPlan,
        pos: ExecId,
    },
    Materialize {
        plan: &'a LogicalPlan,
        pos: ExecId,
    },
    Materializing {
        root: ExecId,
        rows: MaterializedRows,
    },
    Materialized(MaterializedRows),
}

impl<'a> RightInput<'a> {
    fn build<T: Transaction + 'a>(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        probe: Option<RuntimeIndexProbe>,
    ) -> ExecId {
        let (Self::Rebuild { plan, pos } | Self::Materialize { plan, pos }) = self else {
            unreachable!("materialized input no longer builds its RHS")
        };
        arena.nodes.seek(*pos);
        if let Some(probe) = probe {
            arena.push_runtime_probe(probe);
        }
        build_read(
            arena,
            plan_arena,
            plan,
            arena.context(),
            arena.transaction(),
        )
    }

    // Return the first terminal predicate result, or None after exhausting the RHS.
    // The caller supplies Mark semantics; this enum owns reconstruction and replay.
    fn scan<T: Transaction + 'a, A: MetaArena + 'a>(
        self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        probe: Option<RuntimeIndexProbe>,
        mut evaluate: impl FnMut(&Tuple, &ExecMetaArena<A>) -> Result<Option<DataValue>, DatabaseError>,
    ) -> Result<(Self, Option<DataValue>), DatabaseError> {
        match self {
            input @ Self::Rebuild { .. } => {
                let root = input.build(arena, plan_arena, probe);
                while arena.next_tuple(root, plan_arena)? {
                    if let Some(value) = evaluate(arena.result_tuple(), plan_arena)? {
                        return Ok((input, Some(value)));
                    }
                }
                Ok((input, None))
            }
            input @ Self::Materialize { .. } => {
                let root = input.build(arena, plan_arena, None);
                Self::Materializing {
                    root,
                    rows: MaterializedRows::new(),
                }
                .scan(arena, plan_arena, None, evaluate)
            }
            Self::Materializing { root, mut rows } => {
                if let Some(value) = Self::replay(&rows, plan_arena, &mut evaluate)? {
                    return Ok((Self::Materializing { root, rows }, Some(value)));
                }
                while arena.next_tuple(root, plan_arena)? {
                    let right = arena.materialize_tuple();
                    let result = evaluate(&right, plan_arena)?;
                    #[cfg(feature = "spill")]
                    let _ = rows.push(right)?;
                    #[cfg(not(feature = "spill"))]
                    rows.push(right);
                    // A terminal result pauses the executor; cached rows remain a prefix.
                    if result.is_some() {
                        return Ok((Self::Materializing { root, rows }, result));
                    }
                }
                // Only EOF makes the prefix complete; move its cache into the final state.
                Ok((Self::Materialized(rows), None))
            }
            Self::Materialized(rows) => {
                let result = Self::replay(&rows, plan_arena, &mut evaluate)?;
                Ok((Self::Materialized(rows), result))
            }
        }
    }

    fn replay<A: MetaArena>(
        rows: &MaterializedRows,
        plan_arena: &ExecMetaArena<A>,
        mut evaluate: impl FnMut(&Tuple, &ExecMetaArena<A>) -> Result<Option<DataValue>, DatabaseError>,
    ) -> Result<Option<DataValue>, DatabaseError> {
        #[cfg(feature = "spill")]
        return rows.replay(|right| evaluate(right, plan_arena));
        #[cfg(not(feature = "spill"))]
        {
            for right in rows {
                if let Some(value) = evaluate(right, plan_arena)? {
                    return Ok(Some(value));
                }
            }
            Ok(None)
        }
    }
}

pub struct MarkApply<'a> {
    op: &'a MarkApplyOperator,
    left_input: ExecId,
    // Temporarily None while scan owns and transitions the right input.
    right_input: Option<RightInput<'a>>,
    // Retain a streaming inner input across next_tuple calls, not its result rows.
    join_input: Option<(ExecId, Tuple)>,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for MarkApply<'a> {
    type Input = (&'a MarkApplyOperator, &'a LogicalPlan, &'a LogicalPlan);

    fn into_executor(
        (op, left_input, right_input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let left_input = build_read(arena, plan_arena, left_input, cache, transaction);
        let pos = arena.nodes.position();
        build_read(arena, plan_arena, right_input, cache, transaction);
        let right_input = if op.materialize {
            RightInput::Materialize {
                plan: right_input,
                pos,
            }
        } else {
            RightInput::Rebuild {
                plan: right_input,
                pos,
            }
        };
        arena.push(ExecNode::MarkApply(Self {
            op,
            left_input,
            right_input: Some(right_input),
            join_input: None,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for MarkApply<'a> {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.right_input.take() else {
            arena.finish();
            return Ok(());
        };
        if matches!(self.op.kind, MarkApplyKind::InnerJoin) {
            self.next_join_tuple(&input, arena, plan_arena)?;
            self.right_input = Some(input);
            return Ok(());
        }
        if !arena.next_tuple(self.left_input, plan_arena)? {
            arena.finish();
            return Ok(());
        }
        let mut left = arena.materialize_tuple();
        let (input, marker) = self.mark_value(input, arena, plan_arena, &left)?;
        self.right_input = Some(input);
        left.values.push(marker);
        arena.produce_tuple(left);
        Ok(())
    }
}

impl<'a> MarkApply<'a> {
    fn next_join_tuple<T: Transaction + 'a, A: MetaArena + 'a>(
        &mut self,
        input: &RightInput<'a>,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        loop {
            if let Some((root, left)) = &mut self.join_input {
                while arena.next_tuple(*root, plan_arena)? {
                    let right = arena.result_tuple();
                    if Self::predicates_matched(self.op.predicates(), left, right, plan_arena)? {
                        let output = arena.result_tuple_mut();
                        let right_len = output.values.len();
                        output.values.extend(left.values.iter().cloned());
                        output.values.rotate_left(right_len);
                        output.pk = left.pk.clone().or(output.pk.take());
                        arena.resume();
                        return Ok(());
                    }
                }
            }
            if !arena.next_tuple(self.left_input, plan_arena)? {
                self.join_input = None;
                arena.finish();
                return Ok(());
            }
            let left = arena.materialize_tuple();
            let value = self.parameterized_probe_value(&left, plan_arena)?;
            let root = input.build(arena, plan_arena, self.runtime_probe_for(value));
            self.join_input = Some((root, left));
        }
    }

    fn runtime_probe_for(&self, param_value: Option<DataValue>) -> Option<RuntimeIndexProbe> {
        self.op.parameterized_probe()?;
        match param_value {
            Some(value) => Some(RuntimeIndexProbe::Eq(value)),
            None if matches!(
                self.op.kind,
                MarkApplyKind::Quantified(MarkApplyQuantifier::Any)
            ) =>
            {
                Some(RuntimeIndexProbe::Scope {
                    min: std::collections::Bound::Unbounded,
                    max: std::collections::Bound::Unbounded,
                })
            }
            None => None,
        }
    }

    fn parameterized_probe_value(
        &self,
        left_tuple: &Tuple,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<DataValue>, DatabaseError> {
        self.op
            .parameterized_probe()
            .map(|probe| {
                plan_arena
                    .expression(*probe)
                    .eval(plan_arena, Some(left_tuple))
                    .map(|value| value.into_owned())
            })
            .transpose()
    }

    fn mark_value<T: Transaction + 'a, A: MetaArena + 'a>(
        &self,
        input: RightInput<'a>,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        left_tuple: &Tuple,
    ) -> Result<(RightInput<'a>, DataValue), DatabaseError> {
        let probe = self.parameterized_probe_value(left_tuple, plan_arena)?;
        match self.op.kind {
            MarkApplyKind::InnerJoin => unreachable!("inner join streams tuples"),
            MarkApplyKind::Exists => {
                let predicates = self.op.predicates();
                let runtime_probe = self.runtime_probe_for(probe);
                let (input, value) =
                    input.scan(arena, plan_arena, runtime_probe, |right, meta| {
                        Ok(
                            Self::predicates_matched(predicates, left_tuple, right, meta)?
                                .then_some(DataValue::Boolean(true)),
                        )
                    })?;
                Ok((input, value.unwrap_or(DataValue::Boolean(false))))
            }
            MarkApplyKind::Quantified(quantifier) => {
                if matches!(quantifier, MarkApplyQuantifier::Any) {
                    if let Some(probe_value) = probe {
                        if !probe_value.is_null() {
                            let (input, value) = self.scan_quantified_right_input(
                                input,
                                arena,
                                plan_arena,
                                Some(probe_value),
                                quantifier,
                                left_tuple,
                            )?;
                            if value == DataValue::Boolean(true) {
                                return Ok((input, value));
                            }
                            let (input, value) = self.scan_quantified_right_input(
                                input,
                                arena,
                                plan_arena,
                                Some(DataValue::Null),
                                quantifier,
                                left_tuple,
                            )?;
                            return Ok((
                                input,
                                if value.is_null() {
                                    DataValue::Null
                                } else {
                                    DataValue::Boolean(false)
                                },
                            ));
                        }
                    }
                }
                self.scan_quantified_right_input(
                    input, arena, plan_arena, None, quantifier, left_tuple,
                )
            }
        }
    }

    fn scan_quantified_right_input<T: Transaction + 'a, A: MetaArena + 'a>(
        &self,
        input: RightInput<'a>,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        probe: Option<DataValue>,
        quantifier: MarkApplyQuantifier,
        left_tuple: &Tuple,
    ) -> Result<(RightInput<'a>, DataValue), DatabaseError> {
        let mut saw_null = false;
        let predicates = self.op.predicates();
        let runtime_probe = self.runtime_probe_for(probe);
        let (input, value) = input.scan(arena, plan_arena, runtime_probe, |right, meta| {
            match Self::quantified_predicate_outcome(predicates, left_tuple, right, meta)? {
                QuantifiedPredicateOutcome::True
                    if matches!(quantifier, MarkApplyQuantifier::Any) =>
                {
                    return Ok(Some(DataValue::Boolean(true)));
                }
                QuantifiedPredicateOutcome::False
                    if matches!(quantifier, MarkApplyQuantifier::All) =>
                {
                    return Ok(Some(DataValue::Boolean(false)));
                }
                QuantifiedPredicateOutcome::Null => saw_null = true,
                _ => {}
            }
            Ok(None)
        })?;
        Ok((
            input,
            value.unwrap_or_else(|| {
                if saw_null {
                    DataValue::Null
                } else {
                    DataValue::Boolean(matches!(quantifier, MarkApplyQuantifier::All))
                }
            }),
        ))
    }

    fn predicates_matched(
        predicates: &[ExprRef],
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<bool, DatabaseError> {
        let values = SplitTupleRef::new(left_tuple, right_tuple);
        for predicate in predicates {
            match *plan_arena
                .expression(*predicate)
                .eval(plan_arena, Some(&values))?
            {
                DataValue::Boolean(true) => {}
                DataValue::Boolean(false) | DataValue::Null => return Ok(false),
                _ => return Err(DatabaseError::InvalidType),
            }
        }
        Ok(true)
    }

    fn quantified_predicate_outcome(
        predicates: &[ExprRef],
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<QuantifiedPredicateOutcome, DatabaseError> {
        match Self::eval_predicates(predicates, left_tuple, right_tuple, plan_arena)? {
            Some(DataValue::Boolean(true)) => Ok(QuantifiedPredicateOutcome::True),
            Some(DataValue::Boolean(false)) => Ok(QuantifiedPredicateOutcome::False),
            Some(DataValue::Null) => Ok(QuantifiedPredicateOutcome::Null),
            None => Ok(QuantifiedPredicateOutcome::Skip),
            Some(_) => Err(DatabaseError::InvalidType),
        }
    }

    fn eval_predicates(
        predicates: &[ExprRef],
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<DataValue>, DatabaseError> {
        let values = SplitTupleRef::new(left_tuple, right_tuple);
        // probe_predicate is in predicate, always first
        let (probe_predicate, correlated_predicates) =
            predicates.split_first().ok_or(DatabaseError::InvalidType)?;
        for predicate in correlated_predicates {
            match *plan_arena
                .expression(*predicate)
                .eval(plan_arena, Some(&values))?
            {
                DataValue::Boolean(true) => {}
                DataValue::Boolean(false) | DataValue::Null => return Ok(None),
                _ => return Err(DatabaseError::InvalidType),
            }
        }
        Ok(Some(
            plan_arena
                .expression(*probe_predicate)
                .eval(plan_arena, Some(&values))?
                .into_owned(),
        ))
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::execution::{execute_input, try_collect, ExecArena};
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::test::PlanArenaTestExt;
    use crate::planner::{Childrens, ExprRef, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::evaluator::binary_create;
    use crate::types::index::RuntimeIndexProbe;
    use crate::types::tuple::Tuple;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use tempfile::TempDir;

    fn build_values_with_schema(
        arena: &mut crate::planner::PlanArena,
        columns: Vec<(&str, LogicalType)>,
        rows: Vec<Vec<DataValue>>,
    ) -> LogicalPlan {
        let schema_ref = columns
            .into_iter()
            .map(|(name, ty)| {
                arena.alloc_column(ColumnCatalog::new(
                    name.to_string(),
                    true,
                    ColumnDesc::new(ty, None, true, None).unwrap(),
                ))
            })
            .collect();

        LogicalPlan::new(
            Operator::Values(ValuesOperator::new(
                arena.alloc_expression_rows(&rows),
                rows.len(),
                schema_ref,
            )),
            Childrens::None,
        )
    }

    fn build_values(
        arena: &mut crate::planner::PlanArena,
        name: &str,
        rows: Vec<Vec<DataValue>>,
    ) -> LogicalPlan {
        build_values_with_schema(arena, vec![(name, LogicalType::Integer)], rows)
    }

    fn build_test_storage() -> Result<
        (
            TableCache,
            ViewCache,
            StatisticsMetaCache,
            TempDir,
            RocksStorage,
        ),
        DatabaseError,
    > {
        let meta_cache = crate::storage::StatisticsMetaCache::default();
        let view_cache = crate::storage::ViewCache::default();
        let table_cache = crate::storage::TableCache::default();

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;

        Ok((table_cache, view_cache, meta_cache, temp_dir, storage))
    }

    fn build_marker_column(arena: &mut crate::planner::PlanArena) -> ColumnRef {
        arena.alloc_column(ColumnCatalog::new(
            "__exists".to_string(),
            true,
            ColumnDesc::new(LogicalType::Boolean, None, true, None).unwrap(),
        ))
    }

    fn build_equality_predicate(
        plan_arena: &mut crate::planner::PlanArena,
        left_column: ColumnRef,
        left_position: usize,
        right_column: ColumnRef,
        right_position: usize,
    ) -> Result<ExprRef, DatabaseError> {
        let left_expr =
            plan_arena.alloc_expression(ScalarExpression::column_expr(left_column, left_position));
        let right_expr = plan_arena
            .alloc_expression(ScalarExpression::column_expr(right_column, right_position));
        Ok(plan_arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr,
            right_expr,
            evaluator: Some(binary_create(
                Cow::Owned(LogicalType::Integer),
                BinaryOperator::Eq,
            )?),
            ty: LogicalType::Boolean,
        }))
    }

    #[test]
    fn inner_join_apply_emits_all_matches_and_reuses_right_slots() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            "left_key",
            vec![
                vec![DataValue::Int32(2)],
                vec![DataValue::Null],
                vec![DataValue::Int32(99)],
                vec![DataValue::Int32(2)],
            ],
        );
        let mut right = build_values_with_schema(
            &mut plan_arena,
            vec![
                ("right_key", LogicalType::Integer),
                ("flag", LogicalType::Boolean),
            ],
            vec![
                vec![DataValue::Int32(2), DataValue::Boolean(true)],
                vec![DataValue::Int32(2), DataValue::Boolean(false)],
                vec![DataValue::Int32(2), DataValue::Null],
                vec![DataValue::Int32(2), DataValue::Boolean(true)],
                vec![DataValue::Null, DataValue::Boolean(true)],
            ],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_schema = right.output_schema(&mut plan_arena).clone();
        let equality =
            build_equality_predicate(&mut plan_arena, left_column, 0, right_schema[0], 1)?;
        let residual =
            plan_arena.alloc_expression(ScalarExpression::column_expr(right_schema[1], 2));
        let probe = plan_arena.alloc_expression(ScalarExpression::column_expr(left_column, 0));
        let mut op = MarkApplyOperator::new_inner_join(vec![equality, residual], probe);
        // Values has no parameterized IndexScan to consume a runtime probe.
        op.set_parameterized_probe(None);
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let cache = crate::execution::empty_context(&table_cache, &view_cache, &meta_cache);
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(cache, &transaction);
        let root = <MarkApply as ReadExecutor<_>>::into_executor(
            (&op, &left, &right),
            &mut arena,
            &mut plan_arena,
            cache,
            &transaction,
        );
        let address = arena.nodes.items.as_ptr();
        let count = arena.nodes.items.len();
        assert_eq!(count, 3);
        let mut plan_arena = ExecMetaArena::new(plan_arena);
        for _ in 0..4 {
            assert!(arena.next_tuple(root, &mut plan_arena)?);
            assert_eq!(
                arena.materialize_tuple().values,
                vec![
                    DataValue::Int32(2),
                    DataValue::Int32(2),
                    DataValue::Boolean(true)
                ]
            );
            assert_eq!(arena.nodes.items.as_ptr(), address);
            assert_eq!(arena.nodes.items.len(), count);
        }
        assert!(!arena.next_tuple(root, &mut plan_arena)?);
        Ok(())
    }

    #[test]
    fn inner_join_apply_does_not_read_past_the_returned_match() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(&mut plan_arena, "left_key", vec![vec![DataValue::Int32(1)]]);
        let mut right = build_values_with_schema(
            &mut plan_arena,
            vec![("flag", LogicalType::Boolean)],
            // A later row that cannot be cast must not fail the first fetch.
            vec![
                vec![DataValue::Boolean(true)],
                vec![DataValue::from("not-a-boolean".to_string())],
            ],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_column = right.output_schema(&mut plan_arena)[0];
        let predicate = plan_arena.alloc_expression(ScalarExpression::column_expr(right_column, 1));
        let probe = plan_arena.alloc_expression(ScalarExpression::column_expr(left_column, 0));
        let mut op = MarkApplyOperator::new_inner_join(vec![predicate], probe);
        op.set_parameterized_probe(None);
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let mut executor = execute_input::<_, MarkApply>(
            (&op, &left, &right),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        );
        assert_eq!(
            executor.next_tuple()?.unwrap().values,
            vec![DataValue::Int32(1), DataValue::Boolean(true)]
        );
        assert!(executor.next_tuple().is_err());
        Ok(())
    }

    #[test]
    fn mark_exists_apply_appends_boolean_match_column() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values(
            &mut plan_arena,
            "right_c1",
            vec![vec![DataValue::Int32(2)], vec![DataValue::Int32(3)]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_column = right.output_schema(&mut plan_arena)[0];

        let predicate = build_equality_predicate(&mut plan_arena, left_column, 0, right_column, 1)?;
        let op =
            MarkApplyOperator::new_exists(build_marker_column(&mut plan_arena), vec![predicate]);

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let context = crate::execution::empty_context(&table_cache, &view_cache, &meta_cache);
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(context, &transaction);
        let root = <MarkApply as ReadExecutor<_>>::into_executor(
            (&op, &left, &right),
            &mut arena,
            &mut plan_arena,
            context,
            &transaction,
        );
        let count = arena.nodes.items.len();
        let address = arena.nodes.items.as_ptr();
        assert_eq!(count, 3);
        let mut tuples = Vec::new();
        let mut plan_arena = ExecMetaArena::new(plan_arena);
        while arena.next_tuple(root, &mut plan_arena)? {
            tuples.push(arena.materialize_tuple());
            assert_eq!(arena.nodes.items.len(), count);
            assert_eq!(arena.nodes.items.as_ptr(), address);
        }

        assert_eq!(
            tuples
                .into_iter()
                .flat_map(|tuple| tuple.values)
                .collect::<Vec<_>>(),
            vec![
                DataValue::Int32(1),
                DataValue::Boolean(false),
                DataValue::Int32(2),
                DataValue::Boolean(true),
            ]
        );

        Ok(())
    }

    #[test]
    fn mark_exists_apply_treats_null_predicate_as_not_matched() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values(
            &mut plan_arena,
            "right_c1",
            vec![vec![DataValue::Null], vec![DataValue::Int32(2)]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_column = right.output_schema(&mut plan_arena)[0];

        let predicate = build_equality_predicate(&mut plan_arena, left_column, 0, right_column, 1)?;
        let op =
            MarkApplyOperator::new_exists(build_marker_column(&mut plan_arena), vec![predicate]);

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply>(
            (&op, &left, &right),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        ))?;

        assert_eq!(
            tuples
                .into_iter()
                .flat_map(|tuple| tuple.values)
                .collect::<Vec<_>>(),
            vec![
                DataValue::Int32(1),
                DataValue::Boolean(false),
                DataValue::Int32(2),
                DataValue::Boolean(true),
            ]
        );

        Ok(())
    }

    #[test]
    fn mark_exists_apply_sets_runtime_probe_before_residual_predicates() -> Result<(), DatabaseError>
    {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values_with_schema(
            &mut plan_arena,
            vec![
                ("left_c1", LogicalType::Integer),
                ("left_flag", LogicalType::Integer),
            ],
            vec![],
        );
        let mut right = build_values_with_schema(
            &mut plan_arena,
            vec![
                ("right_c1", LogicalType::Integer),
                ("right_flag", LogicalType::Integer),
            ],
            vec![
                vec![DataValue::Int32(2), DataValue::Int32(1)],
                vec![DataValue::Int32(2), DataValue::Null],
            ],
        );
        let left_schema = left.output_schema(&mut plan_arena).clone();
        let right_schema = right.output_schema(&mut plan_arena).clone();
        let left_value_column = left_schema[0];
        let left_flag_column = left_schema[1];
        let right_value_column = right_schema[0];
        let right_flag_column = right_schema[1];

        let probe_predicate =
            build_equality_predicate(&mut plan_arena, left_value_column, 0, right_value_column, 2)?;
        let flag_predicate =
            build_equality_predicate(&mut plan_arena, left_flag_column, 1, right_flag_column, 3)?;
        let mut op = MarkApplyOperator::new_exists(
            build_marker_column(&mut plan_arena),
            vec![probe_predicate, flag_predicate],
        );
        let probe =
            plan_arena.alloc_expression(ScalarExpression::column_expr(left_value_column, 0));
        op.set_parameterized_probe(Some(probe));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            &transaction,
        );

        let mut exec: MarkApply = MarkApply {
            op: &op,
            right_input: Some(RightInput::Rebuild {
                plan: &right,
                pos: arena.nodes.position(),
            }),
            left_input: 0,
            join_input: None,
        };
        let left_tuple = Tuple::new(None, vec![DataValue::Int32(2), DataValue::Int32(1)]);

        let mut plan_arena = ExecMetaArena::new(plan_arena);
        let input = exec.right_input.take().unwrap();
        assert_eq!(
            exec.mark_value(input, &mut arena, &mut plan_arena, &left_tuple)?
                .1,
            DataValue::Boolean(true)
        );
        assert_eq!(
            exec.runtime_probe_for(Some(DataValue::Int32(2))),
            Some(RuntimeIndexProbe::Eq(DataValue::Int32(2)))
        );

        Ok(())
    }

    #[test]
    fn mark_in_apply_sets_eq_runtime_probe_for_non_null_value() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values_with_schema(
            &mut plan_arena,
            vec![("left_c1", LogicalType::Integer)],
            vec![],
        );
        let mut right = build_values_with_schema(
            &mut plan_arena,
            vec![("right_c1", LogicalType::Integer)],
            vec![vec![DataValue::Int32(2)]],
        );
        let left_value_column = left.output_schema(&mut plan_arena)[0];
        let right_value_column = right.output_schema(&mut plan_arena)[0];
        let predicate =
            build_equality_predicate(&mut plan_arena, left_value_column, 0, right_value_column, 1)?;
        let mut op =
            MarkApplyOperator::new_in(build_marker_column(&mut plan_arena), vec![predicate]);
        let probe =
            plan_arena.alloc_expression(ScalarExpression::column_expr(left_value_column, 0));
        op.set_parameterized_probe(Some(probe));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            &transaction,
        );

        let mut exec: MarkApply = MarkApply {
            op: &op,
            right_input: Some(RightInput::Rebuild {
                plan: &right,
                pos: arena.nodes.position(),
            }),
            left_input: 0,
            join_input: None,
        };
        let left_tuple = Tuple::new(None, vec![DataValue::Int32(2)]);

        let mut plan_arena = ExecMetaArena::new(plan_arena);
        let input = exec.right_input.take().unwrap();
        assert_eq!(
            exec.mark_value(input, &mut arena, &mut plan_arena, &left_tuple)?
                .1,
            DataValue::Boolean(true)
        );
        assert_eq!(
            exec.runtime_probe_for(Some(DataValue::Int32(2))),
            Some(RuntimeIndexProbe::Eq(DataValue::Int32(2)))
        );

        Ok(())
    }

    #[test]
    fn mark_in_apply_sets_scope_runtime_probe_for_null_value() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values_with_schema(
            &mut plan_arena,
            vec![("left_c1", LogicalType::Integer)],
            vec![],
        );
        let mut right = build_values_with_schema(
            &mut plan_arena,
            vec![("right_c1", LogicalType::Integer)],
            vec![vec![DataValue::Null], vec![DataValue::Int32(2)]],
        );
        let left_value_column = left.output_schema(&mut plan_arena)[0];
        let right_value_column = right.output_schema(&mut plan_arena)[0];
        let predicate =
            build_equality_predicate(&mut plan_arena, left_value_column, 0, right_value_column, 1)?;
        let mut op =
            MarkApplyOperator::new_in(build_marker_column(&mut plan_arena), vec![predicate]);
        op.set_parameterized_probe(Some(
            plan_arena.alloc_expression(ScalarExpression::column_expr(left_value_column, 0)),
        ));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let mut arena = ExecArena::with_capacity(0);
        arena.init_context(
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            &transaction,
        );

        let mut exec: MarkApply = MarkApply {
            op: &op,
            right_input: Some(RightInput::Rebuild {
                plan: &right,
                pos: arena.nodes.position(),
            }),
            left_input: 0,
            join_input: None,
        };
        let left_tuple = Tuple::new(None, vec![DataValue::Null]);

        let mut plan_arena = ExecMetaArena::new(plan_arena);
        let input = exec.right_input.take().unwrap();
        assert_eq!(
            exec.mark_value(input, &mut arena, &mut plan_arena, &left_tuple)?
                .1,
            DataValue::Null
        );
        assert_eq!(
            exec.runtime_probe_for(None),
            Some(RuntimeIndexProbe::Scope {
                min: std::collections::Bound::Unbounded,
                max: std::collections::Bound::Unbounded,
            })
        );

        Ok(())
    }

    #[test]
    fn mark_in_apply_appends_boolean_match_column() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values(
            &mut plan_arena,
            "right_c1",
            vec![vec![DataValue::Int32(2)], vec![DataValue::Int32(3)]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_column = right.output_schema(&mut plan_arena)[0];

        let predicate = build_equality_predicate(&mut plan_arena, left_column, 0, right_column, 1)?;

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply>(
            (
                &MarkApplyOperator::new_in(build_marker_column(&mut plan_arena), vec![predicate]),
                &left,
                &right,
            ),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        ))?;

        assert_eq!(
            tuples
                .into_iter()
                .flat_map(|tuple| tuple.values)
                .collect::<Vec<_>>(),
            vec![
                DataValue::Int32(1),
                DataValue::Boolean(false),
                DataValue::Int32(2),
                DataValue::Boolean(true),
            ]
        );

        Ok(())
    }

    #[test]
    fn quantified_apply_materializes_once_and_replays_after_short_circuit(
    ) -> Result<(), DatabaseError> {
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let cache = crate::execution::empty_context(&table_cache, &view_cache, &meta_cache);
        // Exercise both a single segment and the default 1024-row spill boundary.
        for (quantifier, repeated_rows) in [
            (MarkApplyQuantifier::Any, 1),
            (MarkApplyQuantifier::All, 1),
            (MarkApplyQuantifier::Any, 1025),
            (MarkApplyQuantifier::All, 1025),
        ] {
            let table_arena = crate::planner::TableArenaCell::default();
            let mut meta = crate::planner::PlanArena::new(&table_arena);
            let mut left = build_values(&mut meta, "left_key", vec![vec![DataValue::Int32(2)]; 8]);
            let mut right_rows = vec![vec![DataValue::Int32(2)]; repeated_rows];
            right_rows.push(vec![DataValue::Null]);
            let mut right = build_values(&mut meta, "right_key", right_rows);
            let left_column = left.output_schema(&mut meta)[0];
            let right_column = right.output_schema(&mut meta)[0];
            let predicate = build_equality_predicate(&mut meta, left_column, 0, right_column, 1)?;
            let mut op = MarkApplyOperator::new_quantified(
                quantifier,
                build_marker_column(&mut meta),
                vec![predicate],
            );
            op.materialize = true;
            let empty = build_values(&mut meta, "empty", vec![]);
            let mut arena = ExecArena::with_capacity(0);
            arena.init_context(cache, &transaction);
            let left_input = build_read(&mut arena, &mut meta, &left, cache, &transaction);
            let pos = arena.nodes.position();
            build_read(&mut arena, &mut meta, &right, cache, &transaction);
            let mut exec = MarkApply {
                op: &op,
                right_input: Some(RightInput::Materialize { plan: &right, pos }),
                left_input,
                join_input: None,
            };
            let mut meta = ExecMetaArena::new(meta);
            for (index, value) in [
                DataValue::Int32(2),
                DataValue::Int32(1),
                DataValue::Null,
                DataValue::Int32(2),
            ]
            .into_iter()
            .enumerate()
            {
                let expected = match (quantifier, &value) {
                    (MarkApplyQuantifier::Any, DataValue::Int32(2)) => DataValue::Boolean(true),
                    (MarkApplyQuantifier::All, DataValue::Int32(1)) => DataValue::Boolean(false),
                    _ => DataValue::Null,
                };
                let tuple = Tuple::new(None, vec![value]);
                let input = exec.right_input.take().unwrap();
                let (input, value) = exec.mark_value(input, &mut arena, &mut meta, &tuple)?;
                exec.right_input = Some(input);
                assert_eq!(value, expected);
                if index == 0 && matches!(quantifier, MarkApplyQuantifier::Any) {
                    let Some(RightInput::Materializing { rows, .. }) = &exec.right_input else {
                        panic!("first match must pause the RHS")
                    };
                    #[cfg(feature = "spill")]
                    {
                        let mut cached = 0;
                        rows.replay::<()>(|_| {
                            cached += 1;
                            Ok(None)
                        })?;
                        assert_eq!(cached, 1);
                    }
                    #[cfg(not(feature = "spill"))]
                    assert_eq!(rows.len(), 1);
                } else {
                    assert!(matches!(
                        exec.right_input,
                        Some(RightInput::Materialized(_))
                    ));
                }
            }
            // An empty RHS completes immediately, including for a NULL left value.
            exec.right_input = Some(RightInput::Materialize { plan: &empty, pos });
            let input = exec.right_input.take().unwrap();
            let (input, value) = exec.mark_value(
                input,
                &mut arena,
                &mut meta,
                &Tuple::new(None, vec![DataValue::Null]),
            )?;
            exec.right_input = Some(input);
            assert_eq!(
                value,
                DataValue::Boolean(matches!(quantifier, MarkApplyQuantifier::All))
            );
            assert!(matches!(
                exec.right_input,
                Some(RightInput::Materialized(_))
            ));
        }
        Ok(())
    }

    #[test]
    fn paused_rhs_survives_left_subquery_rebuilds() -> Result<(), DatabaseError> {
        let tables = crate::planner::TableArenaCell::default();
        let mut meta = crate::planner::PlanArena::new(&tables);
        let mut left = build_values(
            &mut meta,
            "left_key",
            vec![
                vec![DataValue::Int32(1)],
                vec![DataValue::Int32(2)],
                vec![DataValue::Int32(99)],
                vec![DataValue::Int32(1)],
            ],
        );
        let mut lookup = build_values(
            &mut meta,
            "lookup_key",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let left_column = left.output_schema(&mut meta)[0];
        let lookup_column = lookup.output_schema(&mut meta)[0];
        let predicate = build_equality_predicate(&mut meta, left_column, 0, lookup_column, 1)?;
        let marker = build_marker_column(&mut meta);
        let left = MarkApplyOperator::build_exists(left, lookup, marker, vec![predicate]);
        let mut right = build_values(
            &mut meta,
            "right_key",
            vec![
                vec![DataValue::Int32(1)],
                vec![DataValue::Int32(2)],
                vec![DataValue::Int32(3)],
            ],
        );
        let right_column = right.output_schema(&mut meta)[0];
        let predicate = build_equality_predicate(&mut meta, left_column, 0, right_column, 2)?;
        let marker = build_marker_column(&mut meta);
        let mut op =
            MarkApplyOperator::new_quantified(MarkApplyQuantifier::Any, marker, vec![predicate]);
        op.materialize = true;
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply>(
            (&op, &left, &right),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            meta,
            &transaction,
        ))?;
        assert_eq!(
            tuples.into_iter().map(|row| row.values).collect::<Vec<_>>(),
            vec![
                vec![
                    DataValue::Int32(1),
                    DataValue::Boolean(true),
                    DataValue::Boolean(true)
                ],
                vec![
                    DataValue::Int32(2),
                    DataValue::Boolean(true),
                    DataValue::Boolean(true)
                ],
                vec![
                    DataValue::Int32(99),
                    DataValue::Boolean(false),
                    DataValue::Boolean(false)
                ],
                vec![
                    DataValue::Int32(1),
                    DataValue::Boolean(true),
                    DataValue::Boolean(true)
                ],
            ]
        );
        Ok(())
    }

    #[test]
    fn mark_in_apply_treats_null_predicate_as_not_matched() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values(
            &mut plan_arena,
            "right_c1",
            vec![vec![DataValue::Null], vec![DataValue::Int32(2)]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_column = right.output_schema(&mut plan_arena)[0];

        let predicate = build_equality_predicate(&mut plan_arena, left_column, 0, right_column, 1)?;

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply>(
            (
                &MarkApplyOperator::new_in(build_marker_column(&mut plan_arena), vec![predicate]),
                &left,
                &right,
            ),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        ))?;

        assert_eq!(
            tuples
                .into_iter()
                .flat_map(|tuple| tuple.values)
                .collect::<Vec<_>>(),
            vec![
                DataValue::Int32(1),
                DataValue::Null,
                DataValue::Int32(2),
                DataValue::Boolean(true),
            ]
        );

        Ok(())
    }

    #[test]
    fn mark_in_apply_ignores_null_correlated_predicate_rows() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values_with_schema(
            &mut plan_arena,
            vec![
                ("right_c1", LogicalType::Integer),
                ("right_flag", LogicalType::Integer),
            ],
            vec![vec![DataValue::Int32(1), DataValue::Null]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_schema = right.output_schema(&mut plan_arena).clone();
        let right_value_column = right_schema[0];
        let right_flag_column = right_schema[1];

        let probe_predicate =
            build_equality_predicate(&mut plan_arena, left_column, 0, right_value_column, 1)?;
        let correlated_left =
            plan_arena.alloc_expression(ScalarExpression::column_expr(right_flag_column, 2));
        let correlated_right =
            plan_arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(1)));
        let correlated_predicate = plan_arena.alloc_expression(ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: correlated_left,
            right_expr: correlated_right,
            evaluator: Some(binary_create(
                std::borrow::Cow::Owned(LogicalType::Integer),
                BinaryOperator::Eq,
            )?),
            ty: LogicalType::Boolean,
        });

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply>(
            (
                &MarkApplyOperator::new_in(
                    build_marker_column(&mut plan_arena),
                    vec![probe_predicate, correlated_predicate],
                ),
                &left,
                &right,
            ),
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &transaction,
        ))?;

        assert_eq!(
            tuples
                .into_iter()
                .flat_map(|tuple| tuple.values)
                .collect::<Vec<_>>(),
            vec![
                DataValue::Int32(1),
                DataValue::Boolean(false),
                DataValue::Int32(2),
                DataValue::Boolean(false),
            ]
        );

        Ok(())
    }
}
// GRCOV_EXCL_STOP
