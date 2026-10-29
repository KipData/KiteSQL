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
use crate::execution::dql::join::predicates_matched;
use crate::execution::runs::{Runs, RunsBuilder, RunsCursor};
#[cfg(feature = "spill")]
use crate::execution::spill::runs::SpillRuns;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::expression::{BinaryOperator, ScalarExpression, TypeCast};
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier};
use crate::planner::{ExecMetaArena, ExprRef, LogicalPlan, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::evaluator::BinaryEvaluatorRef;
use crate::types::index::RuntimeIndexProbe;
use crate::types::tuple::{SplitTupleRef, TupleImpl};
use crate::types::value::DataValue;
use std::borrow::Cow;
use std::cmp::Ordering;

#[cfg(feature = "spill")]
type KeysBuilder = SpillRuns;
#[cfg(not(feature = "spill"))]
type KeysBuilder = crate::execution::runs::SortedVec;

// The binder coerces both comparison sides to one type, so non-NULL keys of
// one apply share a variant and are totally ordered.
fn compare_keys(left: &DataValue, right: &DataValue) -> Ordering {
    left.partial_cmp(right).unwrap_or(Ordering::Equal)
}

/// Sorted non-NULL RHS comparison keys; NULL keys only decide UNKNOWN results.
struct MaterializedKeys {
    runs: <KeysBuilder as RunsBuilder>::Runs,
    is_empty: bool,
    has_null: bool,
    // Spilled reads size their buffer to one key when the key type is fixed-width.
    fixed_width: Option<usize>,
}

// TODO: range ANY/ALL only need the RHS extrema rather than every key.
enum RightInput {
    Streaming { root: ExecId },
    Materialize { root: ExecId },
    Materialized(MaterializedKeys),
}

impl RightInput {
    fn reset<'a, T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        probe: Option<RuntimeIndexProbe>,
    ) -> Result<ExecId, DatabaseError> {
        let (Self::Streaming { root } | Self::Materialize { root }) = self else {
            unreachable!("materialized input looks up its keys")
        };
        if let Some(probe) = probe {
            arena.push_runtime_probe(probe);
        }
        arena.reset(*root, plan_arena)?;
        Ok(*root)
    }

    // Return the first terminal predicate result, or None after exhausting the RHS.
    fn scan<'a, T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        probe: Option<RuntimeIndexProbe>,
        mut evaluate: impl FnMut(
            &TupleImpl,
            &ExecMetaArena<A>,
        ) -> Result<Option<DataValue>, DatabaseError>,
    ) -> Result<Option<DataValue>, DatabaseError> {
        let root = self.reset(arena, plan_arena, probe)?;
        while arena.next_tuple(root, plan_arena)? {
            if let Some(value) = evaluate(arena.result_tuple(), plan_arena)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

pub struct MarkApply<'a> {
    op: &'a MarkApplyOperator,
    left_input: ExecId,
    right_root: ExecId,
    // Temporarily None while scan owns and transitions the right input.
    right_input: Option<RightInput>,
}

impl<'a, T: StorageTransaction + 'a> ReadExecutor<'a, T> for MarkApply<'a> {
    type Input = (&'a MarkApplyOperator, &'a LogicalPlan, &'a LogicalPlan);

    fn into_executor(
        (op, left_input, right_input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let left_input = build_read(arena, plan_arena, left_input, cache, transaction);
        let right_root = build_read(arena, plan_arena, right_input, cache, transaction);
        let right_input = if op.materialize {
            RightInput::Materialize { root: right_root }
        } else {
            RightInput::Streaming { root: right_root }
        };
        arena.push(ExecNode::MarkApply(Box::new(Self {
            op,
            left_input,
            right_root,
            right_input: Some(right_input),
        })))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for MarkApply<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        // Materialization already makes the RHS reusable across left rows.
        // Keep both complete caches and paused prefixes; only reset the left input.
        if !self.op.materialize || self.right_input.is_none() {
            self.right_input = Some(if self.op.materialize {
                RightInput::Materialize {
                    root: self.right_root,
                }
            } else {
                RightInput::Streaming {
                    root: self.right_root,
                }
            });
        }
        arena.reset(self.left_input, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let Some(input) = self.right_input.take() else {
            arena.finish();
            return Ok(());
        };
        if !arena.next_tuple(self.left_input, plan_arena)? {
            // Return the temporarily owned cache so reset can reuse it after EOF.
            if self.op.materialize {
                self.right_input = Some(input);
            }
            arena.finish();
            return Ok(());
        }
        let mut left = arena.take_tuple();
        let (input, marker) = match self.op.guard {
            Some(guard)
                if *plan_arena.expression(guard).eval(plan_arena, Some(&left))?
                    == DataValue::Boolean(false) =>
            {
                (input, DataValue::Null)
            }
            _ => self.mark_value(input, arena, plan_arena, &left)?,
        };
        self.right_input = Some(input);
        left.push(marker);
        arena.produce(left);
        Ok(())
    }
}

impl<'a> MarkApply<'a> {
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
        left_tuple: &TupleImpl,
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

    fn mark_value<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &self,
        mut input: RightInput,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        left_tuple: &TupleImpl,
    ) -> Result<(RightInput, DataValue), DatabaseError> {
        let probe = self.parameterized_probe_value(left_tuple, plan_arena)?;
        let value = match self.op.kind {
            MarkApplyKind::Exists => {
                let predicates = self.op.predicates();
                let runtime_probe = self.runtime_probe_for(probe);
                input
                    .scan(arena, plan_arena, runtime_probe, |right, meta| {
                        Ok(predicates_matched(predicates, left_tuple, right, meta)?
                            .then_some(DataValue::Boolean(true)))
                    })?
                    .unwrap_or(DataValue::Boolean(false))
            }
            MarkApplyKind::Quantified(quantifier) => {
                if let RightInput::Materialize { root } = input {
                    input = RightInput::Materialized(
                        self.materialize_keys(root, arena, plan_arena, left_tuple)?,
                    );
                }
                match &input {
                    RightInput::Materialized(keys) => {
                        self.lookup_keys(keys, quantifier, plan_arena, left_tuple)?
                    }
                    _ => self.scan_quantified_right_input(
                        &input, arena, plan_arena, probe, quantifier, left_tuple,
                    )?,
                }
            }
        };
        Ok((input, value))
    }

    fn quantified_comparison<'p>(
        predicates: &[ExprRef],
        plan_arena: &'p (dyn MetaArena + '_),
    ) -> Result<(BinaryOperator, ExprRef, ExprRef, &'p BinaryEvaluatorRef), DatabaseError> {
        let [predicate] = predicates else {
            return Err(DatabaseError::InvalidType);
        };
        let ScalarExpression::Binary {
            op,
            left_expr,
            right_expr,
            evaluator,
            ..
        } = plan_arena.expression(*predicate)
        else {
            return Err(DatabaseError::InvalidType);
        };
        let evaluator = evaluator.as_ref().ok_or(DatabaseError::EvaluatorNotFound)?;
        Ok((*op, *left_expr, *right_expr, evaluator))
    }

    fn materialize_keys<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &self,
        root: ExecId,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        left_tuple: &TupleImpl,
    ) -> Result<MaterializedKeys, DatabaseError> {
        let (_, _, right_expr, _) = Self::quantified_comparison(self.op.predicates(), plan_arena)?;
        arena.reset(root, plan_arena)?;
        let mut builder = KeysBuilder::new(compare_keys);
        let (mut is_empty, mut has_null) = (true, false);
        while arena.next_tuple(root, plan_arena)? {
            // RHS positions follow the outer row, so evaluate against the joined layout.
            let values = SplitTupleRef::new(left_tuple, arena.result_tuple());
            let key = plan_arena
                .expression(right_expr)
                .eval(plan_arena, Some(&values))?
                .into_owned();
            is_empty = false;
            if key.is_null() {
                has_null = true;
            } else {
                builder.push(key)?;
            }
        }
        Ok(MaterializedKeys {
            runs: builder.finish()?,
            is_empty,
            has_null,
            fixed_width: plan_arena
                .expression(right_expr)
                .return_type(plan_arena)
                .fixed_encoded_len(),
        })
    }

    fn lookup_keys(
        &self,
        keys: &MaterializedKeys,
        quantifier: MarkApplyQuantifier,
        plan_arena: &(dyn MetaArena + '_),
        left_tuple: &TupleImpl,
    ) -> Result<DataValue, DatabaseError> {
        let any = matches!(quantifier, MarkApplyQuantifier::Any);
        if keys.is_empty {
            return Ok(DataValue::Boolean(!any));
        }
        let (op, left_expr, _, evaluator) =
            Self::quantified_comparison(self.op.predicates(), plan_arena)?;
        let probe = plan_arena
            .expression(left_expr)
            .eval(plan_arena, Some(left_tuple))?;
        let mut cursor = keys.runs.cursor(keys.fixed_width)?;
        let mut saw_null = false;
        // `= ANY` / `<> ALL` reduce to membership. Tuple probes compare NULL
        // fields three-valued, which membership cannot express.
        if matches!(
            (op, quantifier),
            (BinaryOperator::Eq, MarkApplyQuantifier::Any)
                | (BinaryOperator::NotEq, MarkApplyQuantifier::All)
        ) && !matches!(probe.as_ref(), DataValue::Tuple(_))
        {
            if probe.is_null() {
                return Ok(DataValue::Null);
            }
            if cursor.contains_by(|key| Ok(compare_keys(key, &probe)))? {
                return Ok(DataValue::Boolean(any));
            }
        } else if let Some(value) = cursor.visit_all(|key| {
            match evaluator.binary_eval(Cow::Borrowed(probe.as_ref()), key)? {
                DataValue::Boolean(value) if value == any => Ok(Some(value)),
                DataValue::Boolean(_) => Ok(None),
                DataValue::Null => {
                    saw_null = true;
                    Ok(None)
                }
                _ => Err(DatabaseError::InvalidType),
            }
        })? {
            return Ok(DataValue::Boolean(value));
        }
        // No decisive key: a NULL key or comparison makes the result UNKNOWN.
        Ok(if keys.has_null || saw_null {
            DataValue::Null
        } else {
            DataValue::Boolean(!any)
        })
    }

    fn scan_quantified_right_input<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &self,
        input: &RightInput,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
        probe: Option<DataValue>,
        quantifier: MarkApplyQuantifier,
        left_tuple: &TupleImpl,
    ) -> Result<DataValue, DatabaseError> {
        let mut saw_null = false;
        let predicates = self.op.predicates();
        // One fold across all probed ranges, so the mark is the quantifier over their union.
        for runtime_probe in self.quantified_probes(probe) {
            let value = input.scan(arena, plan_arena, runtime_probe, |right, meta| {
                match Self::quantified_row_value(predicates, left_tuple, right, meta)? {
                    Some(DataValue::Boolean(true))
                        if matches!(quantifier, MarkApplyQuantifier::Any) =>
                    {
                        return Ok(Some(DataValue::Boolean(true)));
                    }
                    Some(DataValue::Boolean(false))
                        if matches!(quantifier, MarkApplyQuantifier::All) =>
                    {
                        return Ok(Some(DataValue::Boolean(false)));
                    }
                    Some(DataValue::Null) => saw_null = true,
                    _ => {}
                }
                Ok(None)
            })?;
            if let Some(value) = value {
                return Ok(value);
            }
        }
        Ok(if saw_null {
            DataValue::Null
        } else {
            DataValue::Boolean(matches!(quantifier, MarkApplyQuantifier::All))
        })
    }

    fn quantified_probes(
        &self,
        probe: Option<DataValue>,
    ) -> impl Iterator<Item = Option<RuntimeIndexProbe>> {
        let (first, null_keys) = match probe.filter(|value| !value.is_null()) {
            Some(key) => (
                Some(RuntimeIndexProbe::Eq(key)),
                Some(Some(RuntimeIndexProbe::Eq(DataValue::Null))),
            ),
            None => (self.runtime_probe_for(None), None),
        };
        std::iter::once(first).chain(null_keys)
    }
    fn quantified_row_value(
        predicates: &[ExprRef],
        left_tuple: &TupleImpl,
        right_tuple: &TupleImpl,
        plan_arena: &(dyn MetaArena + '_),
    ) -> Result<Option<DataValue>, DatabaseError> {
        let (comparison, correlated_predicates) =
            predicates.split_first().ok_or(DatabaseError::InvalidType)?;
        if !predicates_matched(correlated_predicates, left_tuple, right_tuple, plan_arena)? {
            return Ok(None);
        }
        let values = SplitTupleRef::new(left_tuple, right_tuple);
        match plan_arena
            .expression(*comparison)
            .eval(plan_arena, Some(&values))?
            .into_owned()
        {
            value @ (DataValue::Boolean(_) | DataValue::Null) => Ok(Some(value)),
            _ => Err(DatabaseError::InvalidType),
        }
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
    use crate::types::evaluator::{binary_create, cast_create};
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
            tuples.push(arena.take_tuple());
            assert_eq!(arena.nodes.items.len(), count);
            assert_eq!(arena.nodes.items.as_ptr(), address);
        }

        assert_eq!(
            tuples
                .iter()
                .flat_map(TupleImpl::iter)
                .cloned()
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
                .flat_map(|mut tuple| tuple.take_values())
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

        let cache = arena.context();
        let right_root = build_read(&mut arena, &mut plan_arena, &right, cache, &transaction);
        let mut exec: MarkApply = MarkApply {
            right_root,
            op: &op,
            right_input: Some(RightInput::Streaming { root: right_root }),
            left_input: 0,
        };
        let left_tuple: TupleImpl =
            Tuple::new(None, vec![DataValue::Int32(2), DataValue::Int32(1)]).into();

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

        let cache = arena.context();
        let right_root = build_read(&mut arena, &mut plan_arena, &right, cache, &transaction);
        let mut exec: MarkApply = MarkApply {
            right_root,
            op: &op,
            right_input: Some(RightInput::Streaming { root: right_root }),
            left_input: 0,
        };
        let left_tuple: TupleImpl = Tuple::new(None, vec![DataValue::Int32(2)]).into();

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

        let cache = arena.context();
        let right_root = build_read(&mut arena, &mut plan_arena, &right, cache, &transaction);
        let mut exec: MarkApply = MarkApply {
            right_root,
            op: &op,
            right_input: Some(RightInput::Streaming { root: right_root }),
            left_input: 0,
        };
        let left_tuple: TupleImpl = Tuple::new(None, vec![DataValue::Null]).into();

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
                .flat_map(|mut tuple| tuple.take_values())
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
    fn guard_false_skips_failing_rhs_but_true_and_null_evaluate_it() -> Result<(), DatabaseError> {
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let cache = crate::execution::empty_context(&table_cache, &view_cache, &meta_cache);
        for kind in [
            MarkApplyKind::Exists,
            MarkApplyKind::Quantified(MarkApplyQuantifier::Any),
            MarkApplyKind::Quantified(MarkApplyQuantifier::All),
        ] {
            for materialize in [false, true] {
                for (flag, use_guard) in [
                    (DataValue::Boolean(false), true),
                    (DataValue::Boolean(true), true),
                    (DataValue::Null, true),
                    (DataValue::Boolean(false), false),
                ] {
                    let tables = crate::planner::TableArenaCell::default();
                    let mut meta = crate::planner::PlanArena::new(&tables);
                    let mut left = build_values_with_schema(
                        &mut meta,
                        vec![
                            ("key", LogicalType::Integer),
                            ("flag", LogicalType::Boolean),
                        ],
                        vec![vec![DataValue::Int32(1), flag.clone()]],
                    );
                    let mut right =
                        build_values(&mut meta, "right_key", vec![vec![DataValue::Int32(1)]]);
                    let text = DataValue::from("not an integer".to_string());
                    let evaluator = cast_create(&text.logical_type(), &LogicalType::Integer)?;
                    let expr = meta.alloc_expression(text.into());
                    let failing = meta.alloc_expression(ScalarExpression::TypeCast {
                        expr,
                        ty: LogicalType::Integer,
                        evaluator: Some(evaluator),
                    });
                    let Operator::Values(values) = &mut right.operator else {
                        unreachable!()
                    };
                    values.rows[0] = failing;
                    let schema = left.output_schema(&mut meta);
                    let left_column = schema[0];
                    let flag_column = schema[1];
                    let right_column = right.output_schema(&mut meta)[0];
                    let predicate =
                        build_equality_predicate(&mut meta, left_column, 0, right_column, 2)?;
                    let mut op =
                        MarkApplyOperator::new_in(build_marker_column(&mut meta), vec![predicate]);
                    op.kind = kind;
                    op.materialize = materialize && !matches!(kind, MarkApplyKind::Exists);
                    if use_guard {
                        op.guard = Some(
                            meta.alloc_expression(ScalarExpression::column_expr(flag_column, 1)),
                        );
                    }
                    let mut arena = ExecArena::with_capacity(0);
                    arena.init_context(cache, &transaction);
                    let left_input = build_read(&mut arena, &mut meta, &left, cache, &transaction);
                    let right_root = build_read(&mut arena, &mut meta, &right, cache, &transaction);
                    let right_input = if op.materialize {
                        RightInput::Materialize { root: right_root }
                    } else {
                        RightInput::Streaming { root: right_root }
                    };
                    let mut exec = MarkApply {
                        right_root,
                        op: &op,
                        left_input,
                        right_input: Some(right_input),
                    };
                    let mut meta = ExecMetaArena::new(meta);
                    let result = exec.next_tuple(&mut arena, &mut meta);
                    if use_guard && flag == DataValue::Boolean(false) {
                        result?;
                        assert_eq!(
                            *arena.result_tuple(),
                            vec![DataValue::Int32(1), flag, DataValue::Null]
                        );
                        assert!(matches!(
                            exec.right_input,
                            Some(RightInput::Streaming { .. } | RightInput::Materialize { .. })
                        ));
                        exec.next_tuple(&mut arena, &mut meta)?;
                        assert_eq!(exec.right_input.is_none(), !op.materialize);
                    } else {
                        assert!(matches!(result, Err(DatabaseError::ParseInt(_))));
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn guard_skips_preserve_materialized_rhs() -> Result<(), DatabaseError> {
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let cache = crate::execution::empty_context(&table_cache, &view_cache, &meta_cache);
        let cases = [
            (99, DataValue::Boolean(false), DataValue::Null),
            (1, DataValue::Boolean(true), DataValue::Boolean(true)),
            (99, DataValue::Boolean(false), DataValue::Null),
            (2, DataValue::Boolean(true), DataValue::Boolean(true)),
            (99, DataValue::Null, DataValue::Null),
            (2, DataValue::Boolean(false), DataValue::Null),
            (1, DataValue::Boolean(true), DataValue::Boolean(true)),
        ];
        for materialize in [false, true] {
            let tables = crate::planner::TableArenaCell::default();
            let mut meta = crate::planner::PlanArena::new(&tables);
            let mut left = build_values_with_schema(
                &mut meta,
                vec![
                    ("key", LogicalType::Integer),
                    ("flag", LogicalType::Boolean),
                ],
                cases
                    .iter()
                    .map(|(key, flag, _)| vec![DataValue::Int32(*key), flag.clone()])
                    .collect(),
            );
            let mut right = build_values(
                &mut meta,
                "right_key",
                vec![
                    vec![DataValue::Int32(1)],
                    vec![DataValue::Int32(2)],
                    vec![DataValue::Null],
                ],
            );
            let schema = left.output_schema(&mut meta);
            let left_column = schema[0];
            let flag_column = schema[1];
            let right_column = right.output_schema(&mut meta)[0];
            let predicate = build_equality_predicate(&mut meta, left_column, 0, right_column, 2)?;
            let mut op = MarkApplyOperator::new_in(build_marker_column(&mut meta), vec![predicate]);
            op.materialize = materialize;
            op.guard = Some(meta.alloc_expression(ScalarExpression::column_expr(flag_column, 1)));
            let mut arena = ExecArena::with_capacity(0);
            arena.init_context(cache, &transaction);
            let left_input = build_read(&mut arena, &mut meta, &left, cache, &transaction);
            let right_root = build_read(&mut arena, &mut meta, &right, cache, &transaction);
            let right_input = if materialize {
                RightInput::Materialize { root: right_root }
            } else {
                RightInput::Streaming { root: right_root }
            };
            let mut exec = MarkApply {
                right_root,
                op: &op,
                left_input,
                right_input: Some(right_input),
            };
            let mut meta = ExecMetaArena::new(meta);
            for (index, (key, flag, marker)) in cases.iter().enumerate() {
                exec.next_tuple(&mut arena, &mut meta)?;
                assert_eq!(
                    *arena.result_tuple(),
                    vec![DataValue::Int32(*key), flag.clone(), marker.clone()]
                );
                if materialize {
                    if index == 0 {
                        assert!(matches!(
                            exec.right_input,
                            Some(RightInput::Materialize { .. })
                        ));
                    } else {
                        // The first unguarded row materializes every RHS key at once.
                        let Some(RightInput::Materialized(keys)) = &exec.right_input else {
                            panic!("guard must retain the materialized RHS")
                        };
                        assert!(!keys.is_empty && keys.has_null);
                    }
                } else {
                    assert!(matches!(
                        exec.right_input,
                        Some(RightInput::Streaming { .. })
                    ));
                }
            }
            exec.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(exec.right_input.is_none(), !materialize);
        }
        Ok(())
    }

    #[test]
    fn quantified_apply_materializes_once_and_reuses_keys_after_short_circuit(
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
            let right_root = build_read(&mut arena, &mut meta, &right, cache, &transaction);
            let mut exec = MarkApply {
                right_root,
                op: &op,
                right_input: Some(RightInput::Materialize { root: right_root }),
                left_input,
            };
            let mut meta = ExecMetaArena::new(meta);
            for value in [
                DataValue::Int32(2),
                DataValue::Int32(1),
                DataValue::Null,
                DataValue::Int32(2),
            ] {
                let expected = match (quantifier, &value) {
                    (MarkApplyQuantifier::Any, DataValue::Int32(2)) => DataValue::Boolean(true),
                    (MarkApplyQuantifier::All, DataValue::Int32(1)) => DataValue::Boolean(false),
                    _ => DataValue::Null,
                };
                let tuple: TupleImpl = Tuple::new(None, vec![value]).into();
                let input = exec.right_input.take().unwrap();
                let (input, value) = exec.mark_value(input, &mut arena, &mut meta, &tuple)?;
                exec.right_input = Some(input);
                assert_eq!(value, expected);
                // A short-circuited match still caches every RHS key.
                let Some(RightInput::Materialized(keys)) = &exec.right_input else {
                    panic!("the first lookup must materialize the RHS")
                };
                assert!(!keys.is_empty && keys.has_null);
                exec.reset(&mut arena, &mut meta)?;
                assert!(matches!(
                    exec.right_input,
                    Some(RightInput::Materialized(_))
                ));
            }
            // Exhausting the left must not discard the RHS cache either.
            while arena.next_tuple(exec.left_input, &mut meta)? {}
            exec.next_tuple(&mut arena, &mut meta)?;
            assert!(matches!(
                exec.right_input,
                Some(RightInput::Materialized(_))
            ));
            exec.reset(&mut arena, &mut meta)?;
            exec.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(
                *arena.result_tuple(),
                vec![
                    DataValue::Int32(2),
                    if matches!(quantifier, MarkApplyQuantifier::Any) {
                        DataValue::Boolean(true)
                    } else {
                        DataValue::Null
                    },
                ]
            );
            assert!(matches!(
                exec.right_input,
                Some(RightInput::Materialized(_))
            ));

            // An empty RHS completes immediately, including for a NULL left value.
            let cache = arena.context();
            exec.right_root = build_read(&mut arena, &mut meta, &empty, cache, &transaction);
            exec.right_input = Some(RightInput::Materialize {
                root: exec.right_root,
            });
            let input = exec.right_input.take().unwrap();
            let (input, value) = exec.mark_value(
                input,
                &mut arena,
                &mut meta,
                &TupleImpl::from(Tuple::new(None, vec![DataValue::Null])),
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
            exec.reset(&mut arena, &mut meta)?;
            assert!(matches!(
                exec.right_input,
                Some(RightInput::Materialized(_))
            ));
            exec.next_tuple(&mut arena, &mut meta)?;
            assert_eq!(
                arena.result_tuple().iter().last(),
                Some(&DataValue::Boolean(matches!(
                    quantifier,
                    MarkApplyQuantifier::All
                )))
            );
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
            tuples
                .into_iter()
                .map(|mut row| row.take_values())
                .collect::<Vec<_>>(),
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
                .flat_map(|mut tuple| tuple.take_values())
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
                .flat_map(|mut tuple| tuple.take_values())
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
