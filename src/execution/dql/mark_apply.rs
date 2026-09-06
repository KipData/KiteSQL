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
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier};
use crate::planner::LogicalPlan;
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

pub struct MarkApply<'a, T: Transaction + 'a> {
    op: MarkApplyOperator,
    right_input_plan: LogicalPlan,
    left_input: ExecId,
    // Retain a streaming inner input across next_tuple calls, not its result rows.
    join_input: Option<(Box<ExecArena<'a, T>>, ExecId, Tuple)>,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for MarkApply<'a, T> {
    type Input = (MarkApplyOperator, LogicalPlan, LogicalPlan);

    fn into_executor(
        (op, left_input, right_input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let left_input = build_read(arena, plan_arena, left_input, cache, transaction);
        arena.push(ExecNode::MarkApply(Self {
            op,
            right_input_plan: right_input,
            left_input,
            join_input: None,
        }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for MarkApply<'a, T> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if matches!(self.op.kind, MarkApplyKind::InnerJoin) {
            return self.next_join_tuple(arena, plan_arena);
        }
        if !arena.next_tuple(self.left_input, plan_arena)? {
            arena.finish();
            return Ok(());
        }

        let mut left_tuple = arena.result_tuple().clone();
        let marker = self.mark_value(arena, plan_arena, &left_tuple)?;

        left_tuple.values.push(marker);
        arena.produce_tuple(left_tuple);
        Ok(())
    }
}

impl<'a, T: Transaction + 'a> MarkApply<'a, T> {
    fn next_join_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            if let Some((inner, root, left)) = &mut self.join_input {
                while inner.next_tuple(*root, plan_arena)? {
                    let right = inner.result_tuple();
                    if Self::predicates_matched(self.op.predicates(), left, right, plan_arena)? {
                        let mut output = left.clone();
                        output.pk = output.pk.or_else(|| right.pk.clone());
                        output.values.extend(right.values.iter().cloned());
                        arena.produce_tuple(output);
                        return Ok(());
                    }
                }
            }
            if !arena.next_tuple(self.left_input, plan_arena)? {
                self.join_input = None;
                arena.finish();
                return Ok(());
            }
            let left: Tuple = arena.result_tuple().clone();
            let value = self.parameterized_probe_value(&left, plan_arena)?;
            let mut inner = self
                .join_input
                .take()
                .map(|(inner, _, _)| inner)
                .unwrap_or_else(|| Box::new(ExecArena::new()));
            inner.reset_for_rebuild();
            inner.init_context(arena.context(), arena.transaction());
            let root = self.build_right_input(&mut inner, plan_arena, value);
            self.join_input = Some((inner, root, left));
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

    fn build_right_input(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        param_value: Option<DataValue>,
    ) -> ExecId {
        if let Some(probe) = self.runtime_probe_for(param_value) {
            arena.push_runtime_probe(probe);
        }
        build_read(
            arena,
            plan_arena,
            self.right_input_plan.clone(),
            arena.context(),
            arena.transaction(),
        )
    }

    fn with_right_input<R>(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        param_value: Option<DataValue>,
        f: impl FnOnce(
            &mut ExecArena<'a, T>,
            &mut crate::planner::PlanArena<'a>,
            ExecId,
        ) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let depth_before = arena.runtime_probe_depth();
        let right_input = self.build_right_input(arena, plan_arena, param_value);
        let result = f(arena, plan_arena, right_input);

        let depth_after = arena.runtime_probe_depth();
        debug_assert!(
            depth_after == depth_before || depth_after == depth_before + 1,
            "parameterized right input should consume at most one runtime probe"
        );
        if depth_after > depth_before {
            let _ = arena.pop_runtime_probe();
        }

        result
    }

    fn parameterized_probe_value(
        &self,
        left_tuple: &Tuple,
        plan_arena: &crate::planner::PlanArena<'_>,
    ) -> Result<Option<DataValue>, DatabaseError> {
        self.op
            .parameterized_probe()
            .map(|probe| {
                plan_arena
                    .expression(*probe)
                    .eval(plan_arena, Some(left_tuple))
            })
            .transpose()
    }

    fn mark_value(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        left_tuple: &Tuple,
    ) -> Result<DataValue, DatabaseError> {
        match self.op.kind {
            MarkApplyKind::InnerJoin => unreachable!("inner join streams tuples"),
            MarkApplyKind::Exists => self.with_right_input(
                arena,
                plan_arena,
                self.parameterized_probe_value(left_tuple, plan_arena)?,
                |arena, plan_arena, right_input| {
                    while arena.next_tuple(right_input, plan_arena)? {
                        let right_tuple = arena.result_tuple();
                        if Self::predicates_matched(
                            self.op.predicates(),
                            left_tuple,
                            right_tuple,
                            plan_arena,
                        )? {
                            return Ok(DataValue::Boolean(true));
                        }
                    }

                    Ok(DataValue::Boolean(false))
                },
            ),
            MarkApplyKind::Quantified(MarkApplyQuantifier::Any) => {
                if let Some(probe_value) = self.parameterized_probe_value(left_tuple, plan_arena)? {
                    if !probe_value.is_null() {
                        if self.with_right_input(
                            arena,
                            plan_arena,
                            Some(probe_value),
                            |arena, plan_arena, right_input| {
                                while arena.next_tuple(right_input, plan_arena)? {
                                    let right_tuple = arena.result_tuple();
                                    if self.quantified_predicate_outcome(
                                        left_tuple,
                                        right_tuple,
                                        plan_arena,
                                    )? == QuantifiedPredicateOutcome::True
                                    {
                                        return Ok(true);
                                    }
                                }

                                Ok(false)
                            },
                        )? {
                            return Ok(DataValue::Boolean(true));
                        }

                        if self.with_right_input(
                            arena,
                            plan_arena,
                            Some(DataValue::Null),
                            |arena, plan_arena, right_input| {
                                while arena.next_tuple(right_input, plan_arena)? {
                                    let right_tuple = arena.result_tuple();
                                    if self.quantified_predicate_outcome(
                                        left_tuple,
                                        right_tuple,
                                        plan_arena,
                                    )? == QuantifiedPredicateOutcome::Null
                                    {
                                        return Ok(true);
                                    }
                                }

                                Ok(false)
                            },
                        )? {
                            return Ok(DataValue::Null);
                        }

                        return Ok(DataValue::Boolean(false));
                    }
                }

                self.with_right_input(arena, plan_arena, None, |arena, plan_arena, right_input| {
                    self.scan_quantified_right_input(
                        arena,
                        plan_arena,
                        right_input,
                        MarkApplyQuantifier::Any,
                        left_tuple,
                    )
                })
            }
            MarkApplyKind::Quantified(MarkApplyQuantifier::All) => {
                self.with_right_input(arena, plan_arena, None, |arena, plan_arena, right_input| {
                    self.scan_quantified_right_input(
                        arena,
                        plan_arena,
                        right_input,
                        MarkApplyQuantifier::All,
                        left_tuple,
                    )
                })
            }
        }
    }

    fn scan_quantified_right_input(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        right_input: ExecId,
        quantifier: MarkApplyQuantifier,
        left_tuple: &Tuple,
    ) -> Result<DataValue, DatabaseError> {
        let mut saw_null = false;

        while arena.next_tuple(right_input, plan_arena)? {
            let right_tuple = arena.result_tuple();
            match self.quantified_predicate_outcome(left_tuple, right_tuple, plan_arena)? {
                QuantifiedPredicateOutcome::True => {
                    if matches!(quantifier, MarkApplyQuantifier::Any) {
                        return Ok(DataValue::Boolean(true));
                    }
                }
                QuantifiedPredicateOutcome::False => {
                    if matches!(quantifier, MarkApplyQuantifier::All) {
                        return Ok(DataValue::Boolean(false));
                    }
                }
                QuantifiedPredicateOutcome::Null => saw_null = true,
                QuantifiedPredicateOutcome::Skip => {}
            }
        }

        if saw_null {
            Ok(DataValue::Null)
        } else {
            Ok(DataValue::Boolean(matches!(
                quantifier,
                MarkApplyQuantifier::All
            )))
        }
    }

    fn predicates_matched(
        predicates: &[crate::planner::ExprRef],
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        plan_arena: &crate::planner::PlanArena<'_>,
    ) -> Result<bool, DatabaseError> {
        let values = SplitTupleRef::new(left_tuple, right_tuple);

        for predicate in predicates {
            match plan_arena
                .expression(*predicate)
                .eval(plan_arena, Some(values))?
            {
                DataValue::Boolean(true) => {}
                DataValue::Boolean(false) | DataValue::Null => return Ok(false),
                _ => return Err(DatabaseError::InvalidType),
            }
        }

        Ok(true)
    }

    fn quantified_predicate_outcome(
        &self,
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        plan_arena: &crate::planner::PlanArena<'_>,
    ) -> Result<QuantifiedPredicateOutcome, DatabaseError> {
        match self.eval_predicates(left_tuple, right_tuple, plan_arena)? {
            Some(DataValue::Boolean(true)) => Ok(QuantifiedPredicateOutcome::True),
            Some(DataValue::Boolean(false)) => Ok(QuantifiedPredicateOutcome::False),
            Some(DataValue::Null) => Ok(QuantifiedPredicateOutcome::Null),
            None => Ok(QuantifiedPredicateOutcome::Skip),
            Some(_) => Err(DatabaseError::InvalidType),
        }
    }

    fn eval_predicates(
        &self,
        left_tuple: &Tuple,
        right_tuple: &Tuple,
        plan_arena: &crate::planner::PlanArena<'_>,
    ) -> Result<Option<DataValue>, DatabaseError> {
        let values = SplitTupleRef::new(left_tuple, right_tuple);
        // probe_predicate is in predicate, always first
        let (probe_predicate, correlated_predicates) = self
            .op
            .predicates()
            .split_first()
            .ok_or(DatabaseError::InvalidType)?;

        for predicate in correlated_predicates {
            match plan_arena
                .expression(*predicate)
                .eval(plan_arena, Some(values))?
            {
                DataValue::Boolean(true) => {}
                DataValue::Boolean(false) | DataValue::Null => return Ok(None),
                _ => return Err(DatabaseError::InvalidType),
            }
        }

        Ok(Some(
            plan_arena
                .expression(*probe_predicate)
                .eval(plan_arena, Some(values))?,
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::execution::{execute_input, try_collect, ExecArena};
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
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
            Operator::Values(ValuesOperator { rows, schema_ref }),
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
    fn inner_join_apply_emits_all_matches_and_reuses_inner_arena() -> Result<(), DatabaseError> {
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
        // Values supplies the inner rows directly; there is no IndexScan to consume a probe.
        op.set_parameterized_probe(None);
        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let cache = crate::execution::empty_context(&table_cache, &view_cache, &meta_cache);
        let mut arena = ExecArena::new();
        arena.init_context(cache, &transaction);
        let left_input = build_read(&mut arena, &mut plan_arena, left, cache, &transaction);
        let mut exec = MarkApply {
            op,
            right_input_plan: right,
            left_input,
            join_input: None,
        };
        let mut inner_address = None;
        for _ in 0..4 {
            exec.next_tuple(&mut arena, &mut plan_arena)?;
            assert_eq!(
                arena.result_tuple().values,
                vec![
                    DataValue::Int32(2),
                    DataValue::Int32(2),
                    DataValue::Boolean(true)
                ]
            );
            let (inner, _, _) = exec.join_input.as_ref().expect("active inner scan");
            let address = &**inner as *const _;
            assert_eq!(*inner_address.get_or_insert(address), address);
            assert_eq!(
                inner.nodes.len(),
                1,
                "inner executors must not accumulate per outer row"
            );
            assert_eq!(inner.runtime_probe_depth(), 0);
        }
        exec.next_tuple(&mut arena, &mut plan_arena)?;
        assert!(exec.join_input.is_none(), "all outer rows exhausted");
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
        let mut executor = execute_input::<_, MarkApply<_>>(
            (op, left, right),
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

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply<_>>(
            (
                MarkApplyOperator::new_exists(
                    build_marker_column(&mut plan_arena),
                    vec![predicate],
                ),
                left,
                right,
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

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, MarkApply<_>>(
            (
                MarkApplyOperator::new_exists(
                    build_marker_column(&mut plan_arena),
                    vec![predicate],
                ),
                left,
                right,
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
        let mut arena = ExecArena::new();
        arena.init_context(
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            &transaction,
        );

        let exec: MarkApply<crate::storage::rocksdb::RocksTransaction> = MarkApply {
            op,
            right_input_plan: right,
            left_input: 0,
            join_input: None,
        };
        let left_tuple = Tuple::new(None, vec![DataValue::Int32(2), DataValue::Int32(1)]);

        assert_eq!(
            exec.mark_value(&mut arena, &mut plan_arena, &left_tuple)?,
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
        let mut arena = ExecArena::new();
        arena.init_context(
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            &transaction,
        );

        let exec: MarkApply<crate::storage::rocksdb::RocksTransaction> = MarkApply {
            op,
            right_input_plan: right,
            left_input: 0,
            join_input: None,
        };
        let left_tuple = Tuple::new(None, vec![DataValue::Int32(2)]);

        assert_eq!(
            exec.mark_value(&mut arena, &mut plan_arena, &left_tuple)?,
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
        let mut arena = ExecArena::new();
        arena.init_context(
            crate::execution::empty_context(&table_cache, &view_cache, &meta_cache),
            &transaction,
        );

        let exec: MarkApply<crate::storage::rocksdb::RocksTransaction> = MarkApply {
            op,
            right_input_plan: right,
            left_input: 0,
            join_input: None,
        };
        let left_tuple = Tuple::new(None, vec![DataValue::Null]);

        assert_eq!(
            exec.mark_value(&mut arena, &mut plan_arena, &left_tuple)?,
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
        let tuples = try_collect(execute_input::<_, MarkApply<_>>(
            (
                MarkApplyOperator::new_in(build_marker_column(&mut plan_arena), vec![predicate]),
                left,
                right,
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
        let tuples = try_collect(execute_input::<_, MarkApply<_>>(
            (
                MarkApplyOperator::new_in(build_marker_column(&mut plan_arena), vec![predicate]),
                left,
                right,
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
        let tuples = try_collect(execute_input::<_, MarkApply<_>>(
            (
                MarkApplyOperator::new_in(
                    build_marker_column(&mut plan_arena),
                    vec![probe_predicate, correlated_predicate],
                ),
                left,
                right,
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
