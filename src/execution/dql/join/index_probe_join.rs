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
use crate::execution::{build_read, ExecArena, ExecNode, ExecutorNode, ReadExecutor};
use crate::planner::operator::index_probe_join::IndexProbeJoinOperator;
use crate::planner::{ExecMetaArena, LogicalPlan, MetaArena};
use crate::storage::StorageTransaction;
use crate::types::index::RuntimeIndexProbe;

pub struct IndexProbeJoin<'a> {
    op: &'a IndexProbeJoinOperator,
    left_input: ExecNode<'a>,
    right_input: ExecNode<'a>,
}

impl<'a, T: StorageTransaction + 'a> ReadExecutor<'a, T> for IndexProbeJoin<'a> {
    type Input = (&'a IndexProbeJoinOperator, &'a LogicalPlan, &'a LogicalPlan);

    fn into_executor(
        (op, left_input, right_input): Self::Input,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> ExecNode<'a> {
        let left_input = build_read(arena, plan_arena, left_input);
        let right_input = build_read(arena, plan_arena, right_input);
        ExecNode::IndexProbeJoin(Box::new(Self {
            op,
            left_input,
            right_input,
        }))
    }
}

impl<'a, T: StorageTransaction + 'a> ExecutorNode<'a, T> for IndexProbeJoin<'a> {
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.left_input.reset(arena, plan_arena)
    }

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        while self.left_input.next_tuple(arena, plan_arena)? {
            let mut left = arena.take_tuple();
            let key = plan_arena
                .expression(self.op.probe)
                .eval(plan_arena, Some(&left))?
                .into_owned();
            if key.has_null() {
                arena.release(left);
                continue;
            }
            arena.push_runtime_probe(RuntimeIndexProbe::Eq(key));
            self.right_input.reset(arena, plan_arena)?;
            if self.right_input.next_tuple(arena, plan_arena)?
                && predicates_matched(&self.op.predicates, &left, arena.result_tuple(), plan_arena)?
            {
                let mut right = arena.take_tuple();
                let pk = left.take_pk().or_else(|| right.take_pk());
                let joined = arena.concat(left, right, pk);
                arena.produce(joined);
                return Ok(());
            }
            arena.release(left);
        }
        arena.finish();
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::execution::{execute_input, ExecArena};
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::test::PlanArenaTestExt;
    use crate::planner::{Childrens, ExprRef, PlanArena, TableArenaCell};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{Storage, TableCache};
    use crate::types::evaluator::binary_create;
    use crate::types::value::DataValue;
    use crate::types::{CharLengthUnits, LogicalType};
    use std::borrow::Cow;
    use tempfile::TempDir;

    fn build_values(
        arena: &mut PlanArena,
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

    fn build_equality(
        arena: &mut PlanArena,
        left: (ColumnRef, usize),
        right: (ColumnRef, usize),
    ) -> Result<ExprRef, DatabaseError> {
        let left_expr = arena.alloc_expression(ScalarExpression::column_expr(left.0, left.1));
        let right_expr = arena.alloc_expression(ScalarExpression::column_expr(right.0, right.1));
        Ok(arena.alloc_expression(ScalarExpression::Binary {
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

    fn build_test_storage() -> Result<(TableCache, TempDir, RocksStorage), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        Ok((TableCache::default(), temp_dir, storage))
    }

    // Values ignores runtime probes, so it stands in for the unique lookup and the pushed
    // probes stay on the stack for inspection.
    #[test]
    fn index_probe_join_moves_single_match_and_reuses_right_slots() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            vec![
                ("left_key", LogicalType::Integer),
                (
                    "payload",
                    LogicalType::Varchar(None, CharLengthUnits::Characters),
                ),
            ],
            vec![
                vec![DataValue::Int32(2), DataValue::from("first".to_string())],
                vec![DataValue::Null, DataValue::from("null".to_string())],
                vec![DataValue::Int32(99), DataValue::from("missing".to_string())],
                vec![DataValue::Int32(2), DataValue::from("again".to_string())],
            ],
        );
        let mut right = build_values(
            &mut plan_arena,
            vec![
                ("right_key", LogicalType::Integer),
                ("flag", LogicalType::Boolean),
            ],
            vec![vec![DataValue::Int32(2), DataValue::Boolean(true)]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_schema = right.output_schema(&mut plan_arena).clone();
        let equality = build_equality(&mut plan_arena, (left_column, 0), (right_schema[0], 2))?;
        let residual =
            plan_arena.alloc_expression(ScalarExpression::column_expr(right_schema[1], 3));
        let probe = plan_arena.alloc_expression(ScalarExpression::column_expr(left_column, 0));
        let op = IndexProbeJoinOperator::new(probe, vec![equality, residual]);
        let (table_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let cache = &table_cache;
        let mut arena = ExecArena::new(cache, &transaction);
        let mut root = <IndexProbeJoin as ReadExecutor<_>>::into_executor(
            (&op, &left, &right),
            &mut arena,
            &mut plan_arena,
        );
        let mut plan_arena = ExecMetaArena::new(plan_arena);
        for payload in ["first", "again"] {
            assert!(root.next_tuple(&mut arena, &mut plan_arena)?);
            assert_eq!(
                arena.take_tuple(),
                vec![
                    DataValue::Int32(2),
                    DataValue::from(payload.to_string()),
                    DataValue::Int32(2),
                    DataValue::Boolean(true)
                ]
            );
        }
        assert!(!root.next_tuple(&mut arena, &mut plan_arena)?);
        // The NULL key is skipped without probing.
        assert_eq!(
            arena.runtime_probe_stack,
            [2, 99, 2].map(|key| RuntimeIndexProbe::Eq(DataValue::Int32(key)))
        );
        Ok(())
    }

    #[test]
    fn index_probe_join_skips_a_rejected_unique_match() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let mut left = build_values(
            &mut plan_arena,
            vec![("left_key", LogicalType::Integer)],
            vec![vec![DataValue::Int32(1)]],
        );
        let mut right = build_values(
            &mut plan_arena,
            vec![("flag", LogicalType::Boolean)],
            vec![vec![DataValue::Boolean(false)]],
        );
        let left_column = left.output_schema(&mut plan_arena)[0];
        let right_column = right.output_schema(&mut plan_arena)[0];
        let predicate = plan_arena.alloc_expression(ScalarExpression::column_expr(right_column, 1));
        let probe = plan_arena.alloc_expression(ScalarExpression::column_expr(left_column, 0));
        let op = IndexProbeJoinOperator::new(probe, vec![predicate]);
        let (table_cache, _temp_dir, storage) = build_test_storage()?;
        let transaction = storage.transaction()?;
        let mut executor = execute_input::<_, IndexProbeJoin>(
            (&op, &left, &right),
            &table_cache,
            plan_arena,
            &transaction,
        );
        assert!(executor.next_tuple()?.is_none());
        Ok(())
    }
}
// GRCOV_EXCL_STOP
