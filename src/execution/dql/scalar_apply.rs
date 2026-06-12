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

use std::mem;

use crate::errors::DatabaseError;
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutorNode, ReadExecutionContext,
};
use crate::planner::operator::scalar_apply::ScalarApplyOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

pub struct ScalarApply {
    left_input: ExecId,
    right_input: ExecId,
    cached_right: Option<Tuple>,
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ScalarApply {
    type Input = (ScalarApplyOperator, LogicalPlan, LogicalPlan);

    fn into_executor(
        (_, left_input, right_input): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let left_input = build_read(arena, plan_arena, left_input, cache, transaction);
        let right_input = build_read(arena, plan_arena, right_input, cache, transaction);
        arena.push(ExecNode::ScalarApply(Self {
            left_input,
            right_input,
            cached_right: None,
        }))
    }

    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        Self::load_right_once(&mut self.cached_right, self.right_input, arena, plan_arena)?;

        let right_tuple = self
            .cached_right
            .as_ref()
            .expect("scalar apply right tuple initialized");
        if !arena.next_tuple(self.left_input, plan_arena)? {
            arena.finish();
            return Ok(());
        }
        arena
            .result_tuple_mut()
            .values
            .extend(right_tuple.values.iter().cloned());
        arena.resume();
        Ok(())
    }
}

impl ScalarApply {
    fn load_right_once<'a, T: Transaction + 'a>(
        cached_right: &mut Option<Tuple>,
        right_input: ExecId,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut crate::planner::PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        if cached_right.is_none() {
            if !arena.next_tuple(right_input, plan_arena)? {
                return Err(DatabaseError::InvalidValue(
                    "scalar apply right input returned no rows".to_string(),
                ));
            }
            *cached_right = Some(mem::take(arena.result_tuple_mut()));
        }

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::execution::{execute_input, try_collect};
    use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use tempfile::TempDir;

    fn build_values(
        arena: &mut crate::planner::PlanArena,
        name: &str,
        rows: Vec<Vec<crate::types::value::DataValue>>,
    ) -> LogicalPlan {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap();
        let schema_ref = vec![arena.alloc_column(ColumnCatalog::new(name.to_string(), true, desc))];

        LogicalPlan::new(
            Operator::Values(ValuesOperator { rows, schema_ref }),
            Childrens::None,
        )
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

    #[test]
    fn scalar_apply_repeats_scalar_result_for_each_left_row() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![
                vec![crate::types::value::DataValue::Int32(1)],
                vec![crate::types::value::DataValue::Int32(2)],
            ],
        );
        let right = ScalarSubqueryOperator::build(build_values(
            &mut plan_arena,
            "right_c1",
            vec![vec![crate::types::value::DataValue::Int32(7)]],
        ));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, ScalarApply>(
            (ScalarApplyOperator, left, right),
            (&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &mut transaction,
        ))?;

        let actual = tuples
            .into_iter()
            .flat_map(|tuple| tuple.values)
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![
                DataValue::Int32(1),
                DataValue::Int32(7),
                DataValue::Int32(2),
                DataValue::Int32(7),
            ]
        );

        Ok(())
    }

    #[test]
    fn scalar_apply_repeats_null_scalar_result_for_each_left_row() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let left = build_values(
            &mut plan_arena,
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let right = ScalarSubqueryOperator::build(build_values(
            &mut plan_arena,
            "right_c1",
            vec![vec![DataValue::Null]],
        ));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute_input::<_, ScalarApply>(
            (ScalarApplyOperator, left, right),
            (&table_cache, &view_cache, &meta_cache),
            plan_arena,
            &mut transaction,
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
                DataValue::Null,
            ]
        );

        Ok(())
    }
}
