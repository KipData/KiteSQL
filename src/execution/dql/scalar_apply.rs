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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::scalar_apply::ScalarApplyOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::Tuple;

pub struct ScalarApply {
    left_input_plan: Option<LogicalPlan>,
    right_input_plan: Option<LogicalPlan>,
    left_input: Option<ExecId>,
    right_input: Option<ExecId>,
    cached_right: Option<Tuple>,
}

impl From<(ScalarApplyOperator, LogicalPlan, LogicalPlan)> for ScalarApply {
    fn from((_, left_input, right_input): (ScalarApplyOperator, LogicalPlan, LogicalPlan)) -> Self {
        Self {
            left_input_plan: Some(left_input),
            right_input_plan: Some(right_input),
            left_input: None,
            right_input: None,
            cached_right: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ScalarApply {
    fn into_executor(
        mut self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId {
        self.left_input = Some(build_read(
            arena,
            self.left_input_plan
                .take()
                .expect("scalar apply left input plan initialized"),
            cache,
            transaction,
        ));
        self.right_input = Some(build_read(
            arena,
            self.right_input_plan
                .take()
                .expect("scalar apply right input plan initialized"),
            cache,
            transaction,
        ));
        arena.push(ExecNode::ScalarApply(self))
    }
}

impl ScalarApply {
    fn load_right_once<'a, T: Transaction + 'a>(
        cached_right: &mut Option<Tuple>,
        right_input: Option<ExecId>,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        if cached_right.is_none() {
            let right_input = right_input
                .expect("scalar apply right input executor initialized");
            if !arena.next_tuple(right_input)? {
                return Err(DatabaseError::InvalidValue(
                    "scalar apply right input returned no rows".to_string(),
                ));
            }
            *cached_right = Some(mem::take(arena.result_tuple_mut()));
        }

        Ok(())
    }

    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        Self::load_right_once(&mut self.cached_right, self.right_input, arena)?;

        let right_tuple = self.cached_right
            .as_ref()
            .expect("scalar apply right tuple initialized");
        let left_input = self
            .left_input
            .expect("scalar apply left input executor initialized");

        if !arena.next_tuple(left_input)? {
            arena.finish();
            return Ok(());
        }
        arena.result_tuple_mut().values.extend(right_tuple.values.iter().cloned());
        arena.resume();
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::execution::{execute, try_collect};
    use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_values(name: &str, rows: Vec<Vec<crate::types::value::DataValue>>) -> LogicalPlan {
        let desc = ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap();
        let schema_ref = Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
            name.to_string(),
            true,
            desc,
        ))]);

        LogicalPlan::new(
            Operator::Values(ValuesOperator { rows, schema_ref }),
            Childrens::None,
        )
    }

    fn build_test_storage() -> Result<
        (
            Arc<TableCache>,
            Arc<ViewCache>,
            Arc<StatisticsMetaCache>,
            TempDir,
            RocksStorage,
        ),
        DatabaseError,
    > {
        let meta_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let view_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);
        let table_cache = Arc::new(SharedLruCache::new(4, 1, RandomState::new())?);

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;

        Ok((table_cache, view_cache, meta_cache, temp_dir, storage))
    }

    #[test]
    fn scalar_apply_repeats_scalar_result_for_each_left_row() -> Result<(), DatabaseError> {
        let left = build_values(
            "left_c1",
            vec![
                vec![crate::types::value::DataValue::Int32(1)],
                vec![crate::types::value::DataValue::Int32(2)],
            ],
        );
        let right = ScalarSubqueryOperator::build(build_values(
            "right_c1",
            vec![vec![crate::types::value::DataValue::Int32(7)]],
        ));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute(
            ScalarApply::from((ScalarApplyOperator, left, right)),
            (&table_cache, &view_cache, &meta_cache),
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
        let left = build_values(
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let right = ScalarSubqueryOperator::build(build_values(
            "right_c1",
            vec![vec![DataValue::Null]],
        ));

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute(
            ScalarApply::from((ScalarApplyOperator, left, right)),
            (&table_cache, &view_cache, &meta_cache),
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
