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
use crate::execution::{build_read, ExecArena, ExecId, ExecNode, ExecutionCaches, ReadExecutor};
use crate::planner::operator::mark_apply::MarkApplyOperator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;
use crate::types::tuple::{Schema, SchemaRef, Tuple};
use crate::types::value::DataValue;
use std::mem;
use std::sync::Arc;

pub struct MarkApply {
    op: MarkApplyOperator,
    left_input_plan: Option<LogicalPlan>,
    right_input_plan: Option<LogicalPlan>,
    left_input: Option<ExecId>,
    predicate_schema: SchemaRef,
    left_tuple: Tuple,
}

impl From<(MarkApplyOperator, LogicalPlan, LogicalPlan)> for MarkApply {
    fn from(
        (op, mut left_input, mut right_input): (MarkApplyOperator, LogicalPlan, LogicalPlan),
    ) -> Self {
        let predicate_schema = Arc::new(
            left_input
                .output_schema()
                .iter()
                .chain(right_input.output_schema().iter())
                .cloned()
                .collect::<Schema>(),
        );
        Self {
            op,
            left_input_plan: Some(left_input),
            right_input_plan: Some(right_input),
            left_input: None,
            predicate_schema,
            left_tuple: Tuple::default(),
        }
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for MarkApply {
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
                .expect("mark apply left input plan initialized"),
            cache,
            transaction,
        ));
        arena.push(ExecNode::MarkApply(self))
    }
}

impl MarkApply {
    fn build_right_input<'a, T: Transaction + 'a>(&self, arena: &mut ExecArena<'a, T>) -> ExecId {
        let cache = (arena.table_cache(), arena.view_cache(), arena.meta_cache());
        let transaction = arena.transaction_mut() as *mut T;
        build_read(
            arena,
            self.right_input_plan
                .clone()
                .expect("mark apply right input plan initialized"),
            cache,
            transaction,
        )
    }

    fn predicate_matched(
        &self,
        left_tuple: &Tuple,
        right_tuple: &Tuple,
    ) -> Result<bool, DatabaseError> {
        // FIXME
        let values = Vec::from_iter(
            left_tuple
                .values
                .iter()
                .chain(right_tuple.values.iter())
                .cloned(),
        );

        for predicate in self.op.predicates() {
            match predicate.eval(Some((values.as_slice(), self.predicate_schema.as_ref())))? {
                DataValue::Boolean(true) => {}
                DataValue::Boolean(false) | DataValue::Null => return Ok(false),
                _ => return Err(DatabaseError::InvalidType),
            }
        }

        Ok(true)
    }

    pub(crate) fn next_tuple<'a, T: Transaction + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
    ) -> Result<(), DatabaseError> {
        let left_input = self
            .left_input
            .expect("mark apply left input executor initialized");

        if !arena.next_tuple(left_input)? {
            arena.finish();
            return Ok(());
        }

        self.left_tuple = mem::take(arena.result_tuple_mut());
        let right_input = self.build_right_input(arena);
        let mut matched = false;

        while arena.next_tuple(right_input)? {
            let right_tuple = arena.result_tuple();
            if self.predicate_matched(&self.left_tuple, right_tuple)? {
                matched = true;
                break;
            }
        }

        arena.produce_tuple(mem::take(&mut self.left_tuple));
        arena.result_tuple_mut().values.push(DataValue::Boolean(matched));
        arena.resume();
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::execution::{execute, try_collect};
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::planner::operator::mark_apply::MarkApplyOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::evaluator::EvaluatorFactory;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use crate::utils::lru::SharedLruCache;
    use std::hash::RandomState;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn build_values(name: &str, rows: Vec<Vec<DataValue>>) -> LogicalPlan {
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

    fn build_marker_column() -> ColumnRef {
        ColumnRef::from(ColumnCatalog::new(
            "__exists".to_string(),
            true,
            ColumnDesc::new(LogicalType::Boolean, None, true, None).unwrap(),
        ))
    }

    #[test]
    fn mark_exists_apply_appends_boolean_match_column() -> Result<(), DatabaseError> {
        let mut left = build_values(
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values(
            "right_c1",
            vec![vec![DataValue::Int32(2)], vec![DataValue::Int32(3)]],
        );
        let left_column = left.output_schema()[0].clone();
        let right_column = right.output_schema()[0].clone();

        let predicate = ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: Box::new(ScalarExpression::column_expr(left_column, 0)),
            right_expr: Box::new(ScalarExpression::column_expr(right_column, 1)),
            evaluator: Some(EvaluatorFactory::binary_create(
                LogicalType::Integer,
                BinaryOperator::Eq,
            )?),
            ty: LogicalType::Boolean,
        };

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute(
            MarkApply::from((
                MarkApplyOperator::new_exists(build_marker_column(), vec![predicate]),
                left,
                right,
            )),
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
                DataValue::Boolean(false),
                DataValue::Int32(2),
                DataValue::Boolean(true),
            ]
        );

        Ok(())
    }

    #[test]
    fn mark_exists_apply_treats_null_predicate_as_not_matched() -> Result<(), DatabaseError> {
        let mut left = build_values(
            "left_c1",
            vec![vec![DataValue::Int32(1)], vec![DataValue::Int32(2)]],
        );
        let mut right = build_values(
            "right_c1",
            vec![vec![DataValue::Null], vec![DataValue::Int32(2)]],
        );
        let left_column = left.output_schema()[0].clone();
        let right_column = right.output_schema()[0].clone();

        let predicate = ScalarExpression::Binary {
            op: BinaryOperator::Eq,
            left_expr: Box::new(ScalarExpression::column_expr(left_column, 0)),
            right_expr: Box::new(ScalarExpression::column_expr(right_column, 1)),
            evaluator: Some(EvaluatorFactory::binary_create(
                LogicalType::Integer,
                BinaryOperator::Eq,
            )?),
            ty: LogicalType::Boolean,
        };

        let (table_cache, view_cache, meta_cache, _temp_dir, storage) = build_test_storage()?;
        let mut transaction = storage.transaction()?;
        let tuples = try_collect(execute(
            MarkApply::from((
                MarkApplyOperator::new_exists(build_marker_column(), vec![predicate]),
                left,
                right,
            )),
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
                DataValue::Boolean(false),
                DataValue::Int32(2),
                DataValue::Boolean(true),
            ]
        );

        Ok(())
    }
}
