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
use crate::execution::spill::{SpillReader, SpillVec};
use crate::execution::{
    build_read, ExecArena, ExecId, ExecNode, ExecutionContext, ExecutorNode, ReadExecutor,
};
use crate::planner::operator::recursive_cte::RecursiveScanOperator;
use crate::planner::{LogicalPlan, PlanArena};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use std::mem;

pub(crate) enum RecursiveInput {
    One(Option<Tuple>),
    Many(SpillReader<Tuple>),
}

impl Iterator for RecursiveInput {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(tuple) => tuple.take().map(Ok),
            Self::Many(rows) => rows.next(),
        }
    }
}

#[derive(Default)]
enum RecursiveRows {
    #[default]
    Empty,
    One {
        tuple: Tuple,
        output_done: bool,
    },
    Writing(SpillVec<'static, Tuple>),
    Reading(SpillReader<Tuple>),
}

impl RecursiveRows {
    fn push(&mut self, tuple: Tuple) -> Result<(), DatabaseError> {
        match self {
            Self::Empty => {
                *self = Self::One {
                    tuple,
                    output_done: false,
                };
            }
            Self::One {
                tuple: first,
                output_done: false,
            } => {
                let first = mem::take(first);
                let mut rows = SpillVec::new();
                let _ = rows.push(first)?;
                let _ = rows.push(tuple)?;
                *self = Self::Writing(rows);
            }
            Self::Writing(rows) => {
                let _ = rows.push(tuple)?;
            }
            Self::One { .. } | Self::Reading { .. } => {
                unreachable!("cannot append to a finished recursive generation")
            }
        }
        Ok(())
    }

    fn finish(self) -> Result<Self, DatabaseError> {
        match self {
            Self::Writing(mut rows) => {
                let _ = rows.flush()?;
                Ok(Self::Reading(rows.into_iter()))
            }
            rows => Ok(rows),
        }
    }

    fn next_output(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        match self {
            Self::Empty => Ok(None),
            Self::One { tuple, output_done } => {
                if *output_done {
                    return Ok(None);
                }
                *output_done = true;
                Ok(Some(tuple.clone()))
            }
            Self::Reading(reader) => reader.next().transpose(),
            Self::Writing(_) => unreachable!("recursive generation must be finished first"),
        }
    }

    fn into_input(self) -> Result<Option<RecursiveInput>, DatabaseError> {
        match self {
            Self::Empty => Ok(None),
            Self::One { tuple, .. } => Ok(Some(RecursiveInput::One(Some(tuple)))),
            Self::Reading(mut reader) => {
                reader.reset()?;
                Ok(Some(RecursiveInput::Many(reader)))
            }
            Self::Writing(_) => unreachable!("recursive generation must be finished first"),
        }
    }
}

enum RecursivePhase {
    Anchor,
    Output,
    Recursive,
}

pub struct RecursiveCte<'a, T: Transaction + 'a> {
    recursive_plan: LogicalPlan,
    anchor_input: ExecId,
    recursive_arena: ExecArena<'a, T>,
    recursive_root: ExecId,
    working: RecursiveRows,
    next: RecursiveRows,
    phase: RecursivePhase,
}

impl<'a, T: Transaction + 'a> RecursiveCte<'a, T> {
    fn new(
        anchor_input: ExecId,
        recursive_plan: LogicalPlan,
        recursive_arena: ExecArena<'a, T>,
    ) -> Self {
        Self {
            recursive_plan,
            anchor_input,
            recursive_arena,
            recursive_root: 0,
            working: RecursiveRows::default(),
            next: RecursiveRows::default(),
            phase: RecursivePhase::Anchor,
        }
    }

    fn start_recursive(&mut self, plan_arena: &mut PlanArena<'a>) -> Result<bool, DatabaseError> {
        let Some(input) = mem::take(&mut self.working).into_input()? else {
            return Ok(false);
        };

        self.recursive_arena.reset_for_rebuild();
        self.recursive_arena.set_recursive_input(input);
        let cache = self.recursive_arena.context();
        let transaction = self.recursive_arena.transaction();
        self.recursive_root = build_read(
            &mut self.recursive_arena,
            plan_arena,
            self.recursive_plan.clone(),
            cache,
            transaction,
        );
        Ok(true)
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for RecursiveCte<'a, T> {
    type Input = (LogicalPlan, LogicalPlan);

    fn into_executor(
        (anchor_plan, recursive_plan): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let mut recursive_arena = ExecArena::new();
        recursive_arena.init_context(arena.context(), arena.transaction());
        let anchor_input = build_read(arena, plan_arena, anchor_plan, cache, transaction);
        arena.push(ExecNode::RecursiveCte(Self::new(
            anchor_input,
            recursive_plan,
            recursive_arena,
        )))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for RecursiveCte<'a, T> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        loop {
            match self.phase {
                RecursivePhase::Anchor => {
                    while arena.next_tuple(self.anchor_input, plan_arena)? {
                        self.next.push(mem::take(arena.result_tuple_mut()))?;
                    }
                    self.working = mem::take(&mut self.next).finish()?;
                    self.phase = RecursivePhase::Output;
                }
                RecursivePhase::Output => {
                    if let Some(tuple) = self.working.next_output()? {
                        arena.produce_tuple(tuple);
                        return Ok(());
                    }
                    if !self.start_recursive(plan_arena)? {
                        arena.finish();
                        return Ok(());
                    }
                    self.phase = RecursivePhase::Recursive;
                }
                RecursivePhase::Recursive => {
                    while self
                        .recursive_arena
                        .next_tuple(self.recursive_root, plan_arena)?
                    {
                        self.next
                            .push(mem::take(self.recursive_arena.result_tuple_mut()))?;
                    }
                    self.recursive_arena.reset_for_rebuild();
                    self.working = mem::take(&mut self.next).finish()?;
                    self.phase = RecursivePhase::Output;
                }
            }
        }
    }
}

pub struct RecursiveScan {
    input: RecursiveInput,
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for RecursiveScan {
    type Input = RecursiveScanOperator;

    fn into_executor(
        _input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut PlanArena<'a>,
        _cache: ExecutionContext<'_>,
        _transaction: &T,
    ) -> ExecId {
        let input = arena.take_recursive_input();
        arena.push(ExecNode::RecursiveScan(Self { input }))
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for RecursiveScan {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        _plan_arena: &mut PlanArena<'a>,
    ) -> Result<(), DatabaseError> {
        match self.input.next().transpose()? {
            Some(tuple) => arena.produce_tuple(tuple),
            None => arena.finish(),
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::{empty_context, execute_input, try_collect};
    use crate::expression::{BinaryOperator, ScalarExpression};
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::recursive_cte::RecursiveScanOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::Childrens;
    use crate::storage::rocksdb::RocksStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::evaluator::binary_create;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::borrow::Cow;
    use tempfile::TempDir;

    #[test]
    fn spilled_generation_is_written_once_and_replayed_for_scan() -> Result<(), DatabaseError> {
        let expected = (0..1100)
            .map(|value| Tuple::new(None, vec![DataValue::Int32(value)]))
            .collect::<Vec<_>>();
        let mut rows = RecursiveRows::default();
        for tuple in expected.iter().cloned() {
            rows.push(tuple)?;
        }

        let mut working = rows.finish()?;
        assert!(matches!(&working, RecursiveRows::Reading(_)));
        let mut output = Vec::new();
        while let Some(tuple) = working.next_output()? {
            output.push(tuple);
        }
        assert_eq!(output, expected);

        let scan = working.into_input()?.unwrap();
        assert_eq!(scan.collect::<Result<Vec<_>, _>>()?, expected);
        Ok(())
    }

    #[test]
    fn empty_generation_has_no_recursive_input() -> Result<(), DatabaseError> {
        let rows = RecursiveRows::default();
        let working = rows.finish()?;
        assert!(working.into_input()?.is_none());
        Ok(())
    }

    #[test]
    fn recursive_cte_replays_each_generation() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let column = plan_arena.alloc_column(ColumnCatalog::new(
            "value".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let schema_ref = vec![column];
        let anchor = LogicalPlan::new(
            Operator::Values(ValuesOperator {
                rows: vec![vec![DataValue::Int32(1)]],
                schema_ref: schema_ref.clone(),
            }),
            Childrens::None,
        );
        let scan = LogicalPlan::new(
            Operator::RecursiveScan(RecursiveScanOperator { schema_ref }),
            Childrens::None,
        );
        let filter = FilterOperator::build(
            ScalarExpression::Binary {
                op: BinaryOperator::Lt,
                left_expr: Box::new(ScalarExpression::column_expr(column, 0)),
                right_expr: Box::new(DataValue::Int32(3).into()),
                evaluator: Some(binary_create(
                    Cow::Owned(LogicalType::Integer),
                    BinaryOperator::Lt,
                )?),
                ty: LogicalType::Boolean,
            },
            scan,
            false,
        );
        let recursive = LogicalPlan::new(
            Operator::Project(ProjectOperator {
                exprs: vec![ScalarExpression::Binary {
                    op: BinaryOperator::Plus,
                    left_expr: Box::new(ScalarExpression::column_expr(column, 0)),
                    right_expr: Box::new(DataValue::Int32(1).into()),
                    evaluator: Some(binary_create(
                        Cow::Owned(LogicalType::Integer),
                        BinaryOperator::Plus,
                    )?),
                    ty: LogicalType::Integer,
                }],
            }),
            Childrens::Only(Box::new(filter)),
        );

        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let storage = RocksStorage::new(temp_dir.path())?;
        let transaction = storage.transaction()?;
        let table_cache = TableCache::default();
        let view_cache = ViewCache::default();
        let meta_cache = StatisticsMetaCache::default();
        let tuples = try_collect(execute_input::<_, RecursiveCte<'_, _>>(
            (anchor, recursive),
            empty_context(&table_cache, &view_cache, &meta_cache),
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
                DataValue::Int32(2),
                DataValue::Int32(3),
            ]
        );
        Ok(())
    }
}
