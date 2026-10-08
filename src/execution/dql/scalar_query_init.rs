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
use crate::expression::ScalarExpression;
use crate::planner::operator::scalar_query_init::ScalarQueryInitOperator;
use crate::planner::ExecMetaArena;
use crate::planner::{ExprRef, LogicalPlan, MetaArena, ScalarQueryRef};
use crate::storage::Transaction;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

enum ScalarQueryInitState {
    Initialize,
    ReadInput,
    ReadOuter,
    EvaluateOuter,
    Finished,
}

pub struct ScalarQueryInit<'a> {
    input: ExecId,
    reference: ScalarQueryRef,
    state: ScalarQueryInitState,
    param_bindings: &'a [(ScalarQueryRef, ExprRef)],
    scratch_tuple: Tuple,

    init_plan: &'a LogicalPlan,
    init_pos: ExecId,
    init: ExecId,
}

impl<'a> ScalarQueryInit<'a> {
    fn evaluate_init<T: Transaction + 'a, A: MetaArena + 'a>(
        &self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        let mut value = DataValue::Null;
        if arena.next_tuple(self.init, plan_arena)? {
            std::mem::swap(&mut value, &mut arena.result_tuple_mut().values[0]);
        }
        if arena.next_tuple(self.init, plan_arena)? {
            return Err(DatabaseError::InvalidValue(
                "scalar subquery returned more than one row".into(),
            ));
        }
        plan_arena.set_init_value(self.reference, value);
        Ok(())
    }

    pub(crate) fn build<T: Transaction + 'a>(
        op: &'a ScalarQueryInitOperator,
        input: ExecId,
        init_plan: &'a LogicalPlan,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let init_pos = arena.nodes.position();
        let init = build_read(arena, plan_arena, init_plan, cache, transaction);
        arena.push(ExecNode::ScalarQueryInit(Self {
            input,
            init,
            reference: op.reference(plan_arena),
            state: if matches!(
                plan_arena.expression(op.value),
                ScalarExpression::OuterValue { .. }
            ) {
                ScalarQueryInitState::ReadOuter
            } else {
                ScalarQueryInitState::Initialize
            },
            param_bindings: &op.param_bindings,
            scratch_tuple: Tuple::default(),
            init_plan,
            init_pos,
        }))
    }
}

impl<'a, T: Transaction + 'a> ReadExecutor<'a, T> for ScalarQueryInit<'a> {
    type Input = (
        &'a ScalarQueryInitOperator,
        &'a LogicalPlan,
        &'a LogicalPlan,
    );

    fn into_executor(
        (op, input, init): Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId {
        let input = build_read(arena, plan_arena, input, cache, transaction);
        Self::build(op, input, init, arena, plan_arena, cache, transaction)
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ScalarQueryInit<'a> {
    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        loop {
            match self.state {
                ScalarQueryInitState::Initialize => {
                    if plan_arena.init_value(self.reference).is_none() {
                        self.evaluate_init(arena, plan_arena)?;
                    }
                    self.state = ScalarQueryInitState::ReadInput;
                }
                ScalarQueryInitState::ReadInput => {
                    if !arena.next_tuple(self.input, plan_arena)? {
                        self.state = ScalarQueryInitState::Finished;
                    }
                    return Ok(());
                }
                ScalarQueryInitState::ReadOuter => {
                    if !arena.next_tuple(self.input, plan_arena)? {
                        self.state = ScalarQueryInitState::Finished;
                        return Ok(());
                    }
                    // Preserve the outer row and lend the previous subquery buffer to the arena.
                    std::mem::swap(arena.result_tuple_mut(), &mut self.scratch_tuple);
                    for (reference, expr) in self.param_bindings {
                        let value = plan_arena
                            .expression(*expr)
                            .eval(plan_arena, Some(&self.scratch_tuple))?
                            .into_owned();
                        plan_arena.set_init_value(*reference, value);
                    }
                    self.state = ScalarQueryInitState::EvaluateOuter;
                }
                ScalarQueryInitState::EvaluateOuter => {
                    let previous = arena.nodes.position();
                    arena.nodes.seek(self.init_pos);
                    self.init = build_read(
                        arena,
                        plan_arena,
                        self.init_plan,
                        arena.context(),
                        arena.transaction(),
                    );
                    arena.nodes.seek(previous);
                    self.evaluate_init(arena, plan_arena)?;
                    std::mem::swap(arena.result_tuple_mut(), &mut self.scratch_tuple);
                    self.state = ScalarQueryInitState::ReadOuter;
                    arena.resume();
                    return Ok(());
                }
                ScalarQueryInitState::Finished => {
                    arena.finish();
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::execution::empty_context;
    use crate::expression::ScalarExpression;
    use crate::planner::operator::scalar_subquery::ScalarSubqueryOperator;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, ExecMetaArena, PlanArena, TableArenaCell};
    use crate::storage::memory::MemoryStorage;
    use crate::storage::{StatisticsMetaCache, Storage, TableCache, ViewCache};
    use crate::types::LogicalType;

    #[test]
    fn scalar_init_shared_slot_caches_null_and_checks_cardinality() -> Result<(), DatabaseError> {
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;
        let tables = TableCache::default();
        let views = ViewCache::default();
        let stats = StatisticsMetaCache::default();
        let cache = empty_context(&tables, &views, &stats);
        for (value, row_count) in [
            (DataValue::Null, 1),
            (DataValue::Int32(7), 1),
            (DataValue::Int32(7), 2),
        ] {
            let table_arena = TableArenaCell::default();
            let mut metadata = PlanArena::new(&table_arena);
            let reference = metadata.alloc_scalar_query_ref(false);
            let column = metadata.alloc_column(ColumnCatalog::new(
                "v".into(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None)?,
            ));
            let expr = metadata.alloc_expression(ScalarExpression::Constant(value.clone()));
            let marker = metadata.alloc_expression(ScalarExpression::InitValue {
                id: reference,
                ty: LogicalType::Integer,
            });
            let make_init = |rows| {
                ScalarSubqueryOperator::build(LogicalPlan::new(
                    Operator::Values(ValuesOperator::new(vec![expr; rows], rows, vec![column])),
                    Childrens::None,
                ))
            };
            let mut first = ScalarQueryInitOperator::build(
                LogicalPlan::new(Operator::Dummy, Childrens::None),
                make_init(row_count),
                marker,
                Vec::new(),
            );
            // This duplicate would error if executed, making cache reuse observable even for NULL.
            let mut duplicate = ScalarQueryInitOperator::build(
                LogicalPlan::new(Operator::Dummy, Childrens::None),
                make_init(2),
                marker,
                Vec::new(),
            );
            first.populate_output_schema_recursive(&mut metadata);
            duplicate.populate_output_schema_recursive(&mut metadata);
            let mut arena = ExecArena::with_capacity(0);
            arena.init_context(cache, &transaction);
            let first_root = build_read(&mut arena, &mut metadata, &first, cache, &transaction);
            let duplicate_root =
                build_read(&mut arena, &mut metadata, &duplicate, cache, &transaction);
            let mut view = ExecMetaArena::new(metadata);
            if row_count == 2 {
                assert!(arena.next_tuple(first_root, &mut view).is_err());
                assert_eq!(view.init_value(reference), None);
            } else {
                assert!(arena.next_tuple(first_root, &mut view)?);
                assert_eq!(view.init_value(reference), Some(&value));
                assert!(arena.next_tuple(duplicate_root, &mut view)?);
                assert!(!arena.next_tuple(duplicate_root, &mut view)?);
            }
        }
        Ok(())
    }
}
