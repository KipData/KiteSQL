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

use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::agg::AggKind;
use crate::expression::visitor::ExprVisitor;
use crate::expression::{AliasType, HasCountStar, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::rule::normalization::{
    remap_expr_positions, remap_exprs_positions, PositionRemapper,
};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::visitor::{OperatorExprVisitor, OperatorVisitor};
use crate::planner::operator::visitor_mut::{OperatorExprVisitorMut, OperatorVisitorMut};
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;
use crate::types::LogicalType;

#[derive(Clone)]
pub struct ColumnPruning;

struct ApplyOutcome {
    changed: bool,
    removed_positions: Vec<usize>,
}

#[derive(Clone, Default)]
struct ReferencedColumns {
    columns: Vec<ColumnRef>,
}

impl ReferencedColumns {
    fn with_arena_capacity(arena: &crate::planner::PlanArena) -> Self {
        Self {
            columns: Vec::with_capacity(arena.allocated_columns_len()),
        }
    }

    fn clear(&mut self) {
        self.columns.clear();
    }

    fn insert(&mut self, column: ColumnRef, arena: &crate::planner::PlanArena) {
        if let Err(index) = self.search(column, arena) {
            self.columns.insert(index, column);
        }
    }

    fn extend(
        &mut self,
        columns: impl IntoIterator<Item = ColumnRef>,
        arena: &crate::planner::PlanArena,
    ) {
        let columns = columns.into_iter();
        self.columns.reserve(columns.size_hint().0);
        for column in columns {
            self.insert(column, arena);
        }
    }

    fn contains(&self, column: ColumnRef, arena: &crate::planner::PlanArena) -> bool {
        self.search(column, arena).is_ok()
    }

    fn search(&self, column: ColumnRef, arena: &crate::planner::PlanArena) -> Result<usize, usize> {
        let summary = arena.column(column).summary();
        self.columns
            .binary_search_by(|candidate| arena.column(*candidate).summary().cmp(summary))
    }
}

struct ReferencedColumnCollector<'a, 'p> {
    referenced_columns: &'a mut ReferencedColumns,
    arena: &'a crate::planner::PlanArena<'p>,
}

impl ExprVisitor<'_> for ReferencedColumnCollector<'_, '_> {
    fn visit_column_ref(
        &mut self,
        column: &crate::catalog::ColumnRef,
    ) -> Result<(), DatabaseError> {
        self.referenced_columns.insert(*column, self.arena);
        Ok(())
    }

    fn visit_alias(
        &mut self,
        expr: &ScalarExpression,
        _ty: &AliasType,
    ) -> Result<(), DatabaseError> {
        self.visit(expr)
    }
}

impl ApplyOutcome {
    fn with_arena_capacity(arena: &crate::planner::PlanArena) -> Self {
        Self {
            changed: false,
            removed_positions: Vec::with_capacity(arena.allocated_columns_len()),
        }
    }
}

impl ColumnPruning {
    fn extend_operator_referenced_columns(
        operator: &Operator,
        referenced_columns: &mut ReferencedColumns,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut collector = ReferencedColumnCollector {
            referenced_columns,
            arena,
        };
        OperatorExprVisitor::new(&mut collector).visit_operator(operator)?;

        struct ReferencedOperatorColumnCollector<'a, 'p> {
            referenced_columns: &'a mut ReferencedColumns,
            arena: &'a crate::planner::PlanArena<'p>,
        }

        impl<'a> OperatorVisitor<'a> for ReferencedOperatorColumnCollector<'_, '_> {
            fn visit_mark_apply(
                &mut self,
                op: &'a crate::planner::operator::mark_apply::MarkApplyOperator,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns
                    .insert(*op.output_column(), self.arena);
                Ok(())
            }

            fn visit_table_scan(
                &mut self,
                op: &'a crate::planner::operator::table_scan::TableScanOperator,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns
                    .extend(op.columns.iter().copied(), self.arena);
                Ok(())
            }

            fn visit_values(
                &mut self,
                op: &'a crate::planner::operator::values::ValuesOperator,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns
                    .extend(op.schema_ref.iter().copied(), self.arena);
                Ok(())
            }

            fn visit_union(
                &mut self,
                op: &'a crate::planner::operator::union::UnionOperator,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns.extend(
                    op.left_schema_ref
                        .iter()
                        .chain(&op._right_schema_ref)
                        .copied(),
                    self.arena,
                );
                Ok(())
            }

            fn visit_set_membership(
                &mut self,
                op: &'a crate::planner::operator::set_membership::SetMembershipOperator,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns.extend(
                    op.left_schema_ref
                        .iter()
                        .chain(&op._right_schema_ref)
                        .copied(),
                    self.arena,
                );
                Ok(())
            }

            fn visit_delete(
                &mut self,
                op: &'a crate::planner::operator::delete::DeleteOperator,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns
                    .extend(op.primary_keys.iter().copied(), self.arena);
                Ok(())
            }
        }

        ReferencedOperatorColumnCollector {
            referenced_columns,
            arena,
        }
        .visit_operator(operator)
    }

    fn extend_expr_referenced_columns<'a>(
        exprs: impl IntoIterator<Item = &'a ScalarExpression>,
        referenced_columns: &mut ReferencedColumns,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut collector = ReferencedColumnCollector {
            referenced_columns,
            arena,
        };
        for expr in exprs {
            collector.visit(expr)?;
        }
        Ok(())
    }

    fn output_column_is_required(
        expr: &ScalarExpression,
        column_references: &ReferencedColumns,
        arena: &mut crate::planner::PlanArena,
    ) -> bool {
        let output_column = expr.output_column_ref(arena);
        column_references.contains(output_column, arena)
    }

    fn clear_exprs(
        column_references: &ReferencedColumns,
        exprs: &mut Vec<ScalarExpression>,
        removed_positions: &mut Vec<usize>,
        output_start: usize,
        arena: &mut crate::planner::PlanArena,
    ) {
        removed_positions.truncate(output_start);
        removed_positions.reserve(exprs.len());
        let mut position = 0;
        exprs.retain(|expr| {
            let keep = Self::output_column_is_required(expr, column_references, arena);
            if !keep {
                removed_positions.push(position);
            }
            position += 1;
            keep
        });
    }

    fn remap_operator_after_child_change(
        operator: &mut Operator,
        removed_positions: &[usize],
    ) -> Result<(), DatabaseError> {
        OperatorExprVisitorMut::new(&mut PositionRemapper { removed_positions })
            .visit_operator(operator)
    }

    fn remap_exprs_after_child_change<'a>(
        exprs: impl IntoIterator<Item = &'a mut ScalarExpression>,
        removed_positions: &[usize],
    ) -> Result<(), DatabaseError> {
        if removed_positions.is_empty() {
            return Ok(());
        }
        remap_exprs_positions(exprs, removed_positions)
    }

    fn apply_only_child(
        referenced_columns: ReferencedColumns,
        all_referenced: bool,
        childrens: &mut Childrens,
        outcome: &mut ApplyOutcome,
        output_start: usize,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let Childrens::Only(child) = childrens else {
            outcome.changed = false;
            outcome.removed_positions.truncate(output_start);
            return Ok(false);
        };
        Self::_apply_appending(
            referenced_columns,
            all_referenced,
            child.as_mut(),
            outcome,
            arena,
        )?;
        Ok(outcome.changed)
    }

    #[allow(clippy::needless_lifetimes)]
    fn apply_twins(
        referenced_columns: ReferencedColumns,
        all_referenced: bool,
        childrens: &mut Childrens,
        outcome: &mut ApplyOutcome,
        output_start: usize,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let Childrens::Twins { left, right } = childrens else {
            outcome.changed = false;
            outcome.removed_positions.truncate(output_start);
            return Ok(false);
        };

        Self::_apply_appending(
            referenced_columns.clone(),
            all_referenced,
            left.as_mut(),
            outcome,
            arena,
        )?;
        let left_changed = outcome.changed;
        outcome.removed_positions.truncate(output_start);

        Self::_apply_appending(
            referenced_columns,
            all_referenced,
            right.as_mut(),
            outcome,
            arena,
        )?;
        let right_changed = outcome.changed;
        outcome.removed_positions.truncate(output_start);

        outcome.changed = left_changed || right_changed;
        Ok(outcome.changed)
    }

    fn offset_removed_positions(removed_positions: &mut [usize], offset: usize) {
        for position in removed_positions {
            *position += offset;
        }
    }

    fn _apply(
        required_columns: ReferencedColumns,
        all_referenced: bool,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<ApplyOutcome, DatabaseError> {
        let mut outcome = ApplyOutcome::with_arena_capacity(arena);
        Self::_apply_appending(required_columns, all_referenced, plan, &mut outcome, arena)?;
        Ok(outcome)
    }

    fn _apply_appending(
        mut required_columns: ReferencedColumns,
        all_referenced: bool,
        plan: &mut LogicalPlan,
        outcome: &mut ApplyOutcome,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let mut changed = false;
        let output_start = outcome.removed_positions.len();
        let (operator, childrens) = (&mut plan.operator, plan.childrens.as_mut());

        match operator {
            Operator::Aggregate(op) => {
                if !all_referenced {
                    Self::clear_exprs(
                        &required_columns,
                        &mut op.agg_calls,
                        &mut outcome.removed_positions,
                        output_start,
                        arena,
                    );
                    if outcome.removed_positions.len() > output_start {
                        changed = true;
                    }

                    if op.agg_calls.is_empty() && op.groupby_exprs.is_empty() {
                        let value = DataValue::Utf8 {
                            value: "*".to_string(),
                            ty: Utf8Type::Variable(None),
                            unit: CharLengthUnits::Characters,
                        };
                        // only single COUNT(*) is not depend on any column
                        // removed all expressions from the aggregate: push a COUNT(*)
                        op.agg_calls.push(ScalarExpression::AggCall {
                            distinct: false,
                            kind: AggKind::Count,
                            args: vec![ScalarExpression::Constant(value)],
                            ty: LogicalType::Integer,
                        });
                        changed = true;
                    }
                } else {
                    outcome.removed_positions.truncate(output_start);
                }

                let child_start = outcome.removed_positions.len();
                let child_changed = {
                    if !op.is_distinct {
                        required_columns.clear();
                    }
                    Self::extend_expr_referenced_columns(
                        op.agg_calls.iter().chain(op.groupby_exprs.iter()),
                        &mut required_columns,
                        arena,
                    )?;

                    Self::apply_only_child(
                        required_columns,
                        false,
                        childrens,
                        outcome,
                        child_start,
                        arena,
                    )?
                };
                if child_changed {
                    Self::remap_operator_after_child_change(
                        operator,
                        &outcome.removed_positions[child_start..],
                    )?;
                    changed = true;
                }
                outcome.removed_positions.truncate(child_start);
            }
            Operator::Project(op) => {
                let mut has_count_star = HasCountStar::default();
                for expr in &op.exprs {
                    has_count_star.visit(expr)?;
                }
                if !has_count_star.value {
                    if !all_referenced {
                        Self::clear_exprs(
                            &required_columns,
                            &mut op.exprs,
                            &mut outcome.removed_positions,
                            output_start,
                            arena,
                        );
                        if outcome.removed_positions.len() > output_start {
                            changed = true;
                        }
                    } else {
                        outcome.removed_positions.truncate(output_start);
                    }

                    let child_start = outcome.removed_positions.len();
                    let child_changed = {
                        required_columns.clear();
                        Self::extend_expr_referenced_columns(
                            op.exprs.iter(),
                            &mut required_columns,
                            arena,
                        )?;

                        Self::apply_only_child(
                            required_columns,
                            false,
                            childrens,
                            outcome,
                            child_start,
                            arena,
                        )?
                    };
                    if child_changed {
                        Self::remap_operator_after_child_change(
                            operator,
                            &outcome.removed_positions[child_start..],
                        )?;
                        changed = true;
                    }
                    outcome.removed_positions.truncate(child_start);
                } else {
                    outcome.removed_positions.truncate(output_start);
                }
            }
            Operator::TableScan(op) => {
                if !all_referenced {
                    outcome.removed_positions.truncate(output_start);
                    outcome.removed_positions.reserve(op.columns.len());
                    let mut position = 0;
                    op.columns.retain(|column| {
                        let current_position = position;
                        position += 1;
                        let keep = required_columns.contains(*column, arena);
                        if !keep {
                            outcome.removed_positions.push(current_position);
                        }
                        keep
                    });
                    if outcome.removed_positions.len() > output_start {
                        changed = true;
                    }
                } else {
                    outcome.removed_positions.truncate(output_start);
                }
            }
            Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::ScalarApply(_)
            | Operator::MarkApply(_)
            | Operator::ScalarSubquery(_)
            | Operator::Join(_)
            | Operator::Filter(_)
            | Operator::Union(_)
            | Operator::SetMembership(_)
            | Operator::TopK(_)
            | Operator::Window(_) => {
                if matches!(operator, Operator::ScalarApply(_) | Operator::MarkApply(_)) {
                    let mut child_required = required_columns;
                    Self::extend_operator_referenced_columns(operator, &mut child_required, arena)?;
                    changed |= Self::apply_twins(
                        child_required,
                        true,
                        childrens,
                        outcome,
                        output_start,
                        arena,
                    )?;
                    outcome.removed_positions.truncate(output_start);
                } else if matches!(operator, Operator::Join(_)) {
                    let (old_left_outputs_len, left_removed_start, right_removed_start) = {
                        let mut child_required = required_columns;
                        Self::extend_operator_referenced_columns(
                            operator,
                            &mut child_required,
                            arena,
                        )?;
                        let old_left_outputs_len = match childrens {
                            Childrens::Twins { left, .. } => left.output_schema(arena).len(),
                            _ => 0,
                        };
                        let Childrens::Twins { left, right } = childrens else {
                            outcome.changed = false;
                            outcome.removed_positions.truncate(output_start);
                            return Ok(());
                        };

                        let left_removed_start = outcome.removed_positions.len();
                        Self::_apply_appending(
                            child_required.clone(),
                            all_referenced,
                            left.as_mut(),
                            outcome,
                            arena,
                        )?;
                        let left_changed = outcome.changed;
                        let right_removed_start = outcome.removed_positions.len();
                        Self::_apply_appending(
                            child_required,
                            all_referenced,
                            right.as_mut(),
                            outcome,
                            arena,
                        )?;
                        changed = left_changed || outcome.changed;
                        (
                            old_left_outputs_len,
                            left_removed_start,
                            right_removed_start,
                        )
                    };
                    if changed {
                        let right_removed_end = outcome.removed_positions.len();
                        let left_removed_len = right_removed_start - left_removed_start;
                        if let Operator::Join(op) = operator {
                            match &mut op.on {
                                JoinCondition::On { on, filter } => {
                                    {
                                        let (left_removed_positions, right_removed_positions) =
                                            outcome.removed_positions
                                                [left_removed_start..right_removed_end]
                                                .split_at(left_removed_len);
                                        for (left_expr, right_expr) in on {
                                            remap_expr_positions(
                                                left_expr,
                                                left_removed_positions,
                                            )?;
                                            remap_expr_positions(
                                                right_expr,
                                                right_removed_positions,
                                            )?;
                                        }
                                    }
                                    Self::offset_removed_positions(
                                        &mut outcome.removed_positions
                                            [right_removed_start..right_removed_end],
                                        old_left_outputs_len,
                                    );
                                    if let Some(filter) = filter {
                                        let removed_positions = &outcome.removed_positions
                                            [left_removed_start..right_removed_end];
                                        if !removed_positions.is_empty() {
                                            remap_expr_positions(filter, removed_positions)?;
                                        }
                                    }
                                }
                                JoinCondition::None => {
                                    Self::offset_removed_positions(
                                        &mut outcome.removed_positions
                                            [right_removed_start..right_removed_end],
                                        old_left_outputs_len,
                                    );
                                }
                            }
                        } else if let Operator::MarkApply(op) = operator {
                            Self::offset_removed_positions(
                                &mut outcome.removed_positions
                                    [right_removed_start..right_removed_end],
                                old_left_outputs_len,
                            );
                            let removed_positions =
                                &outcome.removed_positions[left_removed_start..right_removed_end];
                            Self::remap_exprs_after_child_change(
                                op.predicates_mut().iter_mut(),
                                removed_positions,
                            )?;
                            outcome.removed_positions.truncate(right_removed_start);
                        } else {
                            Self::offset_removed_positions(
                                &mut outcome.removed_positions
                                    [right_removed_start..right_removed_end],
                                old_left_outputs_len,
                            );
                        }
                    } else {
                        outcome.removed_positions.truncate(output_start);
                    }
                } else if matches!(operator, Operator::Union(_) | Operator::SetMembership(_)) {
                    let mut child_required = required_columns;
                    Self::extend_operator_referenced_columns(operator, &mut child_required, arena)?;
                    changed |= Self::apply_twins(
                        child_required,
                        all_referenced,
                        childrens,
                        outcome,
                        output_start,
                        arena,
                    )?;
                    outcome.removed_positions.truncate(output_start);
                } else {
                    let child_start = outcome.removed_positions.len();
                    let child_changed = {
                        let mut child_required = required_columns;
                        Self::extend_operator_referenced_columns(
                            operator,
                            &mut child_required,
                            arena,
                        )?;
                        Self::apply_only_child(
                            child_required,
                            all_referenced,
                            childrens,
                            outcome,
                            child_start,
                            arena,
                        )?
                    };
                    if child_changed {
                        Self::remap_operator_after_child_change(
                            operator,
                            &outcome.removed_positions[child_start..],
                        )?;
                        changed = true;
                    }
                }
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) | Operator::FunctionScan(_) => {
                outcome.removed_positions.truncate(output_start);
            }
            Operator::Explain => {
                let child_start = outcome.removed_positions.len();
                let child_changed = Self::apply_only_child(
                    required_columns,
                    true,
                    childrens,
                    outcome,
                    child_start,
                    arena,
                )?;
                if child_changed {
                    Self::remap_operator_after_child_change(
                        operator,
                        &outcome.removed_positions[child_start..],
                    )?;
                    changed = true;
                }
                outcome.removed_positions.truncate(output_start);
            }
            // DDL Based on Other Plan
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_) => {
                let child_start = outcome.removed_positions.len();
                let child_changed = {
                    required_columns.clear();
                    Self::extend_operator_referenced_columns(
                        operator,
                        &mut required_columns,
                        arena,
                    )?;

                    Self::apply_only_child(
                        required_columns,
                        true,
                        childrens,
                        outcome,
                        child_start,
                        arena,
                    )?
                };
                if child_changed {
                    Self::remap_operator_after_child_change(
                        operator,
                        &outcome.removed_positions[child_start..],
                    )?;
                    changed = true;
                }
                outcome.removed_positions.truncate(output_start);
            }
            // DDL Single Plan
            Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::Describe(_) => {
                outcome.removed_positions.truncate(output_start);
            }
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(_) | Operator::CopyToFile(_) => {
                outcome.removed_positions.truncate(output_start);
            }
        }

        outcome.changed = changed;
        Ok(())
    }
}

impl NormalizationRule for ColumnPruning {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        let outcome = Self::_apply(
            ReferencedColumns::with_arena_capacity(arena),
            true,
            plan,
            arena,
        )?;
        Ok(outcome.changed)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::join::JoinCondition;
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan, PlanArena};

    fn optimize_column_pruning<S: crate::storage::Storage>(
        table_state: &crate::binder::test::TableState<S>,
        arena: &mut PlanArena,
        sql: &str,
    ) -> Result<LogicalPlan, DatabaseError> {
        let plan = table_state.plan_with_arena(sql, arena)?;

        HepOptimizerPipeline::builder()
            .before_batch(
                format!("column_pruning::{sql}"),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .build()
            .instantiate(plan)
            .find_best(None, arena)
    }

    fn contains_operator(plan: &LogicalPlan, predicate: impl Fn(&Operator) -> bool + Copy) -> bool {
        predicate(&plan.operator)
            || plan
                .childrens
                .iter()
                .any(|child| contains_operator(child, predicate))
    }

    fn collect_scan_columns(
        plan: &LogicalPlan,
        table_name: &str,
        arena: &PlanArena,
        scans: &mut Vec<Vec<String>>,
    ) {
        if let Operator::TableScan(op) = &plan.operator {
            if op.table_name.to_string() == table_name {
                scans.push(
                    op.columns
                        .iter()
                        .map(|column| arena.column(*column).name().to_string())
                        .collect(),
                );
            }
        }

        for child in plan.childrens.iter() {
            collect_scan_columns(child, table_name, arena, scans);
        }
    }

    fn assert_single_scan_columns(
        plan: &LogicalPlan,
        table_name: &str,
        arena: &PlanArena,
        expected: &[&str],
    ) {
        let mut scans = Vec::new();
        collect_scan_columns(plan, table_name, arena, &mut scans);
        assert_eq!(
            scans.len(),
            1,
            "expected exactly one scan for table {table_name}"
        );
        let expected = expected
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();
        assert_eq!(scans.pop().unwrap(), expected);
    }

    #[test]
    fn test_column_pruning_project_single_side() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let arena = PlanArena::new(&table_state.table_arena);
        let mut arena = arena;
        let best_plan = optimize_column_pruning(&table_state, &mut arena, "select c1 from t1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Project(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_filter_single_side() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let arena = PlanArena::new(&table_state.table_arena);
        let mut arena = arena;
        let best_plan =
            optimize_column_pruning(&table_state, &mut arena, "select c1 from t1 where c2 > 1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Filter(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1", "c2"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_aggregate_single_side() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let arena = PlanArena::new(&table_state.table_arena);
        let mut arena = arena;
        let best_plan =
            optimize_column_pruning(&table_state, &mut arena, "select sum(c1) from t1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Aggregate(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_sort_single_side() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let arena = PlanArena::new(&table_state.table_arena);
        let mut arena = arena;
        let best_plan =
            optimize_column_pruning(&table_state, &mut arena, "select c1 from t1 order by c2")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Sort(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1", "c2"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_limit_single_side() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let arena = PlanArena::new(&table_state.table_arena);
        let mut arena = arena;
        let best_plan =
            optimize_column_pruning(&table_state, &mut arena, "select c1 from t1 limit 1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Limit(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let plan = table_state
            .plan_with_arena("select c1, c3 from t1 left join t2 on c1 = c3", &mut arena)?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_column_pruning".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .build();
        let best_plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;

        assert!(matches!(best_plan.childrens.as_ref(), Childrens::Only(_)));
        match best_plan.operator {
            Operator::Project(op) => {
                assert_eq!(op.exprs.len(), 2);
            }
            _ => unreachable!("Should be a project operator"),
        }
        let join_op = best_plan.childrens.pop_only();
        match &join_op.operator {
            Operator::Join(op) => match &op.on {
                JoinCondition::On { on, filter } => {
                    assert_eq!(on.len(), 1);
                    assert!(filter.is_none());
                }
                _ => unreachable!("Should be a on condition"),
            },
            _ => unreachable!("Should be a join operator"),
        }
        assert!(matches!(
            join_op.childrens.as_ref(),
            Childrens::Twins { .. }
        ));

        for grandson_plan in join_op.childrens.iter() {
            match &grandson_plan.operator {
                Operator::TableScan(op) => {
                    assert_eq!(op.columns.len(), 1);
                }
                _ => unreachable!("Should be a scan operator"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_column_pruning_keeps_join_filter_columns_on_both_sides() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let best_plan = optimize_column_pruning(
            &table_state,
            &mut arena,
            "select c1 from t1 join t2 on c1 = c3 and c2 > c4",
        )?;

        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1", "c2"]);
        assert_single_scan_columns(&best_plan, "t2", &arena, &["c3", "c4"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_keeps_mark_apply_predicate_columns() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let best_plan = optimize_column_pruning(
            &table_state,
            &mut arena,
            "select c1 from t1 where c2 in (select c4 from t2)",
        )?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::MarkApply(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1", "c2"]);
        assert_single_scan_columns(&best_plan, "t2", &arena, &["c4"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_preserves_union_input_mapping() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let mut arena = PlanArena::new(&table_state.table_arena);
        let best_plan = optimize_column_pruning(
            &table_state,
            &mut arena,
            "select c1 from t1 union all select c3 from t2",
        )?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Union(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &arena, &["c1"]);
        assert_single_scan_columns(&best_plan, "t2", &arena, &["c3"]);

        Ok(())
    }
}
