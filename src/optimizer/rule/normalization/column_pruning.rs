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

use crate::catalog::ColumnSummary;
use crate::errors::DatabaseError;
use crate::expression::agg::AggKind;
use crate::expression::visitor::Visitor;
use crate::expression::{HasCountStar, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::rule::normalization::{remap_expr_positions, remap_exprs_positions};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use bumpalo::Bump;
use sqlparser::ast::CharLengthUnits;
use std::collections::HashSet;

#[derive(Clone)]
pub struct ColumnPruning;

type BumpUsizeVec<'bump> = bumpalo::collections::Vec<'bump, usize>;

struct ApplyOutcome<'bump> {
    changed: bool,
    removed_positions: BumpUsizeVec<'bump>,
}

impl<'bump> ApplyOutcome<'bump> {
    fn new(arena: &'bump Bump) -> Self {
        Self {
            changed: false,
            removed_positions: BumpUsizeVec::new_in(arena),
        }
    }
}

struct JoinChildrenOutcome<'bump> {
    changed: bool,
    left_removed_positions: BumpUsizeVec<'bump>,
    right_removed_positions: BumpUsizeVec<'bump>,
}

impl<'bump> JoinChildrenOutcome<'bump> {
    fn new(arena: &'bump Bump) -> Self {
        Self {
            changed: false,
            left_removed_positions: BumpUsizeVec::new_in(arena),
            right_removed_positions: BumpUsizeVec::new_in(arena),
        }
    }
}

impl ColumnPruning {
    fn copy_removed_positions<'bump>(
        removed_positions: &[usize],
        arena: &'bump Bump,
    ) -> BumpUsizeVec<'bump> {
        let mut copied = BumpUsizeVec::with_capacity_in(removed_positions.len(), arena);
        copied.extend_from_slice(removed_positions);
        copied
    }

    fn extend_operator_referenced_columns<'a>(
        operator: &'a Operator,
        referenced_columns: &mut HashSet<&'a ColumnSummary>,
    ) {
        match operator {
            Operator::Aggregate(op) => {
                Self::extend_expr_referenced_columns(
                    op.agg_calls.iter().chain(op.groupby_exprs.iter()),
                    referenced_columns,
                );
            }
            Operator::Filter(op) => {
                Self::extend_expr_referenced_columns([&op.predicate], referenced_columns);
            }
            Operator::Join(op) => {
                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        Self::extend_expr_referenced_columns(
                            [left_expr, right_expr],
                            referenced_columns,
                        );
                    }
                    if let Some(filter_expr) = filter {
                        Self::extend_expr_referenced_columns([filter_expr], referenced_columns);
                    }
                }
            }
            Operator::Project(op) => {
                Self::extend_expr_referenced_columns(op.exprs.iter(), referenced_columns);
            }
            Operator::TableScan(op) => {
                referenced_columns.extend(op.columns.values().map(|column| column.summary()));
            }
            Operator::FunctionScan(op) => {
                Self::extend_expr_referenced_columns(
                    op.table_function.args.iter(),
                    referenced_columns,
                );
            }
            Operator::Sort(op) => {
                Self::extend_expr_referenced_columns(
                    op.sort_fields.iter().map(|field| &field.expr),
                    referenced_columns,
                );
            }
            Operator::TopK(op) => {
                Self::extend_expr_referenced_columns(
                    op.sort_fields.iter().map(|field| &field.expr),
                    referenced_columns,
                );
            }
            Operator::Values(op) => {
                referenced_columns.extend(op.schema_ref.iter().map(|column| column.summary()));
            }
            Operator::Union(op) => {
                referenced_columns.extend(
                    op.left_schema_ref
                        .iter()
                        .chain(op._right_schema_ref.iter())
                        .map(|column| column.summary()),
                );
            }
            Operator::Except(op) => {
                referenced_columns.extend(
                    op.left_schema_ref
                        .iter()
                        .chain(op._right_schema_ref.iter())
                        .map(|column| column.summary()),
                );
            }
            Operator::Delete(op) => {
                referenced_columns.extend(op.primary_keys.iter().map(|column| column.summary()));
            }
            Operator::Dummy
            | Operator::Limit(_)
            | Operator::ScalarSubquery(_)
            | Operator::Analyze(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Update(_)
            | Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_) => {}
        }
    }

    fn extend_expr_referenced_columns<'a>(
        exprs: impl IntoIterator<Item = &'a ScalarExpression>,
        referenced_columns: &mut HashSet<&'a ColumnSummary>,
    ) {
        struct ColumnSummaryCollector<'a, 'b> {
            referenced_columns: &'b mut HashSet<&'a ColumnSummary>,
        }

        impl<'a> Visitor<'a> for ColumnSummaryCollector<'a, '_> {
            fn visit_column_ref(
                &mut self,
                column: &'a crate::catalog::ColumnRef,
            ) -> Result<(), DatabaseError> {
                self.referenced_columns.insert(column.summary());
                Ok(())
            }
        }

        let mut collector = ColumnSummaryCollector { referenced_columns };
        for expr in exprs {
            collector.visit(expr).unwrap();
        }
    }

    fn output_column_is_required(
        expr: &ScalarExpression,
        column_references: &HashSet<&ColumnSummary>,
    ) -> bool {
        column_references.contains(expr.output_column().summary())
    }

    fn clear_exprs(
        column_references: &HashSet<&ColumnSummary>,
        exprs: &mut Vec<ScalarExpression>,
        removed_positions: &mut BumpUsizeVec<'_>,
    ) {
        removed_positions.clear();
        let mut position = 0;
        exprs.retain(|expr| {
            let keep = Self::output_column_is_required(expr, column_references);
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
        match operator {
            Operator::Aggregate(op) => {
                Self::remap_exprs_after_child_change(
                    op.agg_calls.iter_mut().chain(op.groupby_exprs.iter_mut()),
                    removed_positions,
                )?;
            }
            Operator::Filter(op) => {
                remap_expr_positions(&mut op.predicate, removed_positions)?;
            }
            Operator::Project(op) => {
                remap_exprs_positions(op.exprs.iter_mut(), removed_positions)?;
            }
            Operator::ScalarSubquery(_) => {}
            Operator::Sort(op) => {
                Self::remap_exprs_after_child_change(
                    op.sort_fields.iter_mut().map(|field| &mut field.expr),
                    removed_positions,
                )?;
            }
            Operator::TopK(op) => {
                Self::remap_exprs_after_child_change(
                    op.sort_fields.iter_mut().map(|field| &mut field.expr),
                    removed_positions,
                )?;
            }
            Operator::Update(op) => {
                Self::remap_exprs_after_child_change(
                    op.value_exprs.iter_mut().map(|(_, expr)| expr),
                    removed_positions,
                )?;
            }
            Operator::Limit(_)
            | Operator::Explain
            | Operator::Insert(_)
            | Operator::Delete(_)
            | Operator::Analyze(_)
            | Operator::Dummy
            | Operator::TableScan(_)
            | Operator::Join(_)
            | Operator::Values(_)
            | Operator::FunctionScan(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Describe(_)
            | Operator::Union(_)
            | Operator::Except(_)
            | Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_) => {}
        }

        Ok(())
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

    fn apply_only_child<'bump>(
        referenced_columns: HashSet<&ColumnSummary>,
        all_referenced: bool,
        childrens: &mut Childrens,
        arena: &'bump Bump,
    ) -> Result<ApplyOutcome<'bump>, DatabaseError> {
        let Childrens::Only(child) = childrens else {
            return Ok(ApplyOutcome::new(arena));
        };
        Self::_apply(referenced_columns, all_referenced, child.as_mut(), arena)
    }

    fn apply_join_children<'bump>(
        referenced_columns: HashSet<&ColumnSummary>,
        all_referenced: bool,
        childrens: &mut Childrens,
        arena: &'bump Bump,
    ) -> Result<JoinChildrenOutcome<'bump>, DatabaseError> {
        let Childrens::Twins { left, right } = childrens else {
            return Ok(JoinChildrenOutcome::new(arena));
        };
        let left_outcome = Self::_apply(
            referenced_columns.clone(),
            all_referenced,
            left.as_mut(),
            arena,
        )?;
        let right_outcome =
            Self::_apply(referenced_columns, all_referenced, right.as_mut(), arena)?;

        Ok(JoinChildrenOutcome {
            changed: left_outcome.changed || right_outcome.changed,
            left_removed_positions: left_outcome.removed_positions,
            right_removed_positions: right_outcome.removed_positions,
        })
    }

    #[allow(clippy::needless_lifetimes)]
    fn apply_twins<'bump>(
        referenced_columns: HashSet<&ColumnSummary>,
        all_referenced: bool,
        childrens: &mut Childrens,
        arena: &'bump Bump,
    ) -> Result<bool, DatabaseError> {
        let Childrens::Twins { left, right } = childrens else {
            return Ok(false);
        };

        let left_changed = Self::_apply(
            referenced_columns.clone(),
            all_referenced,
            left.as_mut(),
            arena,
        )?
        .changed;
        let right_changed =
            Self::_apply(referenced_columns, all_referenced, right.as_mut(), arena)?.changed;

        Ok(left_changed || right_changed)
    }

    fn merge_removed_positions<'bump>(
        left_removed_positions: &[usize],
        right_removed_positions: &[usize],
        right_offset: usize,
        arena: &'bump Bump,
    ) -> BumpUsizeVec<'bump> {
        let mut removed_positions = BumpUsizeVec::with_capacity_in(
            left_removed_positions.len() + right_removed_positions.len(),
            arena,
        );
        removed_positions.extend_from_slice(left_removed_positions);
        removed_positions.extend(
            right_removed_positions
                .iter()
                .map(|position| position + right_offset),
        );
        removed_positions
    }

    fn _apply<'bump>(
        required_columns: HashSet<&ColumnSummary>,
        all_referenced: bool,
        plan: &mut LogicalPlan,
        arena: &'bump Bump,
    ) -> Result<ApplyOutcome<'bump>, DatabaseError> {
        let mut changed = false;
        let mut output_removed_positions = BumpUsizeVec::new_in(arena);
        let (operator, childrens) = (&mut plan.operator, plan.childrens.as_mut());

        match operator {
            Operator::Aggregate(op) => {
                if !all_referenced {
                    Self::clear_exprs(
                        &required_columns,
                        &mut op.agg_calls,
                        &mut output_removed_positions,
                    );
                    if !output_removed_positions.is_empty() {
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
                }
                let child_outcome = {
                    let mut child_required = if op.is_distinct {
                        required_columns
                    } else {
                        HashSet::new()
                    };
                    Self::extend_expr_referenced_columns(
                        op.agg_calls.iter().chain(op.groupby_exprs.iter()),
                        &mut child_required,
                    );

                    Self::apply_only_child(child_required, false, childrens, arena)?
                };
                if child_outcome.changed {
                    Self::remap_operator_after_child_change(
                        operator,
                        &child_outcome.removed_positions,
                    )?;
                    changed = true;
                }
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
                            &mut output_removed_positions,
                        );
                        if !output_removed_positions.is_empty() {
                            changed = true;
                        }
                    }
                    let child_outcome = {
                        let mut child_required = HashSet::new();
                        Self::extend_expr_referenced_columns(op.exprs.iter(), &mut child_required);

                        Self::apply_only_child(child_required, false, childrens, arena)?
                    };
                    if child_outcome.changed {
                        Self::remap_operator_after_child_change(
                            operator,
                            &child_outcome.removed_positions,
                        )?;
                        changed = true;
                    }
                }
            }
            Operator::TableScan(op) => {
                if !all_referenced {
                    output_removed_positions.clear();
                    op.columns.retain(|position, column| {
                        let keep = required_columns.contains(column.summary());
                        if !keep {
                            output_removed_positions.push(*position);
                        }
                        keep
                    });
                    if !output_removed_positions.is_empty() {
                        changed = true;
                    }
                }
            }
            Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::ScalarSubquery(_)
            | Operator::Join(_)
            | Operator::Filter(_)
            | Operator::Union(_)
            | Operator::Except(_)
            | Operator::TopK(_) => {
                if matches!(operator, Operator::Join(_)) {
                    let (child_outcome, old_left_outputs_len) = {
                        let mut child_required = required_columns.clone();
                        Self::extend_operator_referenced_columns(operator, &mut child_required);
                        let old_left_outputs_len = match childrens {
                            Childrens::Twins { left, .. } => left.output_schema().len(),
                            _ => 0,
                        };
                        let child_outcome = Self::apply_join_children(
                            child_required,
                            all_referenced,
                            childrens,
                            arena,
                        )?;
                        (child_outcome, old_left_outputs_len)
                    };
                    if child_outcome.changed {
                        let JoinChildrenOutcome {
                            changed: _,
                            left_removed_positions,
                            right_removed_positions,
                        } = child_outcome;
                        if let Operator::Join(op) = operator {
                            match &mut op.on {
                                JoinCondition::On { on, filter } => {
                                    for (left_expr, right_expr) in on {
                                        remap_expr_positions(left_expr, &left_removed_positions)?;
                                        remap_expr_positions(right_expr, &right_removed_positions)?;
                                    }
                                    if let Some(filter) = filter {
                                        if !left_removed_positions.is_empty()
                                            || !right_removed_positions.is_empty()
                                        {
                                            let removed_positions = Self::merge_removed_positions(
                                                &left_removed_positions,
                                                &right_removed_positions,
                                                old_left_outputs_len,
                                                arena,
                                            );
                                            remap_expr_positions(filter, &removed_positions)?;
                                        }
                                    }
                                }
                                JoinCondition::None => {}
                            }
                            if !matches!(op.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
                                output_removed_positions = Self::merge_removed_positions(
                                    &left_removed_positions,
                                    &right_removed_positions,
                                    old_left_outputs_len,
                                    arena,
                                );
                            } else {
                                output_removed_positions =
                                    Self::copy_removed_positions(&left_removed_positions, arena);
                            }
                        }
                        changed = true;
                    }
                } else if matches!(operator, Operator::Union(_) | Operator::Except(_)) {
                    let mut child_required = required_columns;
                    Self::extend_operator_referenced_columns(operator, &mut child_required);
                    changed |= Self::apply_twins(child_required, all_referenced, childrens, arena)?;
                } else {
                    let child_outcome = {
                        let mut child_required = required_columns;
                        Self::extend_operator_referenced_columns(operator, &mut child_required);
                        Self::apply_only_child(child_required, all_referenced, childrens, arena)?
                    };
                    if child_outcome.changed {
                        let removed_positions = child_outcome.removed_positions;
                        Self::remap_operator_after_child_change(operator, &removed_positions)?;
                        output_removed_positions = removed_positions;
                        changed = true;
                    }
                }
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) | Operator::FunctionScan(_) => (),
            Operator::Explain => {
                let child_outcome =
                    Self::apply_only_child(required_columns, true, childrens, arena)?;
                if child_outcome.changed {
                    Self::remap_operator_after_child_change(
                        operator,
                        &child_outcome.removed_positions,
                    )?;
                    changed = true;
                }
            }
            // DDL Based on Other Plan
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_) => {
                let child_outcome = {
                    let mut child_required = HashSet::new();
                    Self::extend_operator_referenced_columns(operator, &mut child_required);

                    Self::apply_only_child(child_required, true, childrens, arena)?
                };
                if child_outcome.changed {
                    Self::remap_operator_after_child_change(
                        operator,
                        &child_outcome.removed_positions,
                    )?;
                    changed = true;
                }
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
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::Describe(_) => (),
        }

        Ok(ApplyOutcome {
            changed,
            removed_positions: output_removed_positions,
        })
    }
}

impl NormalizationRule for ColumnPruning {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        let arena = Bump::new();
        let outcome = Self::_apply(HashSet::<&ColumnSummary>::new(), true, plan, &arena)?;
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
    use crate::planner::{Childrens, LogicalPlan};
    use crate::storage::rocksdb::RocksTransaction;

    fn optimize_column_pruning(sql: &str) -> Result<LogicalPlan, DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan(sql)?;

        HepOptimizerPipeline::builder()
            .before_batch(
                format!("column_pruning::{sql}"),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .build()
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)
    }

    fn contains_operator(plan: &LogicalPlan, predicate: impl Fn(&Operator) -> bool + Copy) -> bool {
        predicate(&plan.operator)
            || plan
                .childrens
                .iter()
                .any(|child| contains_operator(child, predicate))
    }

    fn collect_scan_columns(plan: &LogicalPlan, table_name: &str, scans: &mut Vec<Vec<String>>) {
        if let Operator::TableScan(op) = &plan.operator {
            if op.table_name.to_string() == table_name {
                scans.push(
                    op.columns
                        .values()
                        .map(|column| column.name().to_string())
                        .collect(),
                );
            }
        }

        for child in plan.childrens.iter() {
            collect_scan_columns(child, table_name, scans);
        }
    }

    fn assert_single_scan_columns(plan: &LogicalPlan, table_name: &str, expected: &[&str]) {
        let mut scans = Vec::new();
        collect_scan_columns(plan, table_name, &mut scans);
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
        let best_plan = optimize_column_pruning("select c1 from t1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Project(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &["c1"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_filter_single_side() -> Result<(), DatabaseError> {
        let best_plan = optimize_column_pruning("select c1 from t1 where c2 > 1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Filter(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &["c1", "c2"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_aggregate_single_side() -> Result<(), DatabaseError> {
        let best_plan = optimize_column_pruning("select sum(c1) from t1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Aggregate(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &["c1"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_sort_single_side() -> Result<(), DatabaseError> {
        let best_plan = optimize_column_pruning("select c1 from t1 order by c2")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Sort(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &["c1", "c2"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning_limit_single_side() -> Result<(), DatabaseError> {
        let best_plan = optimize_column_pruning("select c1 from t1 limit 1")?;

        assert!(contains_operator(&best_plan, |op| matches!(
            op,
            Operator::Limit(_)
        )));
        assert_single_scan_columns(&best_plan, "t1", &["c1"]);

        Ok(())
    }

    #[test]
    fn test_column_pruning() -> Result<(), DatabaseError> {
        let table_state = build_t1_table()?;
        let plan = table_state.plan("select c1, c3 from t1 left join t2 on c1 = c3")?;

        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "test_column_pruning".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![NormalizationRuleImpl::ColumnPruning],
            )
            .build();
        let best_plan = pipeline
            .instantiate(plan)
            .find_best::<RocksTransaction>(None)?;

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
}
