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
use crate::expression::visitor::{walk_expr, Visitor};
use crate::expression::{HasCountStar, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::rule::normalization::{
    derive_position_remap_into, derive_retained_positions_into, remap_expr_positions,
    remap_exprs_positions,
};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::LogicalType;
use sqlparser::ast::CharLengthUnits;
use std::collections::HashSet;

#[derive(Clone)]
pub struct ColumnPruning;

impl ColumnPruning {
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

    fn references_any_column(
        expr: &ScalarExpression,
        column_references: &HashSet<&ColumnSummary>,
    ) -> bool {
        struct ReferenceChecker<'a> {
            column_references: &'a HashSet<&'a ColumnSummary>,
            found: bool,
        }

        impl Visitor<'_> for ReferenceChecker<'_> {
            fn visit(&mut self, expr: &ScalarExpression) -> Result<(), DatabaseError> {
                if self.found {
                    return Ok(());
                }
                if self
                    .column_references
                    .contains(expr.output_column().summary())
                {
                    self.found = true;
                    return Ok(());
                }

                walk_expr(self, expr)
            }
        }

        let mut checker = ReferenceChecker {
            column_references,
            found: false,
        };
        checker.visit(expr).unwrap();
        checker.found
    }

    fn clear_exprs(column_references: &HashSet<&ColumnSummary>, exprs: &mut Vec<ScalarExpression>) {
        exprs.retain(|expr| Self::references_any_column(expr, column_references))
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

    fn apply_only_child<'a>(
        referenced_columns: HashSet<&'a ColumnSummary>,
        all_referenced: bool,
        childrens: &mut Childrens,
        retained_positions: &mut Vec<usize>,
        removed_positions: &mut Vec<usize>,
    ) -> Result<bool, DatabaseError> {
        let Childrens::Only(child) = childrens else {
            return Ok(false);
        };
        let child = child.as_mut();
        let old_outputs = child.output_schema().clone();
        let changed = Self::_apply(referenced_columns, all_referenced, child)?;
        if !changed {
            return Ok(false);
        }

        child.reset_output_schema_cache_recursive();
        let new_outputs = child.output_schema().clone();
        derive_retained_positions_into(&old_outputs, &new_outputs, retained_positions);
        derive_position_remap_into(old_outputs.len(), retained_positions, removed_positions);

        Ok(true)
    }

    fn apply_join_children<'a>(
        referenced_columns: HashSet<&'a ColumnSummary>,
        all_referenced: bool,
        childrens: &mut Childrens,
        left_retained_positions: &mut Vec<usize>,
        right_retained_positions: &mut Vec<usize>,
        left_removed_positions: &mut Vec<usize>,
        right_removed_positions: &mut Vec<usize>,
    ) -> Result<bool, DatabaseError> {
        let Childrens::Twins { left, right } = childrens else {
            return Ok(false);
        };
        let left = left.as_mut();
        let right = right.as_mut();

        let old_left_outputs = left.output_schema().clone();
        let old_right_outputs = right.output_schema().clone();

        let left_changed = Self::_apply(referenced_columns.clone(), all_referenced, left)?;
        let right_changed = Self::_apply(referenced_columns, all_referenced, right)?;
        if !left_changed && !right_changed {
            return Ok(false);
        }

        if left_changed {
            left.reset_output_schema_cache_recursive();
            let new_left_outputs = left.output_schema().clone();
            derive_retained_positions_into(
                &old_left_outputs,
                &new_left_outputs,
                left_retained_positions,
            );
            derive_position_remap_into(
                old_left_outputs.len(),
                left_retained_positions,
                left_removed_positions,
            );
        } else {
            left_retained_positions.clear();
            left_removed_positions.clear();
        }

        if right_changed {
            right.reset_output_schema_cache_recursive();
            let new_right_outputs = right.output_schema().clone();
            derive_retained_positions_into(
                &old_right_outputs,
                &new_right_outputs,
                right_retained_positions,
            );
            derive_position_remap_into(
                old_right_outputs.len(),
                right_retained_positions,
                right_removed_positions,
            );
        } else {
            right_retained_positions.clear();
            right_removed_positions.clear();
        }

        Ok(true)
    }

    fn apply_twins<'a>(
        referenced_columns: HashSet<&'a ColumnSummary>,
        all_referenced: bool,
        childrens: &mut Childrens,
    ) -> Result<bool, DatabaseError> {
        let Childrens::Twins { left, right } = childrens else {
            return Ok(false);
        };

        let left_changed = Self::_apply(referenced_columns.clone(), all_referenced, left.as_mut())?;
        let right_changed = Self::_apply(referenced_columns, all_referenced, right.as_mut())?;

        Ok(left_changed || right_changed)
    }

    fn _apply<'a>(
        required_columns: HashSet<&'a ColumnSummary>,
        all_referenced: bool,
        plan: &mut LogicalPlan,
    ) -> Result<bool, DatabaseError> {
        let mut changed = false;
        let mut left_retained_positions = Vec::new();
        let mut right_retained_positions = Vec::new();
        let mut left_removed_positions = Vec::new();
        let mut right_removed_positions = Vec::new();
        let (operator, childrens) = (&mut plan.operator, plan.childrens.as_mut());

        match operator {
            Operator::Aggregate(op) => {
                let required_columns = required_columns;
                if !all_referenced {
                    let before = op.agg_calls.len();
                    Self::clear_exprs(&required_columns, &mut op.agg_calls);
                    if op.agg_calls.len() != before {
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
                let child_changed = {
                    let mut child_required = if op.is_distinct {
                        required_columns
                    } else {
                        HashSet::new()
                    };
                    Self::extend_expr_referenced_columns(
                        op.agg_calls.iter().chain(op.groupby_exprs.iter()),
                        &mut child_required,
                    );

                    Self::apply_only_child(
                        child_required,
                        false,
                        childrens,
                        &mut left_retained_positions,
                        &mut left_removed_positions,
                    )?
                };
                if child_changed {
                    Self::remap_operator_after_child_change(operator, &left_removed_positions)?;
                    changed = true;
                }
            }
            Operator::Project(op) => {
                let required_columns = required_columns;
                let mut has_count_star = HasCountStar::default();
                for expr in &op.exprs {
                    has_count_star.visit(expr)?;
                }
                if !has_count_star.value {
                    if !all_referenced {
                        let before = op.exprs.len();
                        Self::clear_exprs(&required_columns, &mut op.exprs);
                        if op.exprs.len() != before {
                            changed = true;
                        }
                    }
                    let child_changed = {
                        let mut child_required = HashSet::new();
                        Self::extend_expr_referenced_columns(op.exprs.iter(), &mut child_required);

                        Self::apply_only_child(
                            child_required,
                            false,
                            childrens,
                            &mut left_retained_positions,
                            &mut left_removed_positions,
                        )?
                    };
                    if child_changed {
                        Self::remap_operator_after_child_change(operator, &left_removed_positions)?;
                        changed = true;
                    }
                }
            }
            Operator::TableScan(op) => {
                let required_columns = required_columns;
                if !all_referenced {
                    let before = op.columns.len();
                    op.columns
                        .retain(|_, column| required_columns.contains(column.summary()));
                    if op.columns.len() != before {
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
                    let (child_changed, old_left_outputs_len) = {
                        let mut child_required = required_columns.clone();
                        Self::extend_operator_referenced_columns(operator, &mut child_required);
                        let old_left_outputs_len = match childrens {
                            Childrens::Twins { left, .. } => left.output_schema().len(),
                            _ => 0,
                        };
                        let child_changed = Self::apply_join_children(
                            child_required,
                            all_referenced,
                            childrens,
                            &mut left_retained_positions,
                            &mut right_retained_positions,
                            &mut left_removed_positions,
                            &mut right_removed_positions,
                        )?;
                        (child_changed, old_left_outputs_len)
                    };
                    if child_changed {
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
                                            let mut removed_positions = Vec::with_capacity(
                                                left_removed_positions.len()
                                                    + right_removed_positions.len(),
                                            );
                                            removed_positions
                                                .extend_from_slice(&left_removed_positions);
                                            removed_positions.extend(
                                                right_removed_positions.iter().map(|position| {
                                                    position + old_left_outputs_len
                                                }),
                                            );
                                            remap_expr_positions(filter, &removed_positions)?;
                                        }
                                    }
                                }
                                JoinCondition::None => {}
                            }
                        }
                        changed = true;
                    }
                } else if matches!(operator, Operator::Union(_) | Operator::Except(_)) {
                    let mut child_required = required_columns;
                    Self::extend_operator_referenced_columns(operator, &mut child_required);
                    changed |= Self::apply_twins(child_required, all_referenced, childrens)?;
                } else {
                    let child_changed = {
                        let mut child_required = required_columns;
                        Self::extend_operator_referenced_columns(operator, &mut child_required);
                        Self::apply_only_child(
                            child_required,
                            all_referenced,
                            childrens,
                            &mut left_retained_positions,
                            &mut left_removed_positions,
                        )?
                    };
                    if child_changed {
                        Self::remap_operator_after_child_change(operator, &left_removed_positions)?;
                        changed = true;
                    }
                }
            }
            // Last Operator
            Operator::Dummy | Operator::Values(_) | Operator::FunctionScan(_) => (),
            Operator::Explain => {
                let child_changed = Self::apply_only_child(
                    required_columns,
                    true,
                    childrens,
                    &mut left_retained_positions,
                    &mut left_removed_positions,
                )?;
                if child_changed {
                    Self::remap_operator_after_child_change(operator, &left_removed_positions)?;
                    changed = true;
                }
            }
            // DDL Based on Other Plan
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_) => {
                let child_changed = {
                    let mut child_required = HashSet::new();
                    Self::extend_operator_referenced_columns(operator, &mut child_required);

                    Self::apply_only_child(
                        child_required,
                        true,
                        childrens,
                        &mut left_retained_positions,
                        &mut left_removed_positions,
                    )?
                };
                if child_changed {
                    Self::remap_operator_after_child_change(operator, &left_removed_positions)?;
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

        Ok(changed)
    }
}

impl NormalizationRule for ColumnPruning {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        Self::_apply(HashSet::<&ColumnSummary>::new(), true, plan)
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
    use crate::planner::Childrens;
    use crate::storage::rocksdb::RocksTransaction;

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
