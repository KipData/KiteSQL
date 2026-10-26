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

use crate::catalog::{ColumnRef, TableName};
use crate::errors::DatabaseError;
use crate::expression::visitor::ExprVisitor;
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::visitor::{ChildrenVisit, OperatorVisitor};
use crate::planner::operator::{Operator, PhysicalOption, RowCountOption};
use crate::planner::{Childrens, LogicalPlan, MetaArena, PlanArena};
use crate::types::ColumnId;
use std::mem;

#[allow(dead_code)]
pub fn child_count(plan: &LogicalPlan) -> usize {
    match plan.childrens.as_ref() {
        Childrens::None => 0,
        Childrens::Only(_) => 1,
        Childrens::Twins { .. } => 2,
    }
}

#[allow(dead_code)]
pub fn only_child(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan.childrens.as_ref() {
        Childrens::Only(child) => Some(child.as_ref()),
        _ => None,
    }
}

pub fn only_child_mut(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    match plan.childrens.as_mut() {
        Childrens::Only(child) => Some(child.as_mut()),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn left_child_mut(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    match plan.childrens.as_mut() {
        Childrens::Only(child) => Some(child.as_mut()),
        Childrens::Twins { left, .. } => Some(left.as_mut()),
        Childrens::None => None,
    }
}

#[allow(dead_code)]
pub fn right_child_mut(plan: &mut LogicalPlan) -> Option<&mut LogicalPlan> {
    match plan.childrens.as_mut() {
        Childrens::Twins { right, .. } => Some(right.as_mut()),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn child(plan: &LogicalPlan, idx: usize) -> Option<&LogicalPlan> {
    match (plan.childrens.as_ref(), idx) {
        (Childrens::Only(child), 0) => Some(child.as_ref()),
        (Childrens::Twins { left, .. }, 0) => Some(left.as_ref()),
        (Childrens::Twins { right, .. }, 1) => Some(right.as_ref()),
        _ => None,
    }
}

pub fn child_mut(plan: &mut LogicalPlan, idx: usize) -> Option<&mut LogicalPlan> {
    match (plan.childrens.as_mut(), idx) {
        (Childrens::Only(child), 0) => Some(child.as_mut()),
        (Childrens::Twins { left, .. }, 0) => Some(left.as_mut()),
        (Childrens::Twins { right, .. }, 1) => Some(right.as_mut()),
        _ => None,
    }
}

#[allow(dead_code)]
pub fn children(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    match plan.childrens.as_ref() {
        Childrens::None => vec![],
        Childrens::Only(child) => vec![child.as_ref()],
        Childrens::Twins { left, right } => vec![left.as_ref(), right.as_ref()],
    }
}

pub(crate) fn predicate_scan_mut<'plan>(
    plan: &'plan mut LogicalPlan,
    arena: &PlanArena,
) -> Option<(
    &'plan mut TableScanOperator,
    &'plan mut Option<PhysicalOption>,
)> {
    match (&mut plan.operator, plan.childrens.as_mut()) {
        (Operator::TableScan(scan), _) if scan.limit == (None, None) => {
            Some((scan, &mut plan.physical_option))
        }
        (Operator::Sort(_), Childrens::Only(input)) => predicate_scan_mut(input, arena),
        (Operator::ScalarQueryInit(init), Childrens::Twins { left, .. })
            if init.param_bindings.is_empty()
                && matches!(
                    arena.expression(init.value),
                    ScalarExpression::InitValue { .. }
                ) =>
        {
            predicate_scan_mut(left, arena)
        }
        _ => None,
    }
}

pub fn replace_with_only_child(plan: &mut LogicalPlan) -> bool {
    if let Childrens::Only(child) = take_childrens(plan) {
        *plan = *child;
        true
    } else {
        false
    }
}

pub fn wrap_child_with(plan: &mut LogicalPlan, child_idx: usize, operator: Operator) -> bool {
    if let Some(slot) = child_mut(plan, child_idx) {
        let previous = mem::replace(slot, LogicalPlan::new(operator, Childrens::None));
        *slot.childrens = Childrens::Only(Box::new(previous));
        plan.reset_output_schema_cache();
        true
    } else {
        false
    }
}

fn take_childrens(plan: &mut LogicalPlan) -> Childrens {
    mem::replace(&mut *plan.childrens, Childrens::None)
}

struct ScanColumnVisitor<'plan, 'arena, A> {
    column: Option<ColumnRef>,
    source: Option<(&'plan TableName, ColumnId)>,
    arena: &'arena A,
}

impl<'plan, A: MetaArena> OperatorVisitor<'plan> for ScanColumnVisitor<'plan, '_, A> {
    fn visit_plan(&mut self, plan: &'plan LogicalPlan) -> Result<(), DatabaseError> {
        if self.column.is_none() {
            return Ok(());
        }
        self.visit_operator(
            &plan.operator,
            plan.physical_option.as_ref(),
            &mut ChildrenVisit::empty(),
        )?;
        let follow = match &plan.operator {
            Operator::Project(_) | Operator::Filter(_) => true,
            Operator::Join(_) | Operator::Aggregate(_) | Operator::Limit(_) | Operator::TopK(_) => {
                false
            }
            _ => plan
                .physical_option
                .as_ref()
                .is_some_and(|option| matches!(option.row_count, RowCountOption::Follow)),
        };
        if follow && self.column.is_some() {
            self.visit_children(&plan.childrens)?;
        }
        Ok(())
    }

    fn visit_children(&mut self, childrens: &'plan Childrens) -> Result<(), DatabaseError> {
        match childrens {
            Childrens::Only(input) | Childrens::Twins { left: input, .. } => self.visit_plan(input),
            Childrens::None => Ok(()),
        }
    }

    fn visit_table_scan(&mut self, scan: &'plan TableScanOperator) -> Result<(), DatabaseError> {
        if scan.limit == (None, None) {
            if let Some(source) = self.column.and_then(|column| {
                scan.columns
                    .iter()
                    .find(|source| self.arena.same_column(**source, column))
            }) {
                self.source = self
                    .arena
                    .column(*source)
                    .id()
                    .map(|id| (&scan.table_name, id));
            }
        }
        self.column = None;
        Ok(())
    }

    fn visit_project(
        &mut self,
        project: &'plan ProjectOperator,
        _children: &mut ChildrenVisit<'plan>,
    ) -> Result<(), DatabaseError> {
        let Some(column) = self.column.take() else {
            return Ok(());
        };
        let arena = self.arena;
        for &candidate in &project.exprs {
            let source = match arena.expression(candidate) {
                ScalarExpression::Alias {
                    expr,
                    alias: AliasType::Name(_),
                } if arena.column(column).id().is_none()
                    && arena.column(column).table_name().is_none()
                    && candidate.output_name(arena) == arena.column(column).name() =>
                {
                    *expr
                }
                ScalarExpression::Alias {
                    expr,
                    alias: AliasType::Expr(alias),
                } if matches!(arena.expression(*alias), ScalarExpression::ColumnRef { column: target, .. }
                        if arena.same_column(*target, column)) =>
                {
                    *expr
                }
                ScalarExpression::ColumnRef { column: target, .. }
                    if arena.same_column(*target, column) =>
                {
                    candidate
                }
                _ => continue,
            };
            return ExprVisitor::visit(self, source, arena);
        }
        Ok(())
    }
}

impl<A: MetaArena> ExprVisitor<A> for ScanColumnVisitor<'_, '_, A> {
    fn visit_expression(
        &mut self,
        expr: &ScalarExpression,
        _arena: &A,
    ) -> Result<bool, DatabaseError> {
        Ok(matches!(
            expr,
            ScalarExpression::Alias { .. } | ScalarExpression::ColumnRef { .. }
        ))
    }

    fn visit_column_ref(&mut self, column: &ColumnRef, _: usize) -> Result<(), DatabaseError> {
        self.column = Some(*column);
        Ok(())
    }
}

pub(crate) fn scan_column<'plan>(
    plan: &'plan LogicalPlan,
    column: ColumnRef,
    arena: &PlanArena,
) -> Result<Option<(&'plan TableName, ColumnId)>, DatabaseError> {
    let mut visitor = ScanColumnVisitor {
        column: Some(column),
        source: None,
        arena,
    };
    visitor.visit_plan(plan)?;
    Ok(visitor.source)
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(operator: Operator) -> LogicalPlan {
        LogicalPlan::new(operator, Childrens::None)
    }

    #[test]
    fn child_accessors_cover_all_child_shapes() {
        let mut none = leaf(Operator::Dummy);
        assert_eq!(child_count(&none), 0);
        assert!(only_child(&none).is_none());
        assert!(only_child_mut(&mut none).is_none());
        assert!(left_child_mut(&mut none).is_none());
        assert!(right_child_mut(&mut none).is_none());
        assert!(child(&none, 0).is_none());
        assert!(children(&none).is_empty());

        let mut only = LogicalPlan::new(
            Operator::Dummy,
            Childrens::Only(Box::new(leaf(Operator::ShowTable))),
        );
        assert_eq!(child_count(&only), 1);
        assert!(matches!(
            only_child(&only).unwrap().operator,
            Operator::ShowTable
        ));
        assert!(only_child_mut(&mut only).is_some());
        assert!(left_child_mut(&mut only).is_some());
        assert!(right_child_mut(&mut only).is_none());
        assert!(matches!(
            child(&only, 0).unwrap().operator,
            Operator::ShowTable
        ));
        assert!(child(&only, 1).is_none());
        assert_eq!(children(&only).len(), 1);

        let mut twins = LogicalPlan::new(
            Operator::Dummy,
            Childrens::Twins {
                left: Box::new(leaf(Operator::ShowTable)),
                right: Box::new(leaf(Operator::ShowView)),
            },
        );
        assert_eq!(child_count(&twins), 2);
        assert!(only_child(&twins).is_none());
        assert!(left_child_mut(&mut twins).is_some());
        assert!(right_child_mut(&mut twins).is_some());
        assert!(matches!(
            child(&twins, 0).unwrap().operator,
            Operator::ShowTable
        ));
        assert!(matches!(
            child(&twins, 1).unwrap().operator,
            Operator::ShowView
        ));
        assert!(child(&twins, 2).is_none());
        assert_eq!(children(&twins).len(), 2);
    }

    #[test]
    fn replace_and_wrap_child_helpers_update_expected_slots() {
        let mut only = LogicalPlan::new(
            Operator::Dummy,
            Childrens::Only(Box::new(leaf(Operator::ShowTable))),
        );
        assert!(replace_with_only_child(&mut only));
        assert!(matches!(only.operator, Operator::ShowTable));
        assert!(matches!(only.childrens.as_ref(), Childrens::None));
        assert!(!replace_with_only_child(&mut only));

        let mut parent = LogicalPlan::new(
            Operator::Dummy,
            Childrens::Twins {
                left: Box::new(leaf(Operator::ShowTable)),
                right: Box::new(leaf(Operator::Dummy)),
            },
        );

        assert!(wrap_child_with(&mut parent, 1, Operator::ShowView));
        let wrapped = child(&parent, 1).unwrap();
        assert!(matches!(wrapped.operator, Operator::ShowView));
        assert!(matches!(wrapped.childrens.as_ref(), Childrens::Only(_)));
        assert!(matches!(
            only_child(wrapped).unwrap().operator,
            Operator::Dummy
        ));
        assert!(!wrap_child_with(&mut parent, 2, Operator::ShowView));
    }
}
// GRCOV_EXCL_STOP
