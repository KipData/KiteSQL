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
use crate::expression::visitor_mut::{walk_mut_expr, ExprVisitorMut};
use crate::expression::{AliasType, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::rule::normalization::column_pruning::ColumnPruning;
use crate::optimizer::rule::normalization::combine_operators::{
    CollapseGroupByAgg, CollapseProject, CombineFilter,
};
use crate::optimizer::rule::normalization::compilation_in_advance::EvaluatorBind;
use crate::planner::operator::Operator;
use crate::planner::MetaArena;

use crate::optimizer::rule::normalization::min_max_top_k::MinMaxToTopK;
use crate::optimizer::rule::normalization::pushdown_limit::{
    LimitProjectTranspose, PushLimitIntoScan, PushLimitThroughJoin,
};
use crate::optimizer::rule::normalization::pushdown_predicates::{
    PushJoinPredicateIntoScan, PushPredicateIntoScan, PushPredicateThroughJoin,
};
use crate::optimizer::rule::normalization::simplification::ConstantCalculation;
use crate::optimizer::rule::normalization::simplification::SimplifyFilter;
use crate::optimizer::rule::normalization::top_k::TopK;
use crate::planner::{ExprRef, LogicalPlan};
use std::collections::HashMap;
mod column_pruning;
mod combine_operators;
mod compilation_in_advance;
mod elimination;
mod min_max_top_k;
mod parameterized_index;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod top_k;
pub(crate) use compilation_in_advance::evaluator_bind_current;
pub(crate) use elimination::{
    apply_annotated_post_rules, apply_scan_order_hint, EliminateIndexFilter, OrderHintKind,
    ScanOrderHint,
};
pub(crate) use parameterized_index::{ParameterizeInnerJoin, ParameterizeMarkApply};
pub(crate) use simplification::constant_calculation_current;

#[derive(Debug, Copy, Clone)]
pub enum NormalizationRuleImpl {
    ColumnPruning,
    // Combine operators
    CollapseProject,
    CollapseGroupByAgg,
    CombineFilter,
    // PushDown limit
    LimitProjectTranspose,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
    // PushDown predicates
    PushPredicateThroughJoin,
    PushJoinPredicateIntoScan,
    // Tips: need to be used with `SimplifyFilter`
    PushPredicateIntoScan,
    // Simplification
    SimplifyFilter,
    ConstantCalculation,
    // CompilationInAdvance
    EvaluatorBind,
    MinMaxToTopK,
    TopK,
    ParameterizeMarkApply,
    ParameterizeInnerJoin,
    EliminateIndexFilter,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WholeTreePassKind {
    ColumnPruning,
    ExpressionRewrite,
}

#[repr(usize)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NormalizationRuleRootTag {
    Any = 0,
    Aggregate,
    Filter,
    Join,
    Limit,
    MarkApply,
    Project,
    SortLike,
}

impl NormalizationRuleRootTag {
    pub const COUNT: usize = Self::SortLike as usize + 1;

    pub fn from_operator(operator: &Operator) -> Option<Self> {
        match operator {
            Operator::Aggregate(_) => Some(Self::Aggregate),
            Operator::MarkApply(_) => Some(Self::MarkApply),
            Operator::ScalarQueryInit(_) => Some(Self::Any),
            Operator::Filter(_) => Some(Self::Filter),
            Operator::Join(_) => Some(Self::Join),
            Operator::Limit(_) => Some(Self::Limit),
            Operator::Project(_) => Some(Self::Project),
            Operator::Sort(_) | Operator::TopK(_) => Some(Self::SortLike),
            Operator::Dummy
            | Operator::TableScan(_)
            | Operator::ScalarSubquery(_)
            | Operator::Values(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Delete(_)
            | Operator::Analyze(_)
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
            | Operator::FunctionScan(_)
            | Operator::Update(_)
            | Operator::Union(_)
            | Operator::SetMembership(_)
            | Operator::Window(_) => None,
            Operator::RecursiveCte(_) | Operator::RecursiveScan(_) => None,
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(_) | Operator::CopyToFile(_) => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NormalizationPassKind {
    WholeTreePass(WholeTreePassKind),
    LocalRewrite,
}

impl NormalizationRuleImpl {
    pub fn pass_kind(&self) -> NormalizationPassKind {
        match self {
            NormalizationRuleImpl::ColumnPruning => {
                NormalizationPassKind::WholeTreePass(WholeTreePassKind::ColumnPruning)
            }
            NormalizationRuleImpl::ConstantCalculation | NormalizationRuleImpl::EvaluatorBind => {
                NormalizationPassKind::WholeTreePass(WholeTreePassKind::ExpressionRewrite)
            }
            _ => NormalizationPassKind::LocalRewrite,
        }
    }

    pub fn root_tag(&self) -> NormalizationRuleRootTag {
        match self {
            NormalizationRuleImpl::ColumnPruning => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::CollapseProject => NormalizationRuleRootTag::Project,
            NormalizationRuleImpl::CollapseGroupByAgg => NormalizationRuleRootTag::Aggregate,
            NormalizationRuleImpl::CombineFilter => NormalizationRuleRootTag::Filter,
            NormalizationRuleImpl::LimitProjectTranspose
            | NormalizationRuleImpl::PushLimitThroughJoin
            | NormalizationRuleImpl::PushLimitIntoTableScan
            | NormalizationRuleImpl::TopK => NormalizationRuleRootTag::Limit,
            NormalizationRuleImpl::PushPredicateThroughJoin
            | NormalizationRuleImpl::PushPredicateIntoScan
            | NormalizationRuleImpl::SimplifyFilter => NormalizationRuleRootTag::Filter,
            NormalizationRuleImpl::PushJoinPredicateIntoScan => NormalizationRuleRootTag::Join,
            NormalizationRuleImpl::ConstantCalculation => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::EvaluatorBind => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::MinMaxToTopK => NormalizationRuleRootTag::Aggregate,
            NormalizationRuleImpl::EliminateIndexFilter => NormalizationRuleRootTag::Filter,
            NormalizationRuleImpl::ParameterizeInnerJoin => NormalizationRuleRootTag::Join,
            NormalizationRuleImpl::ParameterizeMarkApply => NormalizationRuleRootTag::MarkApply,
        }
    }
}

impl NormalizationRule for NormalizationRuleImpl {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<bool, DatabaseError> {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.apply(plan, arena),
            NormalizationRuleImpl::CollapseProject => CollapseProject.apply(plan, arena),
            NormalizationRuleImpl::CollapseGroupByAgg => CollapseGroupByAgg.apply(plan, arena),
            NormalizationRuleImpl::CombineFilter => CombineFilter.apply(plan, arena),
            NormalizationRuleImpl::LimitProjectTranspose => {
                LimitProjectTranspose.apply(plan, arena)
            }
            NormalizationRuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.apply(plan, arena),
            NormalizationRuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(plan, arena),
            NormalizationRuleImpl::PushPredicateThroughJoin => {
                PushPredicateThroughJoin.apply(plan, arena)
            }
            NormalizationRuleImpl::PushJoinPredicateIntoScan => {
                PushJoinPredicateIntoScan.apply(plan, arena)
            }
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.apply(plan, arena),
            NormalizationRuleImpl::PushPredicateIntoScan => {
                PushPredicateIntoScan.apply(plan, arena)
            }
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.apply(plan, arena),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.apply(plan, arena),
            NormalizationRuleImpl::MinMaxToTopK => MinMaxToTopK.apply(plan, arena),
            NormalizationRuleImpl::TopK => TopK.apply(plan, arena),
            NormalizationRuleImpl::EliminateIndexFilter => EliminateIndexFilter.apply(plan, arena),
            NormalizationRuleImpl::ParameterizeInnerJoin => {
                ParameterizeInnerJoin.apply(plan, arena)
            }
            NormalizationRuleImpl::ParameterizeMarkApply => {
                ParameterizeMarkApply.apply(plan, arena)
            }
        }
    }
}

pub(crate) fn strip_alias(expr: ExprRef, arena: &crate::planner::PlanArena<'_>) -> ExprRef {
    match arena.expression(expr) {
        ScalarExpression::Alias {
            expr,
            alias: AliasType::Name(_),
        } => strip_alias(*expr, arena),
        ScalarExpression::Alias {
            alias: AliasType::Expr(alias_expr),
            ..
        } => strip_alias(*alias_expr, arena),
        _ => expr,
    }
}

pub(crate) fn remap_position(position: &mut usize, removed_positions: &[usize]) {
    match removed_positions.binary_search(position) {
        Ok(_) => {
            debug_assert!(
                false,
                "encountered a reference to pruned output slot {position}"
            );
        }
        Err(shift) => {
            *position -= shift;
        }
    }
}

struct PositionRemapper<'positions, 'visited> {
    removed_positions: &'positions [usize],
    remapped: &'visited mut HashMap<ExprRef, ExprRef>,
    changes: usize,
}

impl<'positions, 'visited> PositionRemapper<'positions, 'visited> {
    pub(super) fn new(
        removed_positions: &'positions [usize],
        remapped: &'visited mut HashMap<ExprRef, ExprRef>,
    ) -> Self {
        remapped.clear();
        Self {
            removed_positions,
            remapped,
            changes: 0,
        }
    }
}

impl ExprVisitorMut for PositionRemapper<'_, '_> {
    fn visit(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        if let Some(remapped) = self.remapped.get(expr) {
            if *expr != *remapped {
                self.changes += 1;
                *expr = *remapped;
            }
            return Ok(());
        }
        let changes = self.changes;
        let original = *expr;
        let expression = arena.expression(original).clone();
        // Preserve shared definitions and rewrite only the current consumer's reference.
        let mut rewritten = arena.alloc_expression(expression);
        walk_mut_expr(self, &mut rewritten, arena)?;
        if self.changes != changes {
            *expr = rewritten;
        }
        self.remapped.insert(original, *expr);
        self.remapped.insert(*expr, *expr);
        Ok(())
    }

    fn visit_column_ref(
        &mut self,
        _column: &mut crate::catalog::ColumnRef,
        position: &mut usize,
        _arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        let original = *position;
        remap_position(position, self.removed_positions);
        if *position != original {
            self.changes += 1;
        }
        Ok(())
    }

    fn visit_alias(
        &mut self,
        expr: &mut ExprRef,
        alias: &mut AliasType,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        match alias {
            AliasType::Expr(alias_expr) => self.visit(alias_expr, arena),
            AliasType::Name(_) => self.visit(expr, arena),
        }
    }
}

pub(crate) fn remap_expr_positions(
    expr: &mut ExprRef,
    removed_positions: &[usize],
    remapped: &mut HashMap<ExprRef, ExprRef>,
    arena: &mut crate::planner::PlanArena<'_>,
) -> Result<(), DatabaseError> {
    PositionRemapper::new(removed_positions, remapped).visit(expr, arena)
}

pub(crate) fn remap_exprs_positions<'a>(
    exprs: impl IntoIterator<Item = &'a mut ExprRef>,
    removed_positions: &[usize],
    remapped: &mut HashMap<ExprRef, ExprRef>,
    arena: &mut crate::planner::PlanArena<'_>,
) -> Result<(), DatabaseError> {
    let mut remapper = PositionRemapper::new(removed_positions, remapped);
    for expr in exprs {
        remapper.visit(expr, arena)?;
    }
    Ok(())
}
