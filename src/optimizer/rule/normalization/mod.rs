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
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::rule::normalization::column_pruning::ColumnPruning;
use crate::optimizer::rule::normalization::combine_operators::{CollapseGroupByAgg, CombineFilter};
use crate::optimizer::rule::normalization::compilation_in_advance::EvaluatorBind;
use crate::optimizer::rule::normalization::constant_propagation::{
    PropagateFilterConstants, PropagateJoinConstants,
};
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
use crate::planner::{ExprRef, LogicalPlan, PlanArena};
use std::collections::HashSet;
mod column_pruning;
mod combine_operators;
mod compilation_in_advance;
mod constant_propagation;
mod elimination;
mod guard_mark_apply;
mod join_reorder;
mod materialize_mark_apply;
mod min_max_top_k;
use guard_mark_apply::GuardMarkApply;
use join_reorder::GreedyJoinReorder;
use materialize_mark_apply::MaterializeMarkApply;
mod parameterized_index;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod top_k;
pub(crate) use compilation_in_advance::evaluator_bind_current;
pub(crate) use elimination::{
    apply_annotated_post_rules, apply_scan_order_hint, EliminateIndexFilter, EliminateProject,
    OrderHintKind, ScanOrderHint,
};
pub(crate) use parameterized_index::{ParameterizeInnerJoin, ParameterizeMarkApply};
pub(crate) use simplification::constant_calculation_current;

#[derive(Debug, Copy, Clone)]
pub enum NormalizationRuleImpl {
    ColumnPruning,
    // Combine operators
    EliminateProject,
    CollapseGroupByAgg,
    CombineFilter,
    // PushDown limit
    LimitProjectTranspose,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
    // PushDown predicates
    PropagateFilterConstants,
    PropagateJoinConstants,
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
    GreedyJoinReorder,
    MaterializeMarkApply,
    GuardMarkApply,
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
            NormalizationRuleImpl::EliminateProject => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::CollapseGroupByAgg => NormalizationRuleRootTag::Aggregate,
            NormalizationRuleImpl::GuardMarkApply => NormalizationRuleRootTag::Project,
            NormalizationRuleImpl::CombineFilter => NormalizationRuleRootTag::Filter,
            NormalizationRuleImpl::LimitProjectTranspose
            | NormalizationRuleImpl::PushLimitThroughJoin
            | NormalizationRuleImpl::PushLimitIntoTableScan
            | NormalizationRuleImpl::TopK => NormalizationRuleRootTag::Limit,
            NormalizationRuleImpl::PropagateFilterConstants
            | NormalizationRuleImpl::PushPredicateThroughJoin
            | NormalizationRuleImpl::PushPredicateIntoScan
            | NormalizationRuleImpl::SimplifyFilter => NormalizationRuleRootTag::Filter,
            NormalizationRuleImpl::PropagateJoinConstants
            | NormalizationRuleImpl::PushJoinPredicateIntoScan => NormalizationRuleRootTag::Join,
            NormalizationRuleImpl::ConstantCalculation => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::EvaluatorBind => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::MinMaxToTopK => NormalizationRuleRootTag::Aggregate,
            NormalizationRuleImpl::EliminateIndexFilter => NormalizationRuleRootTag::Filter,
            NormalizationRuleImpl::ParameterizeInnerJoin
            | NormalizationRuleImpl::GreedyJoinReorder => NormalizationRuleRootTag::Join,
            NormalizationRuleImpl::ParameterizeMarkApply
            | NormalizationRuleImpl::MaterializeMarkApply => NormalizationRuleRootTag::MarkApply,
        }
    }
}

impl NormalizationRule for NormalizationRuleImpl {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        arena: &mut PlanArena,
        loader: Option<&StatisticMetaLoader<'_>>,
    ) -> Result<bool, DatabaseError> {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.apply(plan, arena, loader),
            NormalizationRuleImpl::GreedyJoinReorder => {
                GreedyJoinReorder.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::GuardMarkApply => GuardMarkApply.apply(plan, arena, loader),
            NormalizationRuleImpl::EliminateProject => EliminateProject.apply(plan, arena, loader),
            NormalizationRuleImpl::CollapseGroupByAgg => {
                CollapseGroupByAgg.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::CombineFilter => CombineFilter.apply(plan, arena, loader),
            NormalizationRuleImpl::LimitProjectTranspose => {
                LimitProjectTranspose.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::PushLimitThroughJoin => {
                PushLimitThroughJoin.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::PushLimitIntoTableScan => {
                PushLimitIntoScan.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::PropagateFilterConstants => {
                PropagateFilterConstants.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::PropagateJoinConstants => {
                PropagateJoinConstants.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::PushPredicateThroughJoin => {
                PushPredicateThroughJoin.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::PushJoinPredicateIntoScan => {
                PushJoinPredicateIntoScan.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.apply(plan, arena, loader),
            NormalizationRuleImpl::PushPredicateIntoScan => {
                PushPredicateIntoScan.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::ConstantCalculation => {
                ConstantCalculation.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.apply(plan, arena, loader),
            NormalizationRuleImpl::MinMaxToTopK => MinMaxToTopK.apply(plan, arena, loader),
            NormalizationRuleImpl::TopK => TopK.apply(plan, arena, loader),
            NormalizationRuleImpl::EliminateIndexFilter => {
                EliminateIndexFilter.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::ParameterizeInnerJoin => {
                ParameterizeInnerJoin.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::ParameterizeMarkApply => {
                ParameterizeMarkApply.apply(plan, arena, loader)
            }
            NormalizationRuleImpl::MaterializeMarkApply => {
                MaterializeMarkApply.apply(plan, arena, loader)
            }
        }
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
    visited: &'visited mut HashSet<ExprRef>,
}

impl<'positions, 'visited> PositionRemapper<'positions, 'visited> {
    pub(super) fn new(
        removed_positions: &'positions [usize],
        visited: &'visited mut HashSet<ExprRef>,
    ) -> Self {
        visited.clear();
        Self {
            removed_positions,
            visited,
        }
    }
}

impl<A: MetaArena + ?Sized> ExprVisitorMut<A> for PositionRemapper<'_, '_> {
    fn visit_expression_ref(
        &mut self,
        expr: &mut ExprRef,
        _arena: &mut A,
    ) -> Result<bool, DatabaseError> {
        Ok(self.visited.insert(*expr))
    }

    fn visit_column_ref(
        &mut self,
        _column: &mut crate::catalog::ColumnRef,
        position: &mut usize,
        _arena: &mut A,
    ) -> Result<(), DatabaseError> {
        remap_position(position, self.removed_positions);
        Ok(())
    }
}

pub(crate) fn remap_expr_positions<A: MetaArena + ?Sized>(
    mut expr: ExprRef,
    removed_positions: &[usize],
    visited: &mut HashSet<ExprRef>,
    arena: &mut A,
) -> Result<(), DatabaseError> {
    PositionRemapper::new(removed_positions, visited).visit(&mut expr, arena)
}

pub(crate) fn remap_exprs_positions<'a, A: MetaArena + ?Sized>(
    exprs: impl IntoIterator<Item = &'a mut ExprRef>,
    removed_positions: &[usize],
    visited: &mut HashSet<ExprRef>,
    arena: &mut A,
) -> Result<(), DatabaseError> {
    let mut remapper = PositionRemapper::new(removed_positions, visited);
    for expr in exprs {
        remapper.visit(expr, arena)?;
    }
    Ok(())
}
