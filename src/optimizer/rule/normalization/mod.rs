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
use crate::expression::visitor_mut::{walk_mut_expr, VisitorMut};
use crate::expression::{AliasType, ScalarExpression};
use crate::optimizer::core::rule::{NormalizationContext, NormalizationRule};
use crate::optimizer::rule::normalization::column_pruning::ColumnPruning;
use crate::optimizer::rule::normalization::combine_operators::{
    CollapseGroupByAgg, CollapseProject, CombineFilter,
};
use crate::optimizer::rule::normalization::compilation_in_advance::EvaluatorBind;
use crate::planner::operator::Operator;

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
use crate::planner::LogicalPlan;
mod agg_elimination;
mod column_pruning;
mod combine_operators;
mod compilation_in_advance;
mod min_max_top_k;
mod parameterized_index;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod top_k;
pub(crate) use agg_elimination::{
    apply_annotated_post_rules, apply_scan_order_hint, OrderHintKind, ScanOrderHint,
};
pub(crate) use compilation_in_advance::evaluator_bind_current;
pub(crate) use parameterized_index::ParameterizeMarkApply;
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
            Operator::ScalarApply(_) => Some(Self::Any),
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
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_)
            | Operator::FunctionScan(_)
            | Operator::Update(_)
            | Operator::Union(_)
            | Operator::Except(_) => None,
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
            NormalizationRuleImpl::ParameterizeMarkApply => NormalizationRuleRootTag::MarkApply,
        }
    }
}

impl NormalizationRule for NormalizationRuleImpl {
    fn apply(
        &self,
        plan: &mut LogicalPlan,
        ctx: &mut NormalizationContext,
    ) -> Result<bool, DatabaseError> {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.apply(plan, ctx),
            NormalizationRuleImpl::CollapseProject => CollapseProject.apply(plan, ctx),
            NormalizationRuleImpl::CollapseGroupByAgg => CollapseGroupByAgg.apply(plan, ctx),
            NormalizationRuleImpl::CombineFilter => CombineFilter.apply(plan, ctx),
            NormalizationRuleImpl::LimitProjectTranspose => LimitProjectTranspose.apply(plan, ctx),
            NormalizationRuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.apply(plan, ctx),
            NormalizationRuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(plan, ctx),
            NormalizationRuleImpl::PushPredicateThroughJoin => {
                PushPredicateThroughJoin.apply(plan, ctx)
            }
            NormalizationRuleImpl::PushJoinPredicateIntoScan => {
                PushJoinPredicateIntoScan.apply(plan, ctx)
            }
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.apply(plan, ctx),
            NormalizationRuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.apply(plan, ctx),
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.apply(plan, ctx),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.apply(plan, ctx),
            NormalizationRuleImpl::MinMaxToTopK => MinMaxToTopK.apply(plan, ctx),
            NormalizationRuleImpl::TopK => TopK.apply(plan, ctx),
            NormalizationRuleImpl::ParameterizeMarkApply => ParameterizeMarkApply.apply(plan, ctx),
        }
    }
}

/// Return true when left is subset of right
pub(crate) fn strip_alias(expr: &ScalarExpression) -> &ScalarExpression {
    match expr {
        ScalarExpression::Alias {
            expr,
            alias: AliasType::Name(_),
        } => strip_alias(expr),
        ScalarExpression::Alias {
            alias: AliasType::Expr(alias_expr),
            ..
        } => strip_alias(alias_expr),
        _ => expr,
    }
}

fn strip_all_alias(expr: &ScalarExpression) -> &ScalarExpression {
    match expr {
        ScalarExpression::Alias { expr, .. } => strip_all_alias(expr),
        _ => expr,
    }
}

pub fn is_subset_exprs(left: &[ScalarExpression], right: &[ScalarExpression]) -> bool {
    left.iter().all(|lhs| {
        let lhs_stripped = strip_alias(lhs);
        right.iter().any(|rhs| {
            let rhs_stripped = strip_alias(rhs);
            if lhs_stripped.eq_ignore_colref_pos(rhs_stripped) {
                return true;
            }
            if matches!(lhs_stripped, ScalarExpression::ColumnRef { .. }) {
                return lhs_stripped.eq_ignore_colref_pos(strip_all_alias(rhs));
            }
            false
        })
    })
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

struct PositionRemapper<'a> {
    removed_positions: &'a [usize],
}

impl<'a> VisitorMut<'a> for PositionRemapper<'_> {
    fn visit(&mut self, expr: &'a mut ScalarExpression) -> Result<(), DatabaseError> {
        match expr {
            ScalarExpression::ColumnRef { position, .. } => {
                remap_position(position, self.removed_positions);
                Ok(())
            }
            ScalarExpression::Alias { expr, alias } => match alias {
                AliasType::Expr(alias_expr) => self.visit(alias_expr),
                AliasType::Name(_) => self.visit(expr),
            },
            _ => walk_mut_expr(self, expr),
        }
    }
}

pub(crate) fn remap_expr_positions(
    expr: &mut ScalarExpression,
    removed_positions: &[usize],
) -> Result<(), DatabaseError> {
    PositionRemapper { removed_positions }.visit(expr)
}

pub(crate) fn remap_exprs_positions<'a>(
    exprs: impl IntoIterator<Item = &'a mut ScalarExpression>,
    removed_positions: &[usize],
) -> Result<(), DatabaseError> {
    let mut remapper = PositionRemapper { removed_positions };
    for expr in exprs {
        remapper.visit(expr)?;
    }
    Ok(())
}
