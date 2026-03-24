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
use crate::expression::{AliasType, ScalarExpression};
use crate::optimizer::core::rule::NormalizationRule;
use crate::optimizer::rule::normalization::column_pruning::ColumnPruning;
use crate::optimizer::rule::normalization::combine_operators::{
    CollapseGroupByAgg, CollapseProject, CombineFilter,
};
use crate::optimizer::rule::normalization::compilation_in_advance::{
    BindExpressionPosition, EvaluatorBind,
};
use crate::planner::operator::Operator;

use crate::optimizer::rule::normalization::agg_elimination::{
    EliminateRedundantSort, UseStreamDistinct,
};
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
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod top_k;
pub use agg_elimination::{annotate_sort_preserving_indexes, annotate_stream_distinct_indexes};
pub(crate) use compilation_in_advance::{bind_expression_position_current, evaluator_bind_current};
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
    BindExpressionPosition,
    EvaluatorBind,
    MinMaxToTopK,
    TopK,
    EliminateRedundantSort,
    UseStreamDistinct,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum WholeTreePassKind {
    ColumnPruning,
    ExpressionRewrite,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NormalizationRuleRootTag {
    Any,
    Aggregate,
    Filter,
    Join,
    Limit,
    Project,
    SortLike,
}

impl NormalizationRuleRootTag {
    pub fn matches(self, operator: &Operator) -> bool {
        match self {
            NormalizationRuleRootTag::Any => true,
            NormalizationRuleRootTag::Aggregate => matches!(operator, Operator::Aggregate(_)),
            NormalizationRuleRootTag::Filter => matches!(operator, Operator::Filter(_)),
            NormalizationRuleRootTag::Join => matches!(operator, Operator::Join(_)),
            NormalizationRuleRootTag::Limit => matches!(operator, Operator::Limit(_)),
            NormalizationRuleRootTag::Project => matches!(operator, Operator::Project(_)),
            NormalizationRuleRootTag::SortLike => {
                matches!(operator, Operator::Sort(_) | Operator::TopK(_))
            }
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
            NormalizationRuleImpl::ConstantCalculation
            | NormalizationRuleImpl::BindExpressionPosition
            | NormalizationRuleImpl::EvaluatorBind => {
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
            NormalizationRuleImpl::BindExpressionPosition => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::EvaluatorBind => NormalizationRuleRootTag::Any,
            NormalizationRuleImpl::MinMaxToTopK | NormalizationRuleImpl::UseStreamDistinct => {
                NormalizationRuleRootTag::Aggregate
            }
            NormalizationRuleImpl::EliminateRedundantSort => NormalizationRuleRootTag::SortLike,
        }
    }
}

impl NormalizationRule for NormalizationRuleImpl {
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError> {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.apply(plan),
            NormalizationRuleImpl::CollapseProject => CollapseProject.apply(plan),
            NormalizationRuleImpl::CollapseGroupByAgg => CollapseGroupByAgg.apply(plan),
            NormalizationRuleImpl::CombineFilter => CombineFilter.apply(plan),
            NormalizationRuleImpl::LimitProjectTranspose => LimitProjectTranspose.apply(plan),
            NormalizationRuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.apply(plan),
            NormalizationRuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(plan),
            NormalizationRuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.apply(plan),
            NormalizationRuleImpl::PushJoinPredicateIntoScan => {
                PushJoinPredicateIntoScan.apply(plan)
            }
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.apply(plan),
            NormalizationRuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.apply(plan),
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.apply(plan),
            NormalizationRuleImpl::BindExpressionPosition => BindExpressionPosition.apply(plan),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.apply(plan),
            NormalizationRuleImpl::MinMaxToTopK => MinMaxToTopK.apply(plan),
            NormalizationRuleImpl::TopK => TopK.apply(plan),
            NormalizationRuleImpl::EliminateRedundantSort => EliminateRedundantSort.apply(plan),
            NormalizationRuleImpl::UseStreamDistinct => UseStreamDistinct.apply(plan),
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
            if lhs_stripped == rhs_stripped {
                return true;
            }
            if matches!(lhs, ScalarExpression::ColumnRef { .. }) {
                return lhs_stripped == strip_all_alias(rhs);
            }
            false
        })
    })
}
