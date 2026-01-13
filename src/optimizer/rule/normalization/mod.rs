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
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::rule::normalization::column_pruning::ColumnPruning;
use crate::optimizer::rule::normalization::combine_operators::{
    CollapseGroupByAgg, CollapseProject, CombineFilter,
};
use crate::optimizer::rule::normalization::compilation_in_advance::{
    BindExpressionPosition, EvaluatorBind,
};

use crate::optimizer::rule::normalization::pushdown_limit::{
    LimitProjectTranspose, PushLimitIntoScan, PushLimitThroughJoin,
};
use crate::optimizer::rule::normalization::pushdown_predicates::{
    PushJoinPredicateIntoScan, PushPredicateIntoScan, PushPredicateThroughJoin,
};
use crate::optimizer::rule::normalization::simplification::ConstantCalculation;
use crate::optimizer::rule::normalization::simplification::SimplifyFilter;
use crate::optimizer::rule::normalization::sort_elimination::EliminateRedundantSort;
use crate::optimizer::rule::normalization::top_k::TopK;
use crate::planner::LogicalPlan;
mod column_pruning;
mod combine_operators;
mod compilation_in_advance;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod sort_elimination;
mod top_k;
pub use sort_elimination::annotate_sort_preserving_indexes;

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
    TopK,
    EliminateRedundantSort,
}

impl MatchPattern for NormalizationRuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.pattern(),
            NormalizationRuleImpl::CollapseProject => CollapseProject.pattern(),
            NormalizationRuleImpl::CollapseGroupByAgg => CollapseGroupByAgg.pattern(),
            NormalizationRuleImpl::CombineFilter => CombineFilter.pattern(),
            NormalizationRuleImpl::LimitProjectTranspose => LimitProjectTranspose.pattern(),
            NormalizationRuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.pattern(),
            NormalizationRuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.pattern(),
            NormalizationRuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.pattern(),
            NormalizationRuleImpl::PushJoinPredicateIntoScan => PushJoinPredicateIntoScan.pattern(),
            NormalizationRuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.pattern(),
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.pattern(),
            NormalizationRuleImpl::BindExpressionPosition => BindExpressionPosition.pattern(),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.pattern(),
            NormalizationRuleImpl::TopK => TopK.pattern(),
            NormalizationRuleImpl::EliminateRedundantSort => EliminateRedundantSort.pattern(),
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
            NormalizationRuleImpl::TopK => TopK.apply(plan),
            NormalizationRuleImpl::EliminateRedundantSort => EliminateRedundantSort.apply(plan),
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
