use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::{MatchPattern, NormalizationRule};
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
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
use crate::optimizer::rule::normalization::pushdown_predicates::PushPredicateIntoScan;
use crate::optimizer::rule::normalization::pushdown_predicates::PushPredicateThroughJoin;
use crate::optimizer::rule::normalization::simplification::ConstantCalculation;
use crate::optimizer::rule::normalization::simplification::SimplifyFilter;
use crate::optimizer::rule::normalization::top_k::TopK;
mod column_pruning;
mod combine_operators;
mod compilation_in_advance;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;
mod top_k;

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
    // Tips: need to be used with `SimplifyFilter`
    PushPredicateIntoScan,
    // Simplification
    SimplifyFilter,
    ConstantCalculation,
    // CompilationInAdvance
    BindExpressionPosition,
    EvaluatorBind,
    TopK,
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
            NormalizationRuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.pattern(),
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.pattern(),
            NormalizationRuleImpl::BindExpressionPosition => BindExpressionPosition.pattern(),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.pattern(),
            NormalizationRuleImpl::TopK => TopK.pattern(),
        }
    }
}

impl NormalizationRule for NormalizationRuleImpl {
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), DatabaseError> {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.apply(node_id, graph),
            NormalizationRuleImpl::CollapseProject => CollapseProject.apply(node_id, graph),
            NormalizationRuleImpl::CollapseGroupByAgg => CollapseGroupByAgg.apply(node_id, graph),
            NormalizationRuleImpl::CombineFilter => CombineFilter.apply(node_id, graph),
            NormalizationRuleImpl::LimitProjectTranspose => {
                LimitProjectTranspose.apply(node_id, graph)
            }
            NormalizationRuleImpl::PushLimitThroughJoin => {
                PushLimitThroughJoin.apply(node_id, graph)
            }
            NormalizationRuleImpl::PushLimitIntoTableScan => {
                PushLimitIntoScan.apply(node_id, graph)
            }
            NormalizationRuleImpl::PushPredicateThroughJoin => {
                PushPredicateThroughJoin.apply(node_id, graph)
            }
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.apply(node_id, graph),
            NormalizationRuleImpl::PushPredicateIntoScan => {
                PushPredicateIntoScan.apply(node_id, graph)
            }
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.apply(node_id, graph),
            NormalizationRuleImpl::BindExpressionPosition => {
                BindExpressionPosition.apply(node_id, graph)
            }
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.apply(node_id, graph),
            NormalizationRuleImpl::TopK => TopK.apply(node_id, graph),
        }
    }
}

/// Return true when left is subset of right
pub fn is_subset_exprs(left: &[ScalarExpression], right: &[ScalarExpression]) -> bool {
    left.iter().all(|l| right.contains(l))
}
