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
    EvaluatorBind, ExpressionRemapper,
};
use crate::optimizer::rule::normalization::correlated_subquery::CorrelatedSubquery;
use crate::optimizer::rule::normalization::pushdown_limit::{
    LimitProjectTranspose, PushLimitIntoScan, PushLimitThroughJoin,
};
use crate::optimizer::rule::normalization::pushdown_predicates::PushPredicateIntoScan;
use crate::optimizer::rule::normalization::pushdown_predicates::PushPredicateThroughJoin;
use crate::optimizer::rule::normalization::simplification::ConstantCalculation;
use crate::optimizer::rule::normalization::simplification::SimplifyFilter;

mod column_pruning;
mod combine_operators;
mod compilation_in_advance;
mod correlated_subquery;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;

#[derive(Debug, Copy, Clone)]
pub enum NormalizationRuleImpl {
    ColumnPruning,
    // Combine operators
    CollapseProject,
    CollapseGroupByAgg,
    CombineFilter,
    CorrelateSubquery,
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
    ExpressionRemapper,
    EvaluatorBind,
}

impl MatchPattern for NormalizationRuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            NormalizationRuleImpl::ColumnPruning => ColumnPruning.pattern(),
            NormalizationRuleImpl::CollapseProject => CollapseProject.pattern(),
            NormalizationRuleImpl::CollapseGroupByAgg => CollapseGroupByAgg.pattern(),
            NormalizationRuleImpl::CombineFilter => CombineFilter.pattern(),
            NormalizationRuleImpl::CorrelateSubquery => CorrelatedSubquery.pattern(),
            NormalizationRuleImpl::LimitProjectTranspose => LimitProjectTranspose.pattern(),
            NormalizationRuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.pattern(),
            NormalizationRuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.pattern(),
            NormalizationRuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.pattern(),
            NormalizationRuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            NormalizationRuleImpl::SimplifyFilter => SimplifyFilter.pattern(),
            NormalizationRuleImpl::ConstantCalculation => ConstantCalculation.pattern(),
            NormalizationRuleImpl::ExpressionRemapper => ExpressionRemapper.pattern(),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.pattern(),
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
            NormalizationRuleImpl::CorrelateSubquery => CorrelatedSubquery.apply(node_id, graph),
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
            NormalizationRuleImpl::ExpressionRemapper => ExpressionRemapper.apply(node_id, graph),
            NormalizationRuleImpl::EvaluatorBind => EvaluatorBind.apply(node_id, graph),
        }
    }
}

/// Return true when left is subset of right
pub fn is_subset_exprs(left: &[ScalarExpression], right: &[ScalarExpression]) -> bool {
    left.iter().all(|l| right.contains(l))
}
