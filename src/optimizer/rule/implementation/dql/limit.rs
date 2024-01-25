use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::storage::Transaction;
use crate::optimizer::rule::implementation::HistogramLoader;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::OptimizerError;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::single_mapping;
use lazy_static::lazy_static;

lazy_static! {
    static ref LIMIT_PATTERN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Limit(_)),
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Clone)]
pub struct LimitImplementation;

single_mapping!(LimitImplementation, LIMIT_PATTERN, PhysicalOption::Limit);
