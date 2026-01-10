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
use crate::optimizer::core::memo::GroupExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
use crate::storage::Transaction;

// TODO: Use indexing and other methods for matching optimization to avoid traversal
pub trait MatchPattern {
    fn pattern(&self) -> &Pattern;
}

pub trait NormalizationRule: MatchPattern {
    /// Returns true when the plan tree is modified.
    fn apply(&self, plan: &mut LogicalPlan) -> Result<bool, DatabaseError>;
}

pub trait ImplementationRule<T: Transaction>: MatchPattern {
    fn to_expression(
        &self,
        op: &Operator,
        loader: &StatisticMetaLoader<T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError>;
}
