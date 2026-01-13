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
use crate::optimizer::core::memo::{Expression, GroupExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
use crate::single_mapping;
use crate::storage::Transaction;
use std::sync::LazyLock;

static COPY_TO_FILE_PATTERN: LazyLock<Pattern> = LazyLock::new(|| Pattern {
    predicate: |op| matches!(op, Operator::CopyToFile(_)),
    children: PatternChildrenPredicate::None,
});

#[derive(Clone)]
pub struct CopyToFileImplementation;

single_mapping!(
    CopyToFileImplementation,
    COPY_TO_FILE_PATTERN,
    PhysicalOption::new(PlanImpl::CopyToFile, SortOption::None)
);
