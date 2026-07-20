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

use super::{Operator, PlanImpl};
use crate::expression::ScalarExpression;
use crate::iter_ext::Itertools;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash, Ord, PartialOrd, ReferenceSerialization)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    Full,
    Cross,
}

impl JoinType {
    pub fn is_right(&self) -> bool {
        matches!(self, JoinType::RightOuter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub enum JoinCondition {
    On {
        /// Equijoin clause expressed as pairs of (left, right) join columns
        on: Vec<(ScalarExpression, ScalarExpression)>,
        /// Filters applied during join (non-equi conditions)
        filter: Option<ScalarExpression>,
    },
    None,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct JoinOperator {
    pub on: JoinCondition,
    pub join_type: JoinType,
    pub force_nested_loop: bool,
}

impl JoinOperator {
    pub fn build(
        left: LogicalPlan,
        right: LogicalPlan,
        on: JoinCondition,
        join_type: JoinType,
        force_nested_loop: bool,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Join(JoinOperator {
                on,
                join_type,
                force_nested_loop,
            }),
            Childrens::Twins {
                left: Box::new(left),
                right: Box::new(right),
            },
        )
    }

    pub(crate) fn plan_impl(&self) -> PlanImpl {
        match (&self.on, self.force_nested_loop) {
            (_, true) => PlanImpl::NestLoopJoin,
            (JoinCondition::On { on, .. }, false) if !on.is_empty() => PlanImpl::HashJoin,
            _ => PlanImpl::NestLoopJoin,
        }
    }
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "Inner")?,
            JoinType::LeftOuter => write!(f, "LeftOuter")?,
            JoinType::RightOuter => write!(f, "RightOuter")?,
            JoinType::Full => write!(f, "Full")?,
            JoinType::Cross => write!(f, "Cross")?,
        }

        Ok(())
    }
}

impl fmt::Display for JoinOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{} Join{}", self.join_type, self.on)?;

        Ok(())
    }
}

impl fmt::Display for JoinCondition {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            JoinCondition::On { on, filter } => {
                if !on.is_empty() {
                    let on = on
                        .iter()
                        .map(|(v1, v2)| format!("{v1} = {v2}"))
                        .join(" AND ");

                    write!(f, " On {on}")?;
                }
                if let Some(filter) = filter {
                    write!(f, " Where {filter}")?;
                }
            }
            JoinCondition::None => {
                write!(f, " Nothing")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_nested_loop_overrides_equi_join() {
        let mut operator = JoinOperator {
            on: JoinCondition::On {
                on: vec![(1_i32.into(), 2_i32.into())],
                filter: None,
            },
            join_type: JoinType::Inner,
            force_nested_loop: false,
        };
        assert_eq!(operator.plan_impl(), PlanImpl::HashJoin);

        operator.force_nested_loop = true;
        assert_eq!(operator.plan_impl(), PlanImpl::NestLoopJoin);
    }
}
