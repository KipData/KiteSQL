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
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, ScalarExpression, TypeCast};
use crate::planner::MetaArena;
use crate::planner::{Childrens, Explain, ExprRef, LogicalPlan, PlanArena};
use crate::types::tuple::Schema;
use crate::types::LogicalType;
use kite_sql_serde_macros::ReferenceSerialization;
use std::borrow::Cow;
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
        on: Vec<(ExprRef, ExprRef)>,
        /// Filters applied during join (non-equi conditions)
        filter: Option<ExprRef>,
    },
    None,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct JoinOperator {
    pub on: JoinCondition,
    pub join_type: JoinType,
    pub force_nested_loop: bool,
    pub limit_pushed: bool,
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
                limit_pushed: false,
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

impl Explain for JoinOperator {
    fn fmt(&self, arena: &(dyn MetaArena + '_), f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} Join{}", self.join_type, self.on.explain(arena))
    }
}

impl Explain for JoinCondition {
    fn fmt(&self, arena: &(dyn MetaArena + '_), f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinCondition::On { on, filter } => {
                if !on.is_empty() {
                    f.write_str(" On ")?;
                    for (index, (left, right)) in on.iter().enumerate() {
                        if index > 0 {
                            f.write_str(" AND ")?;
                        }
                        write!(f, "{} = {}", left.explain(arena), right.explain(arena))?;
                    }
                }
                if let Some(filter) = filter {
                    write!(f, " Where {}", filter.explain(arena))?;
                }
                Ok(())
            }
            JoinCondition::None => f.write_str(" Nothing"),
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

/// for sqlrs
/// original idea from datafusion planner.rs
/// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
/// Filters matching this pattern are added to `accum`
/// Filters that don't match this pattern are added to `accum_filter`
/// Examples:
/// ```text
/// foo = bar => accum=[(foo, bar)] accum_filter=[]
/// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
/// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
/// ```
pub(crate) fn extract_join_keys(
    expr: ExprRef,
    accum: &mut Vec<(ExprRef, ExprRef)>,
    accum_filter: &mut Vec<ExprRef>,
    left_schema: &Schema,
    right_schema: &Schema,
    arena: &mut PlanArena,
) -> Result<(), DatabaseError> {
    let fn_contains = |schema: &Schema, column: ColumnRef| {
        let summary = arena.column(column).summary();
        schema
            .iter()
            .any(|candidate| arena.column(*candidate).summary() == summary)
    };
    let fn_or_contains =
        |column: ColumnRef| fn_contains(left_schema, column) || fn_contains(right_schema, column);

    let expr = expr.unpack_alias(arena);
    match arena.expression(expr) {
        ScalarExpression::Binary {
            left_expr,
            right_expr,
            op,
            ..
        } => {
            match op {
                BinaryOperator::Eq => {
                    match (
                        left_expr.unpack_alias_ref(arena),
                        right_expr.unpack_alias_ref(arena),
                    ) {
                        // example: foo = bar
                        (
                            ScalarExpression::ColumnRef { column: l, .. },
                            ScalarExpression::ColumnRef { column: r, .. },
                        ) => {
                            // reorder left and right joins keys to pattern: (left, right)
                            let key = if fn_contains(left_schema, *l)
                                && fn_contains(right_schema, *r)
                            {
                                Some((*left_expr, *right_expr))
                            } else if fn_contains(left_schema, *r) && fn_contains(right_schema, *l)
                            {
                                Some((*right_expr, *left_expr))
                            } else {
                                if fn_or_contains(*l) || fn_or_contains(*r) {
                                    accum_filter.push(expr);
                                }
                                None
                            };
                            // Join keys are compared (and hashed) directly, so cast
                            // both to one type like `l = r` in a filter; otherwise
                            // e.g. `bigint = int` never matches.
                            if let Some((left, right)) = key {
                                let ty = LogicalType::max_logical_type(
                                    &left.return_type(arena),
                                    &right.return_type(arena),
                                )?
                                .into_owned();
                                accum.push((
                                    left.type_cast(Cow::Borrowed(&ty), arena)?,
                                    right.type_cast(Cow::Borrowed(&ty), arena)?,
                                ));
                            }
                        }
                        (ScalarExpression::ColumnRef { column, .. }, _)
                        | (_, ScalarExpression::ColumnRef { column, .. }) => {
                            if fn_or_contains(*column) {
                                accum_filter.push(expr);
                            }
                        }
                        _other => {
                            // example: baz > 1
                            if left_expr.all_referenced_columns(arena, |_, column| {
                                fn_or_contains(*column)
                            })? && right_expr.all_referenced_columns(arena, |_, column| {
                                fn_or_contains(*column)
                            })? {
                                accum_filter.push(expr);
                            }
                        }
                    }
                }
                BinaryOperator::And => {
                    // example: foo = bar AND baz > 1
                    let (left_expr, right_expr) = (*left_expr, *right_expr);
                    extract_join_keys(
                        left_expr,
                        accum,
                        accum_filter,
                        left_schema,
                        right_schema,
                        arena,
                    )?;
                    extract_join_keys(
                        right_expr,
                        accum,
                        accum_filter,
                        left_schema,
                        right_schema,
                        arena,
                    )?;
                }
                BinaryOperator::Or => {
                    accum_filter.push(expr);
                }
                _ => {
                    if left_expr
                        .all_referenced_columns(arena, |_, column| fn_or_contains(*column))?
                        && right_expr
                            .all_referenced_columns(arena, |_, column| fn_or_contains(*column))?
                    {
                        accum_filter.push(expr);
                    }
                }
            }
        }
        _ => {
            if expr.all_referenced_columns(arena, |_, column| fn_or_contains(*column))? {
                // example: baz > 1
                accum_filter.push(expr);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forced_nested_loop_overrides_equi_join() {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = crate::planner::PlanArena::new(&table_arena);
        let mut operator = JoinOperator {
            on: JoinCondition::On {
                on: vec![(
                    arena.alloc_expression(crate::expression::ScalarExpression::from(1_i32)),
                    arena.alloc_expression(crate::expression::ScalarExpression::from(2_i32)),
                )],
                filter: None,
            },
            join_type: JoinType::Inner,
            force_nested_loop: false,
            limit_pushed: false,
        };
        assert_eq!(operator.plan_impl(), PlanImpl::HashJoin);

        operator.force_nested_loop = true;
        assert_eq!(operator.plan_impl(), PlanImpl::NestLoopJoin);
    }
}
