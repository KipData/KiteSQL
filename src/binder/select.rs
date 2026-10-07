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

use crate::planner::operator::join::extract_join_keys;
use crate::planner::MetaArena;
use crate::{
    expression::ScalarExpression,
    planner::{
        operator::{
            filter::FilterOperator, join::JoinOperator as LJoinOperator, limit::LimitOperator,
            mark_apply::MarkApplyOperator, project::ProjectOperator,
            scalar_query_init::ScalarQueryInitOperator, Operator,
        },
        operator::{join::JoinType, table_scan::TableScanOperator},
    },
};
use std::{borrow::Cow, collections::HashSet};

use super::{Binder, BinderContext, QueryBindStep, SetOperatorKind, Source, SubQueryType};

use crate::catalog::{ColumnRef, ColumnRelation, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::join::joins_nullable;
use crate::expression::visitor::ExprVisitor;
use crate::expression::visitor_mut::{walk_mut_expr, ExprVisitorMut, PositionShift};
use crate::expression::{AliasType, BinaryOperator, TypeCast};
use crate::iter_ext::Itertools;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::set_membership::{SetMembershipKind, SetMembershipOperator};
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::union::UnionOperator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena, ScalarQueryRef};
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::{ColumnId, LogicalType};

struct RightSidePositionGlobalizer<'a> {
    right_schema: &'a Schema,
    left_len: usize,
}

impl ExprVisitorMut for RightSidePositionGlobalizer<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        if self
            .right_schema
            .iter()
            .any(|right| arena.same_column(*right, *column))
        {
            *position += self.left_len;
        }
        Ok(())
    }
}

struct AppendedRightOutput {
    column: ColumnRef,
    child_position: usize,
    output_position: usize,
}

struct MarkerPositionGlobalizer<'a> {
    output_column: &'a ColumnRef,
    left_len: usize,
}

impl ExprVisitorMut for MarkerPositionGlobalizer<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        if arena.same_column(*column, *self.output_column) {
            *position = self.left_len;
        }
        Ok(())
    }
}

struct ProjectionOutputBinder<'a> {
    project_exprs: &'a [ExprRef],
}

impl<'a> ProjectionOutputBinder<'a> {
    fn new(project_exprs: &'a [ExprRef]) -> Self {
        Self { project_exprs }
    }

    fn output_ref(&mut self, expr: ExprRef, arena: &mut dyn MetaArena) -> Option<ScalarExpression> {
        self.project_exprs
            .iter()
            .position(|candidate| {
                candidate.eq_ignore_colref_pos(expr, arena)
                    || candidate
                        .unpack_alias(arena)
                        .eq_ignore_colref_pos(expr.unpack_alias(arena), arena)
            })
            .map(|position| {
                let output_expr = self.project_exprs[position];
                ScalarExpression::column_expr(output_expr.output_column_ref(arena), position)
            })
    }
}

impl ExprVisitorMut for ProjectionOutputBinder<'_> {
    fn visit(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut (dyn MetaArena + '_),
    ) -> Result<(), DatabaseError> {
        if let Some(output_ref) = self.output_ref(*expr, arena) {
            *expr = arena.alloc_expression(output_ref);
            return Ok(());
        }
        walk_mut_expr(self, expr, arena)
    }
}

pub(crate) struct BindPlanStart<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(crate) arena: &'s mut PlanArena<'arena>,
}

pub struct BindPlanFrom<'s, 'a, 'b, 'arena, T, A, M = ()>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(crate) arena: &'s mut PlanArena<'arena>,
    pub(crate) plan: LogicalPlan,
    pub(crate) _marker: std::marker::PhantomData<M>,
}

pub struct BindPlanSelectList<'s, 'a, 'b, 'arena, T, A, M = ()>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(crate) arena: &'s mut PlanArena<'arena>,
    pub(super) plan: LogicalPlan,
    pub(super) select_list: Vec<ExprRef>,
    pub(crate) _marker: std::marker::PhantomData<M>,
}

pub(crate) struct BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(super) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(super) arena: &'s mut PlanArena<'arena>,
    pub(super) plan: LogicalPlan,
    pub(super) select_list: Vec<ExprRef>,
}

pub(crate) struct BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ExprRef>,
    having: Option<ExprRef>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanHaving<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ExprRef>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanWindowed<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ExprRef>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ExprRef>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanSorted<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ExprRef>,
}

pub(crate) struct BindPlanProjected<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    plan: LogicalPlan,
    _marker: std::marker::PhantomData<(&'s (), &'a (), &'b (), &'arena (), T, A)>,
}

pub(crate) struct BindPlanComplete {
    plan: LogicalPlan,
}

pub(crate) struct TableAliasInput {
    pub(crate) name: TableName,
    pub(crate) columns: Vec<String>,
}

pub(crate) enum JoinConstraintInput {
    On(ExprRef),
    Using(Vec<String>),
    Natural,
    None,
}

impl<'s, 'a: 'b, 'b, 'arena, T, A, M> BindPlanFrom<'s, 'a, 'b, 'arena, T, A, M>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    #[cfg(feature = "orm")]
    pub(crate) fn typed<N>(self) -> BindPlanFrom<'s, 'a, 'b, 'arena, T, A, N> {
        BindPlanFrom {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            _marker: std::marker::PhantomData,
        }
    }

    #[cfg(feature = "orm")]
    pub(crate) fn filter_expr(mut self, predicate: ExprRef) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_where_expr(self.plan, predicate, self.arena)?;
        Ok(self)
    }

    #[cfg(feature = "orm")]
    pub(crate) fn join_plan(
        mut self,
        right_plan: LogicalPlan,
        right_context: BinderContext<'a, T>,
        join_type: JoinType,
        constraint: JoinConstraintInput,
    ) -> Result<Self, DatabaseError> {
        self.binder.extend(right_context);
        self.plan = self
            .binder
            .bind_join_plans(self.plan, right_plan, join_type, constraint, self.arena)?;
        Ok(self)
    }

    pub(crate) fn select_list(
        self,
        select_list: Vec<ExprRef>,
    ) -> BindPlanSelectList<'s, 'a, 'b, 'arena, T, A, M> {
        BindPlanSelectList {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A, M> BindPlanSelectList<'s, 'a, 'b, 'arena, T, A, M>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    #[cfg(feature = "orm")]
    pub(crate) fn set_select_list(mut self, select_list: Vec<ExprRef>) -> Self {
        self.select_list = select_list;
        self
    }

    #[cfg(feature = "orm")]
    pub(crate) fn group_by_expr(self, expr: ExprRef) -> Result<Self, DatabaseError> {
        let sorted = self
            .filter_expr(None)?
            .aggregate(
                vec![expr],
                None,
                None::<Vec<SortField>>,
                |_binder, _arena, _select_list, order| Ok(order),
            )?
            .having()?
            .window()?
            .distinct(false)?
            .order_by()?;
        Ok(BindPlanSelectList {
            binder: sorted.binder,
            arena: sorted.arena,
            plan: sorted.plan,
            select_list: sorted.select_list,
            _marker: std::marker::PhantomData,
        })
    }

    #[cfg(feature = "orm")]
    pub(crate) fn aggregate_without_group(self) -> Result<Self, DatabaseError> {
        let sorted = self
            .filter_expr(None)?
            .aggregate(
                Vec::new(),
                None,
                None::<Vec<SortField>>,
                |_binder, _arena, _select_list, order| Ok(order),
            )?
            .having()?
            .window()?
            .distinct(false)?
            .order_by()?;
        Ok(BindPlanSelectList {
            binder: sorted.binder,
            arena: sorted.arena,
            plan: sorted.plan,
            select_list: sorted.select_list,
            _marker: std::marker::PhantomData,
        })
    }

    #[cfg(feature = "orm")]
    pub(crate) fn having_expr(mut self, expr: ExprRef) -> Result<Self, DatabaseError> {
        self.plan = self.binder.bind_having(self.plan, expr, self.arena)?;
        Ok(self)
    }

    #[cfg(feature = "orm")]
    pub(crate) fn sort_field(mut self, field: SortField) -> Result<Self, DatabaseError> {
        self.plan = self.binder.bind_sort(self.plan, vec![field], self.arena)?;
        Ok(self)
    }

    #[cfg(feature = "orm")]
    pub fn distinct(mut self) -> Result<Self, DatabaseError> {
        let distinct_outputs = self.select_list.clone();
        self.binder.bind_distinct_output_exprs(
            &distinct_outputs,
            self.select_list.iter_mut(),
            self.arena,
        )?;
        self.plan = self.binder.bind_distinct(self.plan, distinct_outputs)?;
        Ok(self)
    }

    #[cfg(feature = "orm")]
    pub fn limit(mut self, limit: usize) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_limit_values(self.plan, None, Some(limit))?;
        Ok(self)
    }

    #[cfg(feature = "orm")]
    pub fn offset(mut self, offset: usize) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_limit_values(self.plan, Some(offset), None)?;
        Ok(self)
    }

    #[cfg(feature = "orm")]
    pub fn finish(self) -> Result<LogicalPlan, DatabaseError> {
        if self
            .binder
            .context
            .scalar_queries
            .iter()
            .any(|query| !query.param_bindings.is_empty())
        {
            return Err(DatabaseError::UnsupportedStmt(
                "correlated scalar queries in ORM projections are not supported yet".into(),
            ));
        }
        for expr in &self.select_list {
            if expr.has_agg_call(self.arena)? || expr.has_window_call(self.arena)? {
                return self.aggregate_without_group()?.finish();
            }
        }
        let plan = self
            .binder
            .bind_project(self.plan, self.select_list, self.arena)?;
        Ok(plan)
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanStart<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn from_plan(
        self,
        plan: LogicalPlan,
    ) -> Result<BindPlanFrom<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        Ok(BindPlanFrom {
            binder: self.binder,
            arena: self.arena,
            plan,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A, M> BindPlanSelectList<'s, 'a, 'b, 'arena, T, A, M>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn filter_expr(
        mut self,
        predicate: Option<ExprRef>,
    ) -> Result<BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if let Some(predicate) = predicate {
            self.plan = self
                .binder
                .bind_where_expr(self.plan, predicate, self.arena)?;
        }

        Ok(BindPlanFiltered {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list: self.select_list,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn aggregate<O>(
        mut self,
        group_by: Vec<ExprRef>,
        having: Option<ExprRef>,
        orderby: Option<impl IntoIterator<Item = O>>,
        mut bind_sort_field: impl FnMut(
            &mut Binder<'a, 'b, T, A>,
            &mut PlanArena<'arena>,
            &[ExprRef],
            O,
        ) -> Result<SortField, DatabaseError>,
    ) -> Result<BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        self.binder
            .extract_select_join(&mut self.select_list, self.arena);
        self.binder
            .extract_select_aggregate(&mut self.select_list, self.arena)?;

        // Statement-constant scalar values needed by grouping/aggregate arguments are initialized
        // before the aggregate, without turning them into input columns.
        self.plan =
            self.binder
                .bind_scalar_queries(self.plan, &[QueryBindStep::Agg], self.arena)?;
        if self
            .binder
            .context
            .scalar_queries
            .iter()
            .any(|query| !query.param_bindings.is_empty())
        {
            if !group_by.is_empty() || !self.binder.context.agg_calls.is_empty() {
                return Err(DatabaseError::UnsupportedStmt(
                    "correlated scalar queries with outer aggregation are not supported yet".into(),
                ));
            }
        } else if !group_by.is_empty() || !self.binder.context.agg_calls.is_empty() {
            self.plan = self.binder.bind_scalar_queries(
                self.plan,
                &[QueryBindStep::Project],
                self.arena,
            )?;
        }
        if !group_by.is_empty() {
            self.binder.extract_group_by_aggregate_exprs(
                &mut self.select_list,
                group_by,
                self.arena,
            )?;
        }

        let mut having_orderby = (None, None);
        if having.is_some() || orderby.is_some() {
            let select_list = &self.select_list;
            having_orderby = self.binder.extract_having_orderby_aggregate_exprs(
                having,
                orderby,
                |binder, orderby, arena| bind_sort_field(binder, arena, select_list, orderby),
                self.arena,
            )?;
        }

        if !self.binder.context.agg_calls.is_empty()
            || !self.binder.context.group_by_exprs.is_empty()
        {
            if self
                .binder
                .context
                .scalar_queries
                .iter()
                .any(|q| !q.param_bindings.is_empty())
            {
                return Err(DatabaseError::UnsupportedStmt(
                    "correlated scalar queries with outer aggregation are not supported yet".into(),
                ));
            }
            let agg_calls = std::mem::take(&mut self.binder.context.agg_calls);
            let group_by_exprs = std::mem::take(&mut self.binder.context.group_by_exprs);
            let output_exprs = self
                .select_list
                .iter_mut()
                .chain(having_orderby.0.iter_mut())
                .chain(
                    having_orderby
                        .1
                        .iter_mut()
                        .flat_map(|fields| fields.iter_mut().map(|field| &mut field.expr)),
                );
            self.binder.bind_aggregate_output_exprs_with_outputs(
                &agg_calls,
                &group_by_exprs,
                output_exprs,
                self.arena,
            )?;
            self.plan = self
                .binder
                .bind_aggregate(self.plan, agg_calls, group_by_exprs)?;
        }

        Ok(BindPlanAggregated {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list: self.select_list,
            having: having_orderby.0,
            orderby: having_orderby.1,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn having(
        mut self,
    ) -> Result<BindPlanHaving<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if let Some(having) = self.having {
            self.plan = self.binder.bind_having(self.plan, having, self.arena)?;
        }

        Ok(BindPlanHaving {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list: self.select_list,
            orderby: self.orderby,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanHaving<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn window(
        mut self,
    ) -> Result<BindPlanWindowed<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if self
            .binder
            .context
            .scalar_queries
            .iter()
            .any(|query| !query.param_bindings.is_empty())
        {
            for expr in self.select_list.iter().chain(
                self.orderby
                    .iter()
                    .flat_map(|fields| fields.iter().map(|field| &field.expr)),
            ) {
                if expr.has_window_call(self.arena)? {
                    return Err(DatabaseError::UnsupportedStmt(
                        "correlated scalar queries with outer windows are not supported yet".into(),
                    ));
                }
            }
        }
        if self.orderby.is_some()
            && self
                .binder
                .context
                .scalar_queries
                .iter()
                .any(|query| !query.param_bindings.is_empty())
        {
            return Err(DatabaseError::UnsupportedStmt(
                "correlated scalar values across ORDER BY require a row-scoped execution context"
                    .into(),
            ));
        }
        self.plan = self.binder.bind_scalar_queries(
            self.plan,
            &[QueryBindStep::Project, QueryBindStep::Sort],
            self.arena,
        )?;
        self.plan = self.binder.bind_window(
            self.plan,
            &mut self.select_list,
            &mut self.orderby,
            self.arena,
        )?;

        Ok(BindPlanWindowed {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list: self.select_list,
            orderby: self.orderby,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanWindowed<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn distinct(
        mut self,
        distinct: bool,
    ) -> Result<BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if distinct {
            struct OuterResult(bool);
            impl ExprVisitor<dyn MetaArena + '_> for OuterResult {
                fn visit_outer_value(
                    &mut self,
                    _id: ScalarQueryRef,
                    _ty: &LogicalType,
                    _arena: &(dyn MetaArena + '_),
                ) -> Result<(), DatabaseError> {
                    self.0 = true;
                    Ok(())
                }
            }
            let mut outer = OuterResult(false);
            for expr in &self.select_list {
                ExprVisitor::visit(&mut outer, *expr, self.arena)?;
            }
            if outer.0 {
                return Err(DatabaseError::UnsupportedStmt("correlated scalar values across DISTINCT require a row-scoped execution context".into()));
            }
            let distinct_outputs = self.select_list.clone();
            self.binder.bind_distinct_output_exprs(
                &distinct_outputs,
                self.select_list.iter_mut(),
                self.arena,
            )?;
            if let Some(orderby) = self.orderby.as_mut() {
                self.binder
                    .bind_distinct_orderby_exprs(&distinct_outputs, orderby, self.arena)?;
            }
            self.plan = self.binder.bind_distinct(self.plan, distinct_outputs)?;
        }

        Ok(BindPlanDistinct {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list: self.select_list,
            orderby: self.orderby,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn order_by(
        mut self,
    ) -> Result<BindPlanSorted<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if let Some(orderby) = self.orderby {
            self.plan = self.binder.bind_sort(self.plan, orderby, self.arena)?;
        }

        Ok(BindPlanSorted {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            select_list: self.select_list,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanSorted<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn project(
        mut self,
    ) -> Result<BindPlanProjected<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if !self.select_list.is_empty() {
            self.plan = self
                .binder
                .bind_project(self.plan, self.select_list, self.arena)?;
        }

        Ok(BindPlanProjected {
            plan: self.plan,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanProjected<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn insert_into(
        mut self,
        table_name: Option<TableName>,
    ) -> Result<BindPlanComplete, DatabaseError> {
        if let Some(table_name) = table_name {
            self.plan = LogicalPlan::new(
                Operator::Insert(InsertOperator {
                    table_name,
                    is_overwrite: false,
                    is_mapping_by_name: true,
                }),
                Childrens::Only(Box::new(self.plan)),
            )
        }

        Ok(BindPlanComplete { plan: self.plan })
    }
}

impl BindPlanComplete {
    pub(crate) fn finish(self) -> LogicalPlan {
        self.plan
    }
}

impl<'a: 'b, 'b, T: Transaction, A: AsRef<[(usize, LogicalType)]>> Binder<'a, 'b, T, A> {
    pub(crate) fn bind_scalar_queries(
        &mut self,
        mut plan: LogicalPlan,
        on_steps: &[QueryBindStep],
        _arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        for query in self
            .context
            .scalar_queries
            .extract_if(.., |query| on_steps.contains(&query.step))
        {
            plan =
                ScalarQueryInitOperator::build(plan, query.plan, query.value, query.param_bindings);
        }
        Ok(plan)
    }

    pub(crate) fn init_scalar_queries(&mut self, mut plan: LogicalPlan) -> LogicalPlan {
        for query in std::mem::take(&mut self.context.scalar_queries)
            .into_iter()
            .rev()
        {
            plan =
                ScalarQueryInitOperator::build(plan, query.plan, query.value, query.param_bindings);
        }
        plan
    }

    pub(crate) fn build_plan<'s, 'arena>(
        &'s mut self,
        arena: &'s mut PlanArena<'arena>,
    ) -> BindPlanStart<'s, 'a, 'b, 'arena, T, A> {
        BindPlanStart {
            binder: self,
            arena,
        }
    }

    /// Whether `exprs` only renames its input to an alias, as `bind_alias` builds for `FROM t AS x`
    /// (`temp == false`) or for a temp table (`temp == true`).
    fn is_alias_projection(exprs: &[ExprRef], temp: bool, arena: &PlanArena) -> bool {
        !exprs.is_empty()
            && exprs.iter().all(|expr| {
                matches!(
                    arena.expression(*expr),
                    ScalarExpression::Alias {
                        alias: AliasType::Expr(alias_expr),
                        ..
                    } if matches!(
                        alias_expr.unpack_alias_ref(arena),
                        ScalarExpression::ColumnRef { column, .. }
                            if matches!(
                                &arena.column(*column).summary().relation,
                                crate::catalog::ColumnRelation::Table { is_temp, .. }
                                    if *is_temp == temp
                            )
                    )
                )
            })
    }

    pub(crate) fn is_joined_values_source(
        join_type: Option<JoinType>,
        source: &Source<'a>,
        arena: &PlanArena,
    ) -> bool {
        join_type.is_some()
            && matches!(
                source,
                Source::Schema(schema_ref)
                    if !schema_ref.is_empty()
                        && schema_ref.iter().all(|column| {
                            matches!(
                                &arena.column(*column).summary().relation,
                                ColumnRelation::Table { is_temp: true, .. }
                            ) && arena.column(*column).id() == Some(ColumnId::default())
                        })
            )
    }

    pub(crate) fn resolve_source_columns_in_scope<'context>(
        context: &'context BinderContext<'a, T>,
        table_name: &str,
    ) -> Result<(&'context Source<'a>, usize), DatabaseError> {
        let mut position_offset = 0;

        for bound_source in &context.bind_table {
            if bound_source.matches_name(table_name) {
                return Ok((&bound_source.source, position_offset));
            }

            position_offset += bound_source.source.schema_len();
        }

        Err(DatabaseError::invalid_table(table_name))
    }

    fn localize_join_condition_from_join_scope(
        join_condition: &mut JoinCondition,
        left_len: usize,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        let JoinCondition::On { on, .. } = join_condition else {
            return Ok(());
        };

        let mut right_shift = PositionShift {
            delta: -(left_len as isize),
        };
        for (_, right_expr) in on {
            right_shift.visit(right_expr, arena)?;
        }

        Ok(())
    }

    fn localize_appended_right_outputs<'expr>(
        exprs: impl Iterator<Item = &'expr mut ExprRef>,
        appended_outputs: &[AppendedRightOutput],
        arena: &mut PlanArena,
    ) -> Result<(), DatabaseError> {
        struct AppendedRightOutputBinder<'a> {
            appended_outputs: &'a [AppendedRightOutput],
        }

        impl ExprVisitorMut for AppendedRightOutputBinder<'_> {
            fn visit_column_ref(
                &mut self,
                column: &mut ColumnRef,
                position: &mut usize,
                arena: &mut (dyn MetaArena + '_),
            ) -> Result<(), DatabaseError> {
                if let Some(output) = self.appended_outputs.iter().find(|output| {
                    *position == output.child_position && arena.same_column(*column, output.column)
                }) {
                    *position = output.output_position;
                }
                Ok(())
            }
        }

        let mut binder = AppendedRightOutputBinder { appended_outputs };
        for expr in exprs {
            binder.visit(expr, arena)?;
        }

        Ok(())
    }

    fn bind_set_cast(
        &mut self,
        mut left_plan: LogicalPlan,
        mut right_plan: LogicalPlan,
        arena: &mut PlanArena,
    ) -> Result<(LogicalPlan, LogicalPlan), DatabaseError> {
        let mut left_cast = vec![];
        let mut right_cast = vec![];

        let left_schema = left_plan.output_schema(arena);
        let right_schema = right_plan.output_schema(arena);

        for (position, (left_schema, right_schema)) in
            left_schema.iter().zip(right_schema.iter()).enumerate()
        {
            let cast_type = LogicalType::max_logical_type(
                arena.column(*left_schema).datatype(),
                arena.column(*right_schema).datatype(),
            )?
            .into_owned();

            let left_expr = ScalarExpression::column_expr(*left_schema, position)
                .type_cast(Cow::Borrowed(&cast_type), arena)?;
            left_cast.push(arena.alloc_expression(left_expr));

            let right_expr = ScalarExpression::column_expr(*right_schema, position)
                .type_cast(Cow::Owned(cast_type), arena)?;
            right_cast.push(arena.alloc_expression(right_expr));
        }

        if !left_cast.is_empty() {
            left_plan = LogicalPlan::new(
                Operator::Project(ProjectOperator { exprs: left_cast }),
                Childrens::Only(Box::new(left_plan)),
            );
        }

        if !right_cast.is_empty() {
            right_plan = LogicalPlan::new(
                Operator::Project(ProjectOperator { exprs: right_cast }),
                Childrens::Only(Box::new(right_plan)),
            );
        }

        Ok((left_plan, right_plan))
    }

    pub(crate) fn bind_set_operation_plans(
        &mut self,
        op: SetOperatorKind,
        is_all: bool,
        mut left_plan: LogicalPlan,
        mut right_plan: LogicalPlan,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut left_schema = left_plan.output_schema(arena);
        let mut right_schema = right_plan.output_schema(arena);

        let left_len = left_schema.len();

        if left_len != right_schema.len() {
            return Err(DatabaseError::MisMatch(
                "the lens on the left",
                "the lens on the right",
            ));
        }

        if !left_schema
            .iter()
            .zip(right_schema.iter())
            .all(|(left, right)| arena.column(*left).datatype() == arena.column(*right).datatype())
        {
            (left_plan, right_plan) = self.bind_set_cast(left_plan, right_plan, arena)?;
            left_schema = left_plan.output_schema(arena);
            right_schema = right_plan.output_schema(arena);
        }

        match op {
            SetOperatorKind::Union => {
                if is_all {
                    Ok(UnionOperator::build(
                        left_schema.clone(),
                        right_schema.clone(),
                        left_plan,
                        right_plan,
                    ))
                } else {
                    let distinct_exprs = left_schema
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(position, column)| {
                            arena.alloc_expression(ScalarExpression::column_expr(column, position))
                        })
                        .collect_vec();

                    let union_op = Operator::Union(UnionOperator {
                        left_schema_ref: left_schema.clone(),
                        _right_schema_ref: right_schema.clone(),
                    });

                    Ok(self.bind_distinct(
                        LogicalPlan::new(
                            union_op,
                            Childrens::Twins {
                                left: Box::new(left_plan),
                                right: Box::new(right_plan),
                            },
                        ),
                        distinct_exprs,
                    )?)
                }
            }
            SetOperatorKind::Except | SetOperatorKind::Intersect => {
                let kind = match op {
                    SetOperatorKind::Except => SetMembershipKind::Except,
                    SetOperatorKind::Intersect => SetMembershipKind::Intersect,
                    _ => unreachable!(),
                };

                if !is_all {
                    let left_distinct_exprs = left_schema
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(position, column)| {
                            arena.alloc_expression(ScalarExpression::column_expr(column, position))
                        })
                        .collect_vec();
                    let right_distinct_exprs = right_schema
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(position, column)| {
                            arena.alloc_expression(ScalarExpression::column_expr(column, position))
                        })
                        .collect_vec();

                    left_plan = self.bind_distinct(left_plan, left_distinct_exprs)?;
                    right_plan = self.bind_distinct(right_plan, right_distinct_exprs)?;
                    left_schema = left_plan.output_schema(arena);
                    right_schema = right_plan.output_schema(arena);
                }

                Ok(SetMembershipOperator::build(
                    kind,
                    left_schema.clone(),
                    right_schema.clone(),
                    left_plan,
                    right_plan,
                ))
            }
        }
    }

    pub(crate) fn bind_alias(
        &mut self,
        mut plan: LogicalPlan,
        alias_column: &[String],
        table_alias: TableName,
        table_name: TableName,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let input_schema = plan.output_schema(arena);
        let input_schema_len = input_schema.len();
        if !alias_column.is_empty() && alias_column.len() != input_schema_len {
            return Err(DatabaseError::MisMatch("alias", "columns"));
        }
        let mut alias_exprs = Vec::with_capacity(input_schema_len);

        for (position, column) in input_schema.iter().copied().enumerate() {
            let alias = if alias_column.is_empty() {
                arena.column(column).name().to_string()
            } else {
                alias_column[position].clone()
            };
            let (mut alias_column, column_id, is_temp) = {
                let source_column = arena.column(column);
                (
                    source_column.clone(),
                    source_column.id().unwrap_or_default(),
                    matches!(
                        &source_column.summary().relation,
                        ColumnRelation::Table { is_temp: true, .. }
                    ),
                )
            };
            alias_column.set_name(alias.clone());
            alias_column.set_ref_table(table_alias.clone(), column_id, is_temp);
            let alias_column = arena.alloc_column(alias_column);

            let expr = arena.alloc_expression(ScalarExpression::column_expr(column, position));
            let alias_expr =
                arena.alloc_expression(ScalarExpression::column_expr(alias_column, position));
            let alias_column_expr = arena.alloc_expression(ScalarExpression::Alias {
                expr,
                alias: AliasType::Expr(alias_expr),
            });
            self.context
                .add_alias(Some(table_alias.to_string()), alias, alias_column_expr);
            alias_exprs.push(alias_column_expr);
        }
        self.context.add_table_alias(table_alias, table_name);
        self.bind_project(plan, alias_exprs, arena)
    }

    fn bind_schema_source(
        &mut self,
        mut plan: LogicalPlan,
        source_name: TableName,
        arena: &mut PlanArena,
    ) -> LogicalPlan {
        let input_schema = plan.output_schema(arena);
        let input_schema_len = input_schema.len();
        let mut source_exprs = Vec::with_capacity(input_schema_len);

        for (position, column) in input_schema.iter().copied().enumerate() {
            let source_column = {
                let column_catalog = arena.column(column);
                let mut source_column = column_catalog.clone();
                source_column.set_ref_table(
                    source_name.clone(),
                    column_catalog.id().unwrap_or_default(),
                    true,
                );
                source_column
            };
            let source_column = arena.alloc_column(source_column);

            let expr = arena.alloc_expression(ScalarExpression::column_expr(column, position));
            let alias_expr =
                arena.alloc_expression(ScalarExpression::column_expr(source_column, position));
            source_exprs.push(arena.alloc_expression(ScalarExpression::Alias {
                expr,
                alias: AliasType::Expr(alias_expr),
            }));
        }

        Self::build_project_plan(plan, source_exprs)
    }

    pub(crate) fn bind_base_table_ref(
        &mut self,
        join_type: Option<JoinType>,
        table_name: TableName,
        alias: Option<TableAliasInput>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_alias = alias.as_ref().map(|alias| alias.name.clone());

        if let Some(plan_ref) = self.context.cte(&table_name).map(|cte| cte.plan_ref) {
            let mut plan = arena.plan(plan_ref).clone().clone_plan(arena)?;
            if let Some(alias) = alias {
                plan = self.bind_alias(
                    plan,
                    &alias.columns,
                    alias.name.clone(),
                    table_name.clone(),
                    arena,
                )?;
            }
            let output_schema = plan.output_schema(arena).clone();
            self.context.add_bound_source(
                table_name,
                table_alias,
                join_type,
                Source::Schema(output_schema),
            );
            return Ok(plan);
        }

        let with_pk = self.is_scan_with_pk(&table_name);
        let source = self
            .context
            .source_and_bind(table_name.clone(), table_alias.as_ref(), join_type, false)?
            .ok_or(DatabaseError::SourceNotFound)?;
        let mut plan = match source {
            Source::Table(table) => {
                TableScanOperator::build(table_name.clone(), table, with_pk, arena)?
            }
            Source::View(view) => {
                // Cached view expressions live in the persistent arena. Clone the
                // complete graph before optimizer passes rewrite expression nodes.
                view.plan.clone_plan(arena)?
            }
            Source::Schema(_) => {
                return Err(DatabaseError::UnsupportedStmt(
                    "derived source cannot be rebound as a base relation".to_string(),
                ))
            }
        };

        if let Some(alias) = alias {
            plan = self.bind_alias(
                plan,
                &alias.columns,
                alias.name.clone(),
                table_name.clone(),
                arena,
            )?;
            let output_schema = plan.output_schema(arena).clone();
            self.context.add_bound_source(
                table_name,
                Some(alias.name),
                join_type,
                Source::Schema(output_schema),
            );
        }
        Ok(plan)
    }

    pub(crate) fn bind_derived_source(
        &mut self,
        mut plan: LogicalPlan,
        alias: Option<TableAliasInput>,
        joint_type: Option<JoinType>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        if let Some(alias) = alias {
            let source_name = arena.temp_table();

            plan = self.bind_alias(
                plan,
                &alias.columns,
                alias.name.clone(),
                source_name.clone(),
                arena,
            )?;
            let output_schema = plan.output_schema(arena).clone();
            self.context.add_bound_source(
                alias.name.clone(),
                Some(alias.name),
                joint_type,
                Source::Schema(output_schema),
            );
        } else {
            let passthrough_source = {
                let output_schema = plan.output_schema(arena);
                let mut names = output_schema
                    .iter()
                    .filter_map(|column| arena.column(*column).table_name().cloned());
                let first = names.next();
                if first.is_some() && names.all(|name| Some(name) == first) {
                    first
                } else {
                    None
                }
            };
            let needs_virtual_source = passthrough_source.is_none();
            let source_name = passthrough_source.unwrap_or_else(|| arena.temp_table());

            if needs_virtual_source {
                plan = self.bind_schema_source(plan, source_name.clone(), arena);
            }
            let output_schema = plan.output_schema(arena).clone();
            self.context.add_bound_source(
                source_name.clone(),
                None,
                joint_type,
                Source::Schema(output_schema),
            );
        }

        Ok(plan)
    }

    pub(crate) fn bind_table_function_source(
        &mut self,
        expr: ScalarExpression,
        alias: Option<TableAliasInput>,
        joint_type: Option<JoinType>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let ScalarExpression::TableFunction(function) = expr else {
            return Err(DatabaseError::UnsupportedStmt(
                "table function source must be a table function expression".to_string(),
            ));
        };

        let mut table_alias = None;
        let table_name: TableName = function.summary().name.clone();
        let mut plan = FunctionScanOperator::build(function);

        if let Some(alias) = alias {
            table_alias = Some(alias.name.clone());

            plan = self.bind_alias(plan, &alias.columns, alias.name, table_name.clone(), arena)?;
        }

        let source = Source::Schema(plan.output_schema(arena).clone());
        self.context
            .add_bound_source(table_name, table_alias, joint_type, source);
        Ok(plan)
    }

    /// Normalize select item.
    ///
    /// - Qualified name, e.g. `SELECT t.a FROM t`
    /// - Qualified name with wildcard, e.g. `SELECT t.* FROM t,t1`
    /// - Scalar expression or aggregate expression, e.g. `SELECT COUNT(*) + 1 AS count FROM t`
    ///
    #[allow(unused_assignments)]
    pub(crate) fn bind_table_column_refs(
        context: &BinderContext<'a, T>,
        arena: &mut PlanArena,
        exprs: &mut Vec<ExprRef>,
        table_name: TableName,
        is_qualified_wildcard: bool,
    ) -> Result<(), DatabaseError> {
        let (source, position_offset) =
            Self::resolve_source_columns_in_scope(context, table_name.as_ref())?;

        let fn_not_on_using = |column: &ColumnRef, arena: &PlanArena<'_>| {
            let column_catalog = arena.column(*column);
            if context.using.is_empty() {
                return Some(&table_name) == column_catalog.table_name();
            }
            is_qualified_wildcard
                || Some(&table_name) == column_catalog.table_name()
                    && !context
                        .using
                        .values()
                        .any(|using_column| using_column.hides_column(column, arena))
        };

        for (position, column) in source.schema().iter().enumerate() {
            if !fn_not_on_using(column, arena) {
                continue;
            }
            exprs.push(Self::wildcard_column_expr(
                context,
                arena,
                column,
                position_offset + position,
                is_qualified_wildcard,
            )?);
        }
        Ok(())
    }

    fn wildcard_column_expr(
        context: &BinderContext<'a, T>,
        arena: &mut PlanArena,
        column: &ColumnRef,
        position: usize,
        is_qualified_wildcard: bool,
    ) -> Result<ExprRef, DatabaseError> {
        let expr = context
            .using
            .values()
            .find(|using| {
                !is_qualified_wildcard
                    && matches!(using.join_type, JoinType::Full)
                    && arena.same_column(using.left_column, *column)
            })
            .cloned()
            .map(|using| {
                let alias = AliasType::Name(arena.column(*column).name().to_string());
                let expr = using.visible_expr(arena)?;
                let expr = arena.alloc_expression(expr);
                Ok::<_, DatabaseError>(ScalarExpression::Alias { expr, alias })
            })
            .transpose()?
            .unwrap_or_else(|| ScalarExpression::column_expr(*column, position));
        Ok(arena.alloc_expression(expr))
    }

    pub(crate) fn bind_join_plans(
        &mut self,
        mut left: LogicalPlan,
        mut right: LogicalPlan,
        join_type: JoinType,
        constraint: JoinConstraintInput,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let left_len = left.output_schema(arena).len();
        right.output_schema(arena);
        let left_schema = left.output_schema(arena);
        let right_schema = right.output_schema(arena);
        let mut on =
            self.bind_join_constraint(join_type, constraint, left_schema, right_schema, arena)?;
        Self::localize_join_condition_from_join_scope(&mut on, left_len, arena)?;

        Ok(LJoinOperator::build(
            left,
            right,
            on,
            join_type,
            self.force_nested_loop,
        ))
    }

    pub(crate) fn bind_where_expr(
        &mut self,
        mut children: LogicalPlan,
        predicate: ExprRef,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        children = self.bind_scalar_queries(children, &[QueryBindStep::Where], arena)?;
        self.context.step(QueryBindStep::Where);
        if predicate.has_agg_call(arena)? {
            return Err(DatabaseError::AggMiss(
                "aggregate functions are not allowed in WHERE".into(),
            ));
        }

        if let Some(sub_queries) = self.context.sub_queries_at_now() {
            for sub_query in sub_queries {
                match sub_query {
                    SubQueryType::ExistsSubQuery {
                        plan,
                        correlated,
                        output_column,
                    } => {
                        let left_schema = children.output_schema(arena).clone();
                        let (plan, predicates) = Self::prepare_mark_apply(
                            predicate,
                            &output_column,
                            left_schema.as_ref(),
                            plan,
                            correlated,
                            false,
                            Vec::new(),
                            arena,
                        )?;
                        children = MarkApplyOperator::build_exists(
                            children,
                            plan,
                            output_column,
                            predicates,
                        );
                    }
                    SubQueryType::QuantifiedSubQuery {
                        quantifier,
                        plan,
                        correlated,
                        output_column,
                        predicate: mut quantified_predicate,
                        ..
                    } => {
                        if correlated {
                            quantified_predicate = Self::rewrite_correlated_quantified_predicate(
                                quantified_predicate,
                                arena,
                            );
                        }
                        let left_schema = children.output_schema(arena).clone();
                        let (plan, predicates) = Self::prepare_mark_apply(
                            predicate,
                            &output_column,
                            left_schema.as_ref(),
                            plan,
                            correlated,
                            true,
                            vec![arena.alloc_expression(quantified_predicate)],
                            arena,
                        )?;
                        children = MarkApplyOperator::build_quantified(
                            children,
                            plan,
                            quantifier,
                            output_column,
                            predicates,
                        );
                    }
                }
            }
            {
                let passthrough_exprs = children
                    .output_schema(arena)
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(position, column)| {
                        arena.alloc_expression(ScalarExpression::column_expr(column, position))
                    })
                    .collect();
                let filter = FilterOperator::build(predicate, children, false);
                return Ok(LogicalPlan::new(
                    Operator::Project(ProjectOperator {
                        exprs: passthrough_exprs,
                    }),
                    Childrens::Only(Box::new(filter)),
                ));
            }
        }
        Ok(FilterOperator::build(predicate, children, false))
    }

    fn ensure_mark_apply_right_outputs(
        plan: &mut LogicalPlan,
        predicates: &[ExprRef],
        arena: &mut PlanArena,
    ) -> Result<Vec<AppendedRightOutput>, DatabaseError> {
        let output_schema = plan.output_schema(arena).clone();
        let output_len = output_schema.len();
        if let LogicalPlan {
            operator: Operator::Project(op),
            childrens,
            ..
        } = plan
        {
            // An alias projection already outputs every input column under the alias; its input
            // columns may look like the outer query's (`t1` in `FROM t1 AS x`) but are not.
            if Self::is_alias_projection(&op.exprs, false, arena) {
                return Ok(Vec::new());
            }
            let Childrens::Only(child) = childrens.as_mut() else {
                return Ok(Vec::new());
            };
            let child_schema = child.output_schema(arena);
            let mut appended_outputs = Vec::new();
            for (position, column) in child_schema.iter().enumerate() {
                if output_schema.contains(column) {
                    continue;
                }
                let mut referenced = false;
                for expr in predicates {
                    if expr.any_referenced_column(arena, |arena, candidate| {
                        arena.same_column(*candidate, *column)
                    })? {
                        referenced = true;
                        break;
                    }
                }
                if referenced {
                    op.exprs.push(
                        arena.alloc_expression(ScalarExpression::column_expr(*column, position)),
                    );
                    appended_outputs.push(AppendedRightOutput {
                        column: *column,
                        child_position: position,
                        output_position: output_len + appended_outputs.len(),
                    });
                }
            }
            plan.reset_output_schema_cache();
            return Ok(appended_outputs);
        }

        Ok(Vec::new())
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_mark_apply(
        mut predicate: ExprRef,
        output_column: &ColumnRef,
        left_schema: &Schema,
        plan: LogicalPlan,
        correlated: bool,
        preserve_projection: bool,
        mut apply_predicates: Vec<ExprRef>,
        arena: &mut PlanArena,
    ) -> Result<(LogicalPlan, Vec<ExprRef>), DatabaseError> {
        let left_len = left_schema.len();
        MarkerPositionGlobalizer {
            output_column,
            left_len,
        }
        .visit(&mut predicate, arena)?;

        let (mut plan, correlated_filters) = if correlated {
            Self::prepare_correlated_subquery_plan(plan, left_schema, preserve_projection, arena)?
        } else {
            (plan, Vec::new())
        };
        apply_predicates.extend(correlated_filters);

        if correlated {
            let appended_right_outputs =
                Self::ensure_mark_apply_right_outputs(&mut plan, &apply_predicates, arena)?;
            if !appended_right_outputs.is_empty() {
                Self::localize_appended_right_outputs(
                    apply_predicates.iter_mut(),
                    &appended_right_outputs,
                    arena,
                )?;
            }
        }
        let right_schema = plan.output_schema(arena);
        for expr in &mut apply_predicates {
            RightSidePositionGlobalizer {
                right_schema,
                left_len,
            }
            .visit(expr, arena)?;
        }

        Ok((plan, apply_predicates))
    }

    fn rewrite_correlated_quantified_predicate(
        predicate: ScalarExpression,
        arena: &PlanArena<'_>,
    ) -> ScalarExpression {
        let strip_projection_alias = |expr| match arena.expression(expr) {
            ScalarExpression::Alias {
                expr,
                alias: AliasType::Expr(_),
            } => *expr,
            _ => expr,
        };

        match predicate {
            ScalarExpression::Binary {
                op,
                left_expr,
                right_expr,
                ty,
                ..
            } => ScalarExpression::Binary {
                op,
                left_expr: strip_projection_alias(left_expr),
                right_expr: strip_projection_alias(right_expr),
                evaluator: None,
                ty,
            },
            predicate => predicate,
        }
    }

    fn plan_has_correlated_refs(
        plan: &LogicalPlan,
        left_schema: &Schema,
        arena: &mut PlanArena,
    ) -> Result<bool, DatabaseError> {
        if !plan
            .operator
            .visit_referenced_columns(arena, &mut |arena, column| {
                !left_schema
                    .iter()
                    .any(|left| arena.same_column(*left, *column))
            })?
        {
            return Ok(true);
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => Self::plan_has_correlated_refs(child, left_schema, arena),
            Childrens::Twins { left, right } => {
                if Self::plan_has_correlated_refs(left, left_schema, arena)? {
                    Ok(true)
                } else {
                    Self::plan_has_correlated_refs(right, left_schema, arena)
                }
            }
            Childrens::None => Ok(false),
        }
    }

    fn expr_has_correlated_refs(
        expr: ExprRef,
        left_schema: &Schema,
        arena: &mut PlanArena,
    ) -> Result<bool, DatabaseError> {
        expr.any_referenced_column(arena, |arena, column| {
            left_schema
                .iter()
                .any(|left| arena.same_column(*left, *column))
        })
    }

    fn split_conjuncts(expr: ExprRef, exprs: &mut Vec<ExprRef>, arena: &PlanArena<'_>) {
        let expr = expr.unpack_alias(arena);
        match arena.expression(expr) {
            ScalarExpression::Binary {
                op: BinaryOperator::And,
                left_expr,
                right_expr,
                ..
            } => {
                Self::split_conjuncts(*left_expr, exprs, arena);
                Self::split_conjuncts(*right_expr, exprs, arena);
            }
            _ => exprs.push(expr),
        }
    }

    fn combine_conjuncts(exprs: Vec<ExprRef>, arena: &mut PlanArena<'_>) -> Option<ExprRef> {
        exprs.into_iter().reduce(|acc, expr| {
            arena.alloc_expression(ScalarExpression::Binary {
                op: BinaryOperator::And,
                left_expr: acc,
                right_expr: expr,
                evaluator: None,
                ty: LogicalType::Boolean,
            })
        })
    }

    fn prepare_correlated_subquery_plan(
        plan: LogicalPlan,
        left_schema: &Schema,
        preserve_projection: bool,
        arena: &mut PlanArena,
    ) -> Result<(LogicalPlan, Vec<ExprRef>), DatabaseError> {
        match plan.childrens.as_ref() {
            Childrens::Only(_) => {}
            Childrens::Twins { .. } => {
                if Self::plan_has_correlated_refs(&plan, left_schema, arena)? {
                    return Err(DatabaseError::UnsupportedStmt(
                        "correlated EXISTS/NOT EXISTS does not support set or join subqueries"
                            .to_string(),
                    ));
                }
            }
            Childrens::None => {}
        }

        match plan {
            // `FROM t AS x`: the alias gives the subquery's columns their own identity, which its
            // predicates are bound to, so it is kept. It only reads its input, never the outer query.
            plan if matches!(
                &plan.operator,
                Operator::Project(op) if Self::is_alias_projection(&op.exprs, false, arena)
            ) =>
            {
                Ok((plan, vec![]))
            }
            LogicalPlan {
                operator: Operator::Filter(op),
                childrens,
                ..
            } => {
                let child = childrens.pop_only();
                let (child, mut correlated_filters) = Self::prepare_correlated_subquery_plan(
                    child,
                    left_schema,
                    preserve_projection,
                    arena,
                )?;
                let mut local_filters = Vec::new();
                let mut predicates = Vec::new();
                Self::split_conjuncts(op.predicate, &mut predicates, arena);
                for predicate in predicates {
                    if Self::expr_has_correlated_refs(predicate, left_schema, arena)? {
                        correlated_filters.push(predicate);
                    } else {
                        local_filters.push(predicate);
                    }
                }
                let plan = if let Some(predicate) = Self::combine_conjuncts(local_filters, arena) {
                    FilterOperator::build(predicate, child, op.having)
                } else {
                    child
                };
                Ok((plan, correlated_filters))
            }
            LogicalPlan {
                operator: Operator::Project(op),
                childrens,
                ..
            } => {
                let child = childrens.pop_only();
                let (child, mut correlated_filters) = Self::prepare_correlated_subquery_plan(
                    child,
                    left_schema,
                    preserve_projection,
                    arena,
                )?;

                if !preserve_projection || Self::is_alias_projection(&op.exprs, true, arena) {
                    Ok((child, correlated_filters))
                } else {
                    for expr in &op.exprs {
                        if Self::expr_has_correlated_refs(*expr, left_schema, arena)? {
                            return Err(DatabaseError::UnsupportedStmt(
                                "correlated references in the SELECT list of an IN/ANY/ALL subquery are not supported"
                                    .to_string(),
                            ));
                        }
                    }
                    let mut binder = ProjectionOutputBinder::new(&op.exprs);
                    for expr in &mut correlated_filters {
                        binder.visit(expr, arena)?;
                    }
                    Ok((
                        LogicalPlan::new(Operator::Project(op), Childrens::Only(Box::new(child))),
                        correlated_filters,
                    ))
                }
            }
            LogicalPlan {
                operator: Operator::Sort(_),
                childrens,
                ..
            }
            | LogicalPlan {
                operator: Operator::Limit(_),
                childrens,
                ..
            }
            | LogicalPlan {
                operator: Operator::TopK(_),
                childrens,
                ..
            } => Self::prepare_correlated_subquery_plan(
                childrens.pop_only(),
                left_schema,
                preserve_projection,
                arena,
            ),
            plan => {
                if Self::plan_has_correlated_refs(&plan, left_schema, arena)? {
                    Err(DatabaseError::UnsupportedStmt(
                        "correlated EXISTS/NOT EXISTS only supports filter-based subqueries"
                            .to_string(),
                    ))
                } else {
                    Ok((plan, vec![]))
                }
            }
        }
    }

    fn bind_having(
        &mut self,
        mut children: LogicalPlan,
        mut having: ExprRef,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        children = self.bind_scalar_queries(children, &[QueryBindStep::Having], arena)?;
        self.context.step(QueryBindStep::Having);

        self.validate_having_orderby(having, arena)?;
        self.bind_aggregate_output_exprs(std::iter::once(&mut having), arena)?;
        Ok(FilterOperator::build(having, children, true))
    }

    pub(crate) fn build_project_plan(
        children: LogicalPlan,
        select_list: Vec<ExprRef>,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs: select_list }),
            Childrens::Only(Box::new(children)),
        )
    }

    pub(crate) fn bind_project(
        &mut self,
        children: LogicalPlan,
        select_list: Vec<ExprRef>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let children = self.bind_scalar_queries(children, &[QueryBindStep::Project], arena)?;
        self.context.step(QueryBindStep::Project);
        Ok(Self::build_project_plan(children, select_list))
    }

    pub(crate) fn bind_sort(
        &mut self,
        children: LogicalPlan,
        sort_fields: Vec<SortField>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let children = self.bind_scalar_queries(children, &[QueryBindStep::Sort], arena)?;
        self.context.step(QueryBindStep::Sort);
        Ok(LogicalPlan::new(
            Operator::Sort(SortOperator { sort_fields }),
            Childrens::Only(Box::new(children)),
        ))
    }

    pub(crate) fn bind_limit_values(
        &mut self,
        children: LogicalPlan,
        offset_value: Option<usize>,
        limit_value: Option<usize>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Limit);

        Ok(LimitOperator::build(offset_value, limit_value, children))
    }

    pub fn extract_select_join(&mut self, select_items: &mut [ExprRef], arena: &mut PlanArena) {
        if self.context.bind_table.len() < 2 {
            return;
        }

        let mut table_force_nullable = Vec::with_capacity(self.context.bind_table.len());
        let mut left_table_force_nullable = false;
        let mut left_table = None;

        for bound_source in &self.context.bind_table {
            if let Some(join_type) = bound_source.join_type {
                let (left_force_nullable, right_force_nullable) = joins_nullable(&join_type);
                table_force_nullable.push((
                    &bound_source.table_name,
                    &bound_source.source,
                    right_force_nullable,
                ));
                left_table_force_nullable = left_force_nullable;
            } else {
                left_table = Some((&bound_source.table_name, &bound_source.source));
            }
        }

        if let Some((table_name, table)) = left_table {
            table_force_nullable.push((table_name, table, left_table_force_nullable));
        }

        for expr in select_items {
            let mut expression =
                std::mem::replace(&mut *arena.expression_mut(*expr), ScalarExpression::Empty);
            if let ScalarExpression::ColumnRef { column, .. } = &mut expression {
                let _ = table_force_nullable
                    .iter()
                    .find(|(table_name, _source, _)| {
                        arena
                            .column(*column)
                            .table_name()
                            .is_some_and(|column_table| column_table == *table_name)
                    })
                    .map(|(_, _, nullable)| {
                        if let Some(new_column) = arena.nullable_for_join(*column, *nullable) {
                            *column = new_column;
                        }
                    });
            }
            *arena.expression_mut(*expr) = expression;
        }
    }

    fn bind_join_constraint(
        &mut self,
        join_type: JoinType,
        constraint: JoinConstraintInput,
        left_schema: &Schema,
        right_schema: &Schema,
        arena: &mut PlanArena,
    ) -> Result<JoinCondition, DatabaseError> {
        match constraint {
            JoinConstraintInput::On(expr) => {
                // left and right columns that match equi-join pattern
                let mut on_keys: Vec<(ExprRef, ExprRef)> = vec![];
                // expression that didn't match equi-join pattern
                let mut filter = vec![];

                extract_join_keys(
                    expr,
                    &mut on_keys,
                    &mut filter,
                    left_schema,
                    right_schema,
                    arena,
                )?;

                // combine multiple filter exprs into one BinaryExpr
                let join_filter = Self::combine_conjuncts(filter, arena);
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: join_filter,
                })
            }
            JoinConstraintInput::Using(names) => {
                fn find_column<'a>(
                    schema: &'a Schema,
                    name: &'a str,
                    arena: &PlanArena,
                ) -> Option<(usize, &'a ColumnRef)> {
                    schema
                        .iter()
                        .enumerate()
                        .find(|(_, column)| arena.column(**column).name() == name)
                }

                let mut on_keys: Vec<(ExprRef, ExprRef)> = Vec::new();

                for name in names {
                    let (Some((left_position, left_column)), Some((right_position, right_column))) = (
                        find_column(left_schema, &name, arena),
                        find_column(right_schema, &name, arena),
                    ) else {
                        return Err(DatabaseError::invalid_column(
                            "not found column".to_string(),
                        ));
                    };
                    self.context.add_using(
                        name.clone(),
                        join_type,
                        left_column,
                        left_position,
                        right_column,
                        left_schema.len() + right_position,
                    )?;
                    let left_expr = arena.alloc_expression(ScalarExpression::column_expr(
                        *left_column,
                        left_position,
                    ));
                    let right_expr = arena.alloc_expression(ScalarExpression::column_expr(
                        *right_column,
                        left_schema.len() + right_position,
                    ));
                    let ty = LogicalType::max_logical_type(
                        &left_expr.return_type(arena),
                        &right_expr.return_type(arena),
                    )?
                    .into_owned();
                    on_keys.push((
                        left_expr.type_cast(Cow::Borrowed(&ty), arena)?,
                        right_expr.type_cast(Cow::Borrowed(&ty), arena)?,
                    ));
                }
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: None,
                })
            }
            JoinConstraintInput::None => Ok(JoinCondition::None),
            JoinConstraintInput::Natural => {
                let fn_names = |schema: &Schema| -> HashSet<String> {
                    schema
                        .iter()
                        .map(|column| arena.column(*column).name().to_string())
                        .collect()
                };
                let mut on_keys: Vec<(ExprRef, ExprRef)> = Vec::new();

                for name in fn_names(left_schema).intersection(&fn_names(right_schema)) {
                    if let (
                        Some((left_position, left_column)),
                        Some((right_position, right_column)),
                    ) = (
                        left_schema
                            .iter()
                            .enumerate()
                            .find(|(_, column)| arena.column(**column).name() == name),
                        right_schema
                            .iter()
                            .enumerate()
                            .find(|(_, column)| arena.column(**column).name() == name),
                    ) {
                        let left_expr = arena.alloc_expression(ScalarExpression::column_expr(
                            *left_column,
                            left_position,
                        ));
                        let right_expr = arena.alloc_expression(ScalarExpression::column_expr(
                            *right_column,
                            left_schema.len() + right_position,
                        ));

                        self.context.add_using(
                            name.clone(),
                            join_type,
                            left_column,
                            left_position,
                            right_column,
                            left_schema.len() + right_position,
                        )?;
                        let ty = LogicalType::max_logical_type(
                            &left_expr.return_type(arena),
                            &right_expr.return_type(arena),
                        )?
                        .into_owned();
                        on_keys.push((
                            left_expr.type_cast(Cow::Borrowed(&ty), arena)?,
                            right_expr.type_cast(Cow::Borrowed(&ty), arena)?,
                        ));
                    }
                }
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: None,
                })
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{ProjectionOutputBinder, RightSidePositionGlobalizer};
    use crate::binder::test::build_t1_table;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::expression::visitor_mut::ExprVisitorMut;
    use crate::expression::{AliasType, ScalarExpression};
    use crate::planner::operator::join::{JoinCondition, JoinType};
    use crate::planner::operator::mark_apply::{
        MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier,
    };
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena, TableArenaCell};
    use crate::types::LogicalType;

    fn test_column(arena: &mut PlanArena, name: &str, position: usize) -> ExprRef {
        let column = arena.alloc_column(ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        arena.alloc_expression(ScalarExpression::column_expr(column, position))
    }

    #[test]
    fn test_select_bind() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;

        let plan_1 = table_states.plan("select * from t1")?;
        println!("just_col:\n {plan_1:#?}");
        let plan_2 = table_states.plan("select t1.c1, t1.c2 from t1")?;
        println!("table_with_col:\n {plan_2:#?}");
        let plan_3 = table_states.plan("select t1.c1, t1.c2 from t1 where c1 > 2")?;
        println!("table_with_col_and_c1_compare_constant:\n {plan_3:#?}");
        let plan_4 = table_states.plan("select t1.c1, t1.c2 from t1 where c1 > c2")?;
        println!("table_with_col_and_c1_compare_c2:\n {plan_4:#?}");
        let plan_5 = table_states.plan("select avg(t1.c1) from t1")?;
        println!("table_with_col_and_c1_avg:\n {plan_5:#?}");
        let plan_6 = table_states.plan("select t1.c1, t1.c2 from t1 where (t1.c1 - t1.c2) > 1")?;
        println!("table_with_col_nested:\n {plan_6:#?}");

        let plan_7 = table_states.plan("select * from t1 limit 1")?;
        println!("limit:\n {plan_7:#?}");

        let plan_8 = table_states.plan("select * from t1 offset 2")?;
        println!("offset:\n {plan_8:#?}");

        let plan_9 =
            table_states.plan("select c1, c3 from t1 inner join t2 on c1 = c3 and c1 > 1")?;
        println!("join:\n {plan_9:#?}");

        Ok(())
    }

    #[test]
    fn test_right_side_position_globalizer_only_shifts_right_columns() -> Result<(), DatabaseError>
    {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left_column = arena.alloc_column(ColumnCatalog::new(
            "left".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let right_column = arena.alloc_column(ColumnCatalog::new(
            "right".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let right_schema = vec![right_column];
        let left_expr = arena.alloc_expression(ScalarExpression::column_expr(left_column, 0));
        let right_expr = arena.alloc_expression(ScalarExpression::column_expr(right_column, 0));
        let mut expr = arena.alloc_expression(ScalarExpression::Binary {
            op: crate::expression::BinaryOperator::Eq,
            left_expr,
            right_expr,
            evaluator: None,
            ty: LogicalType::Boolean,
        });

        RightSidePositionGlobalizer {
            right_schema: &right_schema,
            left_len: 2,
        }
        .visit(&mut expr, &mut arena)?;

        let ScalarExpression::Binary {
            left_expr,
            right_expr,
            ..
        } = arena.expression(expr)
        else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef {
            position: left_position,
            ..
        } = arena.expression(*left_expr)
        else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = arena.expression(*right_expr)
        else {
            unreachable!()
        };
        assert_eq!((*left_position, *right_position), (0, 2));

        Ok(())
    }

    #[test]
    fn test_projection_output_binder_rewrites_to_project_slot() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let project_inner = test_column(&mut arena, "c1", 0);
        let project_output = arena.alloc_expression(ScalarExpression::Alias {
            expr: project_inner,
            alias: AliasType::Name("v".to_string()),
        });
        let expr_inner = test_column(&mut arena, "c1", 0);
        let mut expr = arena.alloc_expression(ScalarExpression::Alias {
            expr: expr_inner,
            alias: AliasType::Name("v".to_string()),
        });

        ProjectionOutputBinder::new(std::slice::from_ref(&project_output))
            .visit(&mut expr, &mut arena)?;

        let output_column = project_output.output_column_ref(&mut arena);
        let expected = arena.alloc_expression(ScalarExpression::column_expr(output_column, 0));
        assert!(expr.eq_ignore_colref_pos(expected, &arena));
        Ok(())
    }

    fn find_join(plan: &LogicalPlan) -> Option<(&JoinType, &JoinCondition)> {
        if let Operator::Join(op) = &plan.operator {
            return Some((&op.join_type, &op.on));
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => find_join(child),
            Childrens::Twins { left, right } => find_join(left).or_else(|| find_join(right)),
            Childrens::None => None,
        }
    }

    fn find_mark_apply(plan: &LogicalPlan) -> Option<&MarkApplyOperator> {
        if let Operator::MarkApply(op) = &plan.operator {
            return Some(op);
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => find_mark_apply(child),
            Childrens::Twins { left, right } => {
                find_mark_apply(left).or_else(|| find_mark_apply(right))
            }
            Childrens::None => None,
        }
    }

    fn assert_quantified_mark_apply(
        plan: &LogicalPlan,
        quantifier: MarkApplyQuantifier,
        predicate_len: usize,
    ) {
        let Some(mark_apply) = find_mark_apply(plan) else {
            panic!("expected quantified subquery to introduce a mark apply")
        };

        assert_eq!(mark_apply.kind, MarkApplyKind::Quantified(quantifier));
        assert_eq!(mark_apply.predicates().len(), predicate_len);
    }

    #[test]
    fn test_scalar_subquery_in_where_binds_as_init() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let mut arena = PlanArena::new(&table_states.table_arena);
        let mut plan = table_states.plan_with_arena(
            "select * from t1 where c1 = (select max(c3) from t2)",
            &mut arena,
        )?;
        assert!(matches!(plan.operator, Operator::Project(_)));
        assert!(find_join(&plan).is_none());
        let Childrens::Only(filter) = plan.childrens.as_mut() else {
            panic!("expected project input")
        };
        let Childrens::Only(init) = filter.childrens.as_mut() else {
            panic!("expected filter input")
        };
        assert!(matches!(init.operator, Operator::ScalarQueryInit(_)));
        let Childrens::Twins { left, right } = init.childrens.as_mut() else {
            panic!("expected init children")
        };
        assert_eq!(left.output_schema(&mut arena).len(), 2);
        assert_eq!(right.output_schema(&mut arena).len(), 1);
        assert_eq!(plan.output_schema(&mut arena).len(), 2);
        Ok(())
    }

    #[test]
    fn test_in_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states.plan("select * from t1 where c1 in (select c3 from t2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::Any, 1);

        Ok(())
    }

    #[test]
    fn test_any_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states.plan("select * from t1 where c1 < any(select c3 from t2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::Any, 1);

        Ok(())
    }

    #[test]
    fn test_some_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states.plan("select * from t1 where c1 = some(select c3 from t2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::Any, 1);

        Ok(())
    }

    #[test]
    fn test_all_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states.plan("select * from t1 where c1 > all(select c3 from t2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::All, 1);

        Ok(())
    }

    #[test]
    fn test_correlated_in_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan =
            table_states.plan("select * from t1 where c1 in (select c3 from t2 where c4 = c2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::Any, 2);

        Ok(())
    }

    #[test]
    fn test_correlated_any_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states
            .plan("select * from t1 where c1 < any(select c3 from t2 where c4 = c2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::Any, 2);

        Ok(())
    }

    #[test]
    fn test_correlated_all_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states
            .plan("select * from t1 where c1 > all(select c3 from t2 where c4 = c2)")?;
        assert_quantified_mark_apply(&plan, MarkApplyQuantifier::All, 2);

        Ok(())
    }

    #[test]
    fn test_multiple_scalar_subqueries_have_independent_slots() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let mut arena = PlanArena::new(&table_states.table_arena);
        let plan = table_states.plan_with_arena(
            "select * from t1 where c1 <= (select 4) and c1 > (select 1)",
            &mut arena,
        )?;
        let init = plan.childrens.only().childrens.only();
        let Operator::ScalarQueryInit(first) = &init.operator else {
            panic!("expected scalar init")
        };
        let Childrens::Twins { left, .. } = init.childrens.as_ref() else {
            panic!("expected init children")
        };
        let Operator::ScalarQueryInit(second) = &left.operator else {
            panic!("expected second scalar init")
        };
        assert_ne!(first.reference(&arena), second.reference(&arena));
        assert!(find_join(&plan).is_none());
        Ok(())
    }
}
