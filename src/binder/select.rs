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

use crate::{
    expression::ScalarExpression,
    planner::{
        operator::{
            filter::FilterOperator, join::JoinOperator as LJoinOperator, limit::LimitOperator,
            mark_apply::MarkApplyOperator, project::ProjectOperator,
            scalar_apply::ScalarApplyOperator, Operator,
        },
        operator::{join::JoinType, table_scan::TableScanOperator},
    },
    types::value::DataValue,
};
use std::collections::HashSet;

use super::{Binder, BinderContext, QueryBindStep, SetOperatorKind, Source, SubQueryType};

use crate::catalog::{ColumnRef, ColumnRelation, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::join::joins_nullable;
use crate::expression::visitor_mut::{walk_mut_expr, PositionShift, VisitorMut};
use crate::expression::{AliasType, BinaryOperator};
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::set_membership::{SetMembershipKind, SetMembershipOperator};
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::union::UnionOperator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::tuple::Schema;
use crate::types::{ColumnId, LogicalType};
use itertools::Itertools;

struct RightSidePositionGlobalizer<'a, 'p> {
    right_schema: &'a Schema,
    left_len: usize,
    arena: &'a crate::planner::PlanArena<'p>,
}

impl<'a> VisitorMut<'a> for RightSidePositionGlobalizer<'_, '_> {
    fn visit_column_ref(
        &mut self,
        column: &'a mut ColumnRef,
        position: &'a mut usize,
    ) -> Result<(), DatabaseError> {
        if self
            .right_schema
            .iter()
            .any(|right| self.arena.same_column(*right, *column))
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

struct SplitScopePositionRebinder<'a, 'p> {
    left_schema: &'a Schema,
    right_schema: &'a Schema,
    arena: &'a crate::planner::PlanArena<'p>,
}

impl VisitorMut<'_> for SplitScopePositionRebinder<'_, '_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        if let Some(left_position) = self
            .left_schema
            .iter()
            .position(|candidate| self.arena.same_column(*candidate, *column))
        {
            *position = left_position;
        } else if let Some(right_position) = self
            .right_schema
            .iter()
            .position(|candidate| self.arena.same_column(*candidate, *column))
        {
            *position = right_position;
        }
        Ok(())
    }
}

struct MarkerPositionGlobalizer<'a, 'p> {
    output_column: &'a ColumnRef,
    left_len: usize,
    arena: &'a crate::planner::PlanArena<'p>,
}

impl VisitorMut<'_> for MarkerPositionGlobalizer<'_, '_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        if self.arena.same_column(*column, *self.output_column) {
            *position = self.left_len;
        }
        Ok(())
    }
}

struct ProjectionOutputBinder<'a, 'p> {
    project_exprs: &'a [ScalarExpression],
    arena: &'a mut crate::planner::PlanArena<'p>,
}

impl<'a, 'p> ProjectionOutputBinder<'a, 'p> {
    fn new(
        project_exprs: &'a [ScalarExpression],
        arena: &'a mut crate::planner::PlanArena<'p>,
    ) -> Self {
        Self {
            project_exprs,
            arena,
        }
    }

    fn output_ref(&mut self, expr: &ScalarExpression) -> Option<ScalarExpression> {
        self.project_exprs
            .iter()
            .position(|candidate| {
                candidate.eq_ignore_colref_pos(expr, self.arena)
                    || candidate
                        .unpack_alias_ref()
                        .eq_ignore_colref_pos(expr.unpack_alias_ref(), self.arena)
            })
            .map(|position| {
                let output_expr = &self.project_exprs[position];
                ScalarExpression::column_expr(output_expr.output_column_ref(self.arena), position)
            })
    }
}

impl<'a> VisitorMut<'a> for ProjectionOutputBinder<'_, '_> {
    fn visit(&mut self, expr: &'a mut ScalarExpression) -> Result<(), DatabaseError> {
        if let Some(output_ref) = self.output_ref(expr) {
            *expr = output_ref;
            return Ok(());
        }
        walk_mut_expr(self, expr)
    }
}

pub(crate) struct BindPlanStart<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(crate) arena: &'s mut crate::planner::PlanArena<'arena>,
}

pub struct BindPlanFrom<'s, 'a, 'b, 'arena, T, A, M = ()>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(crate) arena: &'s mut crate::planner::PlanArena<'arena>,
    pub(crate) plan: LogicalPlan,
    pub(crate) _marker: std::marker::PhantomData<M>,
}

pub struct BindPlanSelectList<'s, 'a, 'b, 'arena, T, A, M = ()>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(crate) arena: &'s mut crate::planner::PlanArena<'arena>,
    pub(super) plan: LogicalPlan,
    pub(super) select_list: Vec<ScalarExpression>,
    pub(crate) _marker: std::marker::PhantomData<M>,
}

pub(crate) struct BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(super) binder: &'s mut Binder<'a, 'b, T, A>,
    pub(super) arena: &'s mut crate::planner::PlanArena<'arena>,
    pub(super) plan: LogicalPlan,
    pub(super) select_list: Vec<ScalarExpression>,
}

pub(crate) struct BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut crate::planner::PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ScalarExpression>,
    having: Option<ScalarExpression>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanHaving<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut crate::planner::PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ScalarExpression>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut crate::planner::PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ScalarExpression>,
    orderby: Option<Vec<SortField>>,
}

pub(crate) struct BindPlanSorted<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut crate::planner::PlanArena<'arena>,
    plan: LogicalPlan,
    select_list: Vec<ScalarExpression>,
}

pub(crate) struct BindPlanProjected<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
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
    On(ScalarExpression),
    Using(Vec<String>),
    Natural,
    None,
}

impl<'s, 'a: 'b, 'b, 'arena, T, A, M> BindPlanFrom<'s, 'a, 'b, 'arena, T, A, M>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn typed<N>(self) -> BindPlanFrom<'s, 'a, 'b, 'arena, T, A, N> {
        BindPlanFrom {
            binder: self.binder,
            arena: self.arena,
            plan: self.plan,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn filter_expr(
        mut self,
        predicate: ScalarExpression,
    ) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_where_expr(self.plan, predicate, self.arena)?;
        Ok(self)
    }

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
        select_list: Vec<ScalarExpression>,
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
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn set_select_list(mut self, select_list: Vec<ScalarExpression>) -> Self {
        self.select_list = select_list;
        self
    }

    pub(crate) fn group_by_expr(self, expr: ScalarExpression) -> Result<Self, DatabaseError> {
        let sorted = self
            .filter_expr(None)?
            .aggregate(
                vec![expr],
                None,
                None::<Vec<SortField>>,
                |_binder, _arena, order| Ok(order),
            )?
            .having()?
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

    pub(crate) fn aggregate_without_group(self) -> Result<Self, DatabaseError> {
        let sorted = self
            .filter_expr(None)?
            .aggregate(
                Vec::new(),
                None,
                None::<Vec<SortField>>,
                |_binder, _arena, order| Ok(order),
            )?
            .having()?
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

    pub(crate) fn having_expr(mut self, expr: ScalarExpression) -> Result<Self, DatabaseError> {
        self.plan = self.binder.bind_having(self.plan, expr, self.arena)?;
        Ok(self)
    }

    pub(crate) fn sort_field(mut self, field: SortField) -> Result<Self, DatabaseError> {
        self.plan = self.binder.bind_sort(self.plan, vec![field], self.arena)?;
        Ok(self)
    }

    pub fn distinct(mut self) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_distinct(self.plan, self.select_list.clone())?;
        let distinct_outputs = self.select_list.clone();
        self.binder.bind_distinct_output_exprs(
            &distinct_outputs,
            self.select_list.iter_mut(),
            self.arena,
        )?;
        Ok(self)
    }

    pub fn limit(mut self, limit: usize) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_limit_values(self.plan, None, Some(limit))?;
        Ok(self)
    }

    pub fn offset(mut self, offset: usize) -> Result<Self, DatabaseError> {
        self.plan = self
            .binder
            .bind_limit_values(self.plan, Some(offset), None)?;
        Ok(self)
    }

    pub fn finish(self) -> Result<LogicalPlan, DatabaseError> {
        if self.select_list.iter().any(ScalarExpression::has_agg_call) {
            return self.aggregate_without_group()?.finish();
        }
        self.binder
            .bind_project(self.plan, self.select_list, self.arena)
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanStart<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
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
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn filter_expr(
        mut self,
        predicate: Option<ScalarExpression>,
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
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn aggregate<O>(
        mut self,
        group_by: Vec<ScalarExpression>,
        having: Option<ScalarExpression>,
        orderby: Option<impl IntoIterator<Item = O>>,
        mut bind_sort_field: impl FnMut(
            &mut Binder<'a, 'b, T, A>,
            &mut crate::planner::PlanArena<'arena>,
            O,
        ) -> Result<SortField, DatabaseError>,
    ) -> Result<BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        self.binder
            .extract_select_join(&mut self.select_list, self.arena);
        self.binder
            .extract_select_aggregate(&mut self.select_list)?;

        if !group_by.is_empty() {
            self.binder
                .extract_group_by_aggregate_exprs(&mut self.select_list, group_by)?;
        }

        let mut having_orderby = (None, None);
        if having.is_some() || orderby.is_some() {
            having_orderby = self.binder.extract_having_orderby_aggregate_exprs(
                having,
                orderby,
                |binder, orderby| bind_sort_field(binder, self.arena, orderby),
            )?;
        }

        if !self.binder.context.agg_calls.is_empty()
            || !self.binder.context.group_by_exprs.is_empty()
        {
            self.plan = self.binder.bind_aggregate(
                self.plan,
                self.binder.context.agg_calls.clone(),
                self.binder.context.group_by_exprs.clone(),
            )?;
            self.binder
                .bind_aggregate_output_exprs(self.select_list.iter_mut(), self.arena)?;
            if let Some(orderby) = having_orderby.1.as_mut() {
                self.binder.bind_aggregate_output_exprs(
                    orderby.iter_mut().map(|field| &mut field.expr),
                    self.arena,
                )?;
            }
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
    A: AsRef<[(&'static str, DataValue)]>,
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
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn distinct(
        mut self,
        distinct: bool,
    ) -> Result<BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        if distinct {
            self.plan = self
                .binder
                .bind_distinct(self.plan, self.select_list.clone())?;
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
    A: AsRef<[(&'static str, DataValue)]>,
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
    A: AsRef<[(&'static str, DataValue)]>,
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
    A: AsRef<[(&'static str, DataValue)]>,
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

impl<'a: 'b, 'b, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, 'b, T, A> {
    pub(crate) fn build_plan<'s, 'arena>(
        &'s mut self,
        arena: &'s mut crate::planner::PlanArena<'arena>,
    ) -> BindPlanStart<'s, 'a, 'b, 'arena, T, A> {
        BindPlanStart {
            binder: self,
            arena,
        }
    }

    fn is_temp_alias_projection(
        exprs: &[ScalarExpression],
        arena: &crate::planner::PlanArena,
    ) -> bool {
        !exprs.is_empty()
            && exprs.iter().all(|expr| {
                matches!(
                    expr,
                    ScalarExpression::Alias {
                        alias: AliasType::Expr(alias_expr),
                        ..
                    } if matches!(
                        alias_expr.unpack_alias_ref(),
                        ScalarExpression::ColumnRef { column, .. }
                            if matches!(
                                &arena.column(*column).summary().relation,
                                crate::catalog::ColumnRelation::Table { is_temp: true, .. }
                            )
                    )
                )
            })
    }

    pub(crate) fn is_joined_values_source(
        join_type: Option<JoinType>,
        source: &Source<'a>,
        arena: &crate::planner::PlanArena,
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
    ) -> Result<(), DatabaseError> {
        let JoinCondition::On { on, .. } = join_condition else {
            return Ok(());
        };

        let mut right_shift = PositionShift {
            delta: -(left_len as isize),
        };
        for (_, right_expr) in on {
            right_shift.visit(right_expr)?;
        }

        Ok(())
    }

    fn globalize_join_filter_from_split_scope(
        join_condition: &mut JoinCondition,
        left_len: usize,
        right_schema: &Schema,
        arena: &crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let JoinCondition::On { filter, .. } = join_condition else {
            return Ok(());
        };

        if let Some(expr) = filter {
            RightSidePositionGlobalizer {
                right_schema,
                left_len,
                arena,
            }
            .visit(expr)?;
        }

        Ok(())
    }

    fn localize_appended_right_outputs<'expr>(
        exprs: impl Iterator<Item = &'expr mut ScalarExpression>,
        appended_outputs: &[AppendedRightOutput],
        arena: &crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        struct AppendedRightOutputBinder<'a, 'p> {
            appended_outputs: &'a [AppendedRightOutput],
            arena: &'a crate::planner::PlanArena<'p>,
        }

        impl VisitorMut<'_> for AppendedRightOutputBinder<'_, '_> {
            fn visit_column_ref(
                &mut self,
                column: &mut ColumnRef,
                position: &mut usize,
            ) -> Result<(), DatabaseError> {
                if let Some(output) = self.appended_outputs.iter().find(|output| {
                    *position == output.child_position
                        && self.arena.same_column(*column, output.column)
                }) {
                    *position = output.output_position;
                }
                Ok(())
            }
        }

        let mut binder = AppendedRightOutputBinder {
            appended_outputs,
            arena,
        };
        for expr in exprs {
            binder.visit(expr)?;
        }

        Ok(())
    }

    fn rebind_split_scope_positions(
        expr: &mut ScalarExpression,
        left_schema: &Schema,
        right_schema: &Schema,
        arena: &crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        SplitScopePositionRebinder {
            left_schema,
            right_schema,
            arena,
        }
        .visit(expr)
    }

    fn build_join_from_split_scope_predicates(
        mut children: LogicalPlan,
        mut plan: LogicalPlan,
        join_ty: JoinType,
        predicates: impl IntoIterator<Item = ScalarExpression>,
        rebind_positions: bool,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let left_schema = children.output_schema(arena);
        let right_schema = plan.output_schema(arena);
        let mut on_keys = Vec::new();
        let mut filter = Vec::new();

        for mut predicate in predicates {
            if rebind_positions {
                Self::rebind_split_scope_positions(
                    &mut predicate,
                    left_schema,
                    right_schema,
                    arena,
                )?;
            }
            Self::extract_join_keys(
                predicate,
                &mut on_keys,
                &mut filter,
                left_schema,
                right_schema,
                arena,
            )?;
        }

        let mut join_condition = JoinCondition::On {
            on: on_keys,
            filter: Self::combine_conjuncts(filter),
        };
        Self::globalize_join_filter_from_split_scope(
            &mut join_condition,
            left_schema.len(),
            right_schema,
            arena,
        )?;

        Ok(LJoinOperator::build(
            children,
            plan,
            join_condition,
            join_ty,
        ))
    }

    fn bind_set_cast(
        &mut self,
        mut left_plan: LogicalPlan,
        mut right_plan: LogicalPlan,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(LogicalPlan, LogicalPlan), DatabaseError> {
        let mut left_cast = vec![];
        let mut right_cast = vec![];

        let left_schema = left_plan.output_schema(arena);
        let right_schema = right_plan.output_schema(arena);

        for (position, (left_schema, right_schema)) in
            left_schema.iter().zip(right_schema.iter()).enumerate()
        {
            let left_column = arena.column(*left_schema);
            let right_column = arena.column(*right_schema);
            let cast_type =
                LogicalType::max_logical_type(left_column.datatype(), right_column.datatype())?;
            if cast_type.as_ref() != left_column.datatype() {
                left_cast.push(ScalarExpression::type_cast(
                    ScalarExpression::column_expr(*left_schema, position),
                    cast_type.clone(),
                    arena,
                )?);
            } else {
                left_cast.push(ScalarExpression::column_expr(*left_schema, position));
            }
            if cast_type.as_ref() != right_column.datatype() {
                right_cast.push(ScalarExpression::type_cast(
                    ScalarExpression::column_expr(*right_schema, position),
                    cast_type.clone(),
                    arena,
                )?);
            } else {
                right_cast.push(ScalarExpression::column_expr(*right_schema, position));
            }
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
        arena: &mut crate::planner::PlanArena,
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
                        .map(|(position, column)| ScalarExpression::column_expr(column, position))
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
                        .map(|(position, column)| ScalarExpression::column_expr(column, position))
                        .collect_vec();
                    let right_distinct_exprs = right_schema
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(position, column)| ScalarExpression::column_expr(column, position))
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
        arena: &mut crate::planner::PlanArena,
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
                    source_column.id().unwrap_or(ColumnId::new()),
                    matches!(
                        &source_column.summary().relation,
                        ColumnRelation::Table { is_temp: true, .. }
                    ),
                )
            };
            alias_column.set_name(alias.clone());
            alias_column.set_ref_table(table_alias.clone(), column_id, is_temp);
            let alias_column = arena.alloc_column(alias_column);

            let alias_column_expr = ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::column_expr(column, position)),
                alias: AliasType::Expr(Box::new(ScalarExpression::column_expr(
                    alias_column,
                    position,
                ))),
            };
            self.context.add_alias(
                Some(table_alias.to_string()),
                alias,
                alias_column_expr.clone(),
            );
            alias_exprs.push(alias_column_expr);
        }
        self.context.add_table_alias(table_alias, table_name);
        self.bind_project(plan, alias_exprs, arena)
    }

    fn bind_schema_source(
        &mut self,
        mut plan: LogicalPlan,
        source_name: TableName,
        arena: &mut crate::planner::PlanArena,
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
                    column_catalog.id().unwrap_or(ColumnId::new()),
                    true,
                );
                source_column
            };
            let source_column = arena.alloc_column(source_column);

            source_exprs.push(ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::column_expr(column, position)),
                alias: AliasType::Expr(Box::new(ScalarExpression::column_expr(
                    source_column,
                    position,
                ))),
            });
        }

        Self::build_project_plan(plan, source_exprs)
    }

    pub(crate) fn bind_base_table_ref(
        &mut self,
        join_type: Option<JoinType>,
        table_name: TableName,
        alias: Option<TableAliasInput>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_alias = alias.as_ref().map(|alias| alias.name.clone());

        let with_pk = self.is_scan_with_pk(&table_name);
        let source = self
            .context
            .source_and_bind(table_name.clone(), table_alias.as_ref(), join_type, false)?
            .ok_or(DatabaseError::SourceNotFound)?;
        let mut plan = match source {
            Source::Table(table) => {
                TableScanOperator::build(table_name.clone(), table, with_pk, arena)?
            }
            Source::View(view) => LogicalPlan::clone(&view.plan),
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
        arena: &mut crate::planner::PlanArena,
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
        arena: &mut crate::planner::PlanArena,
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
        arena: &mut crate::planner::PlanArena,
        exprs: &mut Vec<ScalarExpression>,
        table_name: TableName,
        is_qualified_wildcard: bool,
    ) -> Result<(), DatabaseError> {
        let (source, position_offset) =
            Self::resolve_source_columns_in_scope(context, table_name.as_ref())?;

        let fn_not_on_using = |column: &ColumnRef| {
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

        let mut pushed_alias_columns = false;

        for alias_column in context
            .expr_aliases
            .keys()
            .filter_map(|(alias_table, alias_column)| {
                matches!(alias_table.as_deref(), Some(alias) if alias == table_name.as_ref())
                    .then_some(alias_column.as_str())
            })
        {
            let Some((position, column)) = source
                .schema()
                .iter()
                .enumerate()
                .find(|(_, column)| arena.column(**column).name() == alias_column)
            else {
                continue;
            };
            if !fn_not_on_using(&column) {
                continue;
            }
            exprs.push(ScalarExpression::column_expr(
                *column,
                position_offset + position,
            ));
            pushed_alias_columns = true;
        }

        if pushed_alias_columns {
            return Ok(());
        }

        for (position, column) in source.schema().iter().enumerate() {
            if !fn_not_on_using(&column) {
                continue;
            }
            exprs.push(ScalarExpression::column_expr(
                *column,
                position_offset + position,
            ));
        }
        Ok(())
    }

    pub(crate) fn bind_join_plans(
        &mut self,
        mut left: LogicalPlan,
        mut right: LogicalPlan,
        join_type: JoinType,
        constraint: JoinConstraintInput,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let left_len = left.output_schema(arena).len();
        right.output_schema(arena);
        let mut on = self.bind_join_constraint(
            join_type,
            constraint,
            left.output_schema(arena),
            right.output_schema(arena),
            arena,
        )?;
        Self::localize_join_condition_from_join_scope(&mut on, left_len)?;

        Ok(LJoinOperator::build(left, right, on, join_type))
    }

    pub(crate) fn bind_where_expr(
        &mut self,
        mut children: LogicalPlan,
        mut predicate: ScalarExpression,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Where);

        if let Some(sub_queries) = self.context.sub_queries_at_now() {
            let mut uses_mark_apply = None;
            for sub_query in sub_queries {
                match sub_query {
                    SubQueryType::ExistsSubQuery {
                        plan,
                        correlated,
                        output_column,
                    } => {
                        if matches!(uses_mark_apply, Some(false)) {
                            return Err(DatabaseError::UnsupportedStmt(
                                "mixed EXISTS/IN with other WHERE subqueries is not supported yet"
                                    .to_string(),
                            ));
                        }
                        uses_mark_apply = Some(true);
                        let left_schema = children.output_schema(arena).clone();
                        let (plan, predicates) = Self::prepare_mark_apply(
                            &mut predicate,
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
                        if matches!(uses_mark_apply, Some(false)) {
                            return Err(DatabaseError::UnsupportedStmt(
                                "mixed EXISTS/IN with other WHERE subqueries is not supported yet"
                                    .to_string(),
                            ));
                        }
                        uses_mark_apply = Some(true);
                        if correlated {
                            quantified_predicate =
                                Self::rewrite_correlated_quantified_predicate(quantified_predicate);
                        }
                        let left_schema = children.output_schema(arena).clone();
                        let (plan, predicates) = Self::prepare_mark_apply(
                            &mut predicate,
                            &output_column,
                            left_schema.as_ref(),
                            plan,
                            correlated,
                            true,
                            vec![quantified_predicate],
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
                    SubQueryType::SubQuery { plan, correlated } => {
                        if matches!(uses_mark_apply, Some(true)) {
                            return Err(DatabaseError::UnsupportedStmt(
                                "mixed EXISTS/IN with other WHERE subqueries is not supported yet"
                                    .to_string(),
                            ));
                        }
                        uses_mark_apply = Some(false);
                        if correlated {
                            return Err(DatabaseError::UnsupportedStmt(
                                "correlated scalar subqueries in WHERE are not supported"
                                    .to_string(),
                            ));
                        }
                        children = Self::build_join_from_split_scope_predicates(
                            children,
                            plan,
                            JoinType::Inner,
                            std::iter::once(predicate.clone()),
                            true,
                            arena,
                        )?;
                    }
                }
            }
            if matches!(uses_mark_apply, Some(true)) {
                let passthrough_exprs = children
                    .output_schema(arena)
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(position, column)| ScalarExpression::column_expr(column, position))
                    .collect();
                let filter = FilterOperator::build(predicate, children, false);
                return Ok(LogicalPlan::new(
                    Operator::Project(ProjectOperator {
                        exprs: passthrough_exprs,
                    }),
                    Childrens::Only(Box::new(filter)),
                ));
            }
            return Ok(children);
        }
        Ok(FilterOperator::build(predicate, children, false))
    }

    fn ensure_mark_apply_right_outputs(
        plan: &mut LogicalPlan,
        predicates: &[ScalarExpression],
        arena: &mut crate::planner::PlanArena,
    ) -> Vec<AppendedRightOutput> {
        let output_schema = plan.output_schema(arena).clone();
        let output_len = output_schema.len();
        if let LogicalPlan {
            operator: Operator::Project(op),
            childrens,
            ..
        } = plan
        {
            let Childrens::Only(child) = childrens.as_mut() else {
                return Vec::new();
            };
            let child_schema = child.output_schema(arena);
            let mut appended_outputs = Vec::new();
            op.exprs.extend(
                child_schema
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| {
                        !output_schema.contains(column)
                            && predicates.iter().any(|expr| {
                                expr.any_referenced_column(arena, |arena, candidate| {
                                    arena.same_column(*candidate, **column)
                                })
                            })
                    })
                    .map(|(position, column)| {
                        appended_outputs.push(AppendedRightOutput {
                            column: *column,
                            child_position: position,
                            output_position: output_len + appended_outputs.len(),
                        });
                        ScalarExpression::column_expr(*column, position)
                    }),
            );
            plan.reset_output_schema_cache();
            return appended_outputs;
        }

        Vec::new()
    }

    fn prepare_mark_apply(
        predicate: &mut ScalarExpression,
        output_column: &ColumnRef,
        left_schema: &Schema,
        plan: LogicalPlan,
        correlated: bool,
        preserve_projection: bool,
        mut apply_predicates: Vec<ScalarExpression>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(LogicalPlan, Vec<ScalarExpression>), DatabaseError> {
        let left_len = left_schema.len();
        MarkerPositionGlobalizer {
            output_column,
            left_len,
            arena,
        }
        .visit(predicate)?;

        let (mut plan, correlated_filters) = if correlated {
            Self::prepare_correlated_subquery_plan(plan, left_schema, preserve_projection, arena)?
        } else {
            (plan, Vec::new())
        };
        apply_predicates.extend(correlated_filters);

        if correlated {
            let appended_right_outputs =
                Self::ensure_mark_apply_right_outputs(&mut plan, &apply_predicates, arena);
            if !appended_right_outputs.is_empty() {
                Self::localize_appended_right_outputs(
                    apply_predicates.iter_mut(),
                    &appended_right_outputs,
                    arena,
                )?;
            }
        }
        let right_schema = plan.output_schema(arena);
        for expr in apply_predicates.iter_mut() {
            RightSidePositionGlobalizer {
                right_schema,
                left_len,
                arena,
            }
            .visit(expr)?;
        }

        Ok((plan, apply_predicates))
    }

    fn rewrite_correlated_quantified_predicate(predicate: ScalarExpression) -> ScalarExpression {
        let strip_projection_alias = |expr: Box<ScalarExpression>| match *expr {
            ScalarExpression::Alias {
                expr,
                alias: AliasType::Expr(_),
            } => expr,
            expr => Box::new(expr),
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
        arena: &mut crate::planner::PlanArena,
    ) -> bool {
        if !plan
            .operator
            .visit_referenced_columns(arena, &mut |arena, column| {
                !left_schema
                    .iter()
                    .any(|left| arena.same_column(*left, *column))
            })
        {
            return true;
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => Self::plan_has_correlated_refs(child, left_schema, arena),
            Childrens::Twins { left, right } => {
                Self::plan_has_correlated_refs(left, left_schema, arena)
                    || Self::plan_has_correlated_refs(right, left_schema, arena)
            }
            Childrens::None => false,
        }
    }

    fn expr_has_correlated_refs(
        expr: &ScalarExpression,
        left_schema: &Schema,
        arena: &mut crate::planner::PlanArena,
    ) -> bool {
        expr.any_referenced_column(arena, |arena, column| {
            left_schema
                .iter()
                .any(|left| arena.same_column(*left, *column))
        })
    }

    fn split_conjuncts(expr: ScalarExpression, exprs: &mut Vec<ScalarExpression>) {
        match expr.unpack_alias() {
            ScalarExpression::Binary {
                op: BinaryOperator::And,
                left_expr,
                right_expr,
                ..
            } => {
                Self::split_conjuncts(*left_expr, exprs);
                Self::split_conjuncts(*right_expr, exprs);
            }
            expr => exprs.push(expr),
        }
    }

    fn combine_conjuncts(exprs: Vec<ScalarExpression>) -> Option<ScalarExpression> {
        exprs
            .into_iter()
            .reduce(|acc, expr| ScalarExpression::Binary {
                op: BinaryOperator::And,
                left_expr: Box::new(acc),
                right_expr: Box::new(expr),
                evaluator: None,
                ty: LogicalType::Boolean,
            })
    }

    fn prepare_correlated_subquery_plan(
        plan: LogicalPlan,
        left_schema: &Schema,
        preserve_projection: bool,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<(LogicalPlan, Vec<ScalarExpression>), DatabaseError> {
        match plan.childrens.as_ref() {
            Childrens::Only(_) => {}
            Childrens::Twins { .. } => {
                if Self::plan_has_correlated_refs(&plan, left_schema, arena) {
                    return Err(DatabaseError::UnsupportedStmt(
                        "correlated EXISTS/NOT EXISTS does not support set or join subqueries"
                            .to_string(),
                    ));
                }
            }
            Childrens::None => {}
        }

        match plan {
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
                Self::split_conjuncts(op.predicate, &mut predicates);
                for predicate in predicates {
                    if Self::expr_has_correlated_refs(&predicate, left_schema, arena) {
                        correlated_filters.push(predicate);
                    } else {
                        local_filters.push(predicate);
                    }
                }
                let plan = if let Some(predicate) = Self::combine_conjuncts(local_filters) {
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

                if !preserve_projection || Self::is_temp_alias_projection(&op.exprs, arena) {
                    Ok((child, correlated_filters))
                } else {
                    let mut binder = ProjectionOutputBinder::new(&op.exprs, arena);
                    for expr in correlated_filters.iter_mut() {
                        binder.visit(expr)?;
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
                if Self::plan_has_correlated_refs(&plan, left_schema, arena) {
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
        children: LogicalPlan,
        mut having: ScalarExpression,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Having);

        self.validate_having_orderby(&having)?;
        self.bind_aggregate_output_exprs(std::iter::once(&mut having), arena)?;
        Ok(FilterOperator::build(having, children, true))
    }

    pub(crate) fn build_project_plan(
        children: LogicalPlan,
        select_list: Vec<ScalarExpression>,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs: select_list }),
            Childrens::Only(Box::new(children)),
        )
    }

    pub(crate) fn bind_project(
        &mut self,
        mut children: LogicalPlan,
        mut select_list: Vec<ScalarExpression>,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Project);

        if let Some(sub_queries) = self.context.sub_queries_at_now() {
            for sub_query in sub_queries {
                let SubQueryType::SubQuery {
                    mut plan,
                    correlated,
                } = sub_query
                else {
                    return Err(DatabaseError::UnsupportedStmt(
                        "only scalar subqueries are supported in SELECT list".to_string(),
                    ));
                };

                if correlated {
                    return Err(DatabaseError::UnsupportedStmt(
                        "correlated scalar subqueries in SELECT list are not supported".to_string(),
                    ));
                }

                let left_len = children.output_schema(arena).len();
                let right_schema = plan.output_schema(arena);
                for expr in select_list.iter_mut() {
                    RightSidePositionGlobalizer {
                        right_schema,
                        left_len,
                        arena,
                    }
                    .visit(expr)?;
                }

                children = ScalarApplyOperator::build(children, plan);
            }
        }

        Ok(Self::build_project_plan(children, select_list))
    }

    pub(crate) fn bind_sort(
        &mut self,
        children: LogicalPlan,
        sort_fields: Vec<SortField>,
        _arena: &mut crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Sort);

        Ok(LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields,
                limit: None,
            }),
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

    pub fn extract_select_join(
        &mut self,
        select_items: &mut [ScalarExpression],
        arena: &mut crate::planner::PlanArena,
    ) {
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

        for column in select_items {
            if let ScalarExpression::ColumnRef { column, .. } = column {
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
        }
    }

    fn bind_join_constraint(
        &mut self,
        join_type: JoinType,
        constraint: JoinConstraintInput,
        left_schema: &Schema,
        right_schema: &Schema,
        arena: &mut crate::planner::PlanArena,
    ) -> Result<JoinCondition, DatabaseError> {
        match constraint {
            JoinConstraintInput::On(expr) => {
                // left and right columns that match equi-join pattern
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = vec![];
                // expression that didn't match equi-join pattern
                let mut filter = vec![];

                Self::extract_join_keys(
                    expr,
                    &mut on_keys,
                    &mut filter,
                    left_schema,
                    right_schema,
                    arena,
                )?;

                // combine multiple filter exprs into one BinaryExpr
                let join_filter = filter
                    .into_iter()
                    .reduce(|acc, expr| ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(acc),
                        right_expr: Box::new(expr),
                        evaluator: None,
                        ty: LogicalType::Boolean,
                    });
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: join_filter,
                })
            }
            JoinConstraintInput::Using(names) => {
                fn find_column<'a>(
                    schema: &'a Schema,
                    name: &'a str,
                    arena: &crate::planner::PlanArena,
                ) -> Option<(usize, &'a ColumnRef)> {
                    schema
                        .iter()
                        .enumerate()
                        .find(|(_, column)| arena.column(**column).name() == name)
                }

                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = Vec::new();

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
                    on_keys.push((
                        ScalarExpression::column_expr(*left_column, left_position),
                        ScalarExpression::column_expr(
                            *right_column,
                            left_schema.len() + right_position,
                        ),
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
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = Vec::new();

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
                        let left_expr = ScalarExpression::column_expr(*left_column, left_position);
                        let right_expr = ScalarExpression::column_expr(
                            *right_column,
                            left_schema.len() + right_position,
                        );

                        self.context.add_using(
                            name.clone(),
                            join_type,
                            left_column,
                            left_position,
                            right_column,
                            left_schema.len() + right_position,
                        )?;
                        on_keys.push((left_expr, right_expr));
                    }
                }
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: None,
                })
            }
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
    fn extract_join_keys(
        expr: ScalarExpression,
        accum: &mut Vec<(ScalarExpression, ScalarExpression)>,
        accum_filter: &mut Vec<ScalarExpression>,
        left_schema: &Schema,
        right_schema: &Schema,
        arena: &crate::planner::PlanArena,
    ) -> Result<(), DatabaseError> {
        let fn_contains = |schema: &Schema, column: ColumnRef| {
            let summary = arena.column(column).summary();
            schema
                .iter()
                .any(|candidate| arena.column(*candidate).summary() == summary)
        };
        let fn_or_contains = |column: ColumnRef| {
            fn_contains(left_schema, column) || fn_contains(right_schema, column)
        };

        match expr.unpack_alias() {
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                op,
                ty,
                ..
            } => {
                match op {
                    BinaryOperator::Eq => {
                        match (left_expr.unpack_alias_ref(), right_expr.unpack_alias_ref()) {
                            // example: foo = bar
                            (
                                ScalarExpression::ColumnRef { column: l, .. },
                                ScalarExpression::ColumnRef { column: r, .. },
                            ) => {
                                // reorder left and right joins keys to pattern: (left, right)
                                if fn_contains(left_schema, *l) && fn_contains(right_schema, *r) {
                                    accum.push((*left_expr, *right_expr));
                                } else if fn_contains(left_schema, *r)
                                    && fn_contains(right_schema, *l)
                                {
                                    accum.push((*right_expr, *left_expr));
                                } else if fn_or_contains(*l) || fn_or_contains(*r) {
                                    accum_filter.push(ScalarExpression::Binary {
                                        left_expr,
                                        right_expr,
                                        op,
                                        ty,
                                        evaluator: None,
                                    });
                                }
                            }
                            (ScalarExpression::ColumnRef { column, .. }, _)
                            | (_, ScalarExpression::ColumnRef { column, .. }) => {
                                if fn_or_contains(*column) {
                                    accum_filter.push(ScalarExpression::Binary {
                                        left_expr,
                                        right_expr,
                                        op,
                                        ty,
                                        evaluator: None,
                                    });
                                }
                            }
                            _other => {
                                // example: baz > 1
                                if left_expr.all_referenced_columns(arena, |_, column| {
                                    fn_or_contains(*column)
                                }) && right_expr.all_referenced_columns(arena, |_, column| {
                                    fn_or_contains(*column)
                                }) {
                                    accum_filter.push(ScalarExpression::Binary {
                                        left_expr,
                                        right_expr,
                                        op,
                                        ty,
                                        evaluator: None,
                                    });
                                }
                            }
                        }
                    }
                    BinaryOperator::And => {
                        // example: foo = bar AND baz > 1
                        Self::extract_join_keys(
                            *left_expr,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                            arena,
                        )?;
                        Self::extract_join_keys(
                            *right_expr,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                            arena,
                        )?;
                    }
                    BinaryOperator::Or => {
                        accum_filter.push(ScalarExpression::Binary {
                            left_expr,
                            right_expr,
                            op,
                            ty,
                            evaluator: None,
                        });
                    }
                    _ => {
                        if left_expr
                            .all_referenced_columns(arena, |_, column| fn_or_contains(*column))
                            && right_expr
                                .all_referenced_columns(arena, |_, column| fn_or_contains(*column))
                        {
                            accum_filter.push(ScalarExpression::Binary {
                                left_expr,
                                right_expr,
                                op,
                                ty,
                                evaluator: None,
                            });
                        }
                    }
                }
            }
            expr => {
                if expr.all_referenced_columns(arena, |_, column| fn_or_contains(*column)) {
                    // example: baz > 1
                    accum_filter.push(expr);
                }
            }
        }

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{ProjectionOutputBinder, RightSidePositionGlobalizer};
    use crate::binder::test::build_t1_table;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::expression::visitor_mut::VisitorMut;
    use crate::expression::{AliasType, ScalarExpression};
    use crate::planner::operator::join::{JoinCondition, JoinType};
    use crate::planner::operator::mark_apply::{
        MarkApplyKind, MarkApplyOperator, MarkApplyQuantifier,
    };
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan, PlanArena};
    use crate::types::LogicalType;

    fn test_column(arena: &mut PlanArena, name: &str, position: usize) -> ScalarExpression {
        ScalarExpression::column_expr(
            arena.alloc_column(ColumnCatalog::new(
                name.to_string(),
                true,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            )),
            position,
        )
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
        let table_arena = crate::planner::TableArenaCell::default();
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
        let right_schema = vec![right_column.clone()];
        let mut expr = ScalarExpression::Binary {
            op: crate::expression::BinaryOperator::Eq,
            left_expr: Box::new(ScalarExpression::column_expr(left_column, 0)),
            right_expr: Box::new(ScalarExpression::column_expr(right_column.clone(), 0)),
            evaluator: None,
            ty: LogicalType::Boolean,
        };

        RightSidePositionGlobalizer {
            right_schema: &right_schema,
            left_len: 2,
            arena: &arena,
        }
        .visit(&mut expr)?;

        let ScalarExpression::Binary {
            left_expr,
            right_expr,
            ..
        } = expr
        else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef {
            position: left_position,
            ..
        } = left_expr.as_ref()
        else {
            unreachable!()
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = right_expr.as_ref()
        else {
            unreachable!()
        };
        assert_eq!((*left_position, *right_position), (0, 2));

        Ok(())
    }

    #[test]
    fn test_projection_output_binder_rewrites_to_project_slot() -> Result<(), DatabaseError> {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let project_output = ScalarExpression::Alias {
            expr: Box::new(test_column(&mut arena, "c1", 0)),
            alias: AliasType::Name("v".to_string()),
        };
        let mut expr = ScalarExpression::Alias {
            expr: Box::new(test_column(&mut arena, "c1", 0)),
            alias: AliasType::Name("v".to_string()),
        };

        ProjectionOutputBinder::new(std::slice::from_ref(&project_output), &mut arena)
            .visit(&mut expr)?;

        let expected =
            ScalarExpression::column_expr(project_output.output_column_ref(&mut arena), 0);
        assert!(expr.eq_ignore_colref_pos(&expected, &arena));
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
    fn test_scalar_subquery_in_where_binds_as_inner_join() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan = table_states.plan("select * from t1 where c1 = (select max(c3) from t2)")?;
        let Some((join_type, join_condition)) = find_join(&plan) else {
            panic!("expected scalar subquery to introduce a join")
        };

        assert_eq!(*join_type, JoinType::Inner);
        assert!(matches!(join_condition, JoinCondition::On { .. }));

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

    fn find_top_join(plan: &LogicalPlan) -> Option<&LogicalPlan> {
        if matches!(plan.operator, Operator::Join(_)) {
            return Some(plan);
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => find_top_join(child),
            Childrens::Twins { .. } | Childrens::None => None,
        }
    }

    fn collect_column_positions(expr: &ScalarExpression, positions: &mut Vec<usize>) {
        match expr.unpack_alias_ref() {
            ScalarExpression::ColumnRef { position, .. } => positions.push(*position),
            ScalarExpression::Binary {
                left_expr,
                right_expr,
                ..
            } => {
                collect_column_positions(left_expr, positions);
                collect_column_positions(right_expr, positions);
            }
            _ => {}
        }
    }

    #[test]
    fn test_multiple_scalar_subqueries_in_where_rebind_positions() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan =
            table_states.plan("select * from t1 where c1 <= (select 4) and c1 > (select 1)")?;
        let outer_join =
            find_top_join(&plan).expect("expected scalar subqueries to introduce a join");
        let Operator::Join(op) = &outer_join.operator else {
            panic!("expected join plan")
        };
        let Childrens::Twins { left, .. } = outer_join.childrens.as_ref() else {
            panic!("expected binary join")
        };
        let JoinCondition::On {
            filter: Some(filter),
            ..
        } = &op.on
        else {
            panic!("expected join filter")
        };
        let mut arena = PlanArena::new(&table_states.table_arena);
        let mut left_plan = left.as_ref().clone();
        let left_len = left_plan.output_schema(&mut arena).len();

        let mut positions = Vec::new();
        collect_column_positions(filter, &mut positions);

        assert_eq!(positions, vec![0, left_len - 1, 0, left_len]);

        Ok(())
    }
}
