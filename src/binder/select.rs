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
use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;

use super::{
    attach_span_if_absent, lower_case_name, lower_ident, Binder, BinderContext, QueryBindStep,
    Source, SubQueryType,
};

use crate::catalog::{
    ColumnCatalog, ColumnDesc, ColumnRef, ColumnRelation, ColumnSummary, TableName,
};
use crate::errors::DatabaseError;
use crate::execution::dql::join::joins_nullable;
use crate::expression::simplify::ConstantCalculator;
use crate::expression::visitor_mut::{walk_mut_expr, PositionShift, VisitorMut};
use crate::expression::{AliasType, BinaryOperator};
use crate::planner::operator::except::ExceptOperator;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::union::UnionOperator;
use crate::planner::{Childrens, LogicalPlan, SchemaOutput};
use crate::storage::Transaction;
use crate::types::tuple::{Schema, SchemaRef};
use crate::types::{ColumnId, LogicalType};
use itertools::Itertools;
use sqlparser::ast::{
    Distinct, Expr, GroupByExpr, Join, JoinConstraint, JoinOperator, LimitClause, OrderByExpr,
    OrderByKind, Query, Select, SelectInto, SelectItem, SelectItemQualifiedWildcardKind, SetExpr,
    SetOperator, SetQuantifier, TableAlias, TableAliasColumnDef, TableFactor, TableWithJoins,
};

struct RightSidePositionGlobalizer<'a> {
    right_schema: &'a Schema,
    left_len: usize,
}

impl<'a> VisitorMut<'a> for RightSidePositionGlobalizer<'_> {
    fn visit_column_ref(
        &mut self,
        column: &'a mut ColumnRef,
        position: &'a mut usize,
    ) -> Result<(), DatabaseError> {
        if self.right_schema.contains(column) {
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

struct SplitScopePositionRebinder<'a> {
    left_schema: &'a Schema,
    right_schema: &'a Schema,
}

impl VisitorMut<'_> for SplitScopePositionRebinder<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        if let Some(left_position) = self
            .left_schema
            .iter()
            .position(|candidate| candidate.same_column(column))
        {
            *position = left_position;
        } else if let Some(right_position) = self
            .right_schema
            .iter()
            .position(|candidate| candidate.same_column(column))
        {
            *position = right_position;
        }
        Ok(())
    }
}

struct MarkerPositionGlobalizer<'a> {
    output_column: &'a ColumnRef,
    left_len: usize,
}

impl VisitorMut<'_> for MarkerPositionGlobalizer<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        if column.same_column(self.output_column) {
            *position = self.left_len;
        }
        Ok(())
    }
}

struct ProjectionOutputBinder<'a> {
    project_exprs: &'a [ScalarExpression],
}

impl<'a> ProjectionOutputBinder<'a> {
    fn new(project_exprs: &'a [ScalarExpression]) -> Self {
        Self { project_exprs }
    }

    fn output_ref(&self, expr: &ScalarExpression) -> Option<ScalarExpression> {
        self.project_exprs
            .iter()
            .position(|candidate| {
                candidate == expr || candidate.unpack_alias_ref() == expr.unpack_alias_ref()
            })
            .map(|position| {
                let output_expr = &self.project_exprs[position];
                ScalarExpression::column_expr(output_expr.output_column(), position)
            })
    }
}

impl<'a> VisitorMut<'a> for ProjectionOutputBinder<'_> {
    fn visit(&mut self, expr: &'a mut ScalarExpression) -> Result<(), DatabaseError> {
        if let Some(output_ref) = self.output_ref(expr) {
            *expr = output_ref;
            return Ok(());
        }
        walk_mut_expr(self, expr)
    }
}

impl<'a: 'b, 'b, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, 'b, T, A> {
    fn is_temp_alias_projection(exprs: &[ScalarExpression]) -> bool {
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
                                &column.summary().relation,
                                crate::catalog::ColumnRelation::Table { is_temp: true, .. }
                            )
                    )
                )
            })
    }

    fn is_joined_values_source(join_type: Option<JoinType>, source: &Source<'a>) -> bool {
        join_type.is_some()
            && matches!(
                source,
                Source::Schema(schema_ref)
                    if !schema_ref.is_empty()
                        && schema_ref.iter().all(|column| {
                            matches!(
                                &column.summary().relation,
                                ColumnRelation::Table { is_temp: true, .. }
                            ) && column.id() == Some(ColumnId::default())
                        })
            )
    }

    fn bind_project_output_exprs<'c>(
        project_exprs: &[ScalarExpression],
        exprs: impl IntoIterator<Item = &'c mut ScalarExpression>,
    ) -> Result<(), DatabaseError> {
        let mut binder = ProjectionOutputBinder::new(project_exprs);
        for expr in exprs {
            binder.visit(expr)?;
        }
        Ok(())
    }

    pub(crate) fn resolve_source_columns_in_scope(
        context: &BinderContext<'a, T>,
        table_schema_buf: &mut std::collections::HashMap<TableName, Option<SchemaOutput>>,
        table_name: &str,
    ) -> Result<(SchemaRef, usize), DatabaseError> {
        let mut position_offset = 0;

        for bound_source in &context.bind_table {
            let schema_buf = table_schema_buf
                .entry(bound_source.table_name.clone())
                .or_default();
            let schema_ref = bound_source.source.schema_ref(schema_buf);

            if bound_source.matches_name(table_name) {
                return Ok((schema_ref, position_offset));
            }

            position_offset += schema_ref.len();
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
    ) -> Result<(), DatabaseError> {
        let JoinCondition::On { filter, .. } = join_condition else {
            return Ok(());
        };

        if let Some(expr) = filter {
            RightSidePositionGlobalizer {
                right_schema,
                left_len,
            }
            .visit(expr)?;
        }

        Ok(())
    }

    fn localize_appended_right_outputs<'expr>(
        exprs: impl Iterator<Item = &'expr mut ScalarExpression>,
        appended_outputs: &[AppendedRightOutput],
    ) -> Result<(), DatabaseError> {
        struct AppendedRightOutputBinder<'a> {
            appended_outputs: &'a [AppendedRightOutput],
        }

        impl VisitorMut<'_> for AppendedRightOutputBinder<'_> {
            fn visit_column_ref(
                &mut self,
                column: &mut ColumnRef,
                position: &mut usize,
            ) -> Result<(), DatabaseError> {
                if let Some(output) = self.appended_outputs.iter().find(|output| {
                    *position == output.child_position && column.same_column(&output.column)
                }) {
                    *position = output.output_position;
                }
                Ok(())
            }
        }

        let mut binder = AppendedRightOutputBinder { appended_outputs };
        for expr in exprs {
            binder.visit(expr)?;
        }

        Ok(())
    }

    fn rebind_split_scope_positions(
        expr: &mut ScalarExpression,
        left_schema: &Schema,
        right_schema: &Schema,
    ) -> Result<(), DatabaseError> {
        SplitScopePositionRebinder {
            left_schema,
            right_schema,
        }
        .visit(expr)
    }

    fn build_join_from_split_scope_predicates(
        mut children: LogicalPlan,
        mut plan: LogicalPlan,
        join_ty: JoinType,
        predicates: impl IntoIterator<Item = ScalarExpression>,
        rebind_positions: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let left_schema = children.output_schema().clone();
        let right_schema = plan.output_schema().clone();
        let mut on_keys = Vec::new();
        let mut filter = Vec::new();

        for mut predicate in predicates {
            if rebind_positions {
                Self::rebind_split_scope_positions(
                    &mut predicate,
                    left_schema.as_ref(),
                    right_schema.as_ref(),
                )?;
            }
            Self::extract_join_keys(
                predicate,
                &mut on_keys,
                &mut filter,
                left_schema.as_ref(),
                right_schema.as_ref(),
            )?;
        }

        let mut join_condition = JoinCondition::On {
            on: on_keys,
            filter: Self::combine_conjuncts(filter),
        };
        Self::globalize_join_filter_from_split_scope(
            &mut join_condition,
            left_schema.len(),
            right_schema.as_ref(),
        )?;

        Ok(LJoinOperator::build(
            children,
            plan,
            join_condition,
            join_ty,
        ))
    }

    pub(crate) fn bind_query(&mut self, query: &Query) -> Result<LogicalPlan, DatabaseError> {
        let origin_step = self.context.step_now();

        if let Some(_with) = &query.with {
            // TODO support with clause.
        }

        let order_by_exprs = if let Some(order_by) = &query.order_by {
            match &order_by.kind {
                OrderByKind::Expressions(exprs) => Some(exprs.as_slice()),
                OrderByKind::All(_) => {
                    return Err(DatabaseError::UnsupportedStmt(
                        "ORDER BY ALL is not supported".to_string(),
                    ))
                }
            }
        } else {
            None
        };
        let is_plain_select = matches!(query.body.borrow(), SetExpr::Select(_));
        let mut plan = match query.body.borrow() {
            SetExpr::Select(select) => self.bind_select(select, order_by_exprs),
            SetExpr::Query(query) => self.bind_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.bind_set_operation(op, set_quantifier, left, right),
            SetExpr::Values(values) => self.bind_temp_values(&values.rows),
            expr => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "query body: {expr:?}"
                )))
            }
        }?;

        if !is_plain_select {
            if let Some(order_by_exprs) = order_by_exprs {
                plan = self.bind_top_level_orderby(plan, order_by_exprs)?;
            }
        }

        if let Some(limit_clause) = query.limit_clause.clone() {
            plan = self.bind_limit(plan, limit_clause)?;
        }

        self.context.step(origin_step);
        Ok(plan)
    }

    fn bind_top_level_orderby(
        &mut self,
        mut plan: LogicalPlan,
        orderbys: &[OrderByExpr],
    ) -> Result<LogicalPlan, DatabaseError> {
        let saved_aliases = self.context.expr_aliases.clone();
        for (position, column) in plan.output_schema().iter().enumerate() {
            self.context.add_alias(
                None,
                column.name().to_string(),
                ScalarExpression::column_expr(column.clone(), position),
            );
        }

        let sort_fields = self.extract_having_orderby_aggregate(&None, orderbys)?.1;
        self.context.expr_aliases = saved_aliases;

        Ok(match sort_fields {
            Some(sort_fields) => self.bind_sort(plan, sort_fields)?,
            None => plan,
        })
    }

    pub(crate) fn bind_select(
        &mut self,
        select: &Select,
        orderby: Option<&[OrderByExpr]>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut plan = if select.from.is_empty() {
            LogicalPlan::new(Operator::Dummy, Childrens::None)
        } else {
            let mut plan = self.bind_table_ref(&select.from[0])?;

            if select.from.len() > 1 {
                for from in select.from[1..].iter() {
                    plan = LJoinOperator::build(
                        plan,
                        self.bind_table_ref(from)?,
                        JoinCondition::None,
                        JoinType::Cross,
                    )
                }
            }
            plan
        };
        let select_bind_step = self.context.step_now();
        self.context.step(QueryBindStep::Project);
        let mut select_list = self.normalize_select_item(&select.projection)?;
        self.context.step(select_bind_step);

        if let Some(predicate) = &select.selection {
            plan = self.bind_where(plan, predicate)?;
        }
        self.extract_select_join(&mut select_list);
        self.extract_select_aggregate(&mut select_list)?;

        match &select.group_by {
            GroupByExpr::Expressions(group_by_exprs, modifiers) => {
                if !modifiers.is_empty() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "GROUP BY modifiers are not supported".to_string(),
                    ));
                }
                if !group_by_exprs.is_empty() {
                    self.extract_group_by_aggregate(&mut select_list, group_by_exprs)?;
                }
            }
            GroupByExpr::All(_) => {
                return Err(DatabaseError::UnsupportedStmt(
                    "GROUP BY ALL is not supported".to_string(),
                ))
            }
        }

        let mut having_orderby = (None, None);

        if select.having.is_some() || orderby.is_some() {
            having_orderby =
                self.extract_having_orderby_aggregate(&select.having, orderby.unwrap_or(&[]))?;
        }

        if !self.context.agg_calls.is_empty() || !self.context.group_by_exprs.is_empty() {
            plan = self.bind_aggregate(
                plan,
                self.context.agg_calls.clone(),
                self.context.group_by_exprs.clone(),
            )?;
            self.bind_aggregate_output_exprs(select_list.iter_mut())?;
            if let Some(orderby) = having_orderby.1.as_mut() {
                self.bind_aggregate_output_exprs(orderby.iter_mut().map(|field| &mut field.expr))?;
            }
        }

        if let Some(having) = having_orderby.0 {
            plan = self.bind_having(plan, having)?;
        }

        if let Some(Distinct::Distinct) = select.distinct {
            plan = self.bind_distinct(plan, select_list.clone())?;
            let distinct_outputs = select_list.clone();
            self.bind_distinct_output_exprs(&distinct_outputs, select_list.iter_mut())?;
            if let Some(orderby) = having_orderby.1.as_mut() {
                self.bind_distinct_orderby_exprs(&distinct_outputs, orderby)?;
            }
        }

        if let Some(orderby) = having_orderby.1 {
            plan = self.bind_sort(plan, orderby)?;
        }

        if !select_list.is_empty() {
            plan = self.bind_project(plan, select_list)?;
        }

        if let Some(SelectInto { name, .. }) = &select.into {
            plan = LogicalPlan::new(
                Operator::Insert(InsertOperator {
                    table_name: lower_case_name(name)?.into(),
                    is_overwrite: false,
                    is_mapping_by_name: true,
                }),
                Childrens::Only(Box::new(plan)),
            )
        }

        Ok(plan)
    }

    /// FIXME: temp values need to register BindContext.bind_table
    fn bind_temp_values(&mut self, expr_rows: &[Vec<Expr>]) -> Result<LogicalPlan, DatabaseError> {
        let values_len = expr_rows[0].len();

        let mut inferred_types: Vec<Option<LogicalType>> = vec![None; values_len];
        let mut rows = Vec::with_capacity(expr_rows.len());

        for expr_row in expr_rows.iter() {
            if expr_row.len() != values_len {
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }

            let mut row = Vec::with_capacity(values_len);

            for (col_index, expr) in expr_row.iter().enumerate() {
                let mut expression = self.bind_expr(expr)?;
                ConstantCalculator.visit(&mut expression)?;

                if let ScalarExpression::Constant(value) = expression {
                    let value_type = value.logical_type();

                    inferred_types[col_index] = match &inferred_types[col_index] {
                        Some(existing) => {
                            Some(LogicalType::max_logical_type(existing, &value_type)?)
                        }
                        None => Some(value_type),
                    };

                    row.push(value);
                } else {
                    return Err(DatabaseError::ColumnsEmpty);
                }
            }

            rows.push(row);
        }

        let value_name = self.context.temp_table();
        let column_refs: Vec<ColumnRef> = inferred_types
            .into_iter()
            .enumerate()
            .map(|(col_index, typ)| {
                let typ = typ.ok_or(DatabaseError::InvalidType)?;
                let mut column_ref = ColumnCatalog::new(
                    col_index.to_string(),
                    false,
                    ColumnDesc::new(typ, None, false, None)?,
                );
                column_ref.set_ref_table(value_name.clone(), ColumnId::default(), true);
                Ok(ColumnRef(Arc::new(column_ref)))
            })
            .collect::<Result<_, DatabaseError>>()?;

        Ok(self.bind_values(rows, Arc::new(column_refs)))
    }

    fn bind_set_cast(
        &self,
        mut left_plan: LogicalPlan,
        mut right_plan: LogicalPlan,
    ) -> Result<(LogicalPlan, LogicalPlan), DatabaseError> {
        let mut left_cast = vec![];
        let mut right_cast = vec![];

        let left_schema = left_plan.output_schema();
        let right_schema = right_plan.output_schema();

        for (position, (left_schema, right_schema)) in
            left_schema.iter().zip(right_schema.iter()).enumerate()
        {
            let cast_type =
                LogicalType::max_logical_type(left_schema.datatype(), right_schema.datatype())?;
            if &cast_type != left_schema.datatype() {
                left_cast.push(ScalarExpression::TypeCast {
                    expr: Box::new(ScalarExpression::column_expr(left_schema.clone(), position)),
                    ty: cast_type.clone(),
                });
            } else {
                left_cast.push(ScalarExpression::column_expr(left_schema.clone(), position));
            }
            if &cast_type != right_schema.datatype() {
                right_cast.push(ScalarExpression::TypeCast {
                    expr: Box::new(ScalarExpression::column_expr(
                        right_schema.clone(),
                        position,
                    )),
                    ty: cast_type.clone(),
                });
            } else {
                right_cast.push(ScalarExpression::column_expr(
                    right_schema.clone(),
                    position,
                ));
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

    pub(crate) fn bind_set_operation(
        &mut self,
        op: &SetOperator,
        set_quantifier: &SetQuantifier,
        left: &SetExpr,
        right: &SetExpr,
    ) -> Result<LogicalPlan, DatabaseError> {
        let is_all = match set_quantifier {
            SetQuantifier::All => true,
            SetQuantifier::Distinct | SetQuantifier::None => false,
            SetQuantifier::ByName | SetQuantifier::AllByName | SetQuantifier::DistinctByName => {
                return Err(DatabaseError::UnsupportedStmt(
                    "set quantifier BY NAME is not supported".to_string(),
                ))
            }
        };
        let BinderContext {
            table_cache,
            view_cache,
            transaction,
            scala_functions,
            table_functions,
            temp_table_id,
            ..
        } = &self.context;
        let mut left_binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                *transaction,
                scala_functions,
                table_functions,
                temp_table_id.clone(),
            ),
            self.args,
            Some(self),
        );
        let mut right_binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                *transaction,
                scala_functions,
                table_functions,
                temp_table_id.clone(),
            ),
            self.args,
            Some(self),
        );

        let mut left_plan = left_binder.bind_set_expr(left)?;
        let mut right_plan = right_binder.bind_set_expr(right)?;

        let mut left_schema = left_plan.output_schema();
        let mut right_schema = right_plan.output_schema();

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
            .all(|(left, right)| left.datatype() == right.datatype())
        {
            (left_plan, right_plan) = self.bind_set_cast(left_plan, right_plan)?;
            left_schema = left_plan.output_schema();
            right_schema = right_plan.output_schema();
        }

        match op {
            SetOperator::Union => {
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
            SetOperator::Except => {
                if is_all {
                    Ok(ExceptOperator::build(
                        left_schema.clone(),
                        right_schema.clone(),
                        left_plan,
                        right_plan,
                    ))
                } else {
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
                    left_schema = left_plan.output_schema();
                    right_schema = right_plan.output_schema();

                    Ok(ExceptOperator::build(
                        left_schema.clone(),
                        right_schema.clone(),
                        left_plan,
                        right_plan,
                    ))
                }
            }
            set_operator => Err(DatabaseError::UnsupportedStmt(format!(
                "set operator: {set_operator:?}"
            ))),
        }
    }

    pub(crate) fn bind_table_ref(
        &mut self,
        from: &TableWithJoins,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::From);

        let TableWithJoins { relation, joins } = from;
        let mut plan = self.bind_single_table_ref(relation, None)?;

        for join in joins {
            plan = self.bind_join(plan, join)?;
        }
        Ok(plan)
    }

    fn bind_single_table_ref(
        &mut self,
        table: &TableFactor,
        joint_type: Option<JoinType>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let plan = match table {
            TableFactor::Table { name, alias, .. } => {
                let table_name = lower_case_name(name)?;

                self._bind_single_table_ref(joint_type, &table_name, alias.as_ref())?
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let BinderContext {
                    table_cache,
                    view_cache,
                    transaction,
                    scala_functions,
                    table_functions,
                    temp_table_id,
                    ..
                } = &self.context;
                let mut binder = Binder::new(
                    BinderContext::new(
                        table_cache,
                        view_cache,
                        *transaction,
                        scala_functions,
                        table_functions,
                        temp_table_id.clone(),
                    ),
                    self.args,
                    Some(self),
                );
                let mut plan = binder.bind_query(subquery)?;

                if let Some(TableAlias {
                    name,
                    columns: alias_column,
                    ..
                }) = alias
                {
                    let source_name = self.context.temp_table();
                    let table_alias: Arc<str> = name.value.to_lowercase().into();

                    plan = self.bind_alias(
                        plan,
                        alias_column,
                        table_alias.clone(),
                        source_name.clone(),
                    )?;
                    self.context.add_bound_source(
                        table_alias.clone(),
                        Some(table_alias),
                        joint_type,
                        Source::Schema(plan.output_schema().clone()),
                    );
                } else {
                    let passthrough_source = {
                        let output_schema = plan.output_schema().clone();
                        let mut names = output_schema
                            .iter()
                            .filter_map(|column| column.table_name().cloned());
                        let first = names.next();
                        if first.is_some() && names.all(|name| Some(name) == first) {
                            first
                        } else {
                            None
                        }
                    };
                    let needs_virtual_source = passthrough_source.is_none();
                    let source_name =
                        passthrough_source.unwrap_or_else(|| self.context.temp_table());

                    if needs_virtual_source {
                        plan = self.bind_schema_source(plan, source_name.clone());
                    }
                    self.context.add_bound_source(
                        source_name.clone(),
                        None,
                        joint_type,
                        Source::Schema(plan.output_schema().clone()),
                    );
                }
                plan
            }
            TableFactor::TableFunction { expr, alias } => {
                if let ScalarExpression::TableFunction(function) = self.bind_expr(expr)? {
                    let mut table_alias = None;
                    let table_name: TableName = function.summary().name.clone();
                    let table = function.table();
                    let mut plan = FunctionScanOperator::build(function);

                    if let Some(TableAlias {
                        name,
                        columns: alias_column,
                        ..
                    }) = alias
                    {
                        table_alias = Some(name.value.to_lowercase().into());

                        plan = self.bind_alias(
                            plan,
                            alias_column,
                            table_alias.clone().unwrap(),
                            table_name.clone(),
                        )?;
                    }

                    let source = if table_alias.is_some() {
                        Source::Schema(plan.output_schema().clone())
                    } else {
                        Source::Table(table)
                    };
                    self.context
                        .add_bound_source(table_name, table_alias, joint_type, source);
                    plan
                } else {
                    unreachable!()
                }
            }
            table => return Err(DatabaseError::UnsupportedStmt(format!("{table:#?}"))),
        };

        Ok(plan)
    }

    pub(crate) fn bind_alias(
        &mut self,
        mut plan: LogicalPlan,
        alias_column: &[TableAliasColumnDef],
        table_alias: TableName,
        table_name: TableName,
    ) -> Result<LogicalPlan, DatabaseError> {
        let input_schema = plan.output_schema();
        if !alias_column.is_empty() && alias_column.len() != input_schema.len() {
            return Err(DatabaseError::MisMatch("alias", "columns"));
        }
        let aliases_with_columns = if alias_column.is_empty() {
            input_schema
                .iter()
                .cloned()
                .map(|column| (column.name().to_string(), column))
                .collect_vec()
        } else {
            alias_column
                .iter()
                .map(|column| lower_ident(&column.name))
                .zip(input_schema.iter().cloned())
                .collect_vec()
        };
        let mut alias_exprs = Vec::with_capacity(aliases_with_columns.len());

        for (alias, column) in aliases_with_columns {
            let mut alias_column = ColumnCatalog::clone(&column);
            alias_column.set_name(alias.clone());
            let is_temp = matches!(
                &column.summary().relation,
                ColumnRelation::Table { is_temp: true, .. }
            );
            alias_column.set_ref_table(
                table_alias.clone(),
                column.id().unwrap_or(ColumnId::new()),
                is_temp,
            );

            let alias_column_expr = ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::column_expr(column, alias_exprs.len())),
                alias: AliasType::Expr(Box::new(ScalarExpression::column_expr(
                    ColumnRef::from(alias_column),
                    alias_exprs.len(),
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
        self.bind_project(plan, alias_exprs)
    }

    fn bind_schema_source(&mut self, mut plan: LogicalPlan, source_name: TableName) -> LogicalPlan {
        let input_schema = plan.output_schema();
        let mut source_exprs = Vec::with_capacity(input_schema.len());

        for (position, column) in input_schema.iter().cloned().enumerate() {
            let mut source_column = ColumnCatalog::clone(&column);
            source_column.set_ref_table(
                source_name.clone(),
                column.id().unwrap_or(ColumnId::new()),
                true,
            );

            source_exprs.push(ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::column_expr(column, position)),
                alias: AliasType::Expr(Box::new(ScalarExpression::column_expr(
                    ColumnRef::from(source_column),
                    position,
                ))),
            });
        }

        Self::build_project_plan(plan, source_exprs)
    }

    pub(crate) fn _bind_single_table_ref(
        &mut self,
        join_type: Option<JoinType>,
        table: &str,
        alias: Option<&TableAlias>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = table.into();
        let mut table_alias: Option<TableName> = None;
        let mut alias_idents = None;

        if let Some(TableAlias { name, columns, .. }) = alias {
            table_alias = Some(name.value.to_lowercase().into());
            alias_idents = Some(columns);
        }

        let with_pk = self.is_scan_with_pk(&table_name);
        let source = self
            .context
            .source_and_bind(table_name.clone(), table_alias.as_ref(), join_type, false)?
            .ok_or(DatabaseError::SourceNotFound)?;
        let mut plan = match source {
            Source::Table(table) => TableScanOperator::build(table_name.clone(), table, with_pk)?,
            Source::View(view) => LogicalPlan::clone(&view.plan),
            Source::Schema(_) => {
                return Err(DatabaseError::UnsupportedStmt(
                    "derived source cannot be rebound as a base relation".to_string(),
                ))
            }
        };

        if let (Some(idents), Some(alias_name)) = (alias_idents, table_alias) {
            plan = self.bind_alias(plan, idents, alias_name.clone(), table_name.clone())?;
            self.context.add_bound_source(
                table_name,
                Some(alias_name),
                join_type,
                Source::Schema(plan.output_schema().clone()),
            );
        }
        Ok(plan)
    }

    /// Normalize select item.
    ///
    /// - Qualified name, e.g. `SELECT t.a FROM t`
    /// - Qualified name with wildcard, e.g. `SELECT t.* FROM t,t1`
    /// - Scalar expression or aggregate expression, e.g. `SELECT COUNT(*) + 1 AS count FROM t`
    ///  
    fn normalize_select_item(
        &mut self,
        items: &[SelectItem],
    ) -> Result<Vec<ScalarExpression>, DatabaseError> {
        let mut select_items = vec![];

        for item in items.iter() {
            match item {
                SelectItem::UnnamedExpr(expr) => select_items.push(self.bind_expr(expr)?),
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expr(expr)?;
                    let alias_name = alias.value.to_lowercase();

                    self.context
                        .add_alias(None, alias_name.clone(), expr.clone());

                    select_items.push(ScalarExpression::Alias {
                        expr: Box::new(expr),
                        alias: AliasType::Name(alias_name),
                    });
                }
                SelectItem::Wildcard(_) => {
                    for visible_name in self
                        .context
                        .bind_table
                        .iter()
                        .filter(|bound_source| {
                            !Self::is_joined_values_source(
                                bound_source.join_type,
                                &bound_source.source,
                            )
                        })
                        .map(|bound_source| bound_source.visible_name())
                        .unique()
                        .cloned()
                    {
                        Self::bind_table_column_refs(
                            &self.context,
                            &mut self.table_schema_buf,
                            &mut select_items,
                            visible_name,
                            false,
                        )?;
                    }
                }
                SelectItem::QualifiedWildcard(table_name, _) => {
                    let table_name: Arc<str> = match table_name {
                        SelectItemQualifiedWildcardKind::ObjectName(name) => {
                            lower_case_name(name)?.into()
                        }
                        SelectItemQualifiedWildcardKind::Expr(expr) => {
                            return Err(DatabaseError::UnsupportedStmt(format!(
                                "qualified wildcard expr: {expr}"
                            )))
                        }
                    };
                    Self::bind_table_column_refs(
                        &self.context,
                        &mut self.table_schema_buf,
                        &mut select_items,
                        table_name,
                        true,
                    )?;
                }
            };
        }

        Ok(select_items)
    }

    #[allow(unused_assignments)]
    fn bind_table_column_refs(
        context: &BinderContext<'a, T>,
        table_schema_buf: &mut std::collections::HashMap<TableName, Option<SchemaOutput>>,
        exprs: &mut Vec<ScalarExpression>,
        table_name: TableName,
        is_qualified_wildcard: bool,
    ) -> Result<(), DatabaseError> {
        let fn_not_on_using = |column: &ColumnRef| {
            if context.using.is_empty() {
                return Some(&table_name) == column.table_name();
            }
            is_qualified_wildcard
                || Some(&table_name) == column.table_name() && !context.using.contains(column)
        };

        let (schema_ref, position_offset) =
            Self::resolve_source_columns_in_scope(context, table_schema_buf, table_name.as_ref())?;
        let mut pushed_alias_columns = false;

        for alias_column in context
            .expr_aliases
            .keys()
            .filter_map(|(alias_table, alias_column)| {
                matches!(alias_table.as_deref(), Some(alias) if alias == table_name.as_ref())
                    .then_some(alias_column.as_str())
            })
        {
            let Some((position, column)) = schema_ref
                .iter()
                .enumerate()
                .find(|(_, column)| column.name() == alias_column)
            else {
                continue;
            };
            if !fn_not_on_using(column) {
                continue;
            }
            exprs.push(ScalarExpression::column_expr(
                column.clone(),
                position_offset + position,
            ));
            pushed_alias_columns = true;
        }

        if pushed_alias_columns {
            return Ok(());
        }

        for (position, column) in schema_ref.iter().enumerate() {
            if !fn_not_on_using(column) {
                continue;
            }
            exprs.push(ScalarExpression::column_expr(
                column.clone(),
                position_offset + position,
            ));
        }
        Ok(())
    }

    fn bind_join(
        &mut self,
        mut left: LogicalPlan,
        join: &Join,
    ) -> Result<LogicalPlan, DatabaseError> {
        let Join {
            relation,
            join_operator,
            ..
        } = join;

        let (join_type, joint_condition) = match join_operator {
            JoinOperator::Join(constraint)
            | JoinOperator::Inner(constraint)
            | JoinOperator::StraightJoin(constraint) => (JoinType::Inner, Some(constraint)),
            JoinOperator::Left(constraint) | JoinOperator::LeftOuter(constraint) => {
                (JoinType::LeftOuter, Some(constraint))
            }
            JoinOperator::Right(constraint) | JoinOperator::RightOuter(constraint) => {
                (JoinType::RightOuter, Some(constraint))
            }
            JoinOperator::FullOuter(constraint) => (JoinType::Full, Some(constraint)),
            JoinOperator::CrossJoin(constraint) => (JoinType::Cross, Some(constraint)),
            JoinOperator::Semi(_)
            | JoinOperator::LeftSemi(_)
            | JoinOperator::Anti(_)
            | JoinOperator::LeftAnti(_)
            | JoinOperator::RightSemi(_)
            | JoinOperator::RightAnti(_)
            | JoinOperator::CrossApply
            | JoinOperator::OuterApply
            | JoinOperator::AsOf { .. } => {
                return Err(DatabaseError::UnsupportedStmt(format!("{join_operator:?}")))
            }
        };
        let BinderContext {
            table_cache,
            view_cache,
            transaction,
            scala_functions,
            table_functions,
            temp_table_id,
            ..
        } = &self.context;
        let mut binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                *transaction,
                scala_functions,
                table_functions,
                temp_table_id.clone(),
            ),
            self.args,
            Some(self),
        );
        let mut right = binder.bind_single_table_ref(relation, Some(join_type))?;
        self.extend(binder.context);

        let mut on = match joint_condition {
            Some(constraint) => self.bind_join_constraint(
                join_type,
                left.output_schema(),
                right.output_schema(),
                constraint,
            )?,
            None => JoinCondition::None,
        };
        Self::localize_join_condition_from_join_scope(&mut on, left.output_schema().len())?;

        Ok(LJoinOperator::build(left, right, on, join_type))
    }

    pub(crate) fn bind_where(
        &mut self,
        mut children: LogicalPlan,
        predicate: &Expr,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Where);

        let mut predicate = self.bind_expr(predicate)?;

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
                        let (plan, predicates) = Self::prepare_mark_apply(
                            &mut predicate,
                            &output_column,
                            children.output_schema(),
                            plan,
                            correlated,
                            false,
                            Vec::new(),
                        )?;
                        children = MarkApplyOperator::build_exists(
                            children,
                            plan,
                            output_column,
                            predicates,
                        );
                    }
                    SubQueryType::InSubQuery {
                        plan,
                        correlated,
                        output_column,
                        predicate: mut in_predicate,
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
                            in_predicate = Self::rewrite_correlated_in_predicate(in_predicate);
                        }
                        let (plan, predicates) = Self::prepare_mark_apply(
                            &mut predicate,
                            &output_column,
                            children.output_schema(),
                            plan,
                            correlated,
                            true,
                            vec![in_predicate],
                        )?;
                        children =
                            MarkApplyOperator::build_in(children, plan, output_column, predicates);
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
                        )?;
                    }
                }
            }
            if matches!(uses_mark_apply, Some(true)) {
                let passthrough_exprs = children
                    .output_schema()
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
    ) -> Vec<AppendedRightOutput> {
        let output_schema = plan.output_schema().clone();
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
            let mut appended_outputs = Vec::new();
            op.exprs.extend(
                child
                    .output_schema()
                    .iter()
                    .enumerate()
                    .filter(|(_, column)| {
                        !output_schema.contains(column)
                            && predicates.iter().any(|expr| {
                                expr.any_referenced_column(true, |candidate| {
                                    candidate.same_column(column)
                                })
                            })
                    })
                    .map(|(position, column)| {
                        appended_outputs.push(AppendedRightOutput {
                            column: column.clone(),
                            child_position: position,
                            output_position: output_len + appended_outputs.len(),
                        });
                        ScalarExpression::column_expr(column.clone(), position)
                    }),
            );
            if !appended_outputs.is_empty() {
                plan.reset_output_schema_cache();
            }
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
    ) -> Result<(LogicalPlan, Vec<ScalarExpression>), DatabaseError> {
        let left_len = left_schema.len();
        MarkerPositionGlobalizer {
            output_column,
            left_len,
        }
        .visit(predicate)?;

        let (mut plan, correlated_filters) = if correlated {
            Self::prepare_correlated_subquery_plan(plan, left_schema, preserve_projection)?
        } else {
            (plan, Vec::new())
        };
        apply_predicates.extend(correlated_filters);

        if correlated {
            let appended_right_outputs =
                Self::ensure_mark_apply_right_outputs(&mut plan, &apply_predicates);
            if !appended_right_outputs.is_empty() {
                Self::localize_appended_right_outputs(
                    apply_predicates.iter_mut(),
                    &appended_right_outputs,
                )?;
            }
        }
        let right_schema = plan.output_schema().clone();
        for expr in apply_predicates.iter_mut() {
            RightSidePositionGlobalizer {
                right_schema: right_schema.as_ref(),
                left_len,
            }
            .visit(expr)?;
        }

        Ok((plan, apply_predicates))
    }

    fn rewrite_correlated_in_predicate(predicate: ScalarExpression) -> ScalarExpression {
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
            } if op == BinaryOperator::Eq => ScalarExpression::Binary {
                op,
                left_expr: strip_projection_alias(left_expr),
                right_expr: strip_projection_alias(right_expr),
                evaluator: None,
                ty,
            },
            predicate => predicate,
        }
    }

    fn plan_has_correlated_refs(plan: &LogicalPlan, left_schema: &Schema) -> bool {
        let contains = |column: &ColumnRef| left_schema.contains(column);

        if plan.operator.any_referenced_column(true, contains) {
            return true;
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => Self::plan_has_correlated_refs(child, left_schema),
            Childrens::Twins { left, right } => {
                Self::plan_has_correlated_refs(left, left_schema)
                    || Self::plan_has_correlated_refs(right, left_schema)
            }
            Childrens::None => false,
        }
    }

    fn expr_has_correlated_refs(expr: &ScalarExpression, left_schema: &Schema) -> bool {
        expr.any_referenced_column(true, |column| left_schema.contains(column))
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
    ) -> Result<(LogicalPlan, Vec<ScalarExpression>), DatabaseError> {
        match plan.childrens.as_ref() {
            Childrens::Only(_) => {}
            Childrens::Twins { .. } => {
                if Self::plan_has_correlated_refs(&plan, left_schema) {
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
                )?;
                let mut local_filters = Vec::new();
                let mut predicates = Vec::new();
                Self::split_conjuncts(op.predicate, &mut predicates);
                for predicate in predicates {
                    if Self::expr_has_correlated_refs(&predicate, left_schema) {
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
                )?;

                if !preserve_projection || Self::is_temp_alias_projection(&op.exprs) {
                    Ok((child, correlated_filters))
                } else {
                    Self::bind_project_output_exprs(&op.exprs, correlated_filters.iter_mut())?;
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
            ),
            plan => {
                if Self::plan_has_correlated_refs(&plan, left_schema) {
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
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Having);

        self.validate_having_orderby(&having)?;
        self.bind_aggregate_output_exprs(std::iter::once(&mut having))?;
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

                let left_len = children.output_schema().len();
                let right_schema = plan.output_schema().clone();
                for expr in select_list.iter_mut() {
                    RightSidePositionGlobalizer {
                        right_schema: right_schema.as_ref(),
                        left_len,
                    }
                    .visit(expr)?;
                }

                children = ScalarApplyOperator::build(children, plan);
            }
        }

        Ok(Self::build_project_plan(children, select_list))
    }

    fn bind_sort(
        &mut self,
        children: LogicalPlan,
        sort_fields: Vec<SortField>,
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

    fn bind_non_negative_limit_value(&mut self, expr: &Expr) -> Result<usize, DatabaseError> {
        let bound_expr = self.bind_expr(expr)?;
        match bound_expr {
            ScalarExpression::Constant(dv) => match &dv {
                DataValue::Int32(v) if *v >= 0 => Ok(*v as usize),
                DataValue::Int64(v) if *v >= 0 => Ok(*v as usize),
                _ => Err(DatabaseError::InvalidType),
            },
            _ => Err(attach_span_if_absent(
                DatabaseError::invalid_column("invalid limit expression.".to_owned()),
                expr,
            )),
        }
    }

    fn bind_limit(
        &mut self,
        children: LogicalPlan,
        limit: LimitClause,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Limit);

        let mut limit_value = None;
        let mut offset_value = None;
        match limit {
            LimitClause::LimitOffset {
                limit: limit_expr,
                offset: offset_expr,
                limit_by,
            } => {
                if !limit_by.is_empty() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "LIMIT BY is not supported".to_string(),
                    ));
                }

                if let Some(limit_ast) = limit_expr.as_ref() {
                    limit_value = Some(self.bind_non_negative_limit_value(limit_ast)?);
                }

                if let Some(offset_ast) = offset_expr.as_ref() {
                    offset_value = Some(self.bind_non_negative_limit_value(&offset_ast.value)?);
                }
            }
            LimitClause::OffsetCommaLimit {
                offset: offset_expr,
                limit: limit_expr,
            } => {
                limit_value = Some(self.bind_non_negative_limit_value(&limit_expr)?);
                offset_value = Some(self.bind_non_negative_limit_value(&offset_expr)?);
            }
        }

        Ok(LimitOperator::build(offset_value, limit_value, children))
    }

    pub fn extract_select_join(&mut self, select_items: &mut [ScalarExpression]) {
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
                    .find(|(table_name, source, _)| {
                        let schema_buf = self
                            .table_schema_buf
                            .entry((*table_name).clone())
                            .or_default();

                        source.column(column.name(), schema_buf).is_some()
                    })
                    .map(|(_, _, nullable)| {
                        if let Some(new_column) = column.nullable_for_join(*nullable) {
                            *column = new_column;
                        }
                    });
            }
        }
    }

    fn bind_join_constraint<'c>(
        &mut self,
        join_type: JoinType,
        left_schema: &'c SchemaRef,
        right_schema: &'c SchemaRef,
        constraint: &JoinConstraint,
    ) -> Result<JoinCondition, DatabaseError> {
        match constraint {
            JoinConstraint::On(expr) => {
                // left and right columns that match equi-join pattern
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = vec![];
                // expression that didn't match equi-join pattern
                let mut filter = vec![];
                let expr = self.bind_expr(expr)?;

                Self::extract_join_keys(
                    expr,
                    &mut on_keys,
                    &mut filter,
                    left_schema,
                    right_schema,
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
            JoinConstraint::Using(idents) => {
                fn find_column<'a>(
                    schema: &'a Schema,
                    name: &'a str,
                ) -> Option<(usize, &'a ColumnRef)> {
                    schema
                        .iter()
                        .enumerate()
                        .find(|(_, column)| column.name() == name)
                }

                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = Vec::new();

                for ident in idents {
                    let name = lower_case_name(ident)?;
                    let (Some((left_position, left_column)), Some((right_position, right_column))) = (
                        find_column(left_schema, &name),
                        find_column(right_schema, &name),
                    ) else {
                        return Err(attach_span_if_absent(
                            DatabaseError::invalid_column("not found column".to_string()),
                            ident,
                        ));
                    };
                    self.context.add_using(join_type, left_column, right_column);
                    on_keys.push((
                        ScalarExpression::column_expr(left_column.clone(), left_position),
                        ScalarExpression::column_expr(
                            right_column.clone(),
                            left_schema.len() + right_position,
                        ),
                    ));
                }
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: None,
                })
            }
            JoinConstraint::None => Ok(JoinCondition::None),
            JoinConstraint::Natural => {
                let fn_names = |schema: &'c Schema| -> HashSet<&'c str> {
                    schema.iter().map(|column| column.name()).collect()
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
                            .find(|(_, column)| column.name() == *name),
                        right_schema
                            .iter()
                            .enumerate()
                            .find(|(_, column)| column.name() == *name),
                    ) {
                        let left_expr =
                            ScalarExpression::column_expr(left_column.clone(), left_position);
                        let right_expr = ScalarExpression::column_expr(
                            right_column.clone(),
                            left_schema.len() + right_position,
                        );

                        self.context.add_using(join_type, left_column, right_column);
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
    ) -> Result<(), DatabaseError> {
        let fn_contains = |schema: &Schema, summary: &ColumnSummary| {
            schema.iter().any(|column| summary == column.summary())
        };
        let fn_or_contains =
            |left_schema: &Schema, right_schema: &Schema, summary: &ColumnSummary| {
                fn_contains(left_schema, summary) || fn_contains(right_schema, summary)
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
                                if fn_contains(left_schema, l.summary())
                                    && fn_contains(right_schema, r.summary())
                                {
                                    accum.push((*left_expr, *right_expr));
                                } else if fn_contains(left_schema, r.summary())
                                    && fn_contains(right_schema, l.summary())
                                {
                                    accum.push((*right_expr, *left_expr));
                                } else if fn_or_contains(left_schema, right_schema, l.summary())
                                    || fn_or_contains(left_schema, right_schema, r.summary())
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
                            (ScalarExpression::ColumnRef { column, .. }, _)
                            | (_, ScalarExpression::ColumnRef { column, .. }) => {
                                if fn_or_contains(left_schema, right_schema, column.summary()) {
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
                                if left_expr.all_referenced_columns(true, |column| {
                                    fn_or_contains(left_schema, right_schema, column.summary())
                                }) && right_expr.all_referenced_columns(true, |column| {
                                    fn_or_contains(left_schema, right_schema, column.summary())
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
                        )?;
                        Self::extract_join_keys(
                            *right_expr,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
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
                        if left_expr.all_referenced_columns(true, |column| {
                            fn_or_contains(left_schema, right_schema, column.summary())
                        }) && right_expr.all_referenced_columns(true, |column| {
                            fn_or_contains(left_schema, right_schema, column.summary())
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
            expr => {
                if expr.all_referenced_columns(true, |column| {
                    fn_or_contains(left_schema, right_schema, column.summary())
                }) {
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
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::errors::DatabaseError;
    use crate::expression::visitor_mut::VisitorMut;
    use crate::expression::{AliasType, ScalarExpression};
    use crate::planner::operator::join::{JoinCondition, JoinType};
    use crate::planner::operator::mark_apply::{MarkApplyKind, MarkApplyOperator};
    use crate::planner::operator::Operator;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::types::LogicalType;

    fn test_column(name: &str, position: usize) -> ScalarExpression {
        ScalarExpression::column_expr(
            ColumnRef::from(ColumnCatalog::new(
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
        let left_column = ColumnRef::from(ColumnCatalog::new(
            "left".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        ));
        let right_column = ColumnRef::from(ColumnCatalog::new(
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
        let project_output = ScalarExpression::Alias {
            expr: Box::new(test_column("c1", 0)),
            alias: AliasType::Name("v".to_string()),
        };
        let mut expr = ScalarExpression::Alias {
            expr: Box::new(test_column("c1", 0)),
            alias: AliasType::Name("v".to_string()),
        };

        ProjectionOutputBinder::new(std::slice::from_ref(&project_output)).visit(&mut expr)?;

        assert_eq!(
            expr,
            ScalarExpression::column_expr(project_output.output_column(), 0)
        );
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
        let Some(mark_apply) = find_mark_apply(&plan) else {
            panic!("expected IN subquery to introduce a mark apply")
        };

        assert_eq!(mark_apply.kind, MarkApplyKind::In);
        assert_eq!(mark_apply.predicates().len(), 1);

        Ok(())
    }

    #[test]
    fn test_correlated_in_subquery_in_where_binds_as_mark_apply() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;
        let plan =
            table_states.plan("select * from t1 where c1 in (select c3 from t2 where c4 = c2)")?;
        let Some(mark_apply) = find_mark_apply(&plan) else {
            panic!("expected correlated IN subquery to introduce a mark apply")
        };

        assert_eq!(mark_apply.kind, MarkApplyKind::In);
        assert_eq!(mark_apply.predicates().len(), 2);

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
        let left_len = left.output_schema_direct().columns().count();

        let mut positions = Vec::new();
        collect_column_positions(filter, &mut positions);

        assert_eq!(positions, vec![0, left_len - 1, 0, left_len]);

        Ok(())
    }
}
