// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{Binder, QueryBindStep};
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
use crate::errors::DatabaseError;
use crate::expression::visitor_mut::{walk_mut_expr, ExprVisitorMut};
use crate::expression::window::{WindowCall, WindowFunction, WindowFunctionKind, WindowSpec};
use crate::expression::ScalarExpression;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::sort::SortOperator;
use crate::planner::operator::window::WindowOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use crate::types::LogicalType;

struct WindowCollector {
    windows: Vec<(WindowCall, ColumnRef)>,
}

impl ExprVisitorMut for WindowCollector {
    fn visit(
        &mut self,
        expr: &mut ExprRef,
        arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        let ScalarExpression::WindowCall(window) = arena.expression(*expr) else {
            return walk_mut_expr(self, expr, arena);
        };
        if let Some((_, output_column)) = self
            .windows
            .iter()
            .find(|(candidate, _)| candidate == window)
        {
            *arena.expression_mut(*expr) = ScalarExpression::column_expr(*output_column, 0);
            return Ok(());
        }

        let output_name = expr.output_name(arena);
        let ScalarExpression::WindowCall(window) =
            std::mem::replace(arena.expression_mut(*expr), ScalarExpression::Empty)
        else {
            unreachable!()
        };
        let output_column = arena.alloc_column(ColumnCatalog::new(
            output_name,
            true,
            ColumnDesc::new(window.function.ty.clone(), None, false, None)?,
        ));
        self.windows.push((window, output_column));
        *arena.expression_mut(*expr) = ScalarExpression::column_expr(output_column, 0);
        Ok(())
    }
}

struct WindowOutputBinder<'a> {
    groups: &'a [WindowGroup],
    base_position: usize,
}

impl ExprVisitorMut for WindowOutputBinder<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        _arena: &mut PlanArena<'_>,
    ) -> Result<(), DatabaseError> {
        if let Some(output_position) = self
            .groups
            .iter()
            .flat_map(|group| &group.output_columns)
            .position(|output_column| output_column == column)
        {
            *position = self.base_position + output_position;
        }
        Ok(())
    }
}

struct WindowGroup {
    partition_by: Vec<ExprRef>,
    order_by: Vec<SortField>,
    functions: Vec<WindowFunction>,
    output_columns: Vec<ColumnRef>,
}

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_window_function(
        &mut self,
        kind: WindowFunctionKind,
        args: Vec<ExprRef>,
        partition_by: Vec<ExprRef>,
        order_by: Vec<SortField>,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        if !matches!(
            self.context.step_now(),
            QueryBindStep::Project | QueryBindStep::Sort
        ) {
            return Err(DatabaseError::UnsupportedStmt(
                "window functions are only allowed in SELECT and ORDER BY".to_string(),
            ));
        }
        for expr in args
            .iter()
            .chain(&partition_by)
            .chain(order_by.iter().map(|field| &field.expr))
        {
            if expr.has_window_call(arena)? {
                return Err(DatabaseError::UnsupportedStmt(
                    "window functions cannot be nested".to_string(),
                ));
            }
        }

        let (args, ty) = match kind {
            WindowFunctionKind::RowNumber
            | WindowFunctionKind::Rank
            | WindowFunctionKind::DenseRank => {
                if !args.is_empty() {
                    return Err(DatabaseError::MisMatch(
                        "number of ranking function parameters",
                        "0",
                    ));
                }
                (args, LogicalType::Bigint)
            }
            WindowFunctionKind::Aggregate(agg_kind) => {
                let ScalarExpression::AggCall { args, ty, .. } =
                    self.bind_aggregate_function(agg_kind, args, false, arena)?
                else {
                    unreachable!()
                };
                (args, ty)
            }
        };

        Ok(ScalarExpression::WindowCall(WindowCall {
            function: WindowFunction { kind, args, ty },
            spec: WindowSpec {
                partition_by,
                order_by,
            },
        }))
    }

    pub(crate) fn bind_window(
        &mut self,
        mut children: LogicalPlan,
        select_list: &mut [ExprRef],
        order_by: &mut Option<Vec<SortField>>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut collector = WindowCollector {
            windows: Vec::new(),
        };
        for expr in select_list.iter_mut() {
            collector.visit(expr, arena)?;
        }
        if let Some(order_by) = order_by.as_mut() {
            for field in order_by {
                collector.visit(&mut field.expr, arena)?;
            }
        }
        if collector.windows.is_empty() {
            return Ok(children);
        }
        let windows = collector.windows;

        let base_position = children.output_schema(arena).len();
        let mut groups: Vec<WindowGroup> = Vec::new();
        for (window, output_column) in windows {
            let WindowCall { function, spec } = window;
            let group_index = groups.iter().position(|group| {
                group.partition_by == spec.partition_by && group.order_by == spec.order_by
            });

            if let Some(index) = group_index {
                groups[index].functions.push(function);
                groups[index].output_columns.push(output_column);
            } else {
                groups.push(WindowGroup {
                    partition_by: spec.partition_by,
                    order_by: spec.order_by,
                    functions: vec![function],
                    output_columns: vec![output_column],
                });
            }
        }

        let mut output_binder = WindowOutputBinder {
            groups: &groups,
            base_position,
        };
        for expr in select_list
            .iter_mut()
            .chain(order_by.iter_mut().flatten().map(|field| &mut field.expr))
        {
            output_binder.visit(expr, arena)?;
        }

        for group in groups {
            let partition_by_len = group.partition_by.len();
            let sort_fields: Vec<_> = group
                .partition_by
                .into_iter()
                .map(SortField::from)
                .chain(group.order_by)
                .collect();
            if !sort_fields.is_empty() {
                children = LogicalPlan::new(
                    Operator::Sort(SortOperator {
                        sort_fields: sort_fields.clone(),
                    }),
                    Childrens::Only(Box::new(children)),
                );
            }
            children = LogicalPlan::new(
                Operator::Window(WindowOperator {
                    sort_fields,
                    partition_by_len,
                    functions: group.functions,
                    output_columns: group.output_columns,
                }),
                Childrens::Only(Box::new(children)),
            );
        }
        self.context.step(QueryBindStep::Window);
        Ok(children)
    }
}
