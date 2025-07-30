use std::borrow::Borrow;
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    expression::ScalarExpression,
    planner::{
        operator::{
            filter::FilterOperator, join::JoinOperator as LJoinOperator, limit::LimitOperator,
            project::ProjectOperator, Operator,
        },
        operator::{join::JoinType, table_scan::TableScanOperator},
    },
    types::value::DataValue,
};

use super::{
    lower_case_name, lower_ident, Binder, BinderContext, QueryBindStep, Source, SubQueryType,
};

use crate::catalog::{ColumnCatalog, ColumnRef, ColumnSummary, TableName};
use crate::errors::DatabaseError;
use crate::execution::dql::join::joins_nullable;
use crate::expression::agg::AggKind;
use crate::expression::{AliasType, BinaryOperator};
use crate::planner::operator::aggregate::AggregateOperator;
use crate::planner::operator::except::ExceptOperator;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::sort::{SortField, SortOperator};
use crate::planner::operator::union::UnionOperator;
use crate::planner::{Childrens, LogicalPlan, SchemaOutput};
use crate::storage::Transaction;
use crate::types::tuple::{Schema, SchemaRef};
use crate::types::value::Utf8Type;
use crate::types::{ColumnId, LogicalType};
use itertools::Itertools;
use sqlparser::ast::{
    CharLengthUnits, Distinct, Expr, Ident, Join, JoinConstraint, JoinOperator, Offset,
    OrderByExpr, Query, Select, SelectInto, SelectItem, SetExpr, SetOperator, SetQuantifier,
    TableAlias, TableFactor, TableWithJoins,
};

impl<'a: 'b, 'b, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, 'b, T, A> {
    pub(crate) fn bind_query(&mut self, query: &Query) -> Result<LogicalPlan, DatabaseError> {
        let origin_step = self.context.step_now();

        if let Some(_with) = &query.with {
            // TODO support with clause.
        }

        let mut plan = match query.body.borrow() {
            SetExpr::Select(select) => self.bind_select(select, &query.order_by),
            SetExpr::Query(query) => self.bind_query(query),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.bind_set_operation(op, set_quantifier, left, right),
            expr => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "query body: {:?}",
                    expr
                )))
            }
        }?;

        let limit = &query.limit;
        let offset = &query.offset;

        if limit.is_some() || offset.is_some() {
            plan = self.bind_limit(plan, limit, offset)?;
        }

        self.context.step(origin_step);
        Ok(plan)
    }

    pub(crate) fn bind_select(
        &mut self,
        select: &Select,
        orderby: &[OrderByExpr],
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
        let mut select_list = self.normalize_select_item(&select.projection, &plan)?;

        if let Some(predicate) = &select.selection {
            plan = self.bind_where(plan, predicate)?;
        }
        self.extract_select_join(&mut select_list);
        self.extract_select_aggregate(&mut select_list)?;

        if !select.group_by.is_empty() {
            self.extract_group_by_aggregate(&mut select_list, &select.group_by)?;
        }

        let mut having_orderby = (None, None);

        if select.having.is_some() || !orderby.is_empty() {
            having_orderby = self.extract_having_orderby_aggregate(&select.having, orderby)?;
        }

        if !self.context.agg_calls.is_empty() || !self.context.group_by_exprs.is_empty() {
            plan = self.bind_aggregate(
                plan,
                self.context.agg_calls.clone(),
                self.context.group_by_exprs.clone(),
            );
        }

        if let Some(having) = having_orderby.0 {
            plan = self.bind_having(plan, having)?;
        }

        if let Some(Distinct::Distinct) = select.distinct {
            plan = self.bind_distinct(plan, select_list.clone());
        }

        if let Some(orderby) = having_orderby.1 {
            plan = self.bind_sort(plan, orderby);
        }

        if !select_list.is_empty() {
            plan = self.bind_project(plan, select_list)?;
        }

        if let Some(SelectInto { name, .. }) = &select.into {
            plan = LogicalPlan::new(
                Operator::Insert(InsertOperator {
                    table_name: Arc::new(lower_case_name(name)?),
                    is_overwrite: false,
                    is_mapping_by_name: true,
                }),
                Childrens::Only(plan),
            )
        }

        Ok(plan)
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

        for (left_schema, right_schema) in left_schema.iter().zip(right_schema.iter()) {
            let cast_type =
                LogicalType::max_logical_type(left_schema.datatype(), right_schema.datatype())?;
            if &cast_type != left_schema.datatype() {
                left_cast.push(ScalarExpression::TypeCast {
                    expr: Box::new(ScalarExpression::ColumnRef(left_schema.clone())),
                    ty: cast_type.clone(),
                });
            } else {
                left_cast.push(ScalarExpression::ColumnRef(left_schema.clone()));
            }
            if &cast_type != right_schema.datatype() {
                right_cast.push(ScalarExpression::TypeCast {
                    expr: Box::new(ScalarExpression::ColumnRef(right_schema.clone())),
                    ty: cast_type.clone(),
                });
            } else {
                right_cast.push(ScalarExpression::ColumnRef(right_schema.clone()));
            }
        }

        if !left_cast.is_empty() {
            left_plan = LogicalPlan::new(
                Operator::Project(ProjectOperator { exprs: left_cast }),
                Childrens::Only(left_plan),
            );
        }

        if !right_cast.is_empty() {
            right_plan = LogicalPlan::new(
                Operator::Project(ProjectOperator { exprs: right_cast }),
                Childrens::Only(right_plan),
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
        };
        let mut left_plan = self.bind_set_expr(left)?;
        let mut right_plan = self.bind_set_expr(right)?;

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
                        .map(ScalarExpression::ColumnRef)
                        .collect_vec();

                    let union_op = Operator::Union(UnionOperator {
                        left_schema_ref: left_schema.clone(),
                        _right_schema_ref: right_schema.clone(),
                    });

                    Ok(self.bind_distinct(
                        LogicalPlan::new(
                            union_op,
                            Childrens::Twins {
                                left: left_plan,
                                right: right_plan,
                            },
                        ),
                        distinct_exprs,
                    ))
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
                    let distinct_exprs = left_schema
                        .iter()
                        .cloned()
                        .map(ScalarExpression::ColumnRef)
                        .collect_vec();

                    let except_op = Operator::Except(ExceptOperator {
                        left_schema_ref: left_schema.clone(),
                        _right_schema_ref: right_schema.clone(),
                    });

                    Ok(self.bind_distinct(
                        LogicalPlan::new(
                            except_op,
                            Childrens::Twins {
                                left: left_plan,
                                right: right_plan,
                            },
                        ),
                        distinct_exprs,
                    ))
                }
            }
            set_operator => Err(DatabaseError::UnsupportedStmt(format!(
                "set operator: {:?}",
                set_operator
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
                let mut plan = self.bind_query(subquery)?;
                let mut tables = plan.referenced_table();

                if let Some(TableAlias {
                    name,
                    columns: alias_column,
                }) = alias
                {
                    if tables.len() > 1 {
                        return Err(DatabaseError::UnsupportedStmt(
                            "Implement virtual tables for multiple table aliases".to_string(),
                        ));
                    }
                    let table_alias = Arc::new(name.value.to_lowercase());

                    plan =
                        self.bind_alias(plan, alias_column, table_alias, tables.pop().unwrap())?;
                }
                plan
            }
            TableFactor::TableFunction { expr, alias } => {
                if let ScalarExpression::TableFunction(function) = self.bind_expr(expr)? {
                    let mut table_alias = None;
                    let table_name = Arc::new(function.summary().name.clone());
                    let table = function.table();
                    let mut plan = FunctionScanOperator::build(function);

                    if let Some(TableAlias {
                        name,
                        columns: alias_column,
                    }) = alias
                    {
                        table_alias = Some(Arc::new(name.value.to_lowercase()));

                        plan = self.bind_alias(
                            plan,
                            alias_column,
                            table_alias.clone().unwrap(),
                            table_name.clone(),
                        )?;
                    }

                    self.context
                        .bind_table
                        .insert((table_name, table_alias, joint_type), Source::Table(table));
                    plan
                } else {
                    unreachable!()
                }
            }
            table => return Err(DatabaseError::UnsupportedStmt(format!("{:#?}", table))),
        };

        Ok(plan)
    }

    pub(crate) fn bind_alias(
        &mut self,
        mut plan: LogicalPlan,
        alias_column: &[Ident],
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
                .map(lower_ident)
                .zip(input_schema.iter().cloned())
                .collect_vec()
        };
        let mut alias_exprs = Vec::with_capacity(aliases_with_columns.len());

        for (alias, column) in aliases_with_columns {
            let mut alias_column = ColumnCatalog::clone(&column);
            alias_column.set_name(alias.clone());
            alias_column.set_ref_table(
                table_alias.clone(),
                column.id().unwrap_or(ColumnId::new()),
                false,
            );

            let alias_column_expr = ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::ColumnRef(column)),
                alias: AliasType::Expr(Box::new(ScalarExpression::ColumnRef(ColumnRef::from(
                    alias_column,
                )))),
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

    pub(crate) fn _bind_single_table_ref(
        &mut self,
        join_type: Option<JoinType>,
        table: &str,
        alias: Option<&TableAlias>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = Arc::new(table.to_string());
        let mut table_alias = None;
        let mut alias_idents = None;

        if let Some(TableAlias { name, columns }) = alias {
            table_alias = Some(Arc::new(name.value.to_lowercase()));
            alias_idents = Some(columns);
        }

        let with_pk = self.is_scan_with_pk(&table_name);
        let source = self
            .context
            .source_and_bind(table_name.clone(), table_alias.as_ref(), join_type, false)?
            .ok_or(DatabaseError::SourceNotFound)?;
        let mut plan = match source {
            Source::Table(table) => TableScanOperator::build(table_name.clone(), table, with_pk),
            Source::View(view) => LogicalPlan::clone(&view.plan),
        };

        if let Some(idents) = alias_idents {
            plan = self.bind_alias(plan, idents, table_alias.unwrap(), table_name.clone())?;
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
        plan: &LogicalPlan,
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
                    if let Operator::Project(op) = &plan.operator {
                        for expr in op.exprs.iter() {
                            select_items.push(expr.clone());
                        }
                        continue;
                    }
                    for (table_name, alias, _) in self.context.bind_table.keys() {
                        let schema_buf =
                            self.table_schema_buf.entry(table_name.clone()).or_default();
                        Self::bind_table_column_refs(
                            &self.context,
                            schema_buf,
                            &mut select_items,
                            alias.as_ref().unwrap_or(table_name).clone(),
                            false,
                        )?;
                    }
                }
                SelectItem::QualifiedWildcard(table_name, _) => {
                    let table_name = Arc::new(lower_case_name(table_name)?);
                    let schema_buf = self.table_schema_buf.entry(table_name.clone()).or_default();

                    Self::bind_table_column_refs(
                        &self.context,
                        schema_buf,
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
        schema_buf: &mut Option<SchemaOutput>,
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

        let bound_alias = context
            .expr_aliases
            .iter()
            .filter(|(_, expr)| {
                if let ScalarExpression::ColumnRef(col) = expr.unpack_alias_ref() {
                    if fn_not_on_using(col) {
                        exprs.push(ScalarExpression::clone(expr));
                        return true;
                    }
                }
                false
            })
            .count()
            > 0;

        if bound_alias {
            return Ok(());
        }
        let mut source = None;

        source = context.table(table_name.clone())?.map(Source::Table);
        if source.is_none() {
            source = context.view(table_name.clone())?.map(Source::View);
        }
        for column in source
            .ok_or(DatabaseError::SourceNotFound)?
            .columns(schema_buf)
        {
            if !fn_not_on_using(column) {
                continue;
            }
            exprs.push(ScalarExpression::ColumnRef(column.clone()));
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
        } = join;

        let (join_type, joint_condition) = match join_operator {
            JoinOperator::Inner(constraint) => (JoinType::Inner, Some(constraint)),
            JoinOperator::LeftOuter(constraint) => (JoinType::LeftOuter, Some(constraint)),
            JoinOperator::RightOuter(constraint) => (JoinType::RightOuter, Some(constraint)),
            JoinOperator::FullOuter(constraint) => (JoinType::Full, Some(constraint)),
            JoinOperator::CrossJoin => (JoinType::Cross, None),
            _ => unimplemented!(),
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

        let on = match joint_condition {
            Some(constraint) => self.bind_join_constraint(
                join_type,
                left.output_schema(),
                right.output_schema(),
                constraint,
            )?,
            None => JoinCondition::None,
        };

        Ok(LJoinOperator::build(left, right, on, join_type))
    }

    pub(crate) fn bind_where(
        &mut self,
        mut children: LogicalPlan,
        predicate: &Expr,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Where);

        let predicate = self.bind_expr(predicate)?;

        if let Some(sub_queries) = self.context.sub_queries_at_now() {
            for sub_query in sub_queries {
                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = vec![];
                let mut filter = vec![];

                let (mut plan, join_ty) = match sub_query {
                    SubQueryType::SubQuery(plan) => (plan, JoinType::Inner),
                    SubQueryType::ExistsSubQuery(is_not, plan) => {
                        let limit = LimitOperator::build(None, Some(1), plan);
                        let mut agg = AggregateOperator::build(
                            limit,
                            vec![ScalarExpression::AggCall {
                                distinct: false,
                                kind: AggKind::Count,
                                args: vec![ScalarExpression::Constant(DataValue::Utf8 {
                                    value: "*".to_string(),
                                    ty: Utf8Type::Fixed(1),
                                    unit: CharLengthUnits::Characters,
                                })],
                                ty: LogicalType::Integer,
                            }],
                            vec![],
                            false,
                        );
                        let filter = FilterOperator::build(
                            ScalarExpression::Binary {
                                op: if is_not {
                                    BinaryOperator::NotEq
                                } else {
                                    BinaryOperator::Eq
                                },
                                left_expr: Box::new(ScalarExpression::ColumnRef(
                                    agg.output_schema()[0].clone(),
                                )),
                                right_expr: Box::new(ScalarExpression::Constant(DataValue::Int32(
                                    1,
                                ))),
                                evaluator: None,
                                ty: LogicalType::Boolean,
                            },
                            agg,
                            false,
                        );
                        let projection = ProjectOperator {
                            exprs: vec![ScalarExpression::Constant(DataValue::Int32(1))],
                        };
                        let plan = LogicalPlan::new(
                            Operator::Project(projection),
                            Childrens::Only(filter),
                        );
                        children = LJoinOperator::build(
                            children,
                            plan,
                            JoinCondition::None,
                            JoinType::Cross,
                        );
                        continue;
                    }
                    SubQueryType::InSubQuery(is_not, plan) => {
                        let join_ty = if is_not {
                            JoinType::LeftAnti
                        } else {
                            JoinType::LeftSemi
                        };
                        (plan, join_ty)
                    }
                };

                Self::extract_join_keys(
                    predicate.clone(),
                    &mut on_keys,
                    &mut filter,
                    children.output_schema(),
                    plan.output_schema(),
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

                children = LJoinOperator::build(
                    children,
                    plan,
                    JoinCondition::On {
                        on: on_keys,
                        filter: join_filter,
                    },
                    join_ty,
                );
            }
            return Ok(children);
        }
        Ok(FilterOperator::build(predicate, children, false))
    }

    fn bind_having(
        &mut self,
        children: LogicalPlan,
        having: ScalarExpression,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Having);

        self.validate_having_orderby(&having)?;
        Ok(FilterOperator::build(having, children, true))
    }

    pub(crate) fn bind_project(
        &mut self,
        children: LogicalPlan,
        select_list: Vec<ScalarExpression>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Project);

        Ok(LogicalPlan::new(
            Operator::Project(ProjectOperator { exprs: select_list }),
            Childrens::Only(children),
        ))
    }

    fn bind_sort(&mut self, children: LogicalPlan, sort_fields: Vec<SortField>) -> LogicalPlan {
        self.context.step(QueryBindStep::Sort);

        LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields,
                limit: None,
            }),
            Childrens::Only(children),
        )
    }

    fn bind_limit(
        &mut self,
        children: LogicalPlan,
        limit_expr: &Option<Expr>,
        offset_expr: &Option<Offset>,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::Limit);

        let mut limit = None;
        let mut offset = None;
        if let Some(expr) = limit_expr {
            let expr = self.bind_expr(expr)?;
            match expr {
                ScalarExpression::Constant(dv) => match &dv {
                    DataValue::Int32(v) if *v >= 0 => limit = Some(*v as usize),
                    DataValue::Int64(v) if *v >= 0 => limit = Some(*v as usize),
                    _ => return Err(DatabaseError::InvalidType),
                },
                _ => {
                    return Err(DatabaseError::InvalidColumn(
                        "invalid limit expression.".to_owned(),
                    ))
                }
            }
        }

        if let Some(expr) = offset_expr {
            let expr = self.bind_expr(&expr.value)?;
            match expr {
                ScalarExpression::Constant(dv) => match &dv {
                    DataValue::Int32(v) if *v >= 0 => offset = Some(*v as usize),
                    DataValue::Int64(v) if *v >= 0 => offset = Some(*v as usize),
                    _ => return Err(DatabaseError::InvalidType),
                },
                _ => {
                    return Err(DatabaseError::InvalidColumn(
                        "invalid limit expression.".to_owned(),
                    ))
                }
            }
        }

        Ok(LimitOperator::build(offset, limit, children))
    }

    pub fn extract_select_join(&mut self, select_items: &mut [ScalarExpression]) {
        let bind_tables = &self.context.bind_table;
        if bind_tables.len() < 2 {
            return;
        }

        let mut table_force_nullable = Vec::with_capacity(bind_tables.len());
        let mut left_table_force_nullable = false;
        let mut left_table = None;

        for ((table_name, _, join_option), table) in bind_tables {
            if let Some(join_type) = join_option {
                let (left_force_nullable, right_force_nullable) = joins_nullable(join_type);
                table_force_nullable.push((table_name, table, right_force_nullable));
                left_table_force_nullable = left_force_nullable;
            } else {
                left_table = Some((table_name, table));
            }
        }

        if let Some((table_name, table)) = left_table {
            table_force_nullable.push((table_name, table, left_table_force_nullable));
        }

        for column in select_items {
            if let ScalarExpression::ColumnRef(col) = column {
                let _ = table_force_nullable
                    .iter()
                    .find(|(table_name, source, _)| {
                        let schema_buf = self
                            .table_schema_buf
                            .entry((*table_name).clone())
                            .or_default();

                        source.column(col.name(), schema_buf).is_some()
                    })
                    .map(|(_, _, nullable)| {
                        if let Some(new_column) = col.nullable_for_join(*nullable) {
                            *col = new_column;
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
                fn find_column<'a>(schema: &'a Schema, name: &'a str) -> Option<&'a ColumnRef> {
                    schema.iter().find(|column| column.name() == name)
                }

                let mut on_keys: Vec<(ScalarExpression, ScalarExpression)> = Vec::new();

                for ident in idents {
                    let name = lower_ident(ident);
                    let (Some(left_column), Some(right_column)) = (
                        find_column(left_schema, &name),
                        find_column(right_schema, &name),
                    ) else {
                        return Err(DatabaseError::InvalidColumn("not found column".to_string()));
                    };
                    self.context.add_using(join_type, left_column, right_column);
                    on_keys.push((
                        ScalarExpression::ColumnRef(left_column.clone()),
                        ScalarExpression::ColumnRef(right_column.clone()),
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
                    if let (Some(left_column), Some(right_column)) = (
                        left_schema.iter().find(|column| column.name() == *name),
                        right_schema.iter().find(|column| column.name() == *name),
                    ) {
                        let left_expr = ScalarExpression::ColumnRef(left_column.clone());
                        let right_expr = ScalarExpression::ColumnRef(right_column.clone());

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
                            (ScalarExpression::ColumnRef(l), ScalarExpression::ColumnRef(r)) => {
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
                            (ScalarExpression::ColumnRef(column), _)
                            | (_, ScalarExpression::ColumnRef(column)) => {
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
                                if left_expr.referenced_columns(true).iter().all(|column| {
                                    fn_or_contains(left_schema, right_schema, column.summary())
                                }) && right_expr.referenced_columns(true).iter().all(|column| {
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
                        if left_expr.referenced_columns(true).iter().all(|column| {
                            fn_or_contains(left_schema, right_schema, column.summary())
                        }) && right_expr.referenced_columns(true).iter().all(|column| {
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
                if expr
                    .referenced_columns(true)
                    .iter()
                    .all(|column| fn_or_contains(left_schema, right_schema, column.summary()))
                {
                    // example: baz > 1
                    accum_filter.push(expr);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;

    #[test]
    fn test_select_bind() -> Result<(), DatabaseError> {
        let table_states = build_t1_table()?;

        let plan_1 = table_states.plan("select * from t1")?;
        println!("just_col:\n {:#?}", plan_1);
        let plan_2 = table_states.plan("select t1.c1, t1.c2 from t1")?;
        println!("table_with_col:\n {:#?}", plan_2);
        let plan_3 = table_states.plan("select t1.c1, t1.c2 from t1 where c1 > 2")?;
        println!("table_with_col_and_c1_compare_constant:\n {:#?}", plan_3);
        let plan_4 = table_states.plan("select t1.c1, t1.c2 from t1 where c1 > c2")?;
        println!("table_with_col_and_c1_compare_c2:\n {:#?}", plan_4);
        let plan_5 = table_states.plan("select avg(t1.c1) from t1")?;
        println!("table_with_col_and_c1_avg:\n {:#?}", plan_5);
        let plan_6 = table_states.plan("select t1.c1, t1.c2 from t1 where (t1.c1 - t1.c2) > 1")?;
        println!("table_with_col_nested:\n {:#?}", plan_6);

        let plan_7 = table_states.plan("select * from t1 limit 1")?;
        println!("limit:\n {:#?}", plan_7);

        let plan_8 = table_states.plan("select * from t1 offset 2")?;
        println!("offset:\n {:#?}", plan_8);

        let plan_9 =
            table_states.plan("select c1, c3 from t1 inner join t2 on c1 = c3 and c1 > 1")?;
        println!("join:\n {:#?}", plan_9);

        Ok(())
    }
}
