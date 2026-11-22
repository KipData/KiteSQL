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

use super::select::{
    AliasPurpose, BindPlanAggregated, BindPlanComplete, BindPlanDistinct, BindPlanFiltered,
    BindPlanFrom, BindPlanProjected, BindPlanSelectList, BindPlanStart, JoinConstraintInput,
    TableAliasInput,
};
use super::{is_valid_identifier, Binder, CteBinding, QueryBindStep, SetOperatorKind};
#[cfg(feature = "copy")]
use crate::ast::CopySource;
use crate::ast::{
    attach_span_if_absent, AlterColumnOperation, AlterTable, AlterTableOperation, Assignment,
    AssignmentTarget, ChangeColumnOption, ColumnDef, ColumnOption, Copy, CreateIndex, CreateTable,
    CreateView, Cte, Delete, Expr, Function, FunctionArg, Ident, Insert, Join, JoinConstraint,
    JoinOperator, KeyColumn, Limit, ObjectName, ObjectType, OrderByExpr, Query, Select, SelectItem,
    SetExpr, SetOperator, Statement, TableAlias, TableConstraint, TableFactor, TableWithJoins,
    Update, Value, ValueWithSpan, WindowSpec,
};
#[cfg(feature = "copy")]
use crate::binder::copy::{ExtSource, FileFormat};
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, TableName};
use crate::db::{
    BindSource, ConsumeMut, Database, DatabaseIter, Executor, OwnedPtr, State, Transaction,
    TransactionIter,
};
use crate::dialect::SqlDialect;
use crate::errors::DatabaseError;
use crate::expression;
use crate::expression::agg::AggKind;
use crate::expression::visitor_mut::ExprVisitorMut;
use crate::expression::window::WindowFunctionKind;
use crate::expression::{AliasType, ScalarExpression, TypeCast};
use crate::iter_ext::Itertools;
use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::operator::create_table::UniqueKey;
use crate::planner::operator::join::{JoinCondition, JoinOperator as LJoinOperator, JoinType};
use crate::planner::operator::mark_apply::MarkApplyQuantifier;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::recursive_cte::{RecursiveCteOperator, RecursiveScanOperator};
use crate::planner::operator::sort::SortField;
use crate::planner::operator::{CommandType, Operator};
use crate::planner::MetaArena;
use crate::planner::{Childrens, ExprRef, LogicalPlan, PlanArena};
use crate::storage::{Storage, StorageTransaction};
use crate::types::index::IndexValType;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{CharLengthUnits, ColumnId, LogicalType};
use std::borrow::Cow;
use std::slice;

pub fn command_type(stmt: &Statement) -> Result<CommandType, DatabaseError> {
    Ok(match stmt {
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView(_)
        | Statement::AlterTable(_)
        | Statement::Drop { .. } => CommandType::DDL,
        Statement::Query(_)
        | Statement::Explain(_)
        | Statement::Describe { .. }
        | Statement::ShowTables
        | Statement::ShowViews => CommandType::DQL,
        Statement::Analyze { .. } => CommandType::Analyze,
        Statement::Truncate { .. }
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Insert(_) => CommandType::DML,
        Statement::Copy(copy) if copy.to => CommandType::DQL,
        Statement::Copy(_) => CommandType::DML,
    })
}

impl<S: Storage, D: SqlDialect> State<S, D> {
    pub(crate) fn prepare_all<T: AsRef<str>>(
        &self,
        sql: T,
    ) -> Result<Vec<Statement>, DatabaseError> {
        let stmts = self.dialect().parse(sql.as_ref())?;
        if stmts.is_empty() {
            return Err(DatabaseError::EmptyStatement);
        }
        Ok(stmts)
    }

    pub(crate) fn parse_statement(&self, sql: &str) -> Result<Statement, DatabaseError> {
        self.prepare_all(sql)?
            .pop()
            .ok_or(DatabaseError::EmptyStatement)
    }
}

/// Statements that only read storage can run in a read transaction.
fn statement_is_read_only(statement: &Statement) -> Result<bool, DatabaseError> {
    Ok(match statement {
        // Locking reads (`FOR UPDATE`) are treated as writes.
        Statement::Query(query) => !query.for_update,
        _ => command_type(statement)?.is_read_only(),
    })
}

fn statement_mutates_catalog_or_statistics(statement: &Statement) -> Result<bool, DatabaseError> {
    Ok(matches!(
        command_type(statement)?,
        CommandType::DDL | CommandType::Analyze
    ))
}

#[maybe_async]
impl<S: Storage, D: SqlDialect> Database<S, D> {
    /// Parses SQL statements using this database's configured parser recursion limit.
    pub fn prepare_all<T: AsRef<str>>(&self, sql: T) -> Result<Vec<Statement>, DatabaseError> {
        self.state.prepare_all(sql)
    }

    pub async fn ddl<T: AsRef<str>>(&mut self, sql: T) -> Result<(), DatabaseError> {
        let sql = sql.as_ref();
        let statements = self
            .state
            .prepare_all(sql)
            .map_err(|err| err.with_sql_context(sql))?;

        for statement in &statements {
            if !matches!(command_type(statement)?, CommandType::DDL) {
                return Err(DatabaseError::UnsupportedStmt(
                    "`Database::ddl` only accepts DDL statements".to_string(),
                )
                .with_sql_context(sql));
            }
        }
        self.run_mut_statements(sql, statements, ()).await
    }

    /// Executes SQL with catalog mutations allowed, lending the last statement's results
    /// to `consume`. On success, unread rows are drained before committing and publishing
    /// catalog updates. A callback or execution error rolls the current statement back.
    /// Earlier statements are completed and committed in order before the callback runs.
    pub async fn run_mut<C>(
        &mut self,
        sql: impl AsRef<str>,
        consume: C,
    ) -> Result<C::Output, DatabaseError>
    where
        C: ConsumeMut<S>,
    {
        let sql = sql.as_ref();
        let statements = self
            .state
            .prepare_all(sql)
            .map_err(|err| err.with_sql_context(sql))?;
        self.run_mut_statements(sql, statements, consume).await
    }

    async fn run_mut_statements<C>(
        &mut self,
        sql: &str,
        mut statements: Vec<Statement>,
        consume: C,
    ) -> Result<C::Output, DatabaseError>
    where
        C: ConsumeMut<S>,
    {
        let last = statements
            .pop()
            .ok_or_else(|| DatabaseError::EmptyStatement.with_sql_context(sql))?;
        for statement in statements {
            self.execute_mut(sql, &[], |binder, arena| binder.bind(&statement, arena))
                .await?;
        }
        self.execute_mut_with(sql, &[], |binder, arena| binder.bind(&last, arena), consume)
            .await
    }

    /// Runs one or more SQL statements and returns an iterator for the final result set.
    ///
    /// Earlier statements in the same SQL string are executed eagerly. The last
    /// statement is exposed as a streaming iterator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::{DataBaseBuilder, ResultIter};
    ///
    /// let mut database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.ddl("create table t (id int primary key)").unwrap();
    /// let mut iter = database.run("select * from t").unwrap();
    /// iter.schema(|schema| assert_eq!(schema.len(), 1));
    /// iter.done().unwrap();
    /// ```
    pub async fn run<T: AsRef<str>>(&self, sql: T) -> Result<DatabaseIter<'_, S>, DatabaseError> {
        let sql = sql.as_ref();
        let statements = self
            .state
            .prepare_all(sql)
            .map_err(|err| err.with_sql_context(sql))?;
        let has_catalog_mutation = statements
            .iter()
            .try_fold(false, |has_mutation, stmt| {
                Ok::<_, DatabaseError>(
                    has_mutation || statement_mutates_catalog_or_statistics(stmt)?,
                )
            })
            .map_err(|err| err.with_sql_context(sql))?;

        if has_catalog_mutation {
            return Err(DatabaseError::UnsupportedStmt(
                "DDL and ANALYZE require `Database::ddl` or `Database::analyze`".to_string(),
            )
            .with_sql_context(sql));
        }

        let read_only = statements
            .iter()
            .try_fold(true, |read_only, stmt| {
                Ok::<_, DatabaseError>(read_only && statement_is_read_only(stmt)?)
            })
            .map_err(|err| err.with_sql_context(sql))?;
        let transaction = OwnedPtr::new(
            self.storage
                .transaction_with_isolation(self.transaction_isolation, read_only)?,
        );
        let mut statements = statements.into_iter().peekable();

        while let Some(statement) = statements.next() {
            let (plan_arena, executor) = match async {
                let tx = unsafe { &mut *transaction.get() };
                let (plan, arena) = self
                    .state
                    .build_plan(&[], tx, |binder, arena| binder.bind(&statement, arena))?;
                let executor = Executor::new(plan, &arena);
                self.state.execute(tx, executor, arena).await
            }
            .await
            {
                Ok(result) => result,
                Err(err) => return Err(err.with_sql_context(sql)),
            };

            if statements.peek().is_some() {
                if let Err(err) = TransactionIter::new(plan_arena, executor).done().await {
                    return Err(err.with_sql_context(sql));
                }
            } else {
                let inner = Some(OwnedPtr::new(TransactionIter::new(plan_arena, executor)));
                return Ok(DatabaseIter {
                    transaction: Some(transaction),
                    inner,
                });
            }
        }

        Err(DatabaseError::EmptyStatement.with_sql_context(sql))
    }
}

#[maybe_async]
impl<'txn, S: Storage, D: SqlDialect> Transaction<'txn, S, D> {
    /// Runs SQL inside the current transaction and returns the final result iterator.
    pub async fn run<'a, T: AsRef<str>>(
        &'a mut self,
        sql: T,
    ) -> Result<TransactionIter<'a, 'a, S::TransactionType<'txn>>, DatabaseError> {
        let sql = sql.as_ref();
        let mut statements = self
            .state
            .prepare_all(sql)
            .map_err(|err| err.with_sql_context(sql))?;
        for statement in &statements {
            if statement_mutates_catalog_or_statistics(statement)? {
                return Err(DatabaseError::UnsupportedStmt(
                    "DDL and ANALYZE are not allowed to execute within a transaction".into(),
                )
                .with_sql_context(sql));
            }
        }
        let last_statement = statements
            .pop()
            .ok_or_else(|| DatabaseError::EmptyStatement.with_sql_context(sql))?;

        for statement in statements {
            BindSource::execute(&mut *self, false, |state, tx| {
                state
                    .build_plan(&[], tx, |binder, arena| binder.bind(&statement, arena))
                    .map(Executor::from_plan)
            })
            .await
            .map_err(|err| err.with_sql_context(sql))?
            .done()
            .await
            .map_err(|err| err.with_sql_context(sql))?;
        }

        BindSource::execute(self, false, |state, tx| {
            state
                .build_plan(&[], tx, |binder, arena| binder.bind(&last_statement, arena))
                .map(Executor::from_plan)
        })
        .await
        .map_err(|err| err.with_sql_context(sql))
    }
}

struct BindStatementStart<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
}

struct BindStatementComplete {
    plan: LogicalPlan,
}

struct UpdateExprTargetRemapper<'a> {
    target_schema: &'a [ColumnRef],
}

impl<A: MetaArena + ?Sized> ExprVisitorMut<A> for UpdateExprTargetRemapper<'_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
        arena: &mut A,
    ) -> Result<(), DatabaseError> {
        let Some(target_position) = self
            .target_schema
            .iter()
            .copied()
            .position(|target_column| arena.same_column(target_column, *column))
        else {
            return Err(DatabaseError::UnsupportedStmt(
                "joined UPDATE SET expressions can only reference target table columns".to_string(),
            ));
        };
        *position = target_position;
        Ok(())
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindStatementStart<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    fn statement(self, stmt: &Statement) -> Result<BindStatementComplete, DatabaseError> {
        let plan = match stmt {
            Statement::Query(query) => self.binder.bind_query(query, self.arena)?,
            Statement::AlterTable(alter) => self.alter_table(alter)?,
            Statement::CreateTable(create) => self.create_table(create)?,
            Statement::Drop {
                object_type,
                name,
                if_exists,
                table,
            } => self.drop_object(*object_type, name, *if_exists, table.as_ref())?,
            Statement::Insert(insert) => self.insert(insert)?,
            Statement::Update(update) => self.update(update)?,
            Statement::Delete(delete) => self.delete(delete)?,
            Statement::Analyze { table_name } => self
                .binder
                .bind_analyze(&sql_table_name(table_name)?, self.arena)?,
            Statement::Truncate { table_name } => {
                self.binder.bind_truncate(&sql_table_name(table_name)?)?
            }
            Statement::ShowTables => self.binder.bind_show_tables()?,
            Statement::ShowViews => self.binder.bind_show_views()?,
            Statement::Copy(copy) => self.copy(copy)?,
            Statement::Explain(statement) => self.explain(statement)?,
            Statement::Describe { table_name } => {
                self.binder.bind_describe(&sql_table_name(table_name)?)?
            }
            Statement::CreateIndex(create) => self.create_index(create)?,
            Statement::CreateView(create) => self.create_view(create)?,
        };

        Ok(BindStatementComplete { plan })
    }

    fn alter_table(self, alter: &AlterTable) -> Result<LogicalPlan, DatabaseError> {
        self.alter_table_operation(sql_table_name(&alter.name)?, &alter.operation)
    }

    fn alter_table_operation(
        mut self,
        table_name: TableName,
        operation: &AlterTableOperation,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.binder
            .context
            .table(&table_name)?
            .ok_or(DatabaseError::TableNotFound)?;

        match operation {
            AlterTableOperation::AddColumn {
                if_not_exists,
                column_def,
            } => {
                let column = self.bind_column(column_def, None)?;

                if !is_valid_identifier(column.name()) {
                    return Err(attach_span_if_absent(
                        DatabaseError::invalid_column("illegal column naming".to_string()),
                        column_def.name.span,
                    ));
                }

                self.binder
                    .bind_add_column(&table_name, column, *if_not_exists)
            }
            AlterTableOperation::DropColumn {
                column_name,
                if_exists,
            } => self
                .binder
                .bind_drop_column(&table_name, column_name.value.clone(), *if_exists),
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old_column_name = lower_ident(old_column_name);
                let new_column_name = lower_ident(new_column_name).into_owned();
                let data_type = self.column_type(&table_name, &old_column_name)?;

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }

                self.binder.bind_change_column(
                    &table_name,
                    old_column_name.into_owned(),
                    new_column_name,
                    data_type,
                    DefaultChange::NoChange,
                    NotNullChange::NoChange,
                )
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let old_column_name = lower_ident(column_name);
                let old_data_type = self.column_type(&table_name, &old_column_name)?;

                let (data_type, default_change, not_null_change) = match op {
                    AlterColumnOperation::SetDataType(data_type) => (
                        data_type.clone(),
                        DefaultChange::NoChange,
                        NotNullChange::NoChange,
                    ),
                    AlterColumnOperation::SetDefault(value) => {
                        let default = self.bind_alter_default_expr(value, &old_data_type)?;
                        (
                            old_data_type,
                            DefaultChange::Set(default),
                            NotNullChange::NoChange,
                        )
                    }
                    AlterColumnOperation::DropDefault => {
                        (old_data_type, DefaultChange::Drop, NotNullChange::NoChange)
                    }
                    AlterColumnOperation::SetNotNull => {
                        (old_data_type, DefaultChange::NoChange, NotNullChange::Set)
                    }
                    AlterColumnOperation::DropNotNull => {
                        (old_data_type, DefaultChange::NoChange, NotNullChange::Drop)
                    }
                };

                self.binder.bind_change_column(
                    &table_name,
                    old_column_name.to_string(),
                    old_column_name.into_owned(),
                    data_type,
                    default_change,
                    not_null_change,
                )
            }
            AlterTableOperation::ModifyColumn {
                column_name,
                data_type,
                options,
            } => {
                let old_column_name = lower_ident(column_name).into_owned();
                self.column_type(&table_name, &old_column_name)?;
                let (default_change, not_null_change) =
                    self.bind_change_column_options(options, data_type)?;

                self.binder.bind_change_column(
                    &table_name,
                    old_column_name.clone(),
                    old_column_name,
                    data_type.clone(),
                    default_change,
                    not_null_change,
                )
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
            } => {
                let old_column_name = lower_ident(old_name);
                let new_column_name = lower_ident(new_name).into_owned();
                self.column_type(&table_name, &old_column_name)?;

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }
                let (default_change, not_null_change) =
                    self.bind_change_column_options(options, data_type)?;

                self.binder.bind_change_column(
                    &table_name,
                    old_column_name.into_owned(),
                    new_column_name,
                    data_type.clone(),
                    default_change,
                    not_null_change,
                )
            }
        }
    }

    /// The type of an existing column of `table_name`.
    fn column_type(
        &self,
        table_name: &TableName,
        column_name: &str,
    ) -> Result<LogicalType, DatabaseError> {
        let table = self
            .binder
            .context
            .table(table_name)?
            .ok_or(DatabaseError::TableNotFound)?;
        table
            .get_column_by_name(column_name)
            .map(|column| self.arena.column(column).datatype().clone())
            .ok_or_else(|| DatabaseError::column_not_found(column_name.to_string()))
    }

    fn bind_alter_default_expr(
        &mut self,
        expr: &Expr,
        ty: &LogicalType,
    ) -> Result<ExprRef, DatabaseError> {
        let mut expr = self
            .binder
            .bind_expr(expr, self.arena)
            .map(|expr| self.arena.alloc_expression(expr))?;

        if expr.any_referenced_column(self.arena, |_, _| true)? {
            return Err(DatabaseError::UnsupportedStmt(
                "column is not allowed to exist in default".to_string(),
            ));
        }
        expr = expr.type_cast(Cow::Borrowed(ty), self.arena)?;

        Ok(expr)
    }

    fn bind_change_column_options(
        &mut self,
        options: &[ChangeColumnOption],
        data_type: &LogicalType,
    ) -> Result<(DefaultChange, NotNullChange), DatabaseError> {
        let mut default_change = DefaultChange::NoChange;
        let mut not_null_change = NotNullChange::NoChange;

        for option in options {
            match option {
                ChangeColumnOption::Null => not_null_change = NotNullChange::Drop,
                ChangeColumnOption::NotNull => not_null_change = NotNullChange::Set,
                ChangeColumnOption::Default(expr) => {
                    default_change =
                        DefaultChange::Set(self.bind_alter_default_expr(expr, data_type)?);
                }
            }
        }

        Ok((default_change, not_null_change))
    }

    fn create_table(mut self, create: &CreateTable) -> Result<LogicalPlan, DatabaseError> {
        let table_name = sql_table_name(&create.name)?;

        if !is_valid_identifier(&table_name) {
            return Err(attach_span_if_absent(
                DatabaseError::invalid_table("illegal table naming".to_string()),
                create.name.span(),
            ));
        }
        for col in create.columns.iter() {
            if !is_valid_identifier(&col.name.value) {
                return Err(attach_span_if_absent(
                    DatabaseError::invalid_column("illegal column naming".to_string()),
                    col.span,
                ));
            }
        }

        let mut columns = Vec::with_capacity(create.columns.len());
        for (i, column) in create.columns.iter().enumerate() {
            columns.push(self.bind_column(column, Some(i))?);
        }
        let mut composite_unique_keys = Vec::new();
        for constraint in &create.constraints {
            match constraint {
                TableConstraint::PrimaryKey(parts) => {
                    for (i, part) in parts.iter().enumerate() {
                        if part.prefix_len.is_some() {
                            return Err(DatabaseError::UnsupportedStmt(
                                "PRIMARY KEY index prefixes".into(),
                            ));
                        }
                        let position = Self::constraint_column(&columns, part)?;
                        columns[position].desc_mut().set_primary(Some(i));
                    }
                }
                TableConstraint::Unique(parts) => match parts.as_slice() {
                    [] => return Err(DatabaseError::InvalidIndex),
                    [part] if part.prefix_len.is_none() => {
                        let position = Self::constraint_column(&columns, part)?;
                        columns[position].desc_mut().set_unique();
                    }
                    parts => {
                        let mut column_positions = Vec::with_capacity(parts.len());
                        let mut index_val_ty = Vec::with_capacity(parts.len());
                        for part in parts {
                            let position = Self::constraint_column(&columns, part)?;
                            column_positions.push(position);
                            index_val_ty.push(IndexValType::new(
                                columns[position].datatype().clone(),
                                part.prefix_len,
                            )?);
                        }
                        composite_unique_keys.push(UniqueKey {
                            column_positions,
                            index_val_ty,
                        });
                    }
                },
            }
        }

        self.binder.bind_create_table(
            table_name,
            columns,
            composite_unique_keys,
            create.if_not_exists,
        )
    }

    fn bind_column(
        &mut self,
        column_def: &ColumnDef,
        column_index: Option<usize>,
    ) -> Result<ColumnCatalog, DatabaseError> {
        let column_name = lower_ident(&column_def.name).into_owned();
        let mut column_desc = ColumnDesc::new(column_def.data_type.clone(), None, false, None)?;
        let mut nullable = true;
        let mut is_primary = false;

        for option in &column_def.options {
            match option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::PrimaryKey => {
                    column_desc.set_primary(column_index);
                    is_primary = true;
                }
                ColumnOption::Unique => column_desc.set_unique(),
                ColumnOption::Default(expr) => {
                    let mut expr = self
                        .binder
                        .bind_expr(expr, self.arena)
                        .map(|expr| self.arena.alloc_expression(expr))?;

                    if expr.any_referenced_column(self.arena, |_, _| true)? {
                        return Err(DatabaseError::UnsupportedStmt(
                            "column is not allowed to exist in `default`".to_string(),
                        ));
                    }
                    expr =
                        expr.type_cast(Cow::Borrowed(&column_desc.column_datatype), self.arena)?;
                    column_desc.default = Some(expr);
                }
            }
        }

        Ok(ColumnCatalog::new(
            column_name,
            nullable && !is_primary,
            column_desc,
        ))
    }

    fn constraint_column(
        table_columns: &[ColumnCatalog],
        part: &KeyColumn,
    ) -> Result<usize, DatabaseError> {
        let column_name = lower_ident(&part.name);
        table_columns
            .iter()
            .position(|column| column.name() == column_name.as_ref())
            .ok_or_else(|| {
                attach_span_if_absent(
                    DatabaseError::column_not_found(column_name.into_owned()),
                    part.name.span,
                )
            })
    }

    fn create_index(self, create: &CreateIndex) -> Result<LogicalPlan, DatabaseError> {
        let table_name = sql_table_name(&create.table_name)?;
        let index_name = sql_object_name(&create.name)?;
        let input = self
            .binder
            .bind_create_index_source(&table_name, self.arena)?;
        let table_id = self.binder.context.table_id(&table_name)?;
        let key_parts = create.columns.iter().map(|index_column| {
            let expr = self.binder.bind_expr(&index_column.expr, self.arena)?;
            let ScalarExpression::ColumnRef { column, .. } = expr else {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "'CREATE INDEX' by {expr:?}"
                )));
            };
            let key_ty = IndexValType::new(
                self.arena.column(column).datatype().clone(),
                index_column.prefix_len,
            )?;
            Ok((column, key_ty))
        });

        CreateIndexOperator::build(
            table_id,
            index_name,
            key_parts,
            create.if_not_exists,
            create.unique,
            input,
        )
    }

    fn create_view(self, create: &CreateView) -> Result<LogicalPlan, DatabaseError> {
        let output_aliases = query_output_aliases(&create.query);
        let view_name = sql_table_name(&create.name)?;
        let column_names = create
            .columns
            .iter()
            .map(|column| lower_ident(column).into_owned())
            .collect();
        let plan = self.binder.bind_query(&create.query, self.arena)?;

        self.binder.bind_create_view(
            view_name,
            create.or_replace,
            plan,
            column_names,
            output_aliases,
            self.arena,
        )
    }

    fn drop_object(
        self,
        object_type: ObjectType,
        name: &ObjectName,
        if_exists: bool,
        table: Option<&ObjectName>,
    ) -> Result<LogicalPlan, DatabaseError> {
        match object_type {
            ObjectType::Table => self
                .binder
                .bind_drop_table(sql_table_name(name)?, if_exists),
            ObjectType::View => self.binder.bind_drop_view(sql_table_name(name)?, if_exists),
            ObjectType::Index => {
                let (table_name, index_name) = match table {
                    Some(table) => (sql_table_name(table)?, sql_object_name(name)?),
                    None => sql_index_name(name)?,
                };
                self.binder
                    .bind_drop_index(table_name, index_name, if_exists)
            }
        }
    }

    #[cfg(feature = "copy")]
    fn copy(self, copy: &Copy) -> Result<LogicalPlan, DatabaseError> {
        let ext_source = ExtSource {
            path: copy.path.clone().into(),
            format: FileFormat::Csv {
                delimiter: copy.format.delimiter,
                quote: copy.format.quote,
                escape: copy.format.escape,
                header: copy.format.header,
            },
        };

        match &copy.source {
            CopySource::Table(table_name) => self.binder.bind_copy_table(
                &sql_table_name(table_name)?,
                copy.to,
                ext_source,
                self.arena,
            ),
            CopySource::Query(query) => {
                if !copy.to {
                    return Err(DatabaseError::UnsupportedStmt(
                        "'COPY FROM query'".to_string(),
                    ));
                }
                let input_plan = self.binder.bind_query(query, self.arena)?;
                self.binder.bind_copy_to_file(ext_source, input_plan)
            }
        }
    }

    #[cfg(not(feature = "copy"))]
    fn copy(self, _: &Copy) -> Result<LogicalPlan, DatabaseError> {
        Err(DatabaseError::UnsupportedStmt(
            "COPY requires the `copy` feature".to_string(),
        ))
    }

    fn insert(mut self, insert: &Insert) -> Result<LogicalPlan, DatabaseError> {
        let table_name = sql_table_name(&insert.table_name)?;
        if let SetExpr::Values(rows) = insert.source.body.as_ref() {
            self.insert_values(table_name, &insert.columns, rows, insert.overwrite)
        } else {
            self.insert_query(
                table_name,
                &insert.columns,
                &insert.source,
                insert.overwrite,
            )
        }
    }

    fn insert_values(
        &mut self,
        table_name: TableName,
        idents: &[Ident],
        expr_rows: &[Vec<Expr>],
        is_overwrite: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.binder.context.allow_default = true;
        let source = self
            .binder
            .context
            .source_and_bind(&table_name, None, None, false, self.arena)?
            .ok_or(DatabaseError::TableNotFound)?;
        let values_len = expr_rows[0].len();

        let schema_ref = if idents.is_empty() {
            if values_len > source.schema_len() {
                return Err(DatabaseError::ValuesLenMismatch(
                    source.schema_len(),
                    values_len,
                ));
            }
            source.schema()[..values_len].to_vec()
        } else {
            let mut columns = Vec::with_capacity(idents.len());
            for ident in idents {
                let expr = self.binder.bind_column_ref_from_identifiers(
                    slice::from_ref(ident),
                    Some(table_name.as_ref()),
                    self.arena,
                )?;
                match expr {
                    ScalarExpression::ColumnRef { column, .. } => columns.push(column),
                    _ => return Err(DatabaseError::UnsupportedStmt(ident.to_string())),
                }
            }
            if values_len != columns.len() {
                return Err(DatabaseError::ValuesLenMismatch(columns.len(), values_len));
            }
            columns
        };
        let mut rows = Vec::with_capacity(expr_rows.len() * values_len);
        for expr_row in expr_rows {
            if expr_row.len() != values_len {
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }
            for (i, expr) in expr_row.iter().enumerate() {
                let expression = self.binder.bind_expr(expr, self.arena)?;
                let expression = if matches!(expression, ScalarExpression::Empty) {
                    ScalarExpression::Constant(
                        self.arena
                            .column(schema_ref[i])
                            .default_value(self.arena)?
                            .ok_or(DatabaseError::DefaultNotExist)?,
                    )
                } else {
                    expression
                };
                rows.push(self.arena.alloc_expression(expression));
            }
        }
        self.binder.context.allow_default = false;

        self.binder.bind_insert_values(
            &table_name,
            schema_ref,
            rows,
            expr_rows.len(),
            is_overwrite,
            self.arena,
        )
    }

    fn insert_query(
        &mut self,
        table_name: TableName,
        idents: &[Ident],
        query: &Query,
        is_overwrite: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut input_plan = self.binder.bind_query(query, self.arena)?;
        let input_len = input_plan.output_schema(self.arena).len();

        let source = self
            .binder
            .context
            .source(&table_name)?
            .ok_or(DatabaseError::TableNotFound)?;
        let targets = if idents.is_empty() {
            let table_schema = source.schema();
            if input_len > table_schema.len() {
                return Err(DatabaseError::ValuesLenMismatch(
                    table_schema.len(),
                    input_len,
                ));
            }
            Cow::Borrowed(&table_schema[..input_len])
        } else {
            if input_len != idents.len() {
                return Err(DatabaseError::ValuesLenMismatch(idents.len(), input_len));
            }
            let mut targets = Vec::with_capacity(idents.len());
            for ident in idents {
                let column_name = lower_ident(ident);
                let column = source.column(&column_name, self.arena).ok_or_else(|| {
                    attach_span_if_absent(DatabaseError::column_not_found(column_name), ident.span)
                })?;
                targets.push(column);
            }
            Cow::Owned(targets)
        };

        self.binder
            .bind_insert_query(&table_name, input_plan, &targets, is_overwrite, self.arena)
    }

    fn update(self, update: &Update) -> Result<LogicalPlan, DatabaseError> {
        self.binder.context.allow_default = true;
        let to = &update.table;
        if let TableFactor::Table { name, .. } = &to.relation {
            let is_joined_update = !to.joins.is_empty();
            let table_name = sql_table_name(name)?;
            let table_id = self.binder.context.table_id(&table_name)?;
            self.binder.with_pk(table_id);

            let mut plan = self.binder.bind_table_ref_sql(to, self.arena)?;
            let (target_source, target_offset) =
                Binder::<'a, 'b, T, A>::resolve_source_columns_in_scope(
                    &self.binder.context,
                    &table_name,
                    self.arena,
                )?;
            let target_schema = target_source.schema().to_vec();

            if let Some(predicate) = update.selection.as_ref() {
                plan = self.binder.bind_where(plan, predicate, self.arena)?;
            }
            let mut value_exprs = Vec::with_capacity(update.assignments.len());

            if update.assignments.is_empty() {
                return Err(DatabaseError::ColumnsEmpty);
            }
            for Assignment { target, value } in &update.assignments {
                let expression = self.binder.bind_expr(value, self.arena)?;
                let mut bind_assignment = |binder: &mut Binder<'_, '_, T, A>,
                                           arena: &mut PlanArena,
                                           name: &ObjectName,
                                           expression: ScalarExpression|
                 -> Result<(), DatabaseError> {
                    let ident = single_ident_from_object_name(name)?;

                    let column = {
                        let expr = binder.bind_column_ref_from_identifiers(
                            slice::from_ref(ident),
                            Some(table_name.as_ref()),
                            arena,
                        )?;
                        match expr {
                            ScalarExpression::ColumnRef { column, .. } => column,
                            _ => {
                                return Err(attach_span_if_absent(
                                    DatabaseError::invalid_column(ident.to_string()),
                                    ident.span,
                                ))
                            }
                        }
                    };

                    let mut expr = match expression {
                        ScalarExpression::Empty => {
                            let column_catalog = arena.column(column);
                            let default_value = column_catalog
                                .default_value(arena)?
                                .ok_or(DatabaseError::DefaultNotExist)?;
                            arena.alloc_expression(ScalarExpression::Constant(default_value))
                        }
                        expression => arena.alloc_expression(expression),
                    };
                    let datatype = arena.column(column).datatype().clone();
                    expr = expr.type_cast(Cow::Owned(datatype), arena)?;
                    if is_joined_update {
                        UpdateExprTargetRemapper {
                            target_schema: &target_schema,
                        }
                        .visit(&mut expr, arena)?;
                    }
                    value_exprs.push((column, expr));
                    Ok(())
                };

                match target {
                    AssignmentTarget::ColumnName(name) => {
                        bind_assignment(self.binder, self.arena, name, expression)?
                    }
                    AssignmentTarget::Tuple(names) => {
                        let expected = names.len();
                        let ScalarExpression::Tuple(exprs) = expression else {
                            return Err(DatabaseError::ValuesLenMismatch(expected, 1));
                        };
                        let got = exprs.len();
                        let mut names = names.iter();
                        let mut exprs = exprs.into_iter();

                        loop {
                            match (names.next(), exprs.next()) {
                                (Some(name), Some(expression)) => {
                                    let expression = std::mem::replace(
                                        &mut *self.arena.expression_mut(expression),
                                        ScalarExpression::Empty,
                                    );
                                    bind_assignment(self.binder, self.arena, name, expression)?
                                }
                                (None, None) => break,
                                _ => return Err(DatabaseError::ValuesLenMismatch(expected, got)),
                            }
                        }
                    }
                }
            }
            self.binder.context.allow_default = false;
            if is_joined_update {
                let exprs = target_schema
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(index, column)| {
                        self.arena.alloc_expression(ScalarExpression::column_expr(
                            column,
                            target_offset + index,
                        ))
                    });
                plan = LogicalPlan::new(
                    Operator::Project(ProjectOperator::new(exprs)),
                    Childrens::Only(Box::new(plan)),
                );
            }
            let steps = [
                QueryBindStep::From,
                QueryBindStep::Where,
                QueryBindStep::Project,
            ];
            plan = self.binder.bind_scalar_queries(plan, &steps, self.arena)?;
            self.binder
                .bind_update(&table_name, value_exprs, plan, self.arena)
        } else {
            Err(DatabaseError::UnsupportedStmt(
                "UPDATE target must be a table".to_string(),
            ))
        }
    }

    fn delete(self, delete: &Delete) -> Result<LogicalPlan, DatabaseError> {
        let table = &delete.table;
        let TableFactor::Table { name, .. } = &table.relation else {
            return Err(DatabaseError::UnsupportedStmt(
                "DELETE target must be a table".to_string(),
            ));
        };
        let table_name = sql_table_name(name)?;
        let target = self
            .binder
            .context
            .table(&table_name)?
            .ok_or(DatabaseError::TableNotFound)?;
        let primary_keys = target
            .primary_keys()
            .iter()
            .map(|(_, column)| *column)
            .collect();
        self.binder.with_pk(target.id());
        let mut plan = self.binder.bind_table_ref_sql(table, self.arena)?;

        if let Some(predicate) = delete.selection.as_ref() {
            plan = self.binder.bind_where(plan, predicate, self.arena)?;
        }

        self.binder.bind_delete(&table_name, primary_keys, plan)
    }

    fn explain(self, statement: &Statement) -> Result<LogicalPlan, DatabaseError> {
        let BindStatementStart { binder, arena } = self;
        let plan = binder.bind(statement, arena)?;
        binder.bind_explain(plan)
    }
}

impl BindStatementComplete {
    fn finish(self) -> LogicalPlan {
        self.plan
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanStart<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn from_sql(
        self,
        from: &[TableWithJoins],
    ) -> Result<BindPlanFrom<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        let mut froms = from.iter();
        let mut plan = if let Some(from) = froms.next() {
            let mut plan = self.binder.bind_table_ref_sql(from, self.arena)?;

            for from in froms {
                plan = LJoinOperator::build(
                    plan,
                    self.binder.bind_table_ref_sql(from, self.arena)?,
                    JoinCondition::None,
                    JoinType::Cross,
                    self.binder.force_nested_loop,
                )
            }
            plan
        } else {
            LogicalPlan::new(Operator::Dummy, Childrens::None)
        };
        plan.output_schema(self.arena);

        self.from_plan(plan)
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanFrom<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn select_list_from_sql(
        self,
        projection: &[SelectItem],
    ) -> Result<BindPlanSelectList<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        let select_list = with_query_bind_step!(self.binder, QueryBindStep::Project, {
            self.binder.normalize_select_item(projection, self.arena)?
        });

        Ok(self.select_list(select_list?))
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanSelectList<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn where_sql(
        self,
        selection: Option<&Expr>,
    ) -> Result<BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        let predicate = if let Some(predicate) = selection {
            Some(with_query_bind_step!(self.binder, QueryBindStep::Where, {
                let predicate = self.binder.bind_expr(predicate, self.arena)?;
                self.arena.alloc_expression(predicate)
            })?)
        } else {
            None
        };

        self.filter_expr(predicate)
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn aggregate_sql(
        self,
        group_by: &[Expr],
        having: Option<&Expr>,
        orderby: Option<&[OrderByExpr]>,
    ) -> Result<BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        let group_by = with_query_bind_step!(self.binder, QueryBindStep::Agg, {
            group_by
                .iter()
                .map(|expr| {
                    self.binder
                        .bind_expr(expr, self.arena)
                        .map(|expr| self.arena.alloc_expression(expr))
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?
        })?;
        let having = having
            .map(|having| {
                with_query_bind_step!(self.binder, QueryBindStep::Having, {
                    let having = self.binder.bind_expr(having, self.arena)?;
                    self.arena.alloc_expression(having)
                })
            })
            .transpose()?;
        self.aggregate(
            group_by,
            having,
            orderby,
            |binder, arena, select_list, orderby| {
                let OrderByExpr {
                    expr,
                    asc,
                    nulls_first,
                } = orderby;
                with_query_bind_step!(binder, QueryBindStep::Sort, {
                    let expr = match expr {
                        Expr::Value(ValueWithSpan {
                            value: Value::Number(n),
                            ..
                        }) => {
                            let position = n.parse::<usize>().map_err(|_| {
                                DatabaseError::InvalidValue(format!(
                                    "non-integer constant in ORDER BY: {n}"
                                ))
                            })?;
                            let item = position
                                .checked_sub(1)
                                .and_then(|i| select_list.get(i))
                                .ok_or_else(|| {
                                    DatabaseError::InvalidValue(format!(
                                        "ORDER BY position {n} is not in select list"
                                    ))
                                })?;
                            item.clone_expression(arena)?
                        }
                        expr => {
                            if let Expr::Identifier(ident) = expr {
                                let name = lower_ident(ident);
                                let mut aliased = select_list.iter().filter_map(|item| match arena
                                    .expression(*item)
                                {
                                    ScalarExpression::Alias {
                                        expr,
                                        alias: AliasType::Name(alias),
                                    } if alias.as_str() == name => Some(*expr),
                                    _ => None,
                                });
                                if let Some(first) = aliased.next() {
                                    if aliased
                                        .any(|other| !first.eq_ignore_colref_pos(other, arena))
                                    {
                                        return Err(DatabaseError::AmbiguousColumn(
                                            name.into_owned(),
                                        ));
                                    }
                                }
                            }
                            let expr = binder.bind_expr(expr, arena)?;
                            arena.alloc_expression(expr)
                        }
                    };
                    SortField::new(
                        expr,
                        asc.is_none_or(|asc| asc),
                        nulls_first.unwrap_or(false),
                    )
                })
            },
        )
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> super::select::BindPlanWindowed<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn distinct_sql(
        self,
        distinct: bool,
    ) -> Result<BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        self.distinct(distinct)
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanProjected<'s, 'a, 'b, 'arena, T, A>
where
    T: StorageTransaction,
    A: AsRef<[(usize, LogicalType)]>,
{
    pub(crate) fn select_into_sql(
        self,
        into: Option<&ObjectName>,
    ) -> Result<BindPlanComplete, DatabaseError> {
        let table_name = into.map(lower_case_name).transpose()?;
        self.insert_into(table_name.as_deref())
    }
}

fn sql_table_name(name: &ObjectName) -> Result<TableName, DatabaseError> {
    Ok(lower_case_name(name)?.into())
}

fn sql_table_alias(alias: &TableAlias) -> TableAliasInput {
    TableAliasInput {
        name: lower_ident(&alias.name).into(),
        columns: alias
            .columns
            .iter()
            .map(|column| lower_ident(column).into_owned())
            .collect(),
    }
}

fn sql_optional_table_alias(alias: Option<&TableAlias>) -> Option<TableAliasInput> {
    alias.map(sql_table_alias)
}

impl TryFrom<&Value> for DataValue {
    type Error = DatabaseError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Ok(match value {
            Value::Number(n) => {
                // use i32 to handle most cases
                if let Ok(v) = n.parse::<i32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<i64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f32>() {
                    v.into()
                } else {
                    return Err(DatabaseError::InvalidValue(n.to_string()));
                }
            }
            Value::String(s) => s.clone().into(),
            Value::HexString(hex) => {
                if hex.len() % 2 != 0 {
                    return Err(DatabaseError::InvalidValue(
                        "hex literal must contain pairs of digits".to_string(),
                    ));
                }
                let mut bytes = Vec::with_capacity(hex.len() / 2);
                for pair in hex.as_bytes().as_chunks::<2>().0 {
                    let Some(high) = (pair[0] as char).to_digit(16) else {
                        return Err(DatabaseError::InvalidValue("invalid hex digit".to_string()));
                    };
                    let Some(low) = (pair[1] as char).to_digit(16) else {
                        return Err(DatabaseError::InvalidValue("invalid hex digit".to_string()));
                    };
                    bytes.push((high * 16 + low) as u8);
                }
                bytes.into()
            }
            Value::Boolean(b) => (*b).into(),
            Value::Null => Self::Null,
            Value::Placeholder(placeholder) => {
                return Err(DatabaseError::InvalidValue(format!(
                    "unbound parameter: {placeholder}"
                )))
            }
        })
    }
}

fn sql_object_name(name: &ObjectName) -> Result<String, DatabaseError> {
    Ok(lower_case_name(name)?.into_owned())
}

pub(super) fn lower_ident(ident: &Ident) -> Cow<'_, str> {
    let value = &ident.value;

    if value.chars().any(char::is_uppercase) {
        Cow::Owned(value.to_lowercase())
    } else {
        Cow::Borrowed(value)
    }
}

/// Convert an object name into lower case.
pub(super) fn lower_case_name(name: &ObjectName) -> Result<Cow<'_, str>, DatabaseError> {
    match name.0.as_slice() {
        [ident] => Ok(lower_ident(ident)),
        _ => Err(attach_span_if_absent(
            DatabaseError::invalid_table(name.to_string()),
            name.span(),
        )),
    }
}

fn single_ident_from_object_name(name: &ObjectName) -> Result<&Ident, DatabaseError> {
    match name.0.as_slice() {
        [ident] => Ok(ident),
        _ => Err(attach_span_if_absent(
            DatabaseError::invalid_column(name.to_string()),
            name.span(),
        )),
    }
}

fn sql_index_name(name: &ObjectName) -> Result<(TableName, String), DatabaseError> {
    let [table_name, index_name] = name.0.as_slice() else {
        return Err(DatabaseError::InvalidIndex);
    };

    Ok((
        lower_ident(table_name).into(),
        lower_ident(index_name).into_owned(),
    ))
}

fn query_output_aliases(query: &Query) -> Vec<Option<String>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };

    select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(lower_ident(alias).into_owned()),
            _ => None,
        })
        .collect()
}

impl<'a, 'parent, T: StorageTransaction, A: AsRef<[(usize, LogicalType)]>>
    Binder<'a, 'parent, T, A>
{
    fn bind_table_ref_sql(
        &mut self,
        from: &TableWithJoins,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.context.step(QueryBindStep::From);

        let TableWithJoins { relation, joins } = from;
        let mut plan = self.bind_single_table_ref_sql(relation, None, arena)?;

        for join in joins {
            plan = self.bind_join_sql(plan, join, arena)?;
        }
        Ok(plan)
    }

    fn bind_single_table_ref_sql(
        &mut self,
        table: &TableFactor,
        joint_type: Option<JoinType>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        match table {
            TableFactor::Table { name, alias } => self.bind_base_table_ref(
                joint_type,
                sql_table_name(name)?,
                sql_optional_table_alias(alias.as_ref()),
                arena,
            ),
            TableFactor::Derived { subquery, alias } => {
                // A non-LATERAL derived table cannot see the sources bound before
                // it in the same FROM clause.
                let mut binder = Binder::new(self.context.fork_empty(), self.args, None);
                let plan = binder.bind_query(subquery, arena)?;
                self.bind_derived_source(
                    plan,
                    sql_optional_table_alias(alias.as_ref()),
                    joint_type,
                    arena,
                )
            }
            TableFactor::TableFunction { expr, alias } => {
                self.table_function_source = matches!(expr, Expr::Function(_));
                let expr = self.bind_expr(expr, arena);
                self.table_function_source = false;
                let expr = expr?;
                self.bind_table_function_source(
                    expr,
                    sql_optional_table_alias(alias.as_ref()),
                    joint_type,
                    arena,
                )
            }
        }
    }

    fn bind_join_sql(
        &mut self,
        left: LogicalPlan,
        join: &Join,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let join_type = match join.join_operator {
            JoinOperator::Inner => JoinType::Inner,
            JoinOperator::LeftOuter => JoinType::LeftOuter,
            JoinOperator::RightOuter => JoinType::RightOuter,
            JoinOperator::FullOuter => JoinType::Full,
            JoinOperator::Cross => JoinType::Cross,
        };
        let (right, context) = {
            let mut binder = Binder::new(self.context.fork_empty(), self.args, Some(&self.context));
            let right = binder.bind_single_table_ref_sql(&join.relation, Some(join_type), arena)?;
            (right, binder.context)
        };
        self.extend(context, arena)?;

        let constraint = self.bind_join_constraint_sql(&join.constraint, arena)?;

        self.bind_join_plans(left, right, join_type, constraint, arena)
    }

    fn bind_join_constraint_sql(
        &mut self,
        constraint: &JoinConstraint,
        arena: &mut PlanArena,
    ) -> Result<JoinConstraintInput, DatabaseError> {
        match constraint {
            JoinConstraint::On(expr) => {
                let expr = self.bind_expr(expr, arena)?;
                Ok(JoinConstraintInput::On(arena.alloc_expression(expr)))
            }
            JoinConstraint::Using(names) => Ok(JoinConstraintInput::Using(
                names
                    .iter()
                    .map(|name| lower_case_name(name).map(Cow::into_owned))
                    .collect::<Result<_, _>>()?,
            )),
            JoinConstraint::Natural => Ok(JoinConstraintInput::Natural),
            JoinConstraint::None => Ok(JoinConstraintInput::None),
        }
    }

    pub(crate) fn bind_expr(
        &mut self,
        expr: &Expr,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident), None, arena)
            }
            Expr::CompoundIdentifier(idents) => {
                self.bind_column_ref_from_identifiers(idents, None, arena)
            }
            Expr::BinaryOp { left, right, op } => {
                let left_expr = self
                    .bind_expr(left, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                let right_expr = self
                    .bind_expr(right, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                self.bind_binary_op_expr(left_expr, right_expr, *op, arena)
            }
            Expr::Value(v) => {
                let value = if let Value::Placeholder(placeholder) = &v.value {
                    let id = DataValue::parameter_id(placeholder).ok_or_else(|| {
                        attach_span_if_absent(
                            DatabaseError::InvalidValue(format!(
                                "parameter must use the $<positive integer> form: {placeholder}"
                            )),
                            v.span,
                        )
                    })?;
                    let ty = self
                        .args
                        .as_ref()
                        .iter()
                        .find_map(|(candidate, ty)| (candidate == &id).then_some(ty))
                        .ok_or_else(|| {
                            attach_span_if_absent(
                                DatabaseError::parameter_not_found(format!("${id}")),
                                v.span,
                            )
                        })?;
                    DataValue::Parameter { id, ty: ty.clone() }
                } else {
                    (&v.value)
                        .try_into()
                        .map_err(|err| attach_span_if_absent(err, v.span))?
                };
                Ok(ScalarExpression::Constant(value))
            }
            Expr::Function(func) => self.bind_function_sql(func, arena),
            Expr::Nested(expr) => self.bind_expr(expr, arena),
            Expr::UnaryOp { expr, op } => {
                // Fold `-<number>` into one literal, as PostgreSQL does: parsing
                // the number first cannot express a type's minimum, e.g.
                // `-9223372036854775808` (9223372036854775808 overflows bigint).
                if let (expression::UnaryOperator::Minus, Expr::Value(v)) = (op, expr.as_ref()) {
                    if let Value::Number(n) = &v.value {
                        let value = DataValue::try_from(&Value::Number(format!("-{n}")))?;
                        return Ok(ScalarExpression::Constant(value));
                    }
                }
                let expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                self.bind_unary_op_expr(expr, *op, arena)
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                let left_expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                let right_expr = self
                    .bind_expr(pattern, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                let op = if *negated {
                    expression::BinaryOperator::NotLike(*escape_char)
                } else {
                    expression::BinaryOperator::Like(*escape_char)
                };
                Ok(ScalarExpression::Binary {
                    op,
                    left_expr,
                    right_expr,
                    evaluator: None,
                    ty: LogicalType::Boolean,
                })
            }
            Expr::IsNull(expr) => {
                let expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                Ok(ScalarExpression::IsNull {
                    negated: false,
                    expr,
                })
            }
            Expr::IsNotNull(expr) => {
                let expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                Ok(ScalarExpression::IsNull {
                    negated: true,
                    expr,
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut args: Vec<ExprRef> = list
                    .iter()
                    .map(|expr| {
                        self.bind_expr(expr, arena)
                            .map(|expr| arena.alloc_expression(expr))
                    })
                    .try_collect()?;
                let mut expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                self.coerce_comparison_operands(&mut expr, &mut args, arena)?;
                Ok(ScalarExpression::In {
                    negated: *negated,
                    evaluator: None,
                    expr,
                    args,
                })
            }
            Expr::Cast { expr, data_type } => {
                let expr = self.bind_expr(expr, arena)?;
                self.bind_cast(expr, data_type.clone(), arena)
            }
            Expr::TypedString {
                data_type,
                value,
                span,
            } => {
                let value = DataValue::Utf8 {
                    value: value.as_str().into(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
                .cast(data_type)
                .map_err(|err| attach_span_if_absent(err, *span))?;

                Ok(ScalarExpression::Constant(value))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let mut expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                let mut bounds = [
                    self.bind_expr(low, arena)
                        .map(|expr| arena.alloc_expression(expr))?,
                    self.bind_expr(high, arena)
                        .map(|expr| arena.alloc_expression(expr))?,
                ];
                self.coerce_comparison_operands(&mut expr, &mut bounds, arena)?;
                let [left_expr, right_expr] = bounds;
                Ok(ScalarExpression::Between {
                    negated: *negated,
                    left_evaluator: None,
                    right_evaluator: None,
                    expr,
                    left_expr,
                    right_expr,
                })
            }
            Expr::Substring {
                expr,
                from: substring_from,
                r#for: substring_for,
            } => {
                let mut for_expr = None;
                let mut from_expr = None;

                if let Some(expr) = substring_for {
                    let expr = self.bind_expr(expr, arena)?;
                    for_expr = Some(arena.alloc_expression(expr))
                }
                if let Some(expr) = substring_from {
                    let expr = self.bind_expr(expr, arena)?;
                    from_expr = Some(arena.alloc_expression(expr))
                }

                let expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                Ok(ScalarExpression::SubString {
                    expr,
                    for_expr,
                    from_expr,
                })
            }
            Expr::Position { expr, r#in } => {
                let expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                let in_expr = self
                    .bind_expr(r#in, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                Ok(ScalarExpression::Position { expr, in_expr })
            }
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
            } => {
                let mut trim_what_expr = None;
                if let Some(trim_what) = trim_what {
                    let trim_what = self.bind_expr(trim_what, arena)?;
                    trim_what_expr = Some(arena.alloc_expression(trim_what))
                }
                let expr = self
                    .bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))?;
                Ok(ScalarExpression::Trim {
                    expr,
                    trim_what_expr,
                    trim_where: *trim_where,
                })
            }
            Expr::Exists { subquery, negated } => {
                self.bind_exists_subquery_plan(*negated, arena, |binder, arena| {
                    binder.bind_query(subquery, arena)
                })
            }
            Expr::Subquery(subquery) => self.bind_scalar_subquery_plan(arena, |binder, arena| {
                binder.bind_query(subquery, arena)
            }),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => self.bind_quantified_subquery(
                MarkApplyQuantifier::Any,
                *negated,
                expr,
                expression::BinaryOperator::Eq,
                subquery,
                arena,
            ),
            Expr::Tuple(exprs) => {
                let mut bound_exprs = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    let expr = self.bind_expr(expr, arena)?;
                    bound_exprs.push(arena.alloc_expression(expr));
                }
                Ok(ScalarExpression::Tuple(bound_exprs))
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
            } => {
                // Branches of different types are unified like `l = r` (e.g.
                // `then bigint else int` => bigint), as in PostgreSQL.
                let fn_check_ty = |ty: &mut LogicalType, result_ty: LogicalType| {
                    if result_ty != LogicalType::SqlNull {
                        if ty == &LogicalType::SqlNull {
                            *ty = result_ty;
                        } else if ty != &result_ty {
                            *ty = LogicalType::max_logical_type(ty, &result_ty)?.into_owned();
                        }
                    }

                    Ok::<(), DatabaseError>(())
                };
                let mut operand_expr = None;
                let mut ty = LogicalType::SqlNull;
                if let Some(expr) = operand {
                    let expr = self.bind_expr(expr, arena)?;
                    operand_expr = Some(arena.alloc_expression(expr));
                }
                let mut expr_pairs = Vec::with_capacity(conditions.len());
                for (condition, result) in conditions {
                    let result = self
                        .bind_expr(result, arena)
                        .map(|expr| arena.alloc_expression(expr))?;
                    let result_ty = result.return_type(arena).into_owned();

                    fn_check_ty(&mut ty, result_ty)?;
                    let condition = self
                        .bind_expr(condition, arena)
                        .map(|expr| arena.alloc_expression(expr))?;
                    expr_pairs.push((condition, result))
                }

                let mut else_expr = None;
                if let Some(expr) = else_result {
                    let temp_expr = self
                        .bind_expr(expr, arena)
                        .map(|expr| arena.alloc_expression(expr))?;
                    let else_ty = temp_expr.return_type(arena).into_owned();

                    fn_check_ty(&mut ty, else_ty)?;
                    else_expr = Some(temp_expr);
                }
                if ty != LogicalType::SqlNull {
                    for (_, result) in expr_pairs.iter_mut() {
                        *result = result.type_cast(Cow::Borrowed(&ty), arena)?;
                    }
                    if let Some(expr) = else_expr.as_mut() {
                        *expr = expr.type_cast(Cow::Borrowed(&ty), arena)?;
                    }
                }

                Ok(ScalarExpression::CaseWhen {
                    operand_expr,
                    expr_pairs,
                    else_expr,
                    ty,
                })
            }
            Expr::AnyOp {
                left,
                compare_op,
                subquery,
            } => self.bind_quantified_subquery(
                MarkApplyQuantifier::Any,
                false,
                left,
                *compare_op,
                subquery,
                arena,
            ),
            Expr::AllOp {
                left,
                compare_op,
                subquery,
            } => self.bind_quantified_subquery(
                MarkApplyQuantifier::All,
                false,
                left,
                *compare_op,
                subquery,
                arena,
            ),
        }
    }

    fn bind_quantified_subquery(
        &mut self,
        quantifier: MarkApplyQuantifier,
        negated: bool,
        expr: &Expr,
        compare_op: expression::BinaryOperator,
        subquery: &Query,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = self
            .bind_expr(expr, arena)
            .map(|expr| arena.alloc_expression(expr))?;
        self.bind_quantified_subquery_plan(
            quantifier,
            negated,
            left_expr,
            compare_op,
            arena,
            |binder, arena| binder.bind_query(subquery, arena),
        )
    }

    pub fn bind_column_ref_from_identifiers(
        &mut self,
        idents: &[Ident],
        bind_table_name: Option<&str>,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let full_name = match idents {
            [column] => (None, lower_ident(column)),
            [table, column] => (Some(lower_ident(table)), lower_ident(column)),
            _ => {
                let invalid_name = idents
                    .iter()
                    .map(|ident| ident.value.clone())
                    .join(".")
                    .to_string();
                let err = DatabaseError::invalid_column(invalid_name);
                return Err(match idents.last() {
                    Some(ident) => attach_span_if_absent(err, ident.span),
                    None => err,
                });
            }
        };
        self.bind_column_ref_by_name(
            full_name.0.as_deref(),
            full_name.1.as_ref(),
            bind_table_name,
            arena,
        )
        .map_err(|err| match idents.last() {
            Some(ident) => attach_span_if_absent(err, ident.span),
            None => err,
        })
    }

    pub(crate) fn bind_function_sql(
        &mut self,
        func: &Function,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        // Only this call may be a FROM-clause table function, not the calls in its arguments.
        let table_function_source = std::mem::take(&mut self.table_function_source);
        let func_span = func.span;
        let mut args = Vec::with_capacity(func.args.len());

        for arg in &func.args {
            match arg {
                FunctionArg::Expr(expr) => {
                    let expr = self.bind_expr(expr, arena)?;
                    args.push(arena.alloc_expression(expr))
                }
                FunctionArg::Wildcard => {
                    if func.distinct {
                        return Err(attach_span_if_absent(
                            DatabaseError::UnsupportedStmt(
                                "DISTINCT cannot be used with *".to_string(),
                            ),
                            func_span,
                        ));
                    }
                    args.push(arena.alloc_expression(Self::wildcard_expr()))
                }
            }
        }
        let function_name = func.name.to_string().to_lowercase();

        if let Some(over) = &func.over {
            let Some(kind) = WindowFunctionKind::from_name(&function_name) else {
                return Err(attach_span_if_absent(
                    DatabaseError::UnsupportedStmt(format!(
                        "window function `{function_name}` is not supported"
                    )),
                    func_span,
                ));
            };
            return self
                .bind_window_call(kind, args, func.distinct, over, arena)
                .map_err(|err| attach_span_if_absent(err, func_span));
        }

        let result = if let Some(kind) = AggKind::from_name(&function_name) {
            self.bind_aggregate_function(kind, args, func.distinct, arena)
        } else {
            self.table_function_source = table_function_source;
            self.bind_function_call(function_name, args, arena)
        };
        result.map_err(|err| attach_span_if_absent(err, func_span))
    }

    fn bind_window_call(
        &mut self,
        kind: WindowFunctionKind,
        args: Vec<ExprRef>,
        is_distinct: bool,
        spec: &WindowSpec,
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
        if is_distinct {
            return Err(DatabaseError::UnsupportedStmt(
                "DISTINCT window aggregates are not supported".to_string(),
            ));
        }

        let partition_by = spec
            .partition_by
            .iter()
            .map(|expr| {
                self.bind_expr(expr, arena)
                    .map(|expr| arena.alloc_expression(expr))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = spec
            .order_by
            .iter()
            .map(|order_by| {
                let expr = self.bind_expr(&order_by.expr, arena)?;
                Ok(SortField::new(
                    arena.alloc_expression(expr),
                    order_by.asc.unwrap_or(true),
                    order_by.nulls_first.unwrap_or(false),
                ))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        self.bind_window_function(kind, args, partition_by, order_by, arena)
    }

    pub fn bind_set_expr(
        &mut self,
        set_expr: &SetExpr,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        match set_expr {
            SetExpr::Select(select) => self.bind_select(select, None, arena),
            SetExpr::Query(query) => self.bind_query(query, arena),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => self.bind_set_operation(*op, *all, left, right, arena),
            SetExpr::Values(_) => Err(DatabaseError::UnsupportedStmt(
                "VALUES is not supported in a set operation".to_string(),
            )),
        }
    }

    fn bind_set_operation(
        &mut self,
        op: SetOperator,
        is_all: bool,
        left: &SetExpr,
        right: &SetExpr,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let op = match op {
            SetOperator::Union => SetOperatorKind::Union,
            SetOperator::Except => SetOperatorKind::Except,
            SetOperator::Intersect => SetOperatorKind::Intersect,
        };
        let left_plan = {
            let mut left_binder = Binder::new(self.context.fork(), self.args, self.parent);
            let plan = left_binder.bind_set_expr(left, arena)?;
            if left_binder.context.has_join_outer_ref() {
                self.context.mark_join_outer_ref();
            }
            plan
        };

        let right_plan = {
            let mut right_binder = Binder::new(self.context.fork(), self.args, self.parent);
            let plan = right_binder.bind_set_expr(right, arena)?;
            if right_binder.context.has_join_outer_ref() {
                self.context.mark_join_outer_ref();
            }
            plan
        };

        self.bind_set_operation_plans(op, is_all, left_plan, right_plan, arena)
    }

    pub(crate) fn bind_select(
        &mut self,
        select: &Select,
        orderby: Option<&[OrderByExpr]>,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let Select {
            optimizer_hint,
            distinct,
            projection,
            into,
            from,
            selection,
            group_by,
            having,
        } = select;
        let has_hint = |expected: &str| {
            optimizer_hint.as_ref().is_some_and(|hint| {
                hint.split(|char: char| char.is_ascii_whitespace() || char == ',')
                    .any(|hint| hint.eq_ignore_ascii_case(expected))
            })
        };
        let force_spill = has_hint("FORCE_AGG_SPILL");
        let force_nested_loop = has_hint("FORCE_NEST_LOOP_JOIN");
        if force_spill && !cfg!(feature = "spill") {
            return Err(DatabaseError::UnsupportedStmt(
                "FORCE_AGG_SPILL requires the `spill` feature".to_string(),
            ));
        }
        let previous_options = (self.force_spill, self.force_nested_loop);
        self.force_spill = force_spill;
        self.force_nested_loop = force_nested_loop;
        let result = (|| {
            Ok(self
                .build_plan(arena)
                .from_sql(from)?
                .select_list_from_sql(projection)?
                .where_sql(selection.as_ref())?
                .aggregate_sql(group_by, having.as_ref(), orderby)?
                .having()?
                .window()?
                .distinct_sql(*distinct)?
                .order_by()?
                .project()?
                .select_into_sql(into.as_ref())?
                .finish())
        })();
        (self.force_spill, self.force_nested_loop) = previous_options;
        result
    }

    /// FIXME: temp values need to register BindContext.bind_table
    fn bind_temp_values(
        &mut self,
        expr_rows: &[Vec<Expr>],
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let values_len = expr_rows[0].len();

        let mut inferred_types: Vec<Option<LogicalType>> = vec![None; values_len];
        let mut rows = Vec::with_capacity(expr_rows.len() * values_len);

        for expr_row in expr_rows {
            if expr_row.len() != values_len {
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }

            for (col_index, expr) in expr_row.iter().enumerate() {
                let expression = self.bind_expr(expr, arena)?;
                let value_type = expression.return_type(arena).into_owned();
                inferred_types[col_index] = match &inferred_types[col_index] {
                    Some(existing) => {
                        Some(LogicalType::max_logical_type(existing, &value_type)?.into_owned())
                    }
                    None => Some(value_type),
                };
                rows.push(arena.alloc_expression(expression));
            }
        }

        let value_source = arena.alloc_source();
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
                column_ref.set_source(value_source, ColumnId::default(), true);
                Ok(arena.alloc_column(column_ref))
            })
            .collect::<Result<_, DatabaseError>>()?;

        Ok(self.bind_values(rows, expr_rows.len(), column_refs))
    }

    fn bind_top_level_orderby(
        &mut self,
        mut plan: LogicalPlan,
        orderbys: &[OrderByExpr],
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let saved_aliases = self.context.expr_aliases.clone();
        let output_schema = plan.output_schema(arena);
        for (position, column) in output_schema.iter().enumerate() {
            self.context.add_alias(
                None,
                arena.column(*column).name().to_string(),
                arena.alloc_expression(ScalarExpression::column_expr(*column, position)),
            );
        }

        let sort_fields = self
            .extract_having_orderby_aggregate_exprs(
                None,
                Some(orderbys),
                |binder, orderby, arena| {
                    let expr = binder.bind_expr(&orderby.expr, arena)?;
                    Ok(SortField::new(
                        arena.alloc_expression(expr),
                        orderby.asc.is_none_or(|asc| asc),
                        orderby.nulls_first.unwrap_or(false),
                    ))
                },
                arena,
            )?
            .1;
        self.context.expr_aliases = saved_aliases;

        Ok(match sort_fields {
            Some(sort_fields) => self.bind_sort(plan, sort_fields, arena)?,
            None => plan,
        })
    }

    pub(crate) fn bind_where(
        &mut self,
        children: LogicalPlan,
        predicate: &Expr,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let predicate = with_query_bind_step!(self, QueryBindStep::Where, {
            let predicate = self.bind_expr(predicate, arena)?;
            arena.alloc_expression(predicate)
        })?;

        self.bind_where_expr(children, predicate, arena)
    }

    pub(crate) fn normalize_select_item(
        &mut self,
        items: &[SelectItem],
        arena: &mut PlanArena,
    ) -> Result<Vec<ExprRef>, DatabaseError> {
        let mut select_items = vec![];

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr, arena)?;
                    select_items.push(arena.alloc_expression(expr));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self
                        .bind_expr(expr, arena)
                        .map(|expr| arena.alloc_expression(expr))?;
                    let alias_name = lower_ident(alias).into_owned();

                    self.context.add_alias(None, alias_name.clone(), expr);

                    select_items.push(arena.alloc_expression(ScalarExpression::Alias {
                        expr,
                        alias: AliasType::Name(alias_name),
                    }));
                }
                SelectItem::Wildcard => {
                    if self.context.bind_table.is_empty() {
                        return Err(DatabaseError::UnsupportedStmt(
                            "SELECT * with no tables specified is not valid".to_string(),
                        ));
                    }
                    for bound_source in &self.context.bind_table {
                        if Self::is_joined_values_source(
                            bound_source.join_type,
                            &bound_source.source,
                            arena,
                        ) {
                            continue;
                        }
                        Self::bind_table_column_refs(
                            &self.context,
                            arena,
                            &mut select_items,
                            bound_source.visible_name(),
                            false,
                        )?;
                    }
                }
                SelectItem::QualifiedWildcard(name) => {
                    let table_name = lower_case_name(name)?;
                    Self::bind_table_column_refs(
                        &self.context,
                        arena,
                        &mut select_items,
                        &table_name,
                        true,
                    )?;
                }
            };
        }

        Ok(select_items)
    }

    fn bind_cte_plan(
        &mut self,
        cte: &Cte,
        alias: &TableAliasInput,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut binder = Binder::new(self.context.fork(), self.args, Some(&self.context));
        let plan = binder.bind_query(&cte.query, arena)?;
        binder.bind_alias(
            plan,
            &alias.columns,
            &alias.name,
            AliasPurpose::Output,
            arena,
        )
    }

    fn add_cte_plan(
        &mut self,
        table_name: TableName,
        plan: LogicalPlan,
        arena: &mut PlanArena,
    ) -> Result<(), DatabaseError> {
        let plan_ref = arena.alloc_plan(plan);
        self.context.add_cte(CteBinding {
            table_name,
            depth: self.context.cte_depth,
            plan_ref,
        })
    }

    fn bind_recursive_cte_plan(
        &mut self,
        cte: &Cte,
        alias: &TableAliasInput,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        if cte.query.with.is_some() || cte.query.order_by.is_some() || cte.query.limit.is_some() {
            return Err(DatabaseError::UnsupportedStmt(
                "recursive CTE query clauses are not supported".to_string(),
            ));
        }

        let SetExpr::SetOperation {
            op: SetOperator::Union,
            all: true,
            left,
            right,
        } = cte.query.body.as_ref()
        else {
            return Err(DatabaseError::UnsupportedStmt(
                "recursive CTEs require a top-level UNION ALL".to_string(),
            ));
        };

        let mut anchor_binder = Binder::new(self.context.fork(), self.args, Some(&self.context));
        let anchor = anchor_binder.bind_set_expr(left, arena)?;
        let mut self_referenced = false;
        anchor.visit_referenced_tables(&mut |table| {
            self_referenced |= arena.table_name(table) == Some(alias.name.as_ref())
        });
        if self_referenced {
            return Err(DatabaseError::UnsupportedStmt(
                "the recursive CTE cannot be referenced by its anchor".to_string(),
            ));
        }

        let mut anchor = anchor_binder.bind_alias(
            anchor,
            &alias.columns,
            &alias.name,
            AliasPurpose::Output,
            arena,
        )?;
        let schema = anchor.output_schema(arena).clone();
        let scan = LogicalPlan::new(
            Operator::RecursiveScan(RecursiveScanOperator {
                schema_ref: schema.clone(),
            }),
            Childrens::None,
        );

        let cte_checkpoint = self.context.cte_checkpoint();
        self.add_cte_plan(alias.name.clone(), scan, arena)?;
        let recursive = {
            let mut binder = Binder::new(self.context.fork(), self.args, Some(&self.context));
            binder.bind_set_expr(right, arena)
        };
        self.context.restore_ctes(cte_checkpoint);
        let mut recursive = recursive?;

        fn recursive_scan_count(plan: &LogicalPlan) -> usize {
            usize::from(matches!(&plan.operator, Operator::RecursiveScan(_)))
                + plan
                    .childrens
                    .iter()
                    .map(recursive_scan_count)
                    .sum::<usize>()
        }

        if recursive_scan_count(&recursive) != 1 {
            return Err(DatabaseError::UnsupportedStmt(
                "the recursive term must reference its CTE exactly once".to_string(),
            ));
        }

        let recursive_schema = recursive.output_schema(arena);
        if schema.len() != recursive_schema.len() {
            return Err(DatabaseError::MisMatch(
                "the anchor column count",
                "the recursive column count",
            ));
        }
        if !schema
            .iter()
            .zip(recursive_schema)
            .all(|(anchor, recursive)| {
                arena.column(*anchor).datatype() == arena.column(*recursive).datatype()
            })
        {
            return Err(DatabaseError::UnsupportedStmt(
                "recursive CTE column types must match the anchor".to_string(),
            ));
        }

        Ok(RecursiveCteOperator::build(schema, anchor, recursive))
    }

    pub(crate) fn bind_query(
        &mut self,
        query: &Query,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let origin_step = self.context.step_now();
        let cte_checkpoint = self.context.cte_checkpoint();
        if let Some(with) = &query.with {
            if with.recursive && with.cte_tables.len() != 1 {
                return Err(DatabaseError::UnsupportedStmt(
                    "only one recursive CTE is supported".to_string(),
                ));
            }

            self.context.cte_depth += 1;
            for cte in &with.cte_tables {
                let alias = sql_table_alias(&cte.alias);
                let plan = if with.recursive {
                    self.bind_recursive_cte_plan(cte, &alias, arena)?
                } else {
                    self.bind_cte_plan(cte, &alias, arena)?
                };
                self.add_cte_plan(alias.name, plan, arena)?;
            }
        }

        let order_by_exprs = query.order_by.as_deref();
        if order_by_exprs.is_some() {
            let mut body = query.body.as_ref();
            while let SetExpr::Query(inner) = body {
                if inner.order_by.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "multiple ORDER BY clauses not allowed".to_string(),
                    ));
                }
                body = inner.body.as_ref();
            }
        }
        let is_plain_select = matches!(query.body.as_ref(), SetExpr::Select(_));
        let mut plan = match query.body.as_ref() {
            SetExpr::Select(select) => self.bind_select(select, order_by_exprs, arena),
            SetExpr::Query(query) => self.bind_query(query, arena),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => self.bind_set_operation(*op, *all, left, right, arena),
            SetExpr::Values(rows) => self.bind_temp_values(rows, arena),
        }?;

        if !is_plain_select {
            if let Some(order_by_exprs) = order_by_exprs {
                plan = self.bind_top_level_orderby(plan, order_by_exprs, arena)?;
            }
        }

        if let Some(limit) = &query.limit {
            plan = self.bind_limit(plan, limit, arena)?;
        }

        self.context.restore_ctes(cte_checkpoint);
        self.context.step(origin_step);
        Ok(plan)
    }

    fn bind_non_negative_limit_value(
        &mut self,
        expr: &Expr,
        arena: &mut PlanArena,
    ) -> Result<usize, DatabaseError> {
        let span = expr.span();
        let bound_expr = self.bind_expr(expr, arena)?;
        match &bound_expr {
            ScalarExpression::Constant(dv) => match dv {
                DataValue::Int32(v) if *v >= 0 => Ok(*v as usize),
                DataValue::Int64(v) if *v >= 0 => Ok(*v as usize),
                _ => Err(DatabaseError::InvalidType),
            },
            _ => Err(attach_span_if_absent(
                DatabaseError::invalid_column("invalid limit expression.".to_owned()),
                span,
            )),
        }
    }

    fn bind_limit(
        &mut self,
        children: LogicalPlan,
        limit: &Limit,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let limit_value = limit
            .limit
            .as_ref()
            .map(|expr| self.bind_non_negative_limit_value(expr, arena))
            .transpose()?;
        let offset_value = limit
            .offset
            .as_ref()
            .map(|expr| self.bind_non_negative_limit_value(expr, arena))
            .transpose()?;

        self.bind_limit_values(children, offset_value, limit_value)
    }

    fn build_statement<'s, 'arena>(
        &'s mut self,
        arena: &'s mut PlanArena<'arena>,
    ) -> BindStatementStart<'s, 'a, 'parent, 'arena, T, A> {
        BindStatementStart {
            binder: self,
            arena,
        }
    }

    pub fn bind(
        &mut self,
        stmt: &Statement,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let plan = self.build_statement(arena).statement(stmt)?.finish();
        Ok(self.init_scalar_queries(plan))
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
#[maybe_async]
mod tests {
    use super::*;
    use crate::binder::test::build_t1_table;
    use crate::db::DataBaseBuilder;

    fn assert_unsupported(err: DatabaseError, expected: &str) {
        let DatabaseError::UnsupportedStmt(message) = err else {
            panic!("expected unsupported statement, got {err:?}");
        };
        assert!(message.contains(expected), "{message}");
    }

    fn expect_err<T>(result: Result<T, DatabaseError>) -> DatabaseError {
        match result {
            Ok(_) => panic!("expected error"),
            Err(err) => err,
        }
    }

    #[test]
    async fn prepare_rejects_non_positional_parameters() -> Result<(), DatabaseError> {
        let db = DataBaseBuilder::path(".").build_in_memory().await?;
        for placeholder in ["$0", "$01", "$name"] {
            assert!(matches!(
                db.program_sql(
                    &format!("select {placeholder}"),
                    &[(1, LogicalType::Integer)]
                ),
                Err(DatabaseError::InvalidValue(_))
            ));
        }
        Ok(())
    }

    #[test]
    async fn test_prepare_and_command_type_classification() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory().await?;
        assert!(matches!(
            database.prepare_all(""),
            Err(DatabaseError::EmptyStatement)
        ));
        assert_eq!(
            command_type(&database.prepare_all("select 1")?.pop().unwrap())?,
            CommandType::DQL
        );
        assert_eq!(
            command_type(
                &database
                    .prepare_all("create table t (id int primary key)")?
                    .pop()
                    .unwrap()
            )?,
            CommandType::DDL
        );
        assert_eq!(
            command_type(&database.prepare_all("analyze table t")?.pop().unwrap())?,
            CommandType::Analyze
        );
        assert_eq!(
            command_type(
                &database
                    .prepare_all("insert into t values (1)")?
                    .pop()
                    .unwrap()
            )?,
            CommandType::DML
        );
        assert_eq!(
            command_type(&database.prepare_all("update t set id = 1")?.pop().unwrap())?,
            CommandType::DML
        );
        assert_eq!(
            command_type(&database.prepare_all("delete from t")?.pop().unwrap())?,
            CommandType::DML
        );
        assert_eq!(
            command_type(&database.prepare_all("truncate table t")?.pop().unwrap())?,
            CommandType::DML
        );
        #[cfg(feature = "copy")]
        for (sql, expected) in [
            ("copy t to 'out.csv'", CommandType::DQL),
            ("copy t from 'in.csv'", CommandType::DML),
        ] {
            assert_eq!(
                command_type(&database.prepare_all(sql)?.pop().unwrap())?,
                expected
            );
        }

        assert_unsupported(
            database.prepare_all("start transaction").unwrap_err(),
            "START TRANSACTION",
        );

        Ok(())
    }

    #[test]
    async fn test_database_entrypoints_reject_catalog_mutation() -> Result<(), DatabaseError> {
        let mut database = DataBaseBuilder::path(".").build_in_memory().await?;
        let params = &[] as &[(usize, LogicalType)];

        assert_unsupported(
            expect_err(database.program_sql("create table t (id int primary key)", params)),
            "DDL and ANALYZE",
        );
        assert_unsupported(
            expect_err(database.run("create table t (id int primary key)").await),
            "DDL and ANALYZE",
        );
        assert_unsupported(
            expect_err(database.ddl("select 1").await),
            "`Database::ddl`",
        );

        let transaction = database.new_transaction(false)?;
        assert_unsupported(
            expect_err(transaction.program_sql("analyze table t", params)),
            "DDL and ANALYZE",
        );
        transaction.commit().await?;

        Ok(())
    }

    #[test]
    async fn test_alter_table_parser_errors() -> Result<(), DatabaseError> {
        let tables = build_t1_table().await?;

        assert_unsupported(
            tables
                .plan("alter table t1 add column c5 int null, add column c6 int null")
                .unwrap_err(),
            "only a single ALTER TABLE operation",
        );
        assert_unsupported(
            tables
                .plan("alter table t1 drop column c1, drop column c2")
                .unwrap_err(),
            "only a single ALTER TABLE operation",
        );

        assert!(matches!(
            tables
                .plan("alter table t1 rename column c1 to \"bad-name\"")
                .unwrap_err(),
            DatabaseError::InvalidColumn { .. }
        ));

        Ok(())
    }

    #[test]
    async fn test_limit_parser_rejects_negative_and_non_constant_values(
    ) -> Result<(), DatabaseError> {
        let tables = build_t1_table().await?;

        assert!(matches!(
            tables.plan("select * from t1 limit -1").unwrap_err(),
            DatabaseError::InvalidType
        ));
        assert!(matches!(
            tables.plan("select * from t1 limit 1.5").unwrap_err(),
            DatabaseError::InvalidType
        ));
        assert!(matches!(
            tables.plan("select * from t1 limit c1").unwrap_err(),
            DatabaseError::InvalidColumn { .. }
        ));

        Ok(())
    }

    #[test]
    async fn test_bind_non_recursive_ctes() -> Result<(), DatabaseError> {
        let tables = build_t1_table().await?;
        let mut arena = PlanArena::new(&tables.table_arena);
        let mut plan = tables.plan_with_arena(
            "with first(a) as (select c1 from t1), \
             second as (select a from first) select a from second",
            &mut arena,
        )?;

        let schema = plan.output_schema(&mut arena);
        assert_eq!(schema.len(), 1);
        assert_eq!(arena.column(schema[0]).name(), "a");

        assert_unsupported(
            tables
                .plan("with recursive cte as (select 1) select * from cte")
                .unwrap_err(),
            "recursive CTEs",
        );
        assert_unsupported(
            tables
                .plan("with cte as (select 1), cte as (select 2) select * from cte")
                .unwrap_err(),
            "duplicate CTE name",
        );

        Ok(())
    }

    #[test]
    async fn force_nest_loop_join_marks_join_operator() -> Result<(), DatabaseError> {
        let tables = build_t1_table().await?;
        let plan =
            tables.plan("select /*+ FORCE_NEST_LOOP_JOIN */ c1, c3 from t1 join t2 on c1 = c3")?;
        let join = plan
            .childrens
            .iter()
            .find_map(|plan| match &plan.operator {
                Operator::Join(operator) => Some(operator),
                _ => None,
            })
            .expect("query should contain a join");

        assert!(join.force_nested_loop);
        Ok(())
    }

    #[cfg(feature = "spill")]
    #[test]
    async fn optimizer_hints_can_be_combined() -> Result<(), DatabaseError> {
        let tables = build_t1_table().await?;
        let plan = tables.plan(
            "select /*+ FORCE_AGG_SPILL, FORCE_NEST_LOOP_JOIN */ c1, count(c3) \
             from t1 join t2 on c1 = c3 group by c1",
        )?;
        let aggregate_plan = plan
            .childrens
            .iter()
            .find(|plan| matches!(plan.operator, Operator::Aggregate(_)))
            .expect("query should contain an aggregate");
        let Operator::Aggregate(aggregate) = &aggregate_plan.operator else {
            unreachable!()
        };
        let join = aggregate_plan
            .childrens
            .iter()
            .find_map(|plan| match &plan.operator {
                Operator::Join(operator) => Some(operator),
                _ => None,
            })
            .expect("aggregate input should contain a join");

        assert!(aggregate.force_spill);
        assert!(join.force_nested_loop);
        Ok(())
    }
}
// GRCOV_EXCL_STOP
