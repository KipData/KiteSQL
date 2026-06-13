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
    BindPlanAggregated, BindPlanComplete, BindPlanDistinct, BindPlanFiltered, BindPlanFrom,
    BindPlanHaving, BindPlanProjected, BindPlanSelectList, BindPlanStart, JoinConstraintInput,
    TableAliasInput,
};
use super::{is_valid_identifier, with_query_bind_step, Binder, QueryBindStep, SetOperatorKind};
#[cfg(feature = "copy")]
use crate::binder::copy::{ExtSource, FileFormat};
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef, TableName};
use crate::db::{BindSource, DBTransaction, Database, DatabaseIter, TransactionIter};
use crate::errors::{DatabaseError, SqlErrorSpan};
use crate::expression;
use crate::expression::simplify::ConstantCalculator;
use crate::expression::visitor_mut::VisitorMut;
use crate::expression::{AliasType, ScalarExpression};
use crate::parser::parse_sql;
use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
use crate::planner::operator::join::{JoinCondition, JoinOperator as LJoinOperator, JoinType};
use crate::planner::operator::mark_apply::MarkApplyQuantifier;
use crate::planner::operator::project::ProjectOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan, PlanArena};
use crate::storage::{Storage, Transaction};
use crate::types::value::{DataValue, Utf8Type};
use crate::types::{CharLengthUnits, ColumnId, LogicalType};
use itertools::Itertools;
pub(super) use sqlparser::ast::{
    AlterColumnOperation, AlterTableOperation, Assignment, AssignmentTarget, BinaryOperator,
    ColumnDef, ColumnOption, CreateView, DataType, DescribeAlias, Distinct, DuplicateTreatment,
    Expr, FromTable, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    IndexColumn, Join, JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart,
    ObjectType, OrderByExpr, OrderByKind, Query, Select, SelectInto, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, SetOperator, SetQuantifier, Spanned, TableAlias,
    TableConstraint, TableFactor, TableObject, TableWithJoins, TypedString, UnaryOperator, Value,
};
#[cfg(feature = "copy")]
pub(super) use sqlparser::ast::{CopyOption, CopySource, CopyTarget};
use sqlparser::tokenizer::Span;
use std::borrow::{Borrow, Cow};
use std::cmp;
use std::slice;

/// Parsed SQL statement type used by KiteSQL SQL frontend APIs.
pub type Statement = sqlparser::ast::Statement;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CommandType {
    DQL,
    DML,
    DDL,
    Analyze,
}

pub(crate) trait AttachSpanSource {
    fn sql_error_span(self) -> Option<SqlErrorSpan>;
}

impl<T: Spanned + ?Sized> AttachSpanSource for &T {
    fn sql_error_span(self) -> Option<SqlErrorSpan> {
        self.span().sql_error_span()
    }
}

impl AttachSpanSource for Span {
    fn sql_error_span(self) -> Option<SqlErrorSpan> {
        if self == Span::empty() {
            return None;
        }

        let start = self.start.column as usize;
        let mut end = self.end.column as usize;
        if end <= start {
            end = start.saturating_add(1);
        }

        Some(SqlErrorSpan {
            start,
            end,
            line: self.start.line as usize,
            highlight: None,
        })
    }
}

pub(crate) fn attach_span_if_absent<T: AttachSpanSource>(
    err: DatabaseError,
    source: T,
) -> DatabaseError {
    if err.sql_error_span().is_some() {
        return err;
    }

    match source.sql_error_span() {
        Some(span) => err.with_span(span),
        None => err,
    }
}

pub fn command_type(stmt: &Statement) -> Result<CommandType, DatabaseError> {
    match stmt {
        Statement::CreateTable(_)
        | Statement::CreateIndex(_)
        | Statement::CreateView(_)
        | Statement::AlterTable(_)
        | Statement::Drop { .. } => Ok(CommandType::DDL),
        Statement::Query(_)
        | Statement::Explain { .. }
        | Statement::ExplainTable { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowViews { .. } => Ok(CommandType::DQL),
        Statement::Analyze(_) => Ok(CommandType::Analyze),
        Statement::Truncate(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::Insert(_) => Ok(CommandType::DML),
        #[cfg(feature = "copy")]
        Statement::Copy { .. } => Ok(CommandType::DML),
        stmt => Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
    }
}

/// Parses a single SQL statement into a reusable [`Statement`].
pub fn prepare<T: AsRef<str>>(sql: T) -> Result<Statement, DatabaseError> {
    let mut stmts = prepare_all(sql)?;
    stmts.pop().ok_or(DatabaseError::EmptyStatement)
}

/// Parses one or more SQL statements into a vector of [`Statement`] values.
pub fn prepare_all<T: AsRef<str>>(sql: T) -> Result<Vec<Statement>, DatabaseError> {
    let stmts = parse_sql(sql)?;
    if stmts.is_empty() {
        return Err(DatabaseError::EmptyStatement);
    }
    Ok(stmts)
}

fn statement_mutates_catalog_or_statistics(statement: &Statement) -> Result<bool, DatabaseError> {
    Ok(matches!(
        command_type(statement)?,
        CommandType::DDL | CommandType::Analyze
    ))
}

impl<S: Storage> Database<S> {
    /// Executes a prepared [`Statement`] inside a database-owned transaction.
    pub fn execute<A, St>(
        &self,
        statement: St,
        params: A,
    ) -> Result<DatabaseIter<'_, S>, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        St: Borrow<Statement>,
    {
        if statement_mutates_catalog_or_statistics(statement.borrow())? {
            return Err(DatabaseError::UnsupportedStmt(
                "DDL and ANALYZE require `Database::ddl` or `Database::analyze`".to_string(),
            ));
        }
        BindSource::execute(self, params, |binder, arena| {
            binder.bind(statement.borrow(), arena)
        })
    }

    pub fn ddl<T: AsRef<str>>(&mut self, sql: T) -> Result<(), DatabaseError> {
        let sql = sql.as_ref();
        let statements = prepare_all(sql).map_err(|err| err.with_sql_context(sql))?;

        for statement in statements {
            if !matches!(command_type(&statement)?, CommandType::DDL) {
                return Err(DatabaseError::UnsupportedStmt(
                    "`Database::ddl` only accepts DDL statements".to_string(),
                )
                .with_sql_context(sql));
            }

            self.execute_mut(sql, &[], |binder, arena| binder.bind(&statement, arena))?;
        }

        Ok(())
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
    pub fn run<T: AsRef<str>>(&self, sql: T) -> Result<DatabaseIter<'_, S>, DatabaseError> {
        let sql = sql.as_ref();
        let statements = prepare_all(sql).map_err(|err| err.with_sql_context(sql))?;
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

        let transaction = Box::into_raw(Box::new(
            self.storage
                .transaction_with_isolation(self.transaction_isolation)?,
        ));
        let mut statements = statements.into_iter().peekable();

        while let Some(statement) = statements.next() {
            let (schema, plan_arena, executor) =
                match self
                    .state
                    .execute(unsafe { &mut *transaction }, &[], |binder, arena| {
                        binder.bind(&statement, arena)
                    }) {
                    Ok(result) => result,
                    Err(err) => {
                        unsafe { drop(Box::from_raw(transaction)) };
                        return Err(err.with_sql_context(sql));
                    }
                };

            if statements.peek().is_some() {
                if let Err(err) =
                    TransactionIter::new(schema, plan_arena, executor, transaction).done()
                {
                    unsafe { drop(Box::from_raw(transaction)) };
                    return Err(err.with_sql_context(sql));
                }
            } else {
                let inner = Box::into_raw(Box::new(TransactionIter::new(
                    schema,
                    plan_arena,
                    executor,
                    transaction,
                )));
                return Ok(DatabaseIter { transaction, inner });
            }
        }

        unsafe { drop(Box::from_raw(transaction)) };
        Err(DatabaseError::EmptyStatement.with_sql_context(sql))
    }
}

impl<'txn, S: Storage> DBTransaction<'txn, S> {
    /// Executes a prepared [`Statement`] inside the current transaction.
    pub fn execute<'a, A, St>(
        &'a mut self,
        statement: St,
        params: A,
    ) -> Result<TransactionIter<'a, S::TransactionType<'txn>>, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        St: Borrow<Statement>,
    {
        if matches!(
            command_type(statement.borrow())?,
            CommandType::DDL | CommandType::Analyze
        ) {
            return Err(DatabaseError::UnsupportedStmt(
                "`DDL` and `ANALYZE` are not allowed to execute within a transaction".to_string(),
            ));
        }
        BindSource::execute(self, params, |binder, arena| {
            binder.bind(statement.borrow(), arena)
        })
    }

    /// Runs SQL inside the current transaction and returns the final result iterator.
    pub fn run<'a, T: AsRef<str>>(
        &'a mut self,
        sql: T,
    ) -> Result<TransactionIter<'a, S::TransactionType<'txn>>, DatabaseError> {
        let sql = sql.as_ref();
        let mut statements = prepare_all(sql).map_err(|err| err.with_sql_context(sql))?;
        let last_statement = statements
            .pop()
            .ok_or_else(|| DatabaseError::EmptyStatement.with_sql_context(sql))?;

        for statement in statements {
            self.execute(&statement, &[])
                .map_err(|err| err.with_sql_context(sql))?
                .done()
                .map_err(|err| err.with_sql_context(sql))?;
        }

        self.execute(&last_statement, &[])
            .map_err(|err| err.with_sql_context(sql))
    }
}

struct BindStatementStart<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    binder: &'s mut Binder<'a, 'b, T, A>,
    arena: &'s mut PlanArena<'arena>,
}

struct BindStatementComplete {
    plan: LogicalPlan,
}

struct UpdateExprTargetRemapper<'a, 'p> {
    target_schema: &'a [ColumnRef],
    arena: &'a PlanArena<'p>,
}

impl VisitorMut<'_> for UpdateExprTargetRemapper<'_, '_> {
    fn visit_column_ref(
        &mut self,
        column: &mut ColumnRef,
        position: &mut usize,
    ) -> Result<(), DatabaseError> {
        let Some(target_position) = self
            .target_schema
            .iter()
            .copied()
            .position(|target_column| self.arena.same_column(target_column, *column))
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
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    fn statement(self, stmt: &Statement) -> Result<BindStatementComplete, DatabaseError> {
        let span = stmt.span();
        (|| {
            let plan = match stmt {
                Statement::Query(query) => self.binder.bind_query(query, self.arena)?,
                Statement::AlterTable(alter) => self.alter_table(alter.clone())?,
                Statement::CreateTable(create) => self.create_table(create.clone())?,
                Statement::Drop {
                    object_type,
                    names,
                    if_exists,
                    ..
                } => self.drop_object(*object_type, names.clone(), *if_exists)?,
                Statement::Insert(insert) => self.insert(insert)?,
                Statement::Update(update) => self.update(update)?,
                Statement::Delete(delete) => self.delete(delete)?,
                Statement::Analyze(analyze) => self.analyze(analyze.clone())?,
                Statement::Truncate(truncate) => self.truncate(truncate.clone())?,
                Statement::ShowTables { .. } => self.binder.bind_show_tables()?,
                Statement::ShowViews { .. } => self.binder.bind_show_views()?,
                #[cfg(feature = "copy")]
                Statement::Copy {
                    source,
                    to,
                    target,
                    options,
                    ..
                } => self.copy(source.clone(), *to, target.clone(), options.clone())?,
                #[cfg(not(feature = "copy"))]
                Statement::Copy { .. } => {
                    return Err(DatabaseError::UnsupportedStmt(
                        "COPY requires the `copy` feature".to_string(),
                    ))
                }
                Statement::Explain { statement, .. } => self.explain(statement)?,
                Statement::ExplainTable {
                    describe_alias: DescribeAlias::Describe | DescribeAlias::Desc,
                    table_name,
                    ..
                } => self
                    .binder
                    .bind_describe(sql_table_name(table_name.clone())?)?,
                Statement::CreateIndex(create) => self.create_index(create.clone())?,
                Statement::CreateView(create) => self.create_view(create.clone())?,
                _ => return Err(DatabaseError::UnsupportedStmt(stmt.to_string())),
            };

            Ok(BindStatementComplete { plan })
        })()
        .map_err(|err| attach_span_if_absent(err, span))
    }

    fn alter_table(self, alter: sqlparser::ast::AlterTable) -> Result<LogicalPlan, DatabaseError> {
        if alter.operations.len() != 1 {
            return Err(DatabaseError::UnsupportedStmt(
                "only a single ALTER TABLE operation is supported".to_string(),
            ));
        }
        let operation = alter.operations.into_iter().next().unwrap();
        self.alter_table_operation(sql_table_name(alter.name)?, operation)
    }

    fn alter_table_operation(
        mut self,
        table_name: TableName,
        operation: AlterTableOperation,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.binder
            .context
            .table(table_name.clone())?
            .ok_or(DatabaseError::TableNotFound)?;

        match operation {
            AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists,
                column_def,
                ..
            } => {
                let column_span = column_def.name.span;
                let column = self.bind_column(column_def, None)?;

                if !is_valid_identifier(column.name()) {
                    return Err(attach_span_if_absent(
                        DatabaseError::invalid_column("illegal column naming".to_string()),
                        column_span,
                    ));
                }

                self.binder
                    .bind_add_column(table_name, column, if_not_exists)
            }
            AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                ..
            } => {
                if column_names.len() != 1 {
                    return Err(DatabaseError::UnsupportedStmt(
                        "only dropping a single column is supported".to_string(),
                    ));
                }
                let column_name = column_names[0].value.clone();

                self.binder
                    .bind_drop_column(table_name, column_name, if_exists)
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                let old_column_name = lower_ident(&old_column_name);
                let new_column_name = lower_ident(&new_column_name).into_owned();
                let data_type = {
                    let table = self
                        .binder
                        .context
                        .table(table_name.clone())?
                        .ok_or(DatabaseError::TableNotFound)?;
                    table
                        .get_column_by_name(old_column_name.as_ref())
                        .map(|column| self.arena.column(column).datatype().clone())
                        .ok_or_else(|| {
                            DatabaseError::column_not_found(old_column_name.to_string())
                        })?
                };

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }

                self.binder.bind_change_column(
                    table_name,
                    old_column_name.into_owned(),
                    new_column_name,
                    data_type,
                    DefaultChange::NoChange,
                    NotNullChange::NoChange,
                )
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let old_column_name = lower_ident(&column_name);
                let old_data_type = {
                    let table = self
                        .binder
                        .context
                        .table(table_name.clone())?
                        .ok_or(DatabaseError::TableNotFound)?;
                    table
                        .get_column_by_name(old_column_name.as_ref())
                        .map(|column| self.arena.column(column).datatype().clone())
                        .ok_or_else(|| {
                            DatabaseError::column_not_found(old_column_name.to_string())
                        })?
                };

                let (data_type, default_change, not_null_change) = match op {
                    AlterColumnOperation::SetDataType {
                        data_type, using, ..
                    } => {
                        if using.is_some() {
                            return Err(DatabaseError::UnsupportedStmt(
                                "ALTER COLUMN TYPE USING is not supported".to_string(),
                            ));
                        }
                        (
                            LogicalType::try_from(data_type)?,
                            DefaultChange::NoChange,
                            NotNullChange::NoChange,
                        )
                    }
                    AlterColumnOperation::SetDefault { value } => (
                        old_data_type.clone(),
                        DefaultChange::Set(self.bind_alter_default_expr(value, &old_data_type)?),
                        NotNullChange::NoChange,
                    ),
                    AlterColumnOperation::DropDefault => (
                        old_data_type.clone(),
                        DefaultChange::Drop,
                        NotNullChange::NoChange,
                    ),
                    AlterColumnOperation::SetNotNull => (
                        old_data_type.clone(),
                        DefaultChange::NoChange,
                        NotNullChange::Set,
                    ),
                    AlterColumnOperation::DropNotNull => (
                        old_data_type.clone(),
                        DefaultChange::NoChange,
                        NotNullChange::Drop,
                    ),
                    _ => {
                        return Err(DatabaseError::UnsupportedStmt(format!(
                            "unsupported alter column operation: {op:?}"
                        )))
                    }
                };

                self.binder.bind_change_column(
                    table_name,
                    old_column_name.to_string(),
                    old_column_name.into_owned(),
                    data_type,
                    default_change,
                    not_null_change,
                )
            }
            AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                column_position,
            } => {
                if column_position.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "MODIFY COLUMN does not currently support column positions".to_string(),
                    ));
                }
                let old_column_name = lower_ident(&col_name);
                {
                    let table = self
                        .binder
                        .context
                        .table(table_name.clone())?
                        .ok_or(DatabaseError::TableNotFound)?;
                    let _ = table
                        .get_column_by_name(old_column_name.as_ref())
                        .ok_or_else(|| {
                            DatabaseError::column_not_found(old_column_name.to_string())
                        })?;
                }
                let old_column_name = old_column_name.into_owned();
                let data_type = LogicalType::try_from(data_type)?;
                let (default_change, not_null_change) =
                    self.bind_change_column_options(options, &data_type)?;

                self.binder.bind_change_column(
                    table_name,
                    old_column_name.clone(),
                    old_column_name,
                    data_type,
                    default_change,
                    not_null_change,
                )
            }
            AlterTableOperation::ChangeColumn {
                old_name,
                new_name,
                data_type,
                options,
                column_position,
            } => {
                if column_position.is_some() {
                    return Err(DatabaseError::UnsupportedStmt(
                        "CHANGE COLUMN does not currently support column positions".to_string(),
                    ));
                }
                let old_column_name = lower_ident(&old_name);
                let new_column_name = lower_ident(&new_name).into_owned();
                {
                    let table = self
                        .binder
                        .context
                        .table(table_name.clone())?
                        .ok_or(DatabaseError::TableNotFound)?;
                    let _ = table
                        .get_column_by_name(old_column_name.as_ref())
                        .ok_or_else(|| {
                            DatabaseError::column_not_found(old_column_name.to_string())
                        })?;
                }

                if !is_valid_identifier(&new_column_name) {
                    return Err(DatabaseError::invalid_column(
                        "illegal column naming".to_string(),
                    ));
                }
                let data_type = LogicalType::try_from(data_type)?;
                let (default_change, not_null_change) =
                    self.bind_change_column_options(options, &data_type)?;

                self.binder.bind_change_column(
                    table_name,
                    old_column_name.into_owned(),
                    new_column_name,
                    data_type,
                    default_change,
                    not_null_change,
                )
            }
            op => Err(DatabaseError::UnsupportedStmt(format!(
                "AlertOperation: {op:?}"
            ))),
        }
    }

    fn bind_alter_default_expr(
        &mut self,
        expr: Expr,
        ty: &LogicalType,
    ) -> Result<ScalarExpression, DatabaseError> {
        let mut expr = self.binder.bind_expr(&expr, self.arena)?;

        if expr.any_referenced_column(self.arena, |_, _| true) {
            return Err(DatabaseError::UnsupportedStmt(
                "column is not allowed to exist in default".to_string(),
            ));
        }
        expr = ScalarExpression::type_cast(expr, Cow::Borrowed(ty), self.arena)?;

        Ok(expr)
    }

    fn bind_change_column_options(
        &mut self,
        options: Vec<ColumnOption>,
        data_type: &LogicalType,
    ) -> Result<(DefaultChange, NotNullChange), DatabaseError> {
        let mut default_change = DefaultChange::NoChange;
        let mut not_null_change = NotNullChange::NoChange;

        for option in options {
            match option {
                ColumnOption::Null => not_null_change = NotNullChange::Drop,
                ColumnOption::NotNull => not_null_change = NotNullChange::Set,
                ColumnOption::Default(expr) => {
                    default_change =
                        DefaultChange::Set(self.bind_alter_default_expr(expr, data_type)?);
                }
                option => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "CHANGE/MODIFY COLUMN does not currently support this option: {option:?}"
                    )))
                }
            }
        }

        Ok((default_change, not_null_change))
    }

    fn create_table(
        mut self,
        create: sqlparser::ast::CreateTable,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = sql_table_name(create.name.clone())?;

        if !is_valid_identifier(&table_name) {
            return Err(attach_span_if_absent(
                DatabaseError::invalid_table("illegal table naming".to_string()),
                &create.name,
            ));
        }
        for col in create.columns.iter() {
            let col_name = &col.name.value;
            if !is_valid_identifier(col_name) {
                return Err(attach_span_if_absent(
                    DatabaseError::invalid_column("illegal column naming".to_string()),
                    col,
                ));
            }
        }

        let mut columns = Vec::with_capacity(create.columns.len());
        for (i, column) in create.columns.into_iter().enumerate() {
            columns.push(self.bind_column(column, Some(i))?);
        }
        for constraint in create.constraints {
            match constraint {
                TableConstraint::PrimaryKey(primary) => {
                    self.bind_constraint(&mut columns, primary.columns, |i, desc| {
                        desc.set_primary(Some(i))
                    })?;
                }
                TableConstraint::Unique(unique) => {
                    self.bind_constraint(&mut columns, unique.columns, |_, desc| {
                        desc.set_unique()
                    })?;
                }
                constraint => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "`CreateTable` does not currently support this constraint: {constraint:?}"
                    )))
                }
            }
        }

        self.binder
            .bind_create_table(table_name, columns, create.if_not_exists)
    }

    fn bind_column(
        &mut self,
        column_def: ColumnDef,
        column_index: Option<usize>,
    ) -> Result<ColumnCatalog, DatabaseError> {
        let column_name = lower_ident(&column_def.name).into_owned();
        let mut column_desc = ColumnDesc::new(
            LogicalType::try_from(column_def.data_type)?,
            None,
            false,
            None,
        )?;
        let mut nullable = true;

        for option_def in column_def.options {
            match option_def.option {
                ColumnOption::Null => nullable = true,
                ColumnOption::NotNull => nullable = false,
                ColumnOption::PrimaryKey(_) => {
                    column_desc.set_primary(column_index);
                    nullable = false;
                    break;
                }
                ColumnOption::Unique(_) => column_desc.set_unique(),
                ColumnOption::Default(expr) => {
                    let mut expr = self.binder.bind_expr(&expr, self.arena)?;

                    if expr.any_referenced_column(self.arena, |_, _| true) {
                        return Err(DatabaseError::UnsupportedStmt(
                            "column is not allowed to exist in `default`".to_string(),
                        ));
                    }
                    expr = ScalarExpression::type_cast(
                        expr,
                        Cow::Borrowed(&column_desc.column_datatype),
                        self.arena,
                    )?;
                    column_desc.default = Some(expr);
                }
                option => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "`Column` does not currently support this option: {option:?}"
                    )))
                }
            }
        }

        Ok(ColumnCatalog::new(column_name, nullable, column_desc))
    }

    fn bind_constraint<F: Fn(usize, &mut ColumnDesc)>(
        &mut self,
        table_columns: &mut [ColumnCatalog],
        exprs: Vec<IndexColumn>,
        fn_constraint: F,
    ) -> Result<(), DatabaseError> {
        for (i, index_column) in exprs.into_iter().enumerate() {
            let Expr::Identifier(ident) = index_column.column.expr else {
                return Err(DatabaseError::UnsupportedStmt(
                    "only identifier columns are supported in `PRIMARY KEY/UNIQUE`".to_string(),
                ));
            };
            let column_name = lower_ident(&ident);

            if let Some(column) = table_columns
                .iter_mut()
                .find(|column| column.name() == column_name.as_ref())
            {
                fn_constraint(i, column.desc_mut())
            }
        }
        Ok(())
    }

    fn create_index(
        self,
        create: sqlparser::ast::CreateIndex,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name = sql_table_name(create.table_name)?;
        let index_name = create
            .name
            .ok_or(DatabaseError::InvalidIndex)
            .and_then(sql_object_name)?;
        let input = self
            .binder
            .bind_create_index_source(table_name.clone(), self.arena)?;
        let mut columns = Vec::with_capacity(create.columns.len());

        for index_column in create.columns {
            match self
                .binder
                .bind_expr(&index_column.column.expr, self.arena)?
            {
                ScalarExpression::ColumnRef { column, .. } => columns.push(column),
                expr => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "'CREATE INDEX' by {expr}"
                    )))
                }
            }
        }

        self.binder.bind_create_index(
            table_name,
            index_name,
            columns,
            create.if_not_exists,
            create.unique,
            input,
        )
    }

    fn create_view(self, create: CreateView) -> Result<LogicalPlan, DatabaseError> {
        let CreateView {
            or_replace,
            name,
            columns,
            query,
            ..
        } = create;
        let output_aliases = query_output_aliases(&query);
        let view_name = sql_table_name(name)?;
        let column_names = columns
            .into_iter()
            .map(|column| lower_ident(&column.name).into_owned())
            .collect();
        let plan = self.binder.bind_query(query.as_ref(), self.arena)?;

        self.binder.bind_create_view(
            view_name,
            or_replace,
            plan,
            column_names,
            output_aliases,
            self.arena,
        )
    }

    fn drop_object(
        self,
        object_type: ObjectType,
        mut names: Vec<ObjectName>,
        if_exists: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        if names.len() > 1 {
            return Err(DatabaseError::UnsupportedStmt(
                "only Drop a single `Table` or `View` is allowed".to_string(),
            ));
        }

        match object_type {
            ObjectType::Table => self
                .binder
                .bind_drop_table(sql_table_name(names.remove(0))?, if_exists),
            ObjectType::View => self
                .binder
                .bind_drop_view(sql_table_name(names.remove(0))?, if_exists),
            ObjectType::Index => {
                let (table_name, index_name) = sql_index_name(names.remove(0))?;
                self.binder
                    .bind_drop_index(table_name, index_name, if_exists)
            }
            _ => Err(DatabaseError::UnsupportedStmt(
                "only `Table` and `View` are allowed to be Dropped".to_string(),
            )),
        }
    }

    #[cfg(feature = "copy")]
    fn copy(
        self,
        source: CopySource,
        to: bool,
        target: CopyTarget,
        options: Vec<CopyOption>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let ext_source = copy_ext_source(target, options)?;

        match source {
            CopySource::Table { table_name, .. } => {
                self.binder
                    .bind_copy_table(sql_table_name(table_name)?, to, ext_source, self.arena)
            }
            CopySource::Query(query) => {
                if !to {
                    return Err(DatabaseError::UnsupportedStmt(
                        "'COPY FROM query'".to_string(),
                    ));
                }
                let input_plan = self.binder.bind_query(query.as_ref(), self.arena)?;
                self.binder.bind_copy_to_file(ext_source, input_plan)
            }
        }
    }

    fn insert(mut self, insert: &sqlparser::ast::Insert) -> Result<LogicalPlan, DatabaseError> {
        let sqlparser::ast::Insert {
            table,
            columns,
            overwrite,
            source,
            ..
        } = insert;
        let table_name = match table {
            TableObject::TableName(table_name) => table_name.clone(),
            TableObject::TableFunction(_) => {
                return Err(DatabaseError::UnsupportedStmt(
                    "insert into table function is not supported".to_string(),
                ))
            }
        };
        let table_name = sql_table_name(table_name)?;
        let source = source.as_ref().ok_or_else(|| {
            DatabaseError::UnsupportedStmt("insert without source is not supported".to_string())
        })?;
        if let SetExpr::Values(values) = source.body.as_ref() {
            self.insert_values(table_name, columns, &values.rows, *overwrite, false)
        } else {
            self.insert_query(table_name, columns, source, *overwrite)
        }
    }

    fn insert_values(
        &mut self,
        table_name: TableName,
        idents: &[Ident],
        expr_rows: &[Vec<Expr>],
        is_overwrite: bool,
        is_mapping_by_name: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        self.binder.context.allow_default = true;
        let source = self
            .binder
            .context
            .source_and_bind(table_name.clone(), None, None, false)?
            .ok_or(DatabaseError::TableNotFound)?;
        let values_len = expr_rows[0].len();

        let schema_ref = if idents.is_empty() {
            if values_len > source.schema_len() {
                return Err(DatabaseError::ValuesLenMismatch(
                    source.schema_len(),
                    values_len,
                ));
            }
            source.schema().to_vec()
        } else {
            let mut columns = Vec::with_capacity(idents.len());
            for ident in idents {
                match self.binder.bind_column_ref_from_identifiers(
                    slice::from_ref(ident),
                    Some(table_name.as_ref()),
                    self.arena,
                )? {
                    ScalarExpression::ColumnRef { column, .. } => columns.push(column),
                    _ => return Err(DatabaseError::UnsupportedStmt(ident.to_string())),
                }
            }
            if values_len != columns.len() {
                return Err(DatabaseError::ValuesLenMismatch(columns.len(), values_len));
            }
            columns
        };
        let mut rows = Vec::with_capacity(expr_rows.len());

        for expr_row in expr_rows {
            if expr_row.len() != values_len {
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }
            let mut row = Vec::with_capacity(expr_row.len());

            for (i, expr) in expr_row.iter().enumerate() {
                let span = expr.span();
                let mut expression = self.binder.bind_expr(expr, self.arena)?;

                ConstantCalculator::new(self.arena).visit(&mut expression)?;
                match expression {
                    ScalarExpression::Constant(mut value) => {
                        let column = self.arena.column(schema_ref[i]);
                        let ty = column.datatype();

                        value = value.cast(ty)?;
                        value.check_len(ty)?;
                        if value.is_null() && !column.nullable() {
                            return Err(attach_span_if_absent(
                                DatabaseError::not_null_column(column.name().to_string()),
                                span,
                            ));
                        }

                        row.push(value);
                    }
                    ScalarExpression::Empty => {
                        let column = self.arena.column(schema_ref[i]);
                        let default_value = column
                            .default_value()?
                            .ok_or(DatabaseError::DefaultNotExist)?;
                        if default_value.is_null() && !column.nullable() {
                            return Err(attach_span_if_absent(
                                DatabaseError::not_null_column(column.name().to_string()),
                                span,
                            ));
                        }
                        row.push(default_value);
                    }
                    _ => {
                        return Err(attach_span_if_absent(
                            DatabaseError::UnsupportedStmt(
                                "INSERT values must be constants or DEFAULT".to_string(),
                            ),
                            span,
                        ))
                    }
                }
            }
            rows.push(row);
        }
        self.binder.context.allow_default = false;

        self.binder.bind_insert_values(
            table_name,
            schema_ref,
            rows,
            is_overwrite,
            is_mapping_by_name,
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
        let input_schema = input_plan.output_schema(self.arena).clone();
        let input_len = input_schema.len();

        let projection = {
            let source = self
                .binder
                .context
                .source(&table_name)?
                .ok_or(DatabaseError::TableNotFound)?;

            if idents.is_empty() {
                let table_schema = source.schema();
                if input_len > table_schema.len() {
                    return Err(DatabaseError::ValuesLenMismatch(
                        table_schema.len(),
                        input_len,
                    ));
                }
                table_schema[..input_len]
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(position, target_column)| ScalarExpression::Alias {
                        expr: Box::new(ScalarExpression::column_expr(
                            input_schema[position],
                            position,
                        )),
                        alias: AliasType::Name(self.arena.column(target_column).name().to_string()),
                    })
                    .collect::<Vec<_>>()
            } else {
                if input_len != idents.len() {
                    return Err(DatabaseError::ValuesLenMismatch(idents.len(), input_len));
                }
                let mut projection = Vec::with_capacity(idents.len());
                for (position, ident) in idents.iter().enumerate() {
                    let column_name = lower_ident(ident);
                    let column = source.column(&column_name, self.arena).ok_or_else(|| {
                        attach_span_if_absent(
                            DatabaseError::column_not_found(column_name),
                            ident.span,
                        )
                    })?;
                    projection.push(ScalarExpression::Alias {
                        expr: Box::new(ScalarExpression::column_expr(
                            input_schema[position],
                            position,
                        )),
                        alias: AliasType::Name(self.arena.column(column).name().to_string()),
                    });
                }
                projection
            }
        };
        input_plan = self
            .binder
            .bind_project(input_plan, projection, self.arena)?;

        self.binder
            .bind_insert_query(table_name, input_plan, is_overwrite)
    }

    fn update(self, update: &sqlparser::ast::Update) -> Result<LogicalPlan, DatabaseError> {
        self.binder.context.allow_default = true;
        let to = &update.table;
        if let TableFactor::Table { name, .. } = &to.relation {
            let is_joined_update = !to.joins.is_empty();
            let table_name = sql_table_name(name.clone())?;
            self.binder.with_pk(table_name.clone());

            let mut plan = self.binder.bind_table_ref_sql(to, self.arena)?;
            let (target_source, target_offset) =
                Binder::<'a, 'b, T, A>::resolve_source_columns_in_scope(
                    &self.binder.context,
                    &table_name,
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
                let mut bind_assignment = |name: &ObjectName,
                                           expression: ScalarExpression|
                 -> Result<(), DatabaseError> {
                    let ident = single_ident_from_object_name(name)?;

                    let column = {
                        match self.binder.bind_column_ref_from_identifiers(
                            slice::from_ref(&ident),
                            Some(table_name.as_ref()),
                            self.arena,
                        )? {
                            ScalarExpression::ColumnRef { column, .. } => column,
                            _ => {
                                return Err(attach_span_if_absent(
                                    DatabaseError::invalid_column(ident.to_string()),
                                    ident.span,
                                ))
                            }
                        }
                    };

                    let mut expr = if matches!(expression, ScalarExpression::Empty) {
                        let column_catalog = self.arena.column(column);
                        let default_value = column_catalog
                            .default_value()?
                            .ok_or(DatabaseError::DefaultNotExist)?;
                        ScalarExpression::Constant(default_value)
                    } else {
                        expression
                    };
                    let column_catalog = self.arena.column(column);
                    expr = ScalarExpression::type_cast(
                        expr,
                        Cow::Borrowed(column_catalog.datatype()),
                        self.arena,
                    )?;
                    if is_joined_update {
                        UpdateExprTargetRemapper {
                            target_schema: &target_schema,
                            arena: self.arena,
                        }
                        .visit(&mut expr)?;
                    }
                    value_exprs.push((column, expr));
                    Ok(())
                };

                match target {
                    AssignmentTarget::ColumnName(name) => bind_assignment(name, expression)?,
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
                                    bind_assignment(name, expression)?
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
                        ScalarExpression::column_expr(column, target_offset + index)
                    })
                    .collect();
                plan = LogicalPlan::new(
                    Operator::Project(ProjectOperator { exprs }),
                    Childrens::Only(Box::new(plan)),
                );
            }
            self.binder.bind_update(table_name, value_exprs, plan)
        } else {
            Err(DatabaseError::UnsupportedStmt(format!(
                "UPDATE target must be a table: {:?}",
                to.relation
            )))
        }
    }

    fn delete(self, delete: &sqlparser::ast::Delete) -> Result<LogicalPlan, DatabaseError> {
        let from = match &delete.from {
            FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
        };
        let table = from
            .iter()
            .next()
            .ok_or_else(|| DatabaseError::invalid_table("DELETE without FROM"))?;

        if let TableFactor::Table { name, .. } = &table.relation {
            let table_name = sql_table_name(name.clone())?;
            let primary_keys = self
                .binder
                .context
                .table(table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?
                .primary_keys()
                .iter()
                .map(|(_, column)| *column)
                .collect();
            self.binder.with_pk(table_name.clone());
            let mut plan = self.binder.bind_table_ref_sql(table, self.arena)?;

            if let Some(predicate) = delete.selection.as_ref() {
                plan = self.binder.bind_where(plan, predicate, self.arena)?;
            }

            self.binder.bind_delete(table_name, primary_keys, plan)
        } else {
            Err(DatabaseError::UnsupportedStmt(format!(
                "DELETE target must be a table: {:?}",
                table.relation
            )))
        }
    }

    fn analyze(self, analyze: sqlparser::ast::Analyze) -> Result<LogicalPlan, DatabaseError> {
        let table_name = analyze.table_name.ok_or_else(|| {
            DatabaseError::UnsupportedStmt("ANALYZE without table is not supported".to_string())
        })?;
        self.binder
            .bind_analyze(sql_table_name(table_name)?, self.arena)
    }

    fn truncate(self, truncate: sqlparser::ast::Truncate) -> Result<LogicalPlan, DatabaseError> {
        if truncate.table_names.len() != 1 {
            return Err(DatabaseError::UnsupportedStmt(
                "only truncate a single table is supported".to_string(),
            ));
        }
        self.binder.bind_truncate(sql_table_name(
            truncate.table_names.into_iter().next().unwrap().name,
        )?)
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
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
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
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
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
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn where_sql(
        self,
        selection: Option<&Expr>,
    ) -> Result<BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        let predicate = if let Some(predicate) = selection {
            Some(with_query_bind_step!(self.binder, QueryBindStep::Where, {
                self.binder.bind_expr(predicate, self.arena)?
            })?)
        } else {
            None
        };

        self.filter_expr(predicate)
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanFiltered<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn aggregate_sql(
        self,
        group_by: &GroupByExpr,
        having: Option<&Expr>,
        orderby: Option<&[OrderByExpr]>,
    ) -> Result<BindPlanAggregated<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        let group_by = with_query_bind_step!(self.binder, QueryBindStep::Agg, {
            match group_by {
                GroupByExpr::Expressions(group_by_exprs, modifiers) => {
                    if !modifiers.is_empty() {
                        return Err(DatabaseError::UnsupportedStmt(
                            "GROUP BY modifiers are not supported".to_string(),
                        ));
                    }
                    group_by_exprs
                        .iter()
                        .map(|expr| self.binder.bind_expr(expr, self.arena))
                        .collect::<Result<Vec<_>, DatabaseError>>()?
                }
                GroupByExpr::All(_) => {
                    return Err(DatabaseError::UnsupportedStmt(
                        "GROUP BY ALL is not supported".to_string(),
                    ))
                }
            }
        })?;
        let having = having
            .map(|having| {
                with_query_bind_step!(self.binder, QueryBindStep::Having, {
                    self.binder.bind_expr(having, self.arena)?
                })
            })
            .transpose()?;
        self.aggregate(group_by, having, orderby, |binder, arena, orderby| {
            let OrderByExpr { expr, options, .. } = orderby;
            with_query_bind_step!(binder, QueryBindStep::Sort, {
                SortField::new(
                    binder.bind_expr(expr, arena)?,
                    options.asc.is_none_or(|asc| asc),
                    options.nulls_first.unwrap_or(false),
                )
            })
        })
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanHaving<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn distinct_sql(
        self,
        distinct: Option<&Distinct>,
    ) -> Result<BindPlanDistinct<'s, 'a, 'b, 'arena, T, A>, DatabaseError> {
        self.distinct(matches!(distinct, Some(Distinct::Distinct)))
    }
}

impl<'s, 'a: 'b, 'b, 'arena, T, A> BindPlanProjected<'s, 'a, 'b, 'arena, T, A>
where
    T: Transaction,
    A: AsRef<[(&'static str, DataValue)]>,
{
    pub(crate) fn select_into_sql(
        self,
        into: Option<&SelectInto>,
    ) -> Result<BindPlanComplete, DatabaseError> {
        self.insert_into(
            into.map(|SelectInto { name, .. }| lower_case_name(name).map(Into::into))
                .transpose()?,
        )
    }
}

fn sql_table_name(name: ObjectName) -> Result<TableName, DatabaseError> {
    Ok(lower_case_name(&name)?.into())
}

fn sql_table_alias(alias: TableAlias) -> TableAliasInput {
    TableAliasInput {
        name: lower_ident(&alias.name).into(),
        columns: alias
            .columns
            .into_iter()
            .map(|column| lower_ident(&column.name).into_owned())
            .collect(),
    }
}

fn sql_optional_table_alias(alias: Option<TableAlias>) -> Option<TableAliasInput> {
    alias.map(sql_table_alias)
}

impl From<sqlparser::ast::CharLengthUnits> for CharLengthUnits {
    fn from(value: sqlparser::ast::CharLengthUnits) -> Self {
        match value {
            sqlparser::ast::CharLengthUnits::Characters => Self::Characters,
            sqlparser::ast::CharLengthUnits::Octets => Self::Octets,
        }
    }
}

impl From<sqlparser::ast::TrimWhereField> for expression::TrimWhereField {
    fn from(value: sqlparser::ast::TrimWhereField) -> Self {
        match value {
            sqlparser::ast::TrimWhereField::Both => Self::Both,
            sqlparser::ast::TrimWhereField::Leading => Self::Leading,
            sqlparser::ast::TrimWhereField::Trailing => Self::Trailing,
        }
    }
}

impl TryFrom<UnaryOperator> for expression::UnaryOperator {
    type Error = DatabaseError;

    fn try_from(value: UnaryOperator) -> Result<Self, Self::Error> {
        match value {
            UnaryOperator::Plus => Ok(Self::Plus),
            UnaryOperator::Minus => Ok(Self::Minus),
            UnaryOperator::Not => Ok(Self::Not),
            op => Err(DatabaseError::UnsupportedStmt(format!("{op}"))),
        }
    }
}

impl TryFrom<BinaryOperator> for expression::BinaryOperator {
    type Error = DatabaseError;

    fn try_from(value: BinaryOperator) -> Result<Self, Self::Error> {
        match value {
            BinaryOperator::Plus => Ok(Self::Plus),
            BinaryOperator::Minus => Ok(Self::Minus),
            BinaryOperator::Multiply => Ok(Self::Multiply),
            BinaryOperator::Divide => Ok(Self::Divide),
            BinaryOperator::Modulo => Ok(Self::Modulo),
            BinaryOperator::StringConcat => Ok(Self::StringConcat),
            BinaryOperator::Gt => Ok(Self::Gt),
            BinaryOperator::Lt => Ok(Self::Lt),
            BinaryOperator::GtEq => Ok(Self::GtEq),
            BinaryOperator::LtEq => Ok(Self::LtEq),
            BinaryOperator::Spaceship => Ok(Self::Spaceship),
            BinaryOperator::Eq => Ok(Self::Eq),
            BinaryOperator::NotEq => Ok(Self::NotEq),
            BinaryOperator::And => Ok(Self::And),
            BinaryOperator::Or => Ok(Self::Or),
            op => Err(DatabaseError::UnsupportedStmt(format!("{op}"))),
        }
    }
}

impl TryFrom<&Value> for DataValue {
    type Error = DatabaseError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        Ok(match value {
            Value::Number(n, _) => {
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
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone().into(),
            Value::Boolean(b) => (*b).into(),
            Value::Null => Self::Null,
            v => return Err(DatabaseError::UnsupportedStmt(format!("{v:?}"))),
        })
    }
}

impl TryFrom<DataType> for LogicalType {
    type Error = DatabaseError;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        match value {
            DataType::Char(char_len) | DataType::Character(char_len) => {
                let mut len = 1;
                let mut char_unit = None;
                if let Some(char_len) = char_len {
                    match char_len {
                        sqlparser::ast::CharacterLength::IntegerLength { length, unit } => {
                            len = cmp::max(len, length);
                            char_unit = unit;
                        }
                        sqlparser::ast::CharacterLength::Max => {
                            return Err(DatabaseError::UnsupportedStmt(
                                "CHAR(MAX) is not supported".to_string(),
                            ));
                        }
                    }
                }
                Ok(Self::Char(
                    len as u32,
                    char_unit
                        .map(Into::into)
                        .unwrap_or(CharLengthUnits::Characters),
                ))
            }
            DataType::CharVarying(varchar_len)
            | DataType::CharacterVarying(varchar_len)
            | DataType::Varchar(varchar_len) => {
                let mut len = None;
                let mut char_unit = None;
                if let Some(varchar_len) = varchar_len {
                    match varchar_len {
                        sqlparser::ast::CharacterLength::IntegerLength { length, unit } => {
                            len = Some(length as u32);
                            char_unit = unit;
                        }
                        sqlparser::ast::CharacterLength::Max => {
                            return Err(DatabaseError::UnsupportedStmt(
                                "VARCHAR(MAX) is not supported".to_string(),
                            ));
                        }
                    }
                }
                Ok(Self::Varchar(
                    len,
                    char_unit
                        .map(Into::into)
                        .unwrap_or(CharLengthUnits::Characters),
                ))
            }
            DataType::String(_) | DataType::Text => {
                Ok(Self::Varchar(None, CharLengthUnits::Characters))
            }
            DataType::Float(_) | DataType::Float4 | DataType::Float32 | DataType::Real => {
                Ok(Self::Float)
            }
            DataType::Double(_)
            | DataType::DoublePrecision
            | DataType::Float8
            | DataType::Float64 => Ok(Self::Double),
            DataType::TinyInt(_) => Ok(Self::Tinyint),
            DataType::TinyIntUnsigned(_) | DataType::UTinyInt => Ok(Self::UTinyint),
            DataType::SmallInt(_) | DataType::Int2(_) => Ok(Self::Smallint),
            DataType::SmallIntUnsigned(_) | DataType::Int2Unsigned(_) | DataType::USmallInt => {
                Ok(Self::USmallint)
            }
            DataType::Int(_) | DataType::Integer(_) | DataType::Int4(_) | DataType::Int32 => {
                Ok(Self::Integer)
            }
            DataType::IntUnsigned(_)
            | DataType::IntegerUnsigned(_)
            | DataType::Int4Unsigned(_)
            | DataType::Unsigned
            | DataType::UnsignedInteger
            | DataType::UInt32 => Ok(Self::UInteger),
            DataType::BigInt(_) | DataType::Int8(_) | DataType::Int64 => Ok(Self::Bigint),
            DataType::BigIntUnsigned(_)
            | DataType::Int8Unsigned(_)
            | DataType::UBigInt
            | DataType::UInt64 => Ok(Self::UBigint),
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Date => {
                #[cfg(feature = "time")]
                {
                    Ok(Self::Date)
                }
                #[cfg(not(feature = "time"))]
                {
                    Err(DatabaseError::UnsupportedStmt(
                        "time types require the `time` feature".to_string(),
                    ))
                }
            }
            DataType::Datetime(precision) => {
                #[cfg(feature = "time")]
                {
                    if precision.is_some() {
                        return Err(DatabaseError::UnsupportedStmt(
                            "time's precision".to_string(),
                        ));
                    }
                    Ok(Self::DateTime)
                }
                #[cfg(not(feature = "time"))]
                {
                    let _ = precision;
                    Err(DatabaseError::UnsupportedStmt(
                        "time types require the `time` feature".to_string(),
                    ))
                }
            }
            DataType::Time(precision, info) => {
                #[cfg(feature = "time")]
                {
                    match precision {
                        Some(0..5) | None => (),
                        _ => {
                            return Err(DatabaseError::UnsupportedStmt(
                                "time's precision must be less than 5".to_string(),
                            ))
                        }
                    }
                    if !matches!(info, sqlparser::ast::TimezoneInfo::None) {
                        return Err(DatabaseError::UnsupportedStmt(
                            "time's zone is not supported".to_string(),
                        ));
                    }
                    Ok(Self::Time(precision))
                }
                #[cfg(not(feature = "time"))]
                {
                    let _ = (precision, info);
                    Err(DatabaseError::UnsupportedStmt(
                        "time types require the `time` feature".to_string(),
                    ))
                }
            }
            DataType::Timestamp(precision, info) => {
                #[cfg(feature = "time")]
                {
                    let mut zone = false;
                    match precision {
                        Some(3 | 6 | 9) | None => (),
                        _ => {
                            return Err(DatabaseError::UnsupportedStmt(
                                "timestamp's precision must be 3,6,9".to_string(),
                            ))
                        }
                    }
                    if matches!(info, sqlparser::ast::TimezoneInfo::WithTimeZone) {
                        zone = true;
                    }
                    Ok(Self::TimeStamp(precision, zone))
                }
                #[cfg(not(feature = "time"))]
                {
                    let _ = (precision, info);
                    Err(DatabaseError::UnsupportedStmt(
                        "time types require the `time` feature".to_string(),
                    ))
                }
            }
            DataType::Decimal(info)
            | DataType::DecimalUnsigned(info)
            | DataType::Dec(info)
            | DataType::DecUnsigned(info)
            | DataType::Numeric(info) => {
                #[cfg(feature = "decimal")]
                {
                    match info {
                        sqlparser::ast::ExactNumberInfo::None => Ok(Self::Decimal(None, None)),
                        sqlparser::ast::ExactNumberInfo::Precision(p) => {
                            Ok(Self::Decimal(Some(p as u8), None))
                        }
                        sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                            Ok(Self::Decimal(Some(p as u8), Some(s as u8)))
                        }
                    }
                }
                #[cfg(not(feature = "decimal"))]
                {
                    let _ = info;
                    Err(DatabaseError::UnsupportedStmt(
                        "DECIMAL requires the `decimal` feature".to_string(),
                    ))
                }
            }
            other => Err(DatabaseError::UnsupportedStmt(format!(
                "unsupported data type: {other}"
            ))),
        }
    }
}

fn sql_object_name(name: ObjectName) -> Result<String, DatabaseError> {
    Ok(lower_case_name(&name)?.into_owned())
}

pub(super) fn lower_ident(ident: &Ident) -> Cow<'_, str> {
    let value = &ident.value;

    if value.chars().any(char::is_uppercase) {
        Cow::Owned(value.to_lowercase())
    } else {
        Cow::Borrowed(value)
    }
}

fn lower_name_part(part: &ObjectNamePart) -> Result<Cow<'_, str>, DatabaseError> {
    part.as_ident()
        .map(lower_ident)
        .ok_or_else(|| attach_span_if_absent(DatabaseError::invalid_table(part.to_string()), part))
}

/// Convert an object name into lower case.
pub(super) fn lower_case_name(name: &ObjectName) -> Result<Cow<'_, str>, DatabaseError> {
    if name.0.len() == 1 {
        return lower_name_part(&name.0[0]);
    }
    Err(attach_span_if_absent(
        DatabaseError::invalid_table(name.to_string()),
        name,
    ))
}

fn single_ident_from_object_name(name: &ObjectName) -> Result<Ident, DatabaseError> {
    if name.0.len() != 1 {
        return Err(attach_span_if_absent(
            DatabaseError::invalid_column(name.to_string()),
            name,
        ));
    }
    match name.0.first() {
        Some(ObjectNamePart::Identifier(ident)) => Ok(ident.clone()),
        Some(part) => Err(DatabaseError::invalid_column(part.to_string())),
        None => Err(DatabaseError::invalid_column(String::new())),
    }
}

fn sql_index_name(name: ObjectName) -> Result<(TableName, String), DatabaseError> {
    let table_name = name.0.first().ok_or_else(|| {
        attach_span_if_absent(DatabaseError::invalid_table(name.to_string()), &name)
    })?;
    let index_name = name.0.get(1).ok_or(DatabaseError::InvalidIndex)?;

    Ok((
        lower_name_part(table_name)?.into(),
        lower_name_part(index_name)?.into_owned(),
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

#[cfg(feature = "copy")]
fn copy_ext_source(
    target: CopyTarget,
    options: Vec<CopyOption>,
) -> Result<ExtSource, DatabaseError> {
    Ok(ExtSource {
        path: match target {
            CopyTarget::File { filename } => filename.into(),
            t => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "copy target: {t:?}"
                )))
            }
        },
        format: copy_file_format(options)?,
    })
}

#[cfg(feature = "copy")]
fn copy_file_format(options: Vec<CopyOption>) -> Result<FileFormat, DatabaseError> {
    let mut delimiter = ',';
    let mut quote = '"';
    let mut escape = None;
    let mut header = false;
    for opt in options {
        match opt {
            CopyOption::Format(fmt) => {
                debug_assert_eq!(fmt.value.to_lowercase(), "csv", "only support CSV format")
            }
            CopyOption::Delimiter(c) => delimiter = c,
            CopyOption::Header(b) => header = b,
            CopyOption::Quote(c) => quote = c,
            CopyOption::Escape(c) => escape = Some(c),
            o => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "copy option: {o:?}"
                )))
            }
        }
    }
    Ok(FileFormat::Csv {
        delimiter,
        quote,
        escape,
        header,
    })
}

impl<'a, 'parent, T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'a, 'parent, T, A> {
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
            TableFactor::Table { name, alias, .. } => self.bind_base_table_ref(
                joint_type,
                sql_table_name(name.clone())?,
                sql_optional_table_alias(alias.clone()),
                arena,
            ),
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let mut binder = Binder::new(self.context.fork(), self.args, Some(&self.context));
                let plan = binder.bind_query(subquery, arena)?;
                self.bind_derived_source(
                    plan,
                    sql_optional_table_alias(alias.clone()),
                    joint_type,
                    arena,
                )
            }
            TableFactor::TableFunction { expr, alias } => {
                let expr = self.bind_expr(expr, arena)?;
                self.bind_table_function_source(
                    expr,
                    sql_optional_table_alias(alias.clone()),
                    joint_type,
                    arena,
                )
            }
            table => Err(DatabaseError::UnsupportedStmt(format!("{table:#?}"))),
        }
    }

    fn bind_join_sql(
        &mut self,
        left: LogicalPlan,
        join: &Join,
        arena: &mut PlanArena,
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
        let (right, context) = {
            let mut binder = Binder::new(self.context.fork_empty(), self.args, Some(&self.context));
            let right = binder.bind_single_table_ref_sql(relation, Some(join_type), arena)?;
            (right, binder.context)
        };
        self.extend(context);

        let constraint = match joint_condition {
            Some(constraint) => self.bind_join_constraint_sql(constraint, arena)?,
            None => JoinConstraintInput::None,
        };

        self.bind_join_plans(left, right, join_type, constraint, arena)
    }

    fn bind_join_constraint_sql(
        &mut self,
        constraint: &JoinConstraint,
        arena: &mut PlanArena,
    ) -> Result<JoinConstraintInput, DatabaseError> {
        match constraint {
            JoinConstraint::On(expr) => Ok(JoinConstraintInput::On(self.bind_expr(expr, arena)?)),
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

    fn parse_like_escape_char(escape_char: &Option<Value>) -> Result<Option<char>, DatabaseError> {
        match escape_char {
            None => Ok(None),
            Some(value) => match value {
                Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
                    let mut chars = s.chars();
                    let ch = chars.next().ok_or(DatabaseError::InvalidValue(
                        "escape character must not be empty".to_string(),
                    ))?;
                    if chars.next().is_some() {
                        return Err(DatabaseError::InvalidValue(
                            "escape character must be a single character".to_string(),
                        ));
                    }
                    Ok(Some(ch))
                }
                _ => Err(DatabaseError::InvalidValue(
                    "escape character must be a quoted string".to_string(),
                )),
            },
        }
    }

    pub(crate) fn bind_expr(
        &mut self,
        expr: &Expr,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let expr_span = expr.span();
        match expr {
            Expr::Identifier(ident) => {
                self.bind_column_ref_from_identifiers(slice::from_ref(ident), None, arena)
            }
            Expr::CompoundIdentifier(idents) => {
                self.bind_column_ref_from_identifiers(idents, None, arena)
            }
            Expr::BinaryOp { left, right, op } => {
                let left_expr = self.bind_expr(left, arena)?;
                let right_expr = self.bind_expr(right, arena)?;
                self.bind_binary_op_expr(left_expr, right_expr, op.clone().try_into()?, arena)
            }
            Expr::Value(v) => {
                let value = if let Value::Placeholder(name) = &v.value {
                    self.args
                        .as_ref()
                        .iter()
                        .find_map(|(key, value)| (key == name).then(|| value.clone()))
                        .ok_or_else(|| {
                            attach_span_if_absent(
                                DatabaseError::parameter_not_found(name.to_string()),
                                v,
                            )
                        })?
                } else {
                    (&v.value)
                        .try_into()
                        .map_err(|err| attach_span_if_absent(err, v))?
                };
                Ok(ScalarExpression::Constant(value))
            }
            Expr::Function(func) => self.bind_function_sql(func, arena),
            Expr::Nested(expr) => self.bind_expr(expr, arena),
            Expr::UnaryOp { expr, op } => {
                let expr = self.bind_expr(expr, arena)?;
                self.bind_unary_op_expr(expr, (*op).try_into()?, arena)
            }
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
                any: _,
            } => {
                let left_expr = Box::new(self.bind_expr(expr, arena)?);
                let right_expr = Box::new(self.bind_expr(pattern, arena)?);
                let escape_char = Self::parse_like_escape_char(escape_char)?;
                let op = if *negated {
                    expression::BinaryOperator::NotLike(escape_char)
                } else {
                    expression::BinaryOperator::Like(escape_char)
                };
                Ok(ScalarExpression::Binary {
                    op,
                    left_expr,
                    right_expr,
                    evaluator: None,
                    ty: LogicalType::Boolean,
                })
            }
            Expr::IsNull(expr) => Ok(ScalarExpression::IsNull {
                negated: false,
                expr: Box::new(self.bind_expr(expr, arena)?),
            }),
            Expr::IsNotNull(expr) => Ok(ScalarExpression::IsNull {
                negated: true,
                expr: Box::new(self.bind_expr(expr, arena)?),
            }),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let args = list
                    .iter()
                    .map(|expr| self.bind_expr(expr, arena))
                    .try_collect()?;
                Ok(ScalarExpression::In {
                    negated: *negated,
                    expr: Box::new(self.bind_expr(expr, arena)?),
                    args,
                })
            }
            Expr::Cast {
                expr, data_type, ..
            } => ScalarExpression::type_cast(
                self.bind_expr(expr, arena)?,
                Cow::Owned(LogicalType::try_from(data_type.clone())?),
                arena,
            ),
            Expr::TypedString(TypedString {
                data_type, value, ..
            }) => {
                let logical_type = LogicalType::try_from(data_type.clone())?;
                let raw = value.clone().into_string().ok_or_else(|| {
                    DatabaseError::InvalidValue("typed string literal must be a string".to_string())
                })?;
                let value = DataValue::Utf8 {
                    value: raw,
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
                .cast(&logical_type)
                .map_err(|err| attach_span_if_absent(err, expr_span))?;

                Ok(ScalarExpression::Constant(value))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(ScalarExpression::Between {
                negated: *negated,
                expr: Box::new(self.bind_expr(expr, arena)?),
                left_expr: Box::new(self.bind_expr(low, arena)?),
                right_expr: Box::new(self.bind_expr(high, arena)?),
            }),
            Expr::Substring {
                expr,
                substring_for,
                substring_from,
                ..
            } => {
                let mut for_expr = None;
                let mut from_expr = None;

                if let Some(expr) = substring_for {
                    for_expr = Some(Box::new(self.bind_expr(expr, arena)?))
                }
                if let Some(expr) = substring_from {
                    from_expr = Some(Box::new(self.bind_expr(expr, arena)?))
                }

                Ok(ScalarExpression::SubString {
                    expr: Box::new(self.bind_expr(expr, arena)?),
                    for_expr,
                    from_expr,
                })
            }
            Expr::Position { expr, r#in } => Ok(ScalarExpression::Position {
                expr: Box::new(self.bind_expr(expr, arena)?),
                in_expr: Box::new(self.bind_expr(r#in, arena)?),
            }),
            Expr::Trim {
                expr,
                trim_what,
                trim_where,
                ..
            } => {
                let mut trim_what_expr = None;
                if let Some(trim_what) = trim_what {
                    trim_what_expr = Some(Box::new(self.bind_expr(trim_what, arena)?))
                }
                Ok(ScalarExpression::Trim {
                    expr: Box::new(self.bind_expr(expr, arena)?),
                    trim_what_expr,
                    trim_where: (*trim_where).map(Into::into),
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
                &BinaryOperator::Eq,
                subquery,
                arena,
            ),
            Expr::Tuple(exprs) => {
                let mut bound_exprs = Vec::with_capacity(exprs.len());

                for expr in exprs {
                    bound_exprs.push(self.bind_expr(expr, arena)?);
                }
                Ok(ScalarExpression::Tuple(bound_exprs))
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let fn_check_ty = |ty: &mut LogicalType, result_ty| {
                    if result_ty != LogicalType::SqlNull {
                        if ty == &LogicalType::SqlNull {
                            *ty = result_ty;
                        } else if ty != &result_ty {
                            return Err(DatabaseError::Incomparable(ty.clone(), result_ty));
                        }
                    }

                    Ok(())
                };
                let mut operand_expr = None;
                let mut ty = LogicalType::SqlNull;
                if let Some(expr) = operand {
                    operand_expr = Some(Box::new(self.bind_expr(expr, arena)?));
                }
                let mut expr_pairs = Vec::with_capacity(conditions.len());
                for when in conditions {
                    let result = self.bind_expr(&when.result, arena)?;
                    let result_ty = result.return_type(arena).into_owned();

                    fn_check_ty(&mut ty, result_ty)?;
                    expr_pairs.push((self.bind_expr(&when.condition, arena)?, result))
                }

                let mut else_expr = None;
                if let Some(expr) = else_result {
                    let temp_expr = Box::new(self.bind_expr(expr, arena)?);
                    let else_ty = temp_expr.return_type(arena).into_owned();

                    fn_check_ty(&mut ty, else_ty)?;
                    else_expr = Some(temp_expr);
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
                right,
                ..
            } => self.bind_quantified_op(MarkApplyQuantifier::Any, left, compare_op, right, arena),
            Expr::AllOp {
                left,
                compare_op,
                right,
            } => self.bind_quantified_op(MarkApplyQuantifier::All, left, compare_op, right, arena),
            expr => Err(DatabaseError::UnsupportedStmt(expr.to_string())),
        }
    }

    fn bind_quantified_op(
        &mut self,
        quantifier: MarkApplyQuantifier,
        left: &Expr,
        compare_op: &BinaryOperator,
        right: &Expr,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let Expr::Subquery(subquery) = right else {
            return Err(DatabaseError::UnsupportedStmt(format!(
                "{quantifier:?} only supports subquery operands"
            )));
        };

        self.bind_quantified_subquery(quantifier, false, left, compare_op, subquery, arena)
    }

    fn bind_quantified_subquery(
        &mut self,
        quantifier: MarkApplyQuantifier,
        negated: bool,
        expr: &Expr,
        compare_op: &BinaryOperator,
        subquery: &Query,
        arena: &mut PlanArena,
    ) -> Result<ScalarExpression, DatabaseError> {
        let left_expr = self.bind_expr(expr, arena)?;
        self.bind_quantified_subquery_plan(
            quantifier,
            negated,
            left_expr,
            compare_op.clone().try_into()?,
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
        let func_span = func.span();
        let Function { name, args, .. } = func;
        let (func_args, is_distinct) = match args {
            FunctionArguments::List(args) => (
                args.args.as_slice(),
                matches!(args.duplicate_treatment, Some(DuplicateTreatment::Distinct)),
            ),
            FunctionArguments::None => (&[][..], false),
            FunctionArguments::Subquery(_) => {
                return Err(DatabaseError::UnsupportedStmt(
                    "subquery function args are not supported".to_string(),
                ))
            }
        };
        let mut args = Vec::with_capacity(func_args.len());

        for arg in func_args {
            let arg_expr = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::ExprNamed { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg_expr {
                FunctionArgExpr::Expr(expr) => args.push(self.bind_expr(expr, arena)?),
                FunctionArgExpr::Wildcard => args.push(Self::wildcard_expr()),
                expr => {
                    return Err(DatabaseError::UnsupportedStmt(format!(
                        "function arg: {expr:#?}"
                    )))
                }
            }
        }
        let function_name = name.to_string().to_lowercase();

        self.bind_function_call(function_name, args, is_distinct, arena)
            .map_err(|err| attach_span_if_absent(err, func_span))
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
                set_quantifier,
                left,
                right,
            } => self.bind_set_operation(op, set_quantifier, left, right, arena),
            expr => Err(DatabaseError::UnsupportedStmt(format!(
                "set expression: {expr:?}"
            ))),
        }
    }

    fn bind_set_operation(
        &mut self,
        op: &SetOperator,
        set_quantifier: &SetQuantifier,
        left: &SetExpr,
        right: &SetExpr,
        arena: &mut PlanArena,
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
        let op = match op {
            SetOperator::Union => SetOperatorKind::Union,
            SetOperator::Except => SetOperatorKind::Except,
            SetOperator::Intersect => SetOperatorKind::Intersect,
            op => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "set operator: {op:?}"
                )))
            }
        };
        let left_plan = {
            let mut left_binder = Binder::new(self.context.fork(), self.args, self.parent);
            let plan = left_binder.bind_set_expr(left, arena)?;
            if left_binder.context.has_outer_refs() {
                self.context.mark_outer_ref();
            }
            plan
        };

        let right_plan = {
            let mut right_binder = Binder::new(self.context.fork(), self.args, self.parent);
            let plan = right_binder.bind_set_expr(right, arena)?;
            if right_binder.context.has_outer_refs() {
                self.context.mark_outer_ref();
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
            projection,
            from,
            selection,
            group_by,
            having,
            distinct,
            into,
            ..
        } = select;
        Ok(self
            .build_plan(arena)
            .from_sql(from)?
            .select_list_from_sql(projection)?
            .where_sql(selection.as_ref())?
            .aggregate_sql(group_by, having.as_ref(), orderby)?
            .having()?
            .distinct_sql(distinct.as_ref())?
            .order_by()?
            .project()?
            .select_into_sql(into.as_ref())?
            .finish())
    }

    /// FIXME: temp values need to register BindContext.bind_table
    fn bind_temp_values(
        &mut self,
        expr_rows: &[Vec<Expr>],
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        let values_len = expr_rows[0].len();

        let mut inferred_types: Vec<Option<LogicalType>> = vec![None; values_len];
        let mut rows = Vec::with_capacity(expr_rows.len());

        for expr_row in expr_rows {
            if expr_row.len() != values_len {
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }

            let mut row = Vec::with_capacity(values_len);

            for (col_index, expr) in expr_row.iter().enumerate() {
                let mut expression = self.bind_expr(expr, arena)?;
                ConstantCalculator::new(arena).visit(&mut expression)?;

                if let ScalarExpression::Constant(value) = expression {
                    let value_type = value.logical_type();

                    inferred_types[col_index] = match &inferred_types[col_index] {
                        Some(existing) => {
                            Some(LogicalType::max_logical_type(existing, &value_type)?.into_owned())
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

        let value_name = arena.temp_table();
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
                Ok(arena.alloc_column(column_ref))
            })
            .collect::<Result<_, DatabaseError>>()?;

        Ok(self.bind_values(rows, column_refs))
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
                ScalarExpression::column_expr(*column, position),
            );
        }

        let sort_fields = self
            .extract_having_orderby_aggregate_exprs(None, Some(orderbys), |binder, orderby| {
                let OrderByExpr { expr, options, .. } = orderby;
                Ok(SortField::new(
                    binder.bind_expr(expr, arena)?,
                    options.asc.is_none_or(|asc| asc),
                    options.nulls_first.unwrap_or(false),
                ))
            })?
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
            self.bind_expr(predicate, arena)?
        })?;

        self.bind_where_expr(children, predicate, arena)
    }

    pub(crate) fn normalize_select_item(
        &mut self,
        items: &[SelectItem],
        arena: &mut PlanArena,
    ) -> Result<Vec<ScalarExpression>, DatabaseError> {
        let mut select_items = vec![];

        for item in items {
            match item {
                SelectItem::UnnamedExpr(expr) => select_items.push(self.bind_expr(expr, arena)?),
                SelectItem::ExprWithAlias { expr, alias } => {
                    let expr = self.bind_expr(expr, arena)?;
                    let alias_name = lower_ident(alias).into_owned();

                    self.context
                        .add_alias(None, alias_name.clone(), expr.clone());

                    select_items.push(ScalarExpression::Alias {
                        expr: Box::new(expr),
                        alias: AliasType::Name(alias_name),
                    });
                }
                SelectItem::Wildcard(_) => {
                    let visible_names = self
                        .context
                        .bind_table
                        .iter()
                        .filter(|bound_source| {
                            !Self::is_joined_values_source(
                                bound_source.join_type,
                                &bound_source.source,
                                arena,
                            )
                        })
                        .map(|bound_source| bound_source.visible_name())
                        .unique()
                        .cloned()
                        .collect_vec();
                    for visible_name in visible_names {
                        Self::bind_table_column_refs(
                            &self.context,
                            arena,
                            &mut select_items,
                            visible_name,
                            false,
                        )?;
                    }
                }
                SelectItem::QualifiedWildcard(table_name, _) => {
                    let table_name: TableName = match table_name {
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
                        arena,
                        &mut select_items,
                        table_name,
                        true,
                    )?;
                }
            };
        }

        Ok(select_items)
    }

    pub(crate) fn bind_query(
        &mut self,
        query: &Query,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
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
        let is_plain_select = matches!(query.body.as_ref(), SetExpr::Select(_));
        let mut plan = match query.body.as_ref() {
            SetExpr::Select(select) => self.bind_select(select, order_by_exprs, arena),
            SetExpr::Query(query) => self.bind_query(query, arena),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.bind_set_operation(op, set_quantifier, left, right, arena),
            SetExpr::Values(values) => self.bind_temp_values(&values.rows, arena),
            expr => {
                return Err(DatabaseError::UnsupportedStmt(format!(
                    "query body: {expr:?}"
                )))
            }
        }?;

        if !is_plain_select {
            if let Some(order_by_exprs) = order_by_exprs {
                plan = self.bind_top_level_orderby(plan, order_by_exprs, arena)?;
            }
        }

        if let Some(limit_clause) = &query.limit_clause {
            plan = self.bind_limit(plan, limit_clause, arena)?;
        }

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
        match bound_expr {
            ScalarExpression::Constant(dv) => match &dv {
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
        limit: &LimitClause,
        arena: &mut PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
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

                if let Some(limit_ast) = limit_expr {
                    limit_value = Some(self.bind_non_negative_limit_value(limit_ast, arena)?);
                }

                if let Some(offset_ast) = offset_expr {
                    offset_value =
                        Some(self.bind_non_negative_limit_value(&offset_ast.value, arena)?);
                }
            }
            LimitClause::OffsetCommaLimit {
                offset: offset_expr,
                limit: limit_expr,
            } => {
                limit_value = Some(self.bind_non_negative_limit_value(limit_expr, arena)?);
                offset_value = Some(self.bind_non_negative_limit_value(offset_expr, arena)?);
            }
        }

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
        Ok(self.build_statement(arena).statement(stmt)?.finish())
    }
}
