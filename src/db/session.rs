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

use super::*;
use sqlparser::ast::{ContextModifier, Expr, Set, Value};

/// Runs SQL with `BEGIN`/`START TRANSACTION`, `COMMIT` and `ROLLBACK` as statements.
///
/// Outside a transaction each statement commits on its own, as with [`Database::run`]; inside
/// one, statements run as with [`Transaction::run`], and a failed statement leaves the
/// transaction open with whatever it wrote. DDL and `ANALYZE` still go through
/// [`Database::ddl`]/[`Database::analyze`]. `COMMIT`/`ROLLBACK` without a transaction do
/// nothing, and dropping the session rolls back. Nested `BEGIN` and `SET autocommit` follow the
/// database's dialect.
///
/// While a transaction is open, send every statement through the session: on single-writer
/// storage such as LMDB, writing through the [`Database`] on the same thread waits for it.
pub struct Session<'db, S: Storage, D: SqlDialect = GenericSqlDialect> {
    database: &'db Database<S, D>,
    transaction: Option<Transaction<'db, S, D>>,
    autocommit: bool,
}

impl<S: Storage, D: SqlDialect> Database<S, D> {
    /// Opens a [`Session`] with autocommit on.
    pub fn session(&self) -> Session<'_, S, D> {
        Session {
            database: self,
            transaction: None,
            autocommit: true,
        }
    }
}

#[maybe_async]
impl<'db, S: Storage, D: SqlDialect> Session<'db, S, D> {
    /// Runs one statement; transaction control statements return no rows.
    pub async fn run<'a>(
        &'a mut self,
        sql: &str,
    ) -> Result<SessionIter<'a, 'db, S, D>, DatabaseError> {
        let mut statements = self
            .database
            .prepare_all(sql)
            .map_err(|err| err.with_sql_context(sql))?;
        if statements.len() != 1 {
            return Err(DatabaseError::UnsupportedStmt(
                "a session runs one statement at a time".to_string(),
            )
            .with_sql_context(sql));
        }
        let statement = statements.pop().ok_or(DatabaseError::EmptyStatement)?;
        self.run_statement(sql, statement)
            .await
            .map_err(|err| err.with_sql_context(sql))
    }

    async fn run_statement<'a>(
        &'a mut self,
        sql: &str,
        statement: Statement,
    ) -> Result<SessionIter<'a, 'db, S, D>, DatabaseError> {
        match &statement {
            Statement::StartTransaction {
                modes,
                modifier,
                statements,
                exception,
                ..
            } => {
                if !modes.is_empty()
                    || modifier.is_some()
                    || !statements.is_empty()
                    || exception.is_some()
                {
                    return Err(DatabaseError::UnsupportedStmt(statement.to_string()));
                }
                self.begin().await?;
                return Ok(self.empty());
            }
            Statement::Commit {
                chain: false,
                end: false,
                modifier: None,
            } => {
                self.finish(true).await?;
                return Ok(self.empty());
            }
            Statement::Rollback {
                chain: false,
                savepoint: None,
            } => {
                self.finish(false).await?;
                return Ok(self.empty());
            }
            Statement::Commit { .. } | Statement::Rollback { .. } => {
                return Err(DatabaseError::UnsupportedStmt(statement.to_string()));
            }
            _ => (),
        }
        if let Some(autocommit) = self.set_autocommit_value(&statement)? {
            if autocommit && !self.autocommit {
                self.finish(true).await?;
            }
            self.autocommit = autocommit;
            return Ok(self.empty());
        }

        if self.transaction.is_none() && !self.autocommit {
            self.begin().await?;
        }
        match self.transaction.as_mut() {
            Some(transaction) => Ok(SessionIter {
                inner: SessionIterInner::Transaction(Box::new(
                    transaction.run_statements(sql, vec![statement]).await?,
                )),
            }),
            None => Ok(SessionIter {
                inner: SessionIterInner::Database(
                    self.database.run_statements(sql, vec![statement]).await?,
                ),
            }),
        }
    }

    fn empty<'a>(&self) -> SessionIter<'a, 'db, S, D> {
        SessionIter {
            inner: SessionIterInner::Empty(&self.database.state),
        }
    }

    async fn begin(&mut self) -> Result<(), DatabaseError> {
        if self.transaction.is_some() {
            if !self
                .database
                .state
                .dialect()
                .begin_commits_active_transaction()
            {
                return Err(DatabaseError::UnsupportedStmt(
                    "BEGIN inside an active transaction".to_string(),
                ));
            }
            self.finish(true).await?;
        }
        self.transaction = Some(self.database.new_transaction(false)?);
        Ok(())
    }

    async fn finish(&mut self, commit: bool) -> Result<(), DatabaseError> {
        match self.transaction.take() {
            Some(transaction) if commit => transaction.commit().await,
            // Dropping the transaction rolls it back.
            _ => Ok(()),
        }
    }

    fn set_autocommit_value(&self, statement: &Statement) -> Result<Option<bool>, DatabaseError> {
        let Statement::Set(Set::SingleAssignment {
            scope: None | Some(ContextModifier::Session),
            hivevar: false,
            variable,
            values,
        }) = statement
        else {
            return Ok(None);
        };
        if !variable.to_string().eq_ignore_ascii_case("autocommit")
            || !self.database.state.dialect().supports_set_autocommit()
        {
            return Ok(None);
        }
        let value = match values.as_slice() {
            [Expr::Value(value)] => match &value.value {
                Value::Number(n, _) if n == "0" => Some(false),
                Value::Number(n, _) if n == "1" => Some(true),
                Value::Boolean(value) => Some(*value),
                _ => None,
            },
            [Expr::Identifier(ident)] if ident.value.eq_ignore_ascii_case("on") => Some(true),
            [Expr::Identifier(ident)] if ident.value.eq_ignore_ascii_case("off") => Some(false),
            _ => None,
        };
        value.map(Some).ok_or_else(|| {
            DatabaseError::UnsupportedStmt("autocommit must be 0, 1, ON or OFF".to_string())
        })
    }

    pub fn autocommit(&self) -> bool {
        self.autocommit
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }
}

/// Result iterator returned by [`Session::run`].
pub struct SessionIter<'a, 'db, S: Storage + 'db, D: SqlDialect = GenericSqlDialect> {
    inner: SessionIterInner<'a, 'db, S, D>,
}

enum SessionIterInner<'a, 'db, S: Storage + 'db, D: SqlDialect> {
    Empty(&'db State<S, D>),
    Transaction(Box<TransactionIter<'a, 'a, S::TransactionType<'db>>>),
    Database(DatabaseIter<'db, S>),
}

#[maybe_async]
impl<'a, 'db, S: Storage + 'db, D: SqlDialect> SessionIter<'a, 'db, S, D> {
    /// Empty for a transaction control statement.
    pub fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        match &self.inner {
            SessionIterInner::Empty(state) => f(&SchemaView::new(
                &Schema::new(),
                state.table_arena().borrow(),
            )),
            SessionIterInner::Transaction(iter) => iter.schema(f),
            SessionIterInner::Database(iter) => iter.schema(f),
        }
    }

    pub async fn next_tuple<R: MaybeSend>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut ResultRow<'_, '_>) -> R + MaybeSend,
    ) -> Result<Option<R>, DatabaseError> {
        match &mut self.inner {
            SessionIterInner::Empty(_) => Ok(None),
            SessionIterInner::Transaction(iter) => iter.next_tuple(f).await,
            SessionIterInner::Database(iter) => iter.next_tuple(f).await,
        }
    }

    /// A statement outside a transaction commits here.
    pub async fn done(self) -> Result<(), DatabaseError> {
        match self.inner {
            SessionIterInner::Empty(_) => Ok(()),
            SessionIterInner::Transaction(iter) => iter.done().await,
            SessionIterInner::Database(iter) => iter.done().await,
        }
    }
}

#[maybe_async]
impl<'db, S: Storage + 'db, D: SqlDialect> ResultIter for SessionIter<'_, 'db, S, D> {
    fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        SessionIter::schema(self, f)
    }

    async fn next_tuple<R: MaybeSend>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut ResultRow<'_, '_>) -> R + MaybeSend,
    ) -> Result<Option<R>, DatabaseError> {
        SessionIter::next_tuple(self, f).await
    }

    async fn done(self) -> Result<(), DatabaseError> {
        SessionIter::done(self).await
    }
}
