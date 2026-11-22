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

/// Runs SQL with `BEGIN`/`START TRANSACTION`, `COMMIT` and `ROLLBACK` as statements.
///
/// Outside a transaction each statement commits on its own, as with [`Database::run`]; inside
/// one, statements run as with [`Transaction::run`], and a failed statement leaves the
/// transaction open with whatever it wrote. DDL and `ANALYZE` still go through
/// [`Database::ddl`]/[`Database::analyze`]. `COMMIT`/`ROLLBACK` without a transaction do
/// nothing, and dropping the session rolls back. Nested `BEGIN` follows
/// [`SqlDialect::begin_commits_active_transaction`]. With `SET autocommit = 0`, which needs a
/// dialect that parses it, a statement outside a transaction opens one that stays open until
/// `COMMIT` or `ROLLBACK`, and `SET autocommit = 1` commits it.
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
            Statement::StartTransaction => {
                self.begin().await?;
                return Ok(self.empty());
            }
            Statement::Commit => {
                self.finish(true).await?;
                return Ok(self.empty());
            }
            Statement::Rollback => {
                self.finish(false).await?;
                return Ok(self.empty());
            }
            Statement::SetAutocommit(autocommit) => {
                if *autocommit && !self.autocommit {
                    self.finish(true).await?;
                }
                self.autocommit = *autocommit;
                return Ok(self.empty());
            }
            _ => (),
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

#[cfg(all(test, feature = "parser", not(target_arch = "wasm32")))]
#[maybe_async]
mod tests {
    use crate::db::{DataBaseBuilder, Database, DatabaseError, Session, Statement};
    use crate::dialect::SqlDialect;
    use crate::parser::generic::SqlParser;
    use crate::storage::Storage;
    use sqlparser::dialect::GenericDialect;

    #[derive(Debug)]
    struct SessionDialect;

    impl SqlDialect for SessionDialect {
        fn name(&self) -> &str {
            "test_session"
        }

        fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError> {
            SqlParser::new(&GenericDialect {})
                .with_set_autocommit()
                .parse(sql)
        }

        fn begin_commits_active_transaction(&self) -> bool {
            true
        }
    }

    async fn exec<S: Storage, D: SqlDialect>(
        session: &mut Session<'_, S, D>,
        sql: &str,
    ) -> Result<(), DatabaseError> {
        session.run(sql).await?.done().await
    }

    async fn count<S: Storage, D: SqlDialect>(db: &Database<S, D>) -> Result<usize, DatabaseError> {
        let mut iter = db.run("select id from t1").await?;
        let mut rows = 0;
        while iter.next_tuple(|_, _| ()).await?.is_some() {
            rows += 1;
        }
        iter.done().await?;
        Ok(rows)
    }

    #[test]
    async fn generic_session_rejects_nested_begin_and_autocommit() -> Result<(), DatabaseError> {
        let mut db = DataBaseBuilder::path(".").build_in_memory().await?;
        db.ddl("create table t1 (id int primary key)").await?;
        let mut session = db.session();

        assert!(exec(&mut session, "set autocommit = 0").await.is_err());
        exec(&mut session, "begin").await?;
        exec(&mut session, "insert into t1 values (1)").await?;
        assert!(exec(&mut session, "begin").await.is_err());
        assert!(session.in_transaction());
        exec(&mut session, "commit").await?;
        drop(session);
        assert_eq!(count(&db).await?, 1);

        Ok(())
    }

    #[test]
    async fn dialect_session_hooks() -> Result<(), DatabaseError> {
        let mut db = DataBaseBuilder::path(".")
            .dialect(SessionDialect)
            .build_in_memory()
            .await?;
        db.ddl("create table t1 (id int primary key)").await?;
        let mut session = db.session();

        exec(&mut session, "begin").await?;
        exec(&mut session, "insert into t1 values (1)").await?;
        exec(&mut session, "begin").await?;
        assert!(session.in_transaction());
        exec(&mut session, "commit").await?;

        exec(&mut session, "set autocommit = 0").await?;
        assert!(!session.autocommit());
        exec(&mut session, "insert into t1 values (2)").await?;
        assert!(session.in_transaction());
        exec(&mut session, "set autocommit = 1").await?;
        assert!(!session.in_transaction());
        drop(session);
        assert_eq!(count(&db).await?, 2);

        Ok(())
    }
}
