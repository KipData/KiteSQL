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

use super::kite::{KiteBackend, KiteTransaction, KiteTxnResult};
use super::sqlite::{SqliteBackend, SqliteResult, SqliteTransaction};
use super::{
    BackendControl, BackendTransaction, DbParam, PreparedStatement, QueryResult, SimpleExecutor,
    StatementSpec,
};
use crate::{TpccError, STOCK_LEVEL_DISTINCT_SQL, STOCK_LEVEL_DISTINCT_SQLITE};
use kite_sql::types::tuple::Tuple;
use std::borrow::Cow;

pub struct DualBackend {
    kite: KiteBackend,
    sqlite: SqliteBackend,
}

impl DualBackend {
    pub fn new(path: &str) -> Result<Self, TpccError> {
        Ok(Self {
            kite: KiteBackend::new(path)?,
            sqlite: SqliteBackend::new_memory()?,
        })
    }
}

impl BackendControl for DualBackend {
    type Transaction<'a>
        = DualTransaction<'a>
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<PreparedStatement>>, TpccError> {
        self.kite.prepare_statements(specs)
    }

    fn new_transaction(&self) -> Result<Self::Transaction<'_>, TpccError> {
        Ok(DualTransaction {
            kite: self.kite.new_transaction()?,
            sqlite: self.sqlite.new_transaction()?,
        })
    }
}

impl SimpleExecutor for DualBackend {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError> {
        self.kite.execute_batch(sql)?;
        if let Some(stmt) = normalize_sqlite_sql(sql) {
            self.sqlite.execute_batch(&stmt)?;
        }
        Ok(())
    }
}

pub struct DualTransaction<'a> {
    kite: KiteTransaction<'a>,
    sqlite: SqliteTransaction<'a>,
}

impl<'a> BackendTransaction for DualTransaction<'a> {
    fn execute<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<QueryResult<'b>, TpccError> {
        let spec = statement.spec().clone();
        let sql_lower = spec.sql.trim_start().to_ascii_lowercase();
        let kite_iter = self.kite.execute_raw(statement, params)?;
        let sqlite_spec = sqlite_statement_spec(&spec);
        let sqlite_stmt = PreparedStatement::Sqlite { spec: sqlite_spec };
        let sqlite_iter = self.sqlite.execute_raw(&sqlite_stmt, params)?;

        if sql_lower.starts_with("select") {
            Ok(QueryResult::from_dual(DualQueryResult::Compare(
                DualResult::new(kite_iter, sqlite_iter, statement.spec().sql),
            )))
        } else {
            drain_sqlite_iter(sqlite_iter)?;
            Ok(QueryResult::from_kite(kite_iter))
        }
    }

    fn commit(self) -> Result<(), TpccError> {
        self.sqlite.commit()?;
        self.kite.commit()
    }
}

pub(crate) enum DualQueryResult<'a> {
    Compare(DualResult<'a>),
}

impl<'a> Iterator for DualQueryResult<'a> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DualQueryResult::Compare(result) => result.next(),
        }
    }
}

pub(crate) struct DualResult<'a> {
    kite: KiteTxnResult<'a>,
    sqlite: SqliteResult<'a>,
    sql: &'static str,
}

impl<'a> DualResult<'a> {
    fn new(kite: KiteTxnResult<'a>, sqlite: SqliteResult<'a>, sql: &'static str) -> Self {
        Self { kite, sqlite, sql }
    }
}

impl Iterator for DualResult<'_> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.kite.next() {
            Some(kite_row) => {
                let sqlite_row = match self.sqlite.next() {
                    Some(row) => row,
                    None => {
                        return Some(Err(TpccError::BackendMismatch(format!(
                            "SQLite returned fewer rows for SQL: {}",
                            self.sql
                        ))))
                    }
                };
                match (kite_row, sqlite_row) {
                    (Ok(kite_tuple), Ok(sqlite_tuple)) => {
                        if kite_tuple.values != sqlite_tuple.values {
                            println!("[Dual] mismatch SQL: {}", self.sql);
                            println!("  KiteSQL row:   {:?}", kite_tuple.values);
                            println!("  SQLite  row:   {:?}", sqlite_tuple.values);
                            return Some(Err(TpccError::BackendMismatch(format!(
                                "Result mismatch for SQL: {}",
                                self.sql
                            ))));
                        }
                        Some(Ok(kite_tuple))
                    }
                    (Err(err), _) => Some(Err(err)),
                    (_, Err(err)) => Some(Err(err)),
                }
            }
            None => {
                if let Some(extra) = self.sqlite.next() {
                    let err = extra.err().unwrap_or_else(|| {
                        TpccError::BackendMismatch(format!(
                            "SQLite returned extra rows for SQL: {}",
                            self.sql
                        ))
                    });
                    return Some(Err(err));
                }
                None
            }
        }
    }
}

fn normalize_sqlite_sql(sql: &str) -> Option<Cow<'_, str>> {
    let trimmed = sql.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("analyze table ") {
        None
    } else {
        Some(Cow::Borrowed(sql))
    }
}

fn drain_sqlite_iter(mut iter: SqliteResult<'_>) -> Result<(), TpccError> {
    while let Some(row) = iter.next() {
        row?;
    }
    Ok(())
}

fn sqlite_statement_spec(spec: &StatementSpec) -> StatementSpec {
    if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
        StatementSpec {
            sql: STOCK_LEVEL_DISTINCT_SQLITE,
            result_types: spec.result_types,
        }
    } else {
        spec.clone()
    }
}
