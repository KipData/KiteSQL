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

use super::kitesql_rocksdb::{KiteSqlRocksDbBackend, KiteSqlRocksDbTransaction, KiteSqlTxnResult};
use super::sqlite::{SqliteBackend, SqliteResult, SqliteTransaction};
use super::{
    BackendControl, BackendTransaction, DbParam, PreparedStatement, SimpleExecutor, StatementSpec,
};
use crate::{TpccError, STOCK_LEVEL_DISTINCT_SQL, STOCK_LEVEL_DISTINCT_SQLITE};
use kite_sql::types::tuple::Tuple;
use kite_sql::types::value::DataValue;
use std::borrow::Cow;
use std::collections::HashMap;

pub struct DualBackend {
    kitesql: KiteSqlRocksDbBackend,
    sqlite: SqliteBackend,
}

impl DualBackend {
    pub fn new(path: &str, rocksdb_stats: bool) -> Result<Self, TpccError> {
        Ok(Self {
            kitesql: KiteSqlRocksDbBackend::new(path, rocksdb_stats)?,
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
        self.kitesql.prepare_statements(specs)
    }

    fn new_transaction(&self) -> Result<Self::Transaction<'_>, TpccError> {
        Ok(DualTransaction {
            kitesql: self.kitesql.new_transaction()?,
            sqlite: self.sqlite.new_transaction()?,
        })
    }

    fn storage_metrics(&self) -> Option<String> {
        self.kitesql.storage_metrics()
    }
}

impl SimpleExecutor for DualBackend {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError> {
        self.kitesql.execute_batch(sql)?;
        if let Some(stmt) = normalize_sqlite_sql(sql) {
            self.sqlite.execute_batch(&stmt)?;
        }
        Ok(())
    }
}

pub struct DualTransaction<'a> {
    kitesql: KiteSqlRocksDbTransaction<'a>,
    sqlite: SqliteTransaction<'a>,
}

impl<'a> BackendTransaction for DualTransaction<'a> {
    fn query_one(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<Tuple, TpccError> {
        let spec = statement.spec().clone();
        let sqlite_stmt = PreparedStatement::Sqlite {
            spec: sqlite_statement_spec(&spec),
        };

        let kitesql_iter = self.kitesql.execute_raw(statement, params)?;
        let sqlite_iter = self.sqlite.execute_raw(&sqlite_stmt, params)?;

        if is_select_sql(&spec) {
            if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
                let kitesql_rows = collect_all_rows(kitesql_iter)?;
                let sqlite_rows = collect_all_rows(sqlite_iter)?;
                compare_unordered_rows(&kitesql_rows, &sqlite_rows, spec.sql)?;
                return kitesql_rows
                    .into_iter()
                    .next()
                    .ok_or(TpccError::EmptyTuples);
            }
            query_ordered_nth(kitesql_iter, sqlite_iter, spec.sql, 0)
        } else {
            drain_sqlite_iter(sqlite_iter)?;
            let mut kitesql_iter = kitesql_iter;
            match kitesql_iter.next() {
                Some(row) => row,
                None => Err(TpccError::EmptyTuples),
            }
        }
    }

    fn query_nth(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        n: usize,
    ) -> Result<Tuple, TpccError> {
        let spec = statement.spec().clone();
        let sqlite_stmt = PreparedStatement::Sqlite {
            spec: sqlite_statement_spec(&spec),
        };

        let kitesql_iter = self.kitesql.execute_raw(statement, params)?;
        let sqlite_iter = self.sqlite.execute_raw(&sqlite_stmt, params)?;

        if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
            let kitesql_rows = collect_all_rows(kitesql_iter)?;
            let sqlite_rows = collect_all_rows(sqlite_iter)?;
            compare_unordered_rows(&kitesql_rows, &sqlite_rows, spec.sql)?;
            return kitesql_rows
                .into_iter()
                .nth(n)
                .ok_or(TpccError::EmptyTuples);
        }

        query_ordered_nth(kitesql_iter, sqlite_iter, spec.sql, n)
    }

    fn execute_drain(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<(), TpccError> {
        let spec = statement.spec().clone();
        let sqlite_stmt = PreparedStatement::Sqlite {
            spec: sqlite_statement_spec(&spec),
        };

        let kitesql_iter = self.kitesql.execute_raw(statement, params)?;
        let sqlite_iter = self.sqlite.execute_raw(&sqlite_stmt, params)?;

        if is_select_sql(&spec) {
            if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
                let kitesql_rows = collect_all_rows(kitesql_iter)?;
                let sqlite_rows = collect_all_rows(sqlite_iter)?;
                compare_unordered_rows(&kitesql_rows, &sqlite_rows, spec.sql)
            } else {
                drain_and_compare_ordered(kitesql_iter, sqlite_iter, spec.sql)
            }
        } else {
            drain_sqlite_iter(sqlite_iter)?;
            drain_kitesql_iter(kitesql_iter)
        }
    }

    fn commit(self) -> Result<(), TpccError> {
        self.sqlite.commit()?;
        self.kitesql.commit()
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

fn drain_kitesql_iter<T: kite_sql::storage::Transaction>(
    mut iter: KiteSqlTxnResult<'_, T>,
) -> Result<(), TpccError> {
    while let Some(row) = iter.next() {
        row?;
    }
    Ok(())
}

fn collect_all_rows<I>(mut iter: I) -> Result<Vec<Tuple>, TpccError>
where
    I: Iterator<Item = Result<Tuple, TpccError>>,
{
    let mut rows = Vec::new();
    while let Some(row) = iter.next() {
        rows.push(row?);
    }
    Ok(rows)
}

fn query_ordered_nth<T: kite_sql::storage::Transaction>(
    mut kitesql_iter: KiteSqlTxnResult<'_, T>,
    mut sqlite_iter: SqliteResult<'_>,
    sql: &'static str,
    n: usize,
) -> Result<Tuple, TpccError> {
    let mut result = None;
    let mut index = 0usize;

    loop {
        match kitesql_iter.next() {
            Some(kitesql_row) => {
                let kitesql_tuple = kitesql_row?;
                let sqlite_tuple = match sqlite_iter.next() {
                    Some(row) => row?,
                    None => {
                        return Err(TpccError::BackendMismatch(format!(
                            "SQLite returned fewer rows for SQL: {}",
                            sql
                        )))
                    }
                };
                if kitesql_tuple.values != sqlite_tuple.values {
                    println!("[Dual] mismatch SQL: {}", sql);
                    println!("  KiteSQL row:   {:?}", kitesql_tuple.values);
                    println!("  SQLite  row:   {:?}", sqlite_tuple.values);
                    return Err(TpccError::BackendMismatch(format!(
                        "Result mismatch for SQL: {}",
                        sql
                    )));
                }
                if index == n {
                    result = Some(kitesql_tuple.clone());
                }
                index += 1;
            }
            None => {
                if let Some(extra) = sqlite_iter.next() {
                    extra?;
                    return Err(TpccError::BackendMismatch(format!(
                        "SQLite returned extra rows for SQL: {}",
                        sql
                    )));
                }
                return result.ok_or(TpccError::EmptyTuples);
            }
        }
    }
}

fn drain_and_compare_ordered<T: kite_sql::storage::Transaction>(
    mut kitesql_iter: KiteSqlTxnResult<'_, T>,
    mut sqlite_iter: SqliteResult<'_>,
    sql: &'static str,
) -> Result<(), TpccError> {
    loop {
        match kitesql_iter.next() {
            Some(kitesql_row) => {
                let kitesql_tuple = kitesql_row?;
                let sqlite_tuple = match sqlite_iter.next() {
                    Some(row) => row?,
                    None => {
                        return Err(TpccError::BackendMismatch(format!(
                            "SQLite returned fewer rows for SQL: {}",
                            sql
                        )))
                    }
                };
                if kitesql_tuple.values != sqlite_tuple.values {
                    println!("[Dual] mismatch SQL: {}", sql);
                    println!("  KiteSQL row:   {:?}", kitesql_tuple.values);
                    println!("  SQLite  row:   {:?}", sqlite_tuple.values);
                    return Err(TpccError::BackendMismatch(format!(
                        "Result mismatch for SQL: {}",
                        sql
                    )));
                }
            }
            None => {
                if let Some(extra) = sqlite_iter.next() {
                    extra?;
                    return Err(TpccError::BackendMismatch(format!(
                        "SQLite returned extra rows for SQL: {}",
                        sql
                    )));
                }
                return Ok(());
            }
        }
    }
}

fn compare_unordered_rows(
    kitesql_rows: &[Tuple],
    sqlite_rows: &[Tuple],
    sql: &'static str,
) -> Result<(), TpccError> {
    if kitesql_rows.len() != sqlite_rows.len() {
        return Err(TpccError::BackendMismatch(format!(
            "SQLite returned different row count for SQL: {}",
            sql
        )));
    }

    let mut counts: HashMap<Vec<DataValue>, usize> = HashMap::new();
    for row in kitesql_rows {
        *counts.entry(row.values.clone()).or_insert(0) += 1;
    }
    for row in sqlite_rows {
        match counts.get_mut(&row.values) {
            Some(count) => {
                if *count == 1 {
                    counts.remove(&row.values);
                } else {
                    *count -= 1;
                }
            }
            None => {
                return Err(TpccError::BackendMismatch(format!(
                    "SQLite returned different distinct set for SQL: {}",
                    sql
                )));
            }
        }
    }

    if counts.is_empty() {
        Ok(())
    } else {
        Err(TpccError::BackendMismatch(format!(
            "SQLite returned different distinct set for SQL: {}",
            sql
        )))
    }
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

fn is_select_sql(spec: &StatementSpec) -> bool {
    spec.sql
        .trim_start()
        .get(..6)
        .map(|prefix| prefix.eq_ignore_ascii_case("select"))
        .unwrap_or(false)
}
