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
use super::sqlite::{SqliteBackend, SqlitePreparedStatement, SqliteResult, SqliteTransaction};
use super::{
    BackendControl, BackendTransaction, DbParam, KiteSqlPreparedStatement, PreparedStatement,
    SimpleExecutor, StatementSpec,
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

pub struct DualPreparedStatement<'a> {
    kitesql: KiteSqlPreparedStatement,
    sqlite: SqlitePreparedStatement<'a>,
    spec: StatementSpec,
}

impl PreparedStatement for DualPreparedStatement<'_> {
    fn spec(&self) -> &StatementSpec {
        &self.spec
    }
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
    type PreparedStatement<'a>
        = DualPreparedStatement<'a>
    where
        Self: 'a;

    type Transaction<'a>
        = DualTransaction<'a>
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<Self::PreparedStatement<'_>>>, TpccError> {
        let sqlite_specs: Vec<Vec<StatementSpec>> = specs
            .iter()
            .map(|group| group.iter().map(sqlite_statement_spec).collect())
            .collect();
        let kitesql_groups = self.kitesql.prepare_statements(specs)?;
        let sqlite_groups = self.sqlite.prepare_statements(&sqlite_specs)?;
        let mut groups = Vec::with_capacity(kitesql_groups.len());

        for (kitesql_group, sqlite_group) in kitesql_groups.into_iter().zip(sqlite_groups) {
            let mut group = Vec::with_capacity(kitesql_group.len());
            for (kitesql, sqlite) in kitesql_group.into_iter().zip(sqlite_group) {
                let spec = kitesql.spec().clone();
                group.push(DualPreparedStatement {
                    kitesql,
                    sqlite,
                    spec,
                });
            }
            groups.push(group);
        }

        Ok(groups)
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
    fn execute_batch(&mut self, sql: &str) -> Result<(), TpccError> {
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
    type PreparedStatement = DualPreparedStatement<'a>;

    fn execute_drain(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam],
    ) -> Result<(), TpccError> {
        let spec = statement.spec.clone();

        let kitesql_iter = self.kitesql.execute_raw(&mut statement.kitesql, params)?;
        let sqlite_iter = self.sqlite.execute_raw(&mut statement.sqlite, params)?;

        if is_select_sql(&spec) {
            if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
                let (kitesql_counts, kitesql_len) = collect_kitesql_value_counts(kitesql_iter)?;
                let sqlite_rows = collect_sqlite_rows(sqlite_iter)?;
                compare_unordered_rows(kitesql_counts, kitesql_len, &sqlite_rows, spec.sql)
            } else {
                drain_and_compare_ordered(kitesql_iter, sqlite_iter, spec.sql)
            }
        } else {
            drain_sqlite_iter(sqlite_iter)?;
            drain_kitesql_iter(kitesql_iter)
        }
    }

    fn with_query_one(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam],
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        self.with_query_nth(statement, params, 0, visitor)
    }

    fn with_query_nth(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam],
        n: usize,
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        let spec = statement.spec.clone();

        let kitesql_iter = self.kitesql.execute_raw(&mut statement.kitesql, params)?;
        let sqlite_iter = self.sqlite.execute_raw(&mut statement.sqlite, params)?;

        if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
            let (kitesql_counts, kitesql_len) = collect_kitesql_value_counts(kitesql_iter)?;
            let sqlite_rows = collect_sqlite_rows(sqlite_iter)?;
            compare_unordered_rows(kitesql_counts, kitesql_len, &sqlite_rows, spec.sql)?;
            let tuple = sqlite_rows.get(n).ok_or(TpccError::EmptyTuples)?;
            return visitor(tuple);
        }

        if !is_select_sql(&spec) {
            drain_sqlite_iter(sqlite_iter)?;
            return with_kitesql_nth(kitesql_iter, n, visitor);
        }

        with_ordered_nth(kitesql_iter, sqlite_iter, spec.sql, n, visitor)
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

fn drain_sqlite_iter(mut iter: SqliteResult<'_, '_>) -> Result<(), TpccError> {
    while let Some(row) = iter.next() {
        row?;
    }
    Ok(())
}

fn drain_kitesql_iter<T: kite_sql::storage::Transaction>(
    mut iter: KiteSqlTxnResult<'_, T>,
) -> Result<(), TpccError> {
    while iter.skip_next_tuple()? {}
    Ok(())
}

fn collect_sqlite_rows(mut iter: SqliteResult<'_, '_>) -> Result<Vec<Tuple>, TpccError> {
    let mut rows = Vec::new();
    while let Some(row) = iter.next() {
        rows.push(row?);
    }
    Ok(rows)
}

fn collect_kitesql_value_counts<T: kite_sql::storage::Transaction>(
    mut iter: KiteSqlTxnResult<'_, T>,
) -> Result<(HashMap<Vec<DataValue>, usize>, usize), TpccError> {
    let mut counts = HashMap::new();
    let mut len = 0;
    while let Some(()) = iter.with_next_tuple(|tuple| {
        *counts.entry(tuple.values.clone()).or_insert(0) += 1;
        len += 1;
        Ok(())
    })? {}
    Ok((counts, len))
}

fn with_kitesql_nth<T: kite_sql::storage::Transaction>(
    mut iter: KiteSqlTxnResult<'_, T>,
    n: usize,
    visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
) -> Result<(), TpccError> {
    for _ in 0..n {
        if !iter.skip_next_tuple()? {
            return Err(TpccError::EmptyTuples);
        }
    }
    iter.with_next_tuple(|tuple| visitor(tuple))?
        .ok_or(TpccError::EmptyTuples)
}

fn with_ordered_nth<T: kite_sql::storage::Transaction>(
    mut kitesql_iter: KiteSqlTxnResult<'_, T>,
    mut sqlite_iter: SqliteResult<'_, '_>,
    sql: &'static str,
    n: usize,
    visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
) -> Result<(), TpccError> {
    let mut visited = false;
    let mut index = 0usize;

    loop {
        match kitesql_iter.with_next_tuple(|kitesql_tuple| {
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
                visitor(&sqlite_tuple)?;
                visited = true;
            }
            index += 1;
            Ok(())
        })? {
            Some(()) => {}
            None => {
                if let Some(extra) = sqlite_iter.next() {
                    extra?;
                    return Err(TpccError::BackendMismatch(format!(
                        "SQLite returned extra rows for SQL: {}",
                        sql
                    )));
                }
                return if visited {
                    Ok(())
                } else {
                    Err(TpccError::EmptyTuples)
                };
            }
        }
    }
}

fn drain_and_compare_ordered<T: kite_sql::storage::Transaction>(
    mut kitesql_iter: KiteSqlTxnResult<'_, T>,
    mut sqlite_iter: SqliteResult<'_, '_>,
    sql: &'static str,
) -> Result<(), TpccError> {
    loop {
        match kitesql_iter.with_next_tuple(|kitesql_tuple| {
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
            Ok(())
        })? {
            Some(()) => {}
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
    mut counts: HashMap<Vec<DataValue>, usize>,
    kitesql_len: usize,
    sqlite_rows: &[Tuple],
    sql: &'static str,
) -> Result<(), TpccError> {
    if kitesql_len != sqlite_rows.len() {
        return Err(TpccError::BackendMismatch(format!(
            "SQLite returned different row count for SQL: {}",
            sql
        )));
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
