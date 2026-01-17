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
use kite_sql::types::value::DataValue;
use std::borrow::Cow;
use std::collections::HashMap;

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
            if spec.sql == STOCK_LEVEL_DISTINCT_SQL {
                // DISTINCT without ORDER BY has undefined ordering; compare as sets.
                let kite_rows = collect_all_rows(kite_iter)?;
                let sqlite_rows = collect_all_rows(sqlite_iter)?;
                compare_unordered_rows(&kite_rows, &sqlite_rows, statement.spec().sql)?;
                return Ok(QueryResult::from_dual(DualQueryResult::CompareUnordered(
                    DualUnorderedResult::new(kite_rows),
                )));
            }
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
    CompareUnordered(DualUnorderedResult),
}

impl<'a> Iterator for DualQueryResult<'a> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DualQueryResult::Compare(result) => result.next(),
            DualQueryResult::CompareUnordered(result) => result.next(),
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

pub(crate) struct DualUnorderedResult {
    rows: std::vec::IntoIter<Tuple>,
}

impl DualUnorderedResult {
    fn new(rows: Vec<Tuple>) -> Self {
        Self {
            rows: rows.into_iter(),
        }
    }
}

impl Iterator for DualUnorderedResult {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(Ok)
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

fn compare_unordered_rows(
    kite_rows: &[Tuple],
    sqlite_rows: &[Tuple],
    sql: &'static str,
) -> Result<(), TpccError> {
    if kite_rows.len() != sqlite_rows.len() {
        return Err(TpccError::BackendMismatch(format!(
            "SQLite returned different row count for SQL: {}",
            sql
        )));
    }

    let mut counts: HashMap<Vec<DataValue>, usize> = HashMap::new();
    for row in kite_rows {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{
        BackendControl, BackendTransaction, ColumnType, DbParam, PreparedStatement, StatementSpec,
    };
    use chrono::NaiveDateTime;
    use kite_sql::types::value::DataValue;
    use rand::Rng;
    use std::fs;
    use std::path::PathBuf;

    const DISTINCT_ORDER_BY_SQL: &str = "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ?1 AND ol_d_id = ?2 AND ol_o_id < ?3 AND ol_o_id >= (?4 - 20) ORDER BY ol_i_id";
    const DISTINCT_DELIVERY_SQL: &str =
        "SELECT DISTINCT ol_delivery_d FROM order_line WHERE ol_w_id = ?1 AND ol_d_id = ?2 ORDER BY ol_delivery_d";
    const INT32_RESULT: [ColumnType; 1] = [ColumnType::Int32];
    const DATETIME_RESULT: [ColumnType; 1] = [ColumnType::NullableDateTime];

    struct TempKiteDir {
        path: PathBuf,
    }

    impl TempKiteDir {
        fn new() -> Result<Self, TpccError> {
            let mut path = std::env::temp_dir();
            let suffix: u64 = rand::thread_rng().gen();
            path.push(format!("tpcc_dual_test_{suffix}"));
            if path.exists() {
                fs::remove_dir_all(&path)?;
            }
            Ok(Self { path })
        }
    }

    impl Drop for TempKiteDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn prepare_single<B: BackendControl>(
        backend: &B,
        spec: StatementSpec,
    ) -> Result<PreparedStatement, TpccError> {
        let mut groups = backend.prepare_statements(&[vec![spec]])?;
        Ok(groups
            .pop()
            .and_then(|mut group| group.pop())
            .expect("missing prepared statement"))
    }

    fn collect_i32<B: BackendControl>(
        backend: &B,
        sql: &'static str,
        params: &[DbParam],
    ) -> Result<Vec<i32>, TpccError> {
        let statement = prepare_single(
            backend,
            StatementSpec {
                sql,
                result_types: &INT32_RESULT,
            },
        )?;
        let mut tx = backend.new_transaction()?;
        let rows = {
            let mut iter = tx.execute(&statement, params)?;
            let mut rows = Vec::new();
            while let Some(row) = iter.next() {
                let tuple = row?;
                let value = tuple.values[0].i32().expect("expected Int32 column");
                rows.push(value);
            }
            rows
        };
        tx.commit()?;
        Ok(rows)
    }

    fn collect_datetime<B: BackendControl>(
        backend: &B,
        sql: &'static str,
        params: &[DbParam],
    ) -> Result<Vec<Option<NaiveDateTime>>, TpccError> {
        let statement = prepare_single(
            backend,
            StatementSpec {
                sql,
                result_types: &DATETIME_RESULT,
            },
        )?;
        let mut tx = backend.new_transaction()?;
        let rows = {
            let mut iter = tx.execute(&statement, params)?;
            let mut rows = Vec::new();
            while let Some(row) = iter.next() {
                let tuple = row?;
                rows.push(tuple.values[0].datetime());
            }
            rows
        };
        tx.commit()?;
        Ok(rows)
    }

    fn parse_dt(value: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").expect("invalid datetime literal")
    }

    #[test]
    fn dual_stock_level_distinct_ordering_mismatch() -> Result<(), TpccError> {
        let temp_dir = TempKiteDir::new()?;
        let backend = DualBackend::new(
            temp_dir
                .path
                .to_str()
                .expect("temporary path should be valid utf-8"),
        )?;

        backend.execute_batch(
            "create table order_line (
                ol_o_id int not null,
                ol_d_id tinyint not null,
                ol_w_id smallint not null,
                ol_number tinyint not null,
                ol_i_id int,
                ol_supply_w_id smallint,
                ol_delivery_d datetime,
                ol_quantity tinyint,
                ol_amount decimal(6,2),
                ol_dist_info char(24),
                PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
            );",
        )?;
        backend.execute_batch(
            "CREATE INDEX fkey_order_line_1 ON order_line (ol_o_id, ol_d_id, ol_w_id);",
        )?;

        // Large distinct set makes accidental order matches vanishingly unlikely.
        let i_ids = [
            87181, 5901, 26250, 58438, 92124, 84344, 72959, 10014, 46998, 2731, 11001, 12002,
            13003, 14004, 15005, 16006, 17007, 18008, 19009, 20010,
        ];
        let delivery_samples = [
            None,
            Some("2024-01-02 00:00:00"),
            Some("2024-01-01 00:00:00"),
            Some("2023-12-31 00:00:00"),
        ];

        for (idx, i_id) in i_ids.iter().enumerate() {
            let o_id = idx + 1;
            let delivery = delivery_samples
                .get(idx)
                .and_then(|value| *value)
                .map(|value| format!("'{value}'"))
                .unwrap_or_else(|| "NULL".to_string());
            backend.execute_batch(&format!(
                "insert into order_line values ({o_id}, 1, 1, 1, {i_id}, 1, {delivery}, 1, 1.00, 'dist')"
            ))?;
        }
        backend.execute_batch(
            "insert into order_line values (21, 1, 1, 1, 87181, 1, NULL, 1, 1.00, 'dist')",
        )?;
        backend.execute_batch(
            "insert into order_line values (22, 2, 1, 1, 99999, 1, NULL, 1, 1.00, 'dist')",
        )?;

        let params: [DbParam; 4] = [
            ("?1", DataValue::Int16(1)),
            ("?2", DataValue::Int8(1)),
            ("?3", DataValue::Int32(21)),
            ("?4", DataValue::Int32(21)),
        ];

        let kite_unordered = collect_i32(&backend.kite, STOCK_LEVEL_DISTINCT_SQL, &params)?;
        let sqlite_unordered = collect_i32(&backend.sqlite, STOCK_LEVEL_DISTINCT_SQLITE, &params)?;
        let dual_unordered = collect_i32(&backend, STOCK_LEVEL_DISTINCT_SQL, &params)?;

        let mut kite_sorted = kite_unordered.clone();
        let mut sqlite_sorted = sqlite_unordered.clone();
        kite_sorted.sort_unstable();
        sqlite_sorted.sort_unstable();
        assert_eq!(
            kite_sorted, sqlite_sorted,
            "distinct sets diverge; ordering is the only allowed difference"
        );
        assert_ne!(
            kite_unordered, sqlite_unordered,
            "unordered DISTINCT should not be compared row-by-row"
        );
        let mut dual_sorted = dual_unordered.clone();
        dual_sorted.sort_unstable();
        assert_eq!(
            dual_sorted, kite_sorted,
            "dual comparison should match KiteSQL distinct set"
        );

        let kite_ordered = collect_i32(&backend.kite, DISTINCT_ORDER_BY_SQL, &params)?;
        let sqlite_ordered = collect_i32(&backend.sqlite, DISTINCT_ORDER_BY_SQL, &params)?;
        assert_eq!(
            kite_ordered, sqlite_ordered,
            "explicit ORDER BY should align results across engines"
        );

        let params_delivery: [DbParam; 2] =
            [("?1", DataValue::Int16(1)), ("?2", DataValue::Int8(1))];
        let expected_delivery = vec![
            None,
            Some(parse_dt("2023-12-31 00:00:00")),
            Some(parse_dt("2024-01-01 00:00:00")),
            Some(parse_dt("2024-01-02 00:00:00")),
        ];
        let kite_delivery =
            collect_datetime(&backend.kite, DISTINCT_DELIVERY_SQL, &params_delivery)?;
        let sqlite_delivery =
            collect_datetime(&backend.sqlite, DISTINCT_DELIVERY_SQL, &params_delivery)?;
        assert_eq!(kite_delivery, expected_delivery);
        assert_eq!(sqlite_delivery, expected_delivery);

        Ok(())
    }
}
