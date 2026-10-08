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

use kite_sql::db::Database;
use kite_sql::errors::DatabaseError;
use kite_sql::storage::lmdb::LmdbStorage;
use kite_sql::types::value::DataValue;
use sqllogictest::{DBOutput, DefaultColumnType, DB};
use std::time::Instant;

pub struct SQLBase {
    pub db: Database<LmdbStorage>,
}

impl DB for SQLBase {
    type Error = DatabaseError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        println!("|— Input SQL: {}", sql);
        let output = run_sql(&mut self.db, sql, |_, value| value.to_string())?;
        println!(" |— time spent: {:?}", start.elapsed());
        Ok(output)
    }
}

/// Runs every statement in `sql` and returns the output of the last one, rendering each value of
/// column `i` with `format(i, value)`.
pub fn run_sql(
    db: &mut Database<LmdbStorage>,
    sql: &str,
    mut format: impl FnMut(usize, &DataValue) -> String,
) -> Result<DBOutput<DefaultColumnType>, DatabaseError> {
    db.run_mut(sql, |iter| {
        let width = iter.schema(|schema| schema.len());
        let types = vec![DefaultColumnType::Any; width];
        let mut rows = Vec::new();
        while let Some(row) = iter.next_tuple(|_, tuple| {
            tuple
                .values
                .iter()
                .enumerate()
                .map(|(i, value)| format(i, value))
                .collect()
        })? {
            rows.push(row);
        }
        if rows.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    })
}
