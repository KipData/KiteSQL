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

use kite_sql::db::{Database, ResultIter};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::rocksdb::RocksStorage;
use sqllogictest::{DBOutput, DefaultColumnType, DB};
use std::time::Instant;

pub struct SQLBase {
    pub db: Database<RocksStorage>,
}

impl DB for SQLBase {
    type Error = DatabaseError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let start = Instant::now();
        let mut iter = self.db.run(sql)?;
        println!("|— Input SQL: {}", sql);
        let types = vec![DefaultColumnType::Any; iter.schema().len()];
        let mut rows = Vec::new();

        for tuple in iter.by_ref() {
            rows.push(
                tuple?
                    .values
                    .into_iter()
                    .map(|value| format!("{}", value))
                    .collect(),
            )
        }
        iter.done()?;
        println!(" |— time spent: {:?}", start.elapsed());
        if rows.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }
        Ok(DBOutput::Rows { types, rows })
    }
}
