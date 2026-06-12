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

use kite_sql::binder::{command_type, CommandType};
use kite_sql::db::{prepare_all, Database, DatabaseIter, Statement};
use kite_sql::errors::DatabaseError;
use kite_sql::orm::StatementSource;
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
        println!("|— Input SQL: {}", sql);
        let mut statements = prepare_all(sql)?.into_iter().peekable();

        while let Some(statement) = statements.next() {
            let is_last = statements.peek().is_none();
            match command_type(&statement)? {
                CommandType::DDL => {
                    self.db
                        .execute_ddl_statement("SQLLOGICTEST DDL", &statement)?;
                    if is_last {
                        println!(" |— time spent: {:?}", start.elapsed());
                        return Ok(DBOutput::StatementComplete(0));
                    }
                }
                _ if matches!(statement, Statement::Analyze(_)) => {
                    execute_analyze_statement(&mut self.db, &statement)?;
                    if is_last {
                        println!(" |— time spent: {:?}", start.elapsed());
                        return Ok(DBOutput::StatementComplete(0));
                    }
                }
                _ => {
                    let iter = (&self.db).execute_statement(&statement, &[])?;
                    if is_last {
                        let output = collect_output(iter)?;
                        println!(" |— time spent: {:?}", start.elapsed());
                        return Ok(output);
                    }
                    iter.done()?;
                }
            }
        }

        println!(" |— time spent: {:?}", start.elapsed());
        Ok(DBOutput::StatementComplete(0))
    }
}

fn collect_output(
    mut iter: DatabaseIter<'_, RocksStorage>,
) -> Result<DBOutput<DefaultColumnType>, DatabaseError> {
    let types = vec![DefaultColumnType::Any; iter.schema().len()];
    let mut rows = Vec::new();

    while let Some(tuple) = iter.next_borrowed_tuple()? {
        rows.push(
            tuple
                .values
                .iter()
                .map(|value| format!("{}", value))
                .collect(),
        );
    }
    iter.done()?;
    if rows.is_empty() {
        return Ok(DBOutput::StatementComplete(0));
    }
    Ok(DBOutput::Rows { types, rows })
}

fn execute_analyze_statement(
    db: &mut Database<RocksStorage>,
    statement: &Statement,
) -> Result<(), DatabaseError> {
    let Statement::Analyze(analyze) = statement else {
        unreachable!("execute_analyze_statement only accepts ANALYZE")
    };
    let table_name = analyze
        .table_name
        .as_ref()
        .ok_or_else(|| DatabaseError::UnsupportedStmt("ANALYZE requires table name".to_string()))?;
    db.analyze(table_name.to_string())
}
