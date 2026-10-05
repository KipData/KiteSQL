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

//! End-to-end SQL fuzzing against an LMDB database.
//!
//! The input is treated as a `;`-separated SQL script. Every chunk is run on
//! its own, so a mutation that breaks one statement does not discard the rest
//! of the script (e.g. the `CREATE TABLE` / `INSERT` that later queries need).
//!
//! sqllogictest syntax is stripped first, so `tests/slt` can be used directly
//! as the seed corpus; plain SQL inputs pass through unchanged.
//!
//! `Err` results are expected and ignored. Any panic, sanitizer report or OOM
//! is a bug.
//!
//! TODO: timeouts are skipped (`make fuzz` runs libFuzzer in fork mode with
//! `-ignore_timeouts=1`). Mutations easily produce legitimately unbounded
//! queries, e.g. a recursive CTE that never converges, and KiteSQL cannot
//! interrupt a running statement, so they are indistinguishable from real
//! hangs. Revisit once the executor has a cheap interrupt point or the
//! structured generator avoids unbounded queries.

#![no_main]

use kite_sql::binder::{command_type, CommandType};
use kite_sql::db::{prepare_all, DataBaseBuilder, Database, Statement};
use kite_sql::storage::lmdb::LmdbStorage;
use kite_sql::types::value::DataValue;
use libfuzzer_sys::fuzz_target;
use std::cell::RefCell;

fuzz_target!(|data: &[u8]| {
    let Ok(script) = std::str::from_utf8(data) else {
        return;
    };
    DB.with_borrow_mut(|db| {
        reset(db);
        for sql in strip_slt(script).split(';') {
            run_one(db, sql);
        }
    });
});

thread_local! {
    // One database per process, reused by every input (emptied before each).
    static DB: RefCell<Database<LmdbStorage>> = RefCell::new(open_db());
}

fn open_db() -> Database<LmdbStorage> {
    let path = std::env::temp_dir().join(format!("kitesql-fuzz-sql_exec-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    DataBaseBuilder::path(path)
        .lmdb_no_sync(true)
        .build_lmdb()
        .expect("lmdb database")
}

/// Drops every view and table the previous input left behind.
fn reset(db: &mut Database<LmdbStorage>) {
    for (show, drop) in [("show views", "drop view"), ("show tables", "drop table")] {
        let mut names = Vec::new();
        if let Ok(mut iter) = db.run(show) {
            while let Ok(Some(())) = iter.next_tuple(|_, tuple| {
                if let Some(DataValue::Utf8 { value, .. }) = tuple.values.first() {
                    names.push(value.clone());
                }
            }) {}
        }
        for name in names {
            let _ = db.ddl(format!("{drop} {name}"));
        }
    }
}

/// sqllogictest record headers and directives; such lines never start SQL.
const SLT_DIRECTIVES: &[&str] = &[
    "statement",
    "query",
    "onlyif",
    "skipif",
    "control",
    "halt",
    "hash-threshold",
    "subtest",
    "sleep",
    "include",
];

/// Keeps only the SQL of an sqllogictest script.
///
/// Directives, comments and blank lines become `;` (a record ends at a blank
/// line and `query` SQL usually has no trailing `;`). Expected results, from
/// `----` up to the next blank line, are dropped.
fn strip_slt(script: &str) -> String {
    let mut sql = String::with_capacity(script.len());
    let mut in_results = false;

    for line in script.lines() {
        let trimmed = line.trim();
        if in_results {
            if trimmed.is_empty() {
                in_results = false;
                sql.push(';');
            }
            continue;
        }
        if trimmed.starts_with("----") {
            in_results = true;
            sql.push(';');
        } else if trimmed.is_empty()
            || trimmed.starts_with('#')
            || trimmed
                .split_whitespace()
                .next()
                .is_some_and(|word| SLT_DIRECTIVES.contains(&word))
        {
            sql.push(';');
        } else {
            sql.push_str(line);
            sql.push('\n');
        }
    }
    sql
}

fn run_one(db: &mut Database<LmdbStorage>, sql: &str) {
    let Ok(statements) = prepare_all(sql) else {
        return;
    };
    // `split(';')` leaves one statement per chunk except for `;` inside
    // string literals; such chunks usually fail to parse and are skipped.
    let [statement] = statements.as_slice() else {
        return;
    };
    let Ok(kind) = command_type(statement) else {
        return;
    };

    match kind {
        CommandType::DDL => {
            let _ = db.ddl(sql);
        }
        CommandType::Analyze => {
            if let Statement::Analyze(analyze) = statement {
                if let Some(table_name) = analyze.table_name.as_ref() {
                    let _ = db.analyze(table_name.to_string());
                }
            }
        }
        _ => {
            let Ok(mut iter) = db.run(sql) else {
                return;
            };
            // Drain and render every value, like the sqllogictest harness does.
            loop {
                match iter.next_tuple(|_, tuple| {
                    for value in tuple.values.iter() {
                        let _ = value.to_string();
                    }
                }) {
                    Ok(Some(())) => continue,
                    Ok(None) => {
                        let _ = iter.done();
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
    }
}
