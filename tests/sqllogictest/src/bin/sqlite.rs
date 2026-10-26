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

//! Runs the SQLite sqllogictest corpus converted by `kitesql-testing/scripts/convert.py`.
//!
//! Unlike the main harness, every file gets its own database, values are rendered the way the
//! SQLite reference runner does (by the column type declared in the query), queries that exceed
//! the parser recursion limit are skipped, and failures are counted instead of aborting the run.

use clap::Parser;
use kite_sql::db::{DataBaseBuilder, Database};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::lmdb::LmdbStorage;
use kite_sql::types::value::DataValue;
use sqllogictest::{DBOutput, DefaultColumnType, QueryExpect, Record, Runner, DB};
use sqllogictest_test::run_sql;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::TempDir;

const STACK_SIZE: usize = 64 * 1024 * 1024;
const MAX_ERROR_LINES: usize = 30;

#[derive(Parser, Debug)]
struct Args {
    #[clap(long, default_value = "tests/kitesql-testing/data/sqlite/**/*.slt")]
    path: String,
    /// Failures printed per file; the rest are only counted.
    #[clap(long, default_value_t = 5)]
    max_errors: usize,
    /// Print the location of every record before running it, to find a hanging query.
    #[clap(long)]
    trace: bool,
    /// Include select1–5, which are excluded while correlated scalar queries and join costs are investigated.
    #[clap(long)]
    include_select: bool,
    /// Override the ANALYZE bucket count for small corpus fixtures.
    #[clap(long)]
    histogram_buckets: Option<usize>,
    /// Override SQL parser recursion depth for deeply nested corpus queries.
    #[clap(long)]
    parser_recursion_limit: Option<usize>,
    /// Override the HyperLogLog precision (4..=26) used by ANALYZE for NDV.
    #[clap(long)]
    hll_precision: Option<u8>,
    /// Override the relative error (0, 1) of ANALYZE's KLL and Count-Min sketches.
    #[clap(long)]
    statistics_relative_error: Option<f64>,
}

/// Database settings shared by every file in a run.
#[derive(Clone, Copy)]
struct DbOptions {
    histogram_buckets: Option<usize>,
    parser_recursion_limit: Option<usize>,
    hll_precision: Option<u8>,
    statistics_relative_error: Option<f64>,
}

impl DbOptions {
    fn builder(self, path: PathBuf) -> DataBaseBuilder {
        // Always NO_SYNC, matching the reference SQLite runner's `PRAGMA synchronous=OFF`;
        // otherwise every autocommit statement pays an fdatasync that dominates the run.
        let mut builder = DataBaseBuilder::path(path).lmdb_no_sync(true);
        if let Some(buckets) = self.histogram_buckets {
            builder = builder.histogram_buckets(buckets);
        }
        if let Some(limit) = self.parser_recursion_limit {
            builder = builder.parser_recursion_limit(limit);
        }
        if let Some(precision) = self.hll_precision {
            builder = builder.hll_precision(precision);
        }
        if let Some(relative_error) = self.statistics_relative_error {
            builder = builder.statistics_relative_error(relative_error);
        }
        builder
    }
}

struct SqliteDB {
    db: Database<LmdbStorage>,
    /// Column types declared by the query being run.
    types: Arc<Mutex<Vec<DefaultColumnType>>>,
}

impl DB for SqliteDB {
    type Error = DatabaseError;
    type ColumnType = DefaultColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let types = self.types.lock().unwrap().clone();
        run_sql(&mut self.db, sql, |i, value| {
            format_value(types.get(i), value)
        })
    }
}

/// Renders like the SQLite reference runner: `I` as `%d`, `R` as `%.3f`, NULL as `NULL`, an
/// empty string as `(empty)` and unprintable characters as `@`.
fn format_value(ty: Option<&DefaultColumnType>, value: &DataValue) -> String {
    if value.is_null() {
        return "NULL".to_string();
    }
    match (ty, value) {
        (Some(DefaultColumnType::Integer), DataValue::Boolean(b)) => (*b as i64).to_string(),
        (
            Some(DefaultColumnType::Integer),
            DataValue::Int8(_)
            | DataValue::Int16(_)
            | DataValue::Int32(_)
            | DataValue::Int64(_)
            | DataValue::UInt8(_)
            | DataValue::UInt16(_)
            | DataValue::UInt32(_)
            | DataValue::UInt64(_),
        ) => value.to_string(),
        (Some(DefaultColumnType::Integer), _) => (as_f64(value) as i64).to_string(),
        (Some(DefaultColumnType::FloatingPoint), _) => format!("{:.3}", as_f64(value)),
        _ => {
            let text = value.to_string();
            if text.is_empty() {
                return "(empty)".to_string();
            }
            text.chars()
                .map(|c| if (' '..='~').contains(&c) { c } else { '@' })
                .collect()
        }
    }
}

fn as_f64(value: &DataValue) -> f64 {
    match value {
        DataValue::Boolean(b) => *b as i64 as f64,
        DataValue::Float32(f) => f.0 as f64,
        DataValue::Float64(f) => f.0,
        DataValue::Utf8 { value, .. } => value.trim().parse().unwrap_or(0.0),
        other => other.to_string().parse().unwrap_or(0.0),
    }
}

#[derive(Default, Clone, Copy)]
struct Summary {
    passed: usize,
    failed: usize,
    skipped: usize,
    /// A statement failed, so the rest of the file was not run.
    halted: bool,
}

fn run_file(
    path: &Path,
    max_errors: usize,
    trace: bool,
    options: DbOptions,
) -> Result<Summary, String> {
    let script = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
    let records =
        sqllogictest::parse_with_name::<DefaultColumnType>(&script, path.display().to_string())
            .map_err(|e| e.to_string())?;
    let dir = TempDir::new().map_err(|e| e.to_string())?;
    let db_path = dir.path().to_path_buf();
    let types = Arc::new(Mutex::new(Vec::new()));
    let db_types = types.clone();
    let mut runner = Runner::new(move || {
        let types = db_types.clone();
        let db = options.builder(db_path.clone()).build_lmdb();
        std::future::ready(db.map(|db| SqliteDB { db, types }))
    });
    // The corpus was generated with this threshold, even in files that never declare it.
    runner.with_hash_threshold(8);

    let mut summary = Summary::default();
    for record in records {
        let is_statement = match &record {
            Record::Statement { loc, .. } => {
                if trace {
                    println!("   run {loc}");
                }
                true
            }
            Record::Query { loc, expected, .. } => {
                if trace {
                    println!("   run {loc}");
                }
                if let QueryExpect::Results {
                    types: expected, ..
                } = expected
                {
                    *types.lock().unwrap() = expected.clone();
                }
                false
            }
            Record::Halt { .. } => break,
            _ => {
                runner.run(record).map_err(|e| e.to_string())?;
                continue;
            }
        };
        let Err(err) = runner.run(record) else {
            summary.passed += 1;
            continue;
        };
        let message = err.to_string();
        if message.contains("recursion limit exceeded") {
            summary.skipped += 1;
            continue;
        }
        summary.failed += 1;
        if summary.failed <= max_errors {
            let lines: Vec<_> = message.lines().take(MAX_ERROR_LINES).collect();
            println!("   {}", lines.join("\n   "));
        }
        if is_statement {
            summary.halted = true;
            break;
        }
    }
    Ok(summary)
}

fn main() {
    let args = Args::parse();
    std::env::set_current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")).unwrap();
    let files: Vec<PathBuf> = glob::glob(&args.path)
        .expect("invalid --path pattern")
        .filter_map(Result::ok)
        .collect();
    if files.is_empty() {
        eprintln!(
            "no file matches {} (run `git submodule update --init`?)",
            args.path
        );
        std::process::exit(2);
    }

    let start = Instant::now();
    let (mut total, mut broken, mut excluded) = (Summary::default(), 0, 0);
    for path in files {
        if !args.include_select
            && matches!(
                path.file_stem().and_then(|name| name.to_str()),
                Some("select1" | "select2" | "select3" | "select4" | "select5")
            )
        {
            println!(
                "-> {}: excluded (select1–5; use --include-select to run)",
                path.display()
            );
            excluded += 1;
            continue;
        }
        println!("-> {}", path.display());
        let file_start = Instant::now();
        let (file, max_errors, trace) = (path.clone(), args.max_errors, args.trace);
        let options = DbOptions {
            histogram_buckets: args.histogram_buckets,
            parser_recursion_limit: args.parser_recursion_limit,
            hll_precision: args.hll_precision,
            statistics_relative_error: args.statistics_relative_error,
        };
        let result = std::thread::Builder::new()
            .stack_size(STACK_SIZE)
            .spawn(move || run_file(&file, max_errors, trace, options))
            .expect("failed to spawn test thread")
            .join();
        match result {
            Ok(Ok(s)) => {
                let halted = if s.halted {
                    ", halted on a failed statement"
                } else {
                    ""
                };
                println!(
                    "   passed {} failed {} skipped {}{halted} ({:?})",
                    s.passed,
                    s.failed,
                    s.skipped,
                    file_start.elapsed()
                );
                total.passed += s.passed;
                total.failed += s.failed;
                total.skipped += s.skipped;
            }
            Ok(Err(err)) => {
                broken += 1;
                println!("   error: {err}");
            }
            Err(_) => {
                broken += 1;
                println!("   panicked");
            }
        }
    }
    println!(
        "total: passed {} failed {} skipped {}, {broken} file(s) not run, {excluded} file(s) excluded ({:?})",
        total.passed,
        total.failed,
        total.skipped,
        start.elapsed()
    );
    if total.failed > 0 || broken > 0 {
        std::process::exit(1);
    }
}
