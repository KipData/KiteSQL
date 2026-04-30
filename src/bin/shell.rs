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

use comfy_table::{Cell, Table};
use kite_sql::db::{BorrowResultIter, DBTransaction, DataBaseBuilder, Database};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::rocksdb::RocksStorage;
use rustyline::config::Configurer;
use rustyline::error::ReadlineError;
use rustyline::{Config, DefaultEditor};
use std::env;
use std::io::{self, IsTerminal};
use std::process::ExitCode;
use std::time::Instant;

const DEFAULT_PATH: &str = "./kitesql_data";

const HELP: &str = "\
kitesql-shell

Usage:
  kitesql-shell [--path PATH] [-e SQL]

Options:
  --path PATH   RocksDB data directory (default: ./kitesql_data)
  -e, --execute Execute one SQL statement or one metacommand and exit
  -h, --help    Show this help

Metacommands:
  .help         Show help
  .quit         Exit shell
  .tables       Show tables
  .views        Show views
  .schema NAME  Describe a table or view

Transaction commands:
  BEGIN
  COMMIT
  ROLLBACK
";

enum Mode {
    Continue,
    Exit,
}

struct Args {
    path: String,
    execute: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut path = DEFAULT_PATH.to_string();
    let mut execute = None;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--path" => {
                path = args
                    .next()
                    .ok_or_else(|| "--path expects a value".to_string())?;
            }
            "-e" | "--execute" => {
                execute = Some(
                    args.next()
                        .ok_or_else(|| "--execute expects a value".to_string())?,
                );
            }
            "-h" | "--help" => {
                println!("{HELP}");
                return Err(String::new());
            }
            other => {
                return Err(format!("unknown argument: {other}\n\n{HELP}"));
            }
        }
    }

    Ok(Args { path, execute })
}

fn prompt(is_tx: bool, continuation: bool) -> &'static str {
    match (is_tx, continuation) {
        (false, false) => "kite> ",
        (true, false) => "kite(tx)> ",
        (false, true) => " ...> ",
        (true, true) => " ...(tx)> ",
    }
}

enum Input {
    Interactive(Box<DefaultEditor>),
    Plain { stdin: io::Stdin, line: String },
}

impl Input {
    fn new(interactive: bool) -> Result<Self, DatabaseError> {
        if interactive {
            let config = Config::builder()
                .history_ignore_dups(true)
                .map_err(readline_error)?
                .build();
            let mut editor = DefaultEditor::with_config(config).map_err(readline_error)?;
            editor.set_auto_add_history(false);
            return Ok(Self::Interactive(Box::new(editor)));
        }

        Ok(Self::Plain {
            stdin: io::stdin(),
            line: String::new(),
        })
    }

    fn read_line(&mut self, prompt: &str) -> Result<Option<String>, DatabaseError> {
        match self {
            Self::Interactive(editor) => match editor.readline(prompt) {
                Ok(line) => {
                    if !line.trim().is_empty() {
                        editor
                            .add_history_entry(line.as_str())
                            .map_err(readline_error)?;
                    }
                    Ok(Some(format!("{line}\n")))
                }
                Err(ReadlineError::Interrupted | ReadlineError::Eof) => Ok(None),
                Err(err) => Err(readline_error(err)),
            },
            Self::Plain { stdin, line } => {
                line.clear();
                let bytes = stdin.read_line(line).map_err(DatabaseError::from)?;
                if bytes == 0 {
                    Ok(None)
                } else {
                    Ok(Some(line.clone()))
                }
            }
        }
    }
}

fn readline_error(err: impl std::fmt::Display) -> DatabaseError {
    DatabaseError::IO(io::Error::other(err.to_string()))
}

fn print_table<I>(mut iter: I) -> Result<(), DatabaseError>
where
    I: BorrowResultIter,
{
    let mut table = Table::new();
    let schema = iter.schema().clone();

    if !schema.is_empty() {
        let header = schema
            .iter()
            .map(|column| Cell::new(column.full_name()))
            .collect::<Vec<_>>();
        table.set_header(header);
    }

    let mut row_count = 0usize;
    while let Some(tuple) = iter.next_borrowed_tuple()? {
        row_count += 1;
        let row = tuple
            .values
            .iter()
            .map(|value| Cell::new(format!("{value}")))
            .collect::<Vec<_>>();
        table.add_row(row);
    }
    iter.done()?;

    if schema.is_empty() {
        println!("OK");
    } else if row_count == 0 {
        println!("{table}");
        println!("0 rows");
    } else {
        println!("{table}");
        println!("{row_count} row{}", if row_count == 1 { "" } else { "s" });
    }

    Ok(())
}

fn run_sql<'a>(
    database: &Database<RocksStorage>,
    tx: &mut Option<DBTransaction<'a, RocksStorage>>,
    sql: &str,
) -> Result<(), DatabaseError> {
    let started = Instant::now();
    if let Some(tx) = tx.as_mut() {
        print_table(tx.run(sql)?)?;
    } else {
        print_table(database.run(sql)?)?;
    }
    eprintln!("elapsed: {:.2?}", started.elapsed());
    Ok(())
}

fn handle_meta_command<'a>(
    command: &str,
    database: &Database<RocksStorage>,
    tx: &mut Option<DBTransaction<'a, RocksStorage>>,
) -> Result<Mode, DatabaseError> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return Ok(Mode::Continue);
    }

    if trimmed.eq_ignore_ascii_case(".quit") || trimmed.eq_ignore_ascii_case(".exit") {
        return Ok(Mode::Exit);
    }

    if trimmed.eq_ignore_ascii_case(".help") {
        println!("{HELP}");
        return Ok(Mode::Continue);
    }

    if trimmed.eq_ignore_ascii_case(".tables") {
        run_sql(database, tx, "show tables")?;
        return Ok(Mode::Continue);
    }

    if trimmed.eq_ignore_ascii_case(".views") {
        run_sql(database, tx, "show views")?;
        return Ok(Mode::Continue);
    }

    if let Some(name) = trimmed.strip_prefix(".schema ") {
        let name = name.trim();
        if name.is_empty() {
            eprintln!(".schema expects a table or view name");
        } else {
            run_sql(database, tx, &format!("describe {name}"))?;
        }
        return Ok(Mode::Continue);
    }

    eprintln!("unknown metacommand: {trimmed}");
    Ok(Mode::Continue)
}

fn handle_command<'a>(
    command: &str,
    database: &'a Database<RocksStorage>,
    tx: &mut Option<DBTransaction<'a, RocksStorage>>,
) -> Result<Mode, DatabaseError> {
    let trimmed = command.trim();
    if trimmed.is_empty() {
        return Ok(Mode::Continue);
    }

    if trimmed.starts_with('.') {
        return handle_meta_command(trimmed, database, tx);
    }

    let normalized = trimmed.trim_end_matches(';').trim();

    if normalized.eq_ignore_ascii_case("begin")
        || normalized.eq_ignore_ascii_case("start transaction")
    {
        if tx.is_some() {
            eprintln!("transaction already started");
        } else {
            *tx = Some(database.new_transaction()?);
            println!("OK");
        }
        return Ok(Mode::Continue);
    }

    if normalized.eq_ignore_ascii_case("commit") || normalized.eq_ignore_ascii_case("commit work") {
        if let Some(transaction) = tx.take() {
            transaction.commit()?;
            println!("OK");
        } else {
            eprintln!("no active transaction");
        }
        return Ok(Mode::Continue);
    }

    if normalized.eq_ignore_ascii_case("rollback") {
        if tx.take().is_some() {
            println!("OK");
        } else {
            eprintln!("no active transaction");
        }
        return Ok(Mode::Continue);
    }

    run_sql(database, tx, trimmed)?;
    Ok(Mode::Continue)
}

fn repl(database: &Database<RocksStorage>, path: &str) -> Result<(), DatabaseError> {
    let interactive = io::stdin().is_terminal() && io::stdout().is_terminal();
    let mut input = Input::new(interactive)?;
    let mut tx = None;
    let mut buffer = String::new();

    if interactive {
        println!("KiteSQL shell");
        println!("data path: {path}");
        println!("type .help for help, .quit to exit");
    }

    loop {
        let Some(line) = input.read_line(prompt(tx.is_some(), !buffer.trim().is_empty()))? else {
            break;
        };

        let trimmed = line.trim();
        if buffer.is_empty() && trimmed.starts_with('.') {
            match handle_command(trimmed, database, &mut tx) {
                Ok(Mode::Continue) => {}
                Ok(Mode::Exit) => break,
                Err(err) => eprintln!("{err}"),
            }
            continue;
        }

        buffer.push_str(&line);

        let should_execute = trimmed.is_empty() || trimmed.ends_with(';');
        if !should_execute {
            continue;
        }

        let command = buffer.trim();
        if command.is_empty() {
            buffer.clear();
            continue;
        }

        match handle_command(command, database, &mut tx) {
            Ok(Mode::Continue) => {}
            Ok(Mode::Exit) => break,
            Err(err) => eprintln!("{err}"),
        }
        buffer.clear();
    }

    Ok(())
}

fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(err) if err.is_empty() => return ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };

    let database = match DataBaseBuilder::path(&args.path).build_rocksdb() {
        Ok(database) => database,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
    };

    if let Some(sql) = args.execute.as_deref() {
        let mut tx = None;
        return match handle_command(sql, &database, &mut tx) {
            Ok(_) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("{err}");
                ExitCode::FAILURE
            }
        };
    }

    match repl(&database, &args.path) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}
