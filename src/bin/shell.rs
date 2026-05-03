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

#[cfg(not(target_arch = "wasm32"))]
mod native {
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

    #[derive(Debug, PartialEq, Eq)]
    enum Mode {
        Continue,
        Exit,
    }

    #[derive(Debug)]
    struct Args {
        path: String,
        execute: Option<String>,
    }

    fn parse_args() -> Result<Args, String> {
        parse_args_from(env::args().skip(1))
    }

    fn parse_args_from<I, S>(args: I) -> Result<Args, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut path = DEFAULT_PATH.to_string();
        let mut execute = None;
        let mut args = args.into_iter().map(Into::into);

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

    fn should_execute_line(trimmed_line: &str) -> bool {
        trimmed_line.is_empty() || trimmed_line.ends_with(';')
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

        if normalized.eq_ignore_ascii_case("commit")
            || normalized.eq_ignore_ascii_case("commit work")
        {
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
            let Some(line) = input.read_line(prompt(tx.is_some(), !buffer.trim().is_empty()))?
            else {
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

            let should_execute = should_execute_line(trimmed);
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

    pub(super) fn run() -> ExitCode {
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

    #[cfg(test)]
    mod tests {
        use super::*;
        use tempfile::TempDir;

        fn test_database() -> (TempDir, Database<RocksStorage>) {
            let temp_dir = TempDir::new().expect("failed to create temp dir");
            let database = DataBaseBuilder::path(temp_dir.path())
                .build_rocksdb()
                .expect("failed to create test database");
            (temp_dir, database)
        }

        #[test]
        fn parse_args_uses_defaults() {
            let args = parse_args_from(Vec::<String>::new()).expect("args should parse");

            assert_eq!(args.path, DEFAULT_PATH);
            assert_eq!(args.execute, None);
        }

        #[test]
        fn parse_args_accepts_path_and_execute_sql() {
            let args = parse_args_from(["--path", "/tmp/kite", "-e", "select 1;"])
                .expect("args should parse");

            assert_eq!(args.path, "/tmp/kite");
            assert_eq!(args.execute.as_deref(), Some("select 1;"));
        }

        #[test]
        fn parse_args_rejects_missing_values_and_unknown_flags() {
            assert_eq!(
                parse_args_from(["--path"]).expect_err("--path should require a value"),
                "--path expects a value"
            );
            assert_eq!(
                parse_args_from(["--execute"]).expect_err("--execute should require a value"),
                "--execute expects a value"
            );
            assert!(parse_args_from(["--unknown"])
                .expect_err("unknown flags should be rejected")
                .starts_with("unknown argument: --unknown"));
        }

        #[test]
        fn prompt_reflects_transaction_and_continuation_state() {
            assert_eq!(prompt(false, false), "kite> ");
            assert_eq!(prompt(true, false), "kite(tx)> ");
            assert_eq!(prompt(false, true), " ...> ");
            assert_eq!(prompt(true, true), " ...(tx)> ");
        }

        #[test]
        fn should_execute_line_matches_repl_completion_rules() {
            assert!(should_execute_line(""));
            assert!(should_execute_line("select 1;"));
            assert!(should_execute_line("   ;"));
            assert!(!should_execute_line("select 1"));
            assert!(!should_execute_line("select 1; -- comment"));
        }

        #[test]
        fn meta_command_exit_aliases_stop_the_shell() {
            let (_temp_dir, database) = test_database();
            let mut tx = None;

            assert_eq!(
                handle_meta_command(".quit", &database, &mut tx).expect(".quit should parse"),
                Mode::Exit
            );
            assert_eq!(
                handle_meta_command(".exit", &database, &mut tx).expect(".exit should parse"),
                Mode::Exit
            );
        }

        #[test]
        fn transaction_commands_update_shell_transaction_state() {
            let (_temp_dir, database) = test_database();
            let mut tx = None;

            assert_eq!(
                handle_command("BEGIN", &database, &mut tx).expect("begin should succeed"),
                Mode::Continue
            );
            assert!(tx.is_some());

            assert_eq!(
                handle_command("COMMIT", &database, &mut tx).expect("commit should succeed"),
                Mode::Continue
            );
            assert!(tx.is_none());

            assert_eq!(
                handle_command("start transaction", &database, &mut tx)
                    .expect("start transaction should succeed"),
                Mode::Continue
            );
            assert!(tx.is_some());

            assert_eq!(
                handle_command("ROLLBACK", &database, &mut tx).expect("rollback should succeed"),
                Mode::Continue
            );
            assert!(tx.is_none());
        }

        #[test]
        fn sql_and_schema_metacommands_run_against_database() {
            let (_temp_dir, database) = test_database();
            let mut tx = None;

            handle_command(
                "create table users (id int primary key, name varchar);",
                &database,
                &mut tx,
            )
            .expect("create table should succeed");
            handle_command("insert into users values (1, 'alice');", &database, &mut tx)
                .expect("insert should succeed");

            assert_eq!(
                handle_meta_command(".tables", &database, &mut tx).expect(".tables should succeed"),
                Mode::Continue
            );
            assert_eq!(
                handle_meta_command(".schema users", &database, &mut tx)
                    .expect(".schema should succeed"),
                Mode::Continue
            );
            assert_eq!(
                handle_command("select * from users;", &database, &mut tx)
                    .expect("select should succeed"),
                Mode::Continue
            );
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> std::process::ExitCode {
    native::run()
}

#[cfg(target_arch = "wasm32")]
fn main() {}
