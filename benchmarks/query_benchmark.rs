use criterion::{criterion_group, criterion_main, Criterion};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::errors::DatabaseError;
#[cfg(unix)]
use pprof::criterion::{Output, PProfProfiler};
use sqlite::Error;
use std::fs;
use std::path::Path;

const QUERY_BENCH_kite_sql_PATH: &'static str = "./kitesql_bench";
const QUERY_BENCH_SQLITE_PATH: &'static str = "./sqlite_bench";
const TABLE_ROW_NUM: u64 = 200_000;

fn query_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        ("Full  Read", "select * from t1"),
        ("Point Read", "select * from t1 where c1 = 1000"),
        (
            "Range Read",
            "select * from t1 where c1 > 500 and c1 < 1000",
        ),
    ]
}

fn init_kitesql_query_bench() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path(QUERY_BENCH_kite_sql_PATH).build()?;
    database
        .run("create table t1 (c1 int primary key, c2 int)")?
        .done()?;
    let pb = ProgressBar::new(TABLE_ROW_NUM);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}")
            .unwrap(),
    );
    for i in 0..TABLE_ROW_NUM {
        database
            .run(format!("insert into t1 values({}, {})", i, i + 1).as_str())?
            .done()?;
        pb.set_position(i + 1);
    }
    pb.finish_with_message("Insert completed!");

    database.run("analyze table t1")?.done()?;

    Ok(())
}

fn init_sqlite_query_bench() -> Result<(), Error> {
    let connection = sqlite::open(QUERY_BENCH_SQLITE_PATH.to_owned())?;

    let _ = connection.execute("create table t1 (c1 int primary key, c2 int)")?;

    let pb = ProgressBar::new(TABLE_ROW_NUM);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}")
            .unwrap(),
    );
    for i in 0..TABLE_ROW_NUM {
        let _ = connection.execute(format!("insert into t1 values({}, {})", i, i + 1))?;
        pb.set_position(i + 1);
    }
    pb.finish_with_message("Insert completed!");

    Ok(())
}

fn path_exists_and_is_directory(path: &str) -> bool {
    match fs::metadata(path) {
        Ok(metadata) => metadata.is_dir(),
        Err(_) => false,
    }
}

fn query_on_execute(c: &mut Criterion) {
    if !Path::new(QUERY_BENCH_SQLITE_PATH).exists() {
        println!(
            "SQLITE: The table is not initialized and data insertion is started. => {}",
            TABLE_ROW_NUM
        );

        init_sqlite_query_bench().unwrap();
    }
    if !path_exists_and_is_directory(QUERY_BENCH_kite_sql_PATH) {
        println!(
            "KiteSQL: The table is not initialized and data insertion is started. => {}",
            TABLE_ROW_NUM
        );

        init_kitesql_query_bench().unwrap();
    }
    let database = DataBaseBuilder::path(QUERY_BENCH_kite_sql_PATH)
        .build()
        .unwrap();
    println!("Table initialization completed");

    for (name, case) in query_cases() {
        c.bench_function(format!("KiteSQL: {} by '{}'", name, case).as_str(), |b| {
            b.iter(|| {
                for tuple in database.run(case).unwrap() {
                    let _ = tuple.unwrap();
                }
            })
        });

        let connection = sqlite::open(QUERY_BENCH_SQLITE_PATH.to_owned()).unwrap();
        c.bench_function(format!("SQLite: {} by '{}'", name, case).as_str(), |b| {
            b.iter(|| {
                for row in connection.prepare(case).unwrap() {
                    let _ = row.unwrap();
                }
            })
        });
    }
}

#[cfg(unix)]
criterion_group!(
    name = query_benches;
    config = Criterion::default().sample_size(10).with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = query_on_execute
);
#[cfg(windows)]
criterion_group!(
    name = query_benches;
    config = Criterion::default().sample_size(10);
    targets = query_on_execute
);

criterion_main!(query_benches,);
