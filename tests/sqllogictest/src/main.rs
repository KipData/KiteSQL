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

use clap::Parser;
use kite_sql::db::DataBaseBuilder;
use sqllogictest::Runner;
use sqllogictest_test::SQLBase;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::time::Instant;
use tempfile::TempDir;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "tests/slt/**/*.slt")]
    path: String,
}

fn main() {
    let args = Args::parse();

    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    std::env::set_current_dir(path).unwrap();

    println!("KiteSQL Test Start!\n");
    init_20000_row_csv().expect("failed to init csv");
    init_distinct_rows_csv().expect("failed to init distinct csv");
    let mut file_num = 0;
    let start = Instant::now();

    for slt_file in glob::glob(&args.path).expect("failed to find slt files") {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let filepath = slt_file.expect("failed to read slt file");
        println!(
            "-> Now the test file is: {}, num: {}",
            filepath.display(),
            file_num
        );

        let db = DataBaseBuilder::path(temp_dir.path())
            .build()
            .expect("init db error");
        let mut tester = Runner::new(SQLBase { db });

        if let Err(err) = tester.run_file(filepath) {
            panic!("test error: {}", err);
        }
        println!("-> Pass!\n");
        file_num += 1;
    }
    println!("Passed all tests for a total of {} files!!!", file_num + 1);
    println!("|- Total time spent: {:?}", start.elapsed());
    if cfg!(debug_assertions) {
        println!("|- Debug mode");
    } else {
        println!("|- Release mode");
    }
}

fn init_20000_row_csv() -> io::Result<()> {
    let path = "tests/data/row_20000.csv";

    if !Path::new(path).exists() {
        let mut file = File::create(path)?;

        for i in 0..20_000 {
            let row = (0..3)
                .map(|j| (i * 3 + j).to_string())
                .collect::<Vec<_>>()
                .join("|");
            writeln!(file, "{}", row)?;
        }
    }

    Ok(())
}

fn init_distinct_rows_csv() -> io::Result<()> {
    let path = "tests/data/distinct_rows.csv";

    if !Path::new(path).exists() {
        let mut file = File::create(path)?;
        let rows = 200_000usize;
        let distinct = 20usize;

        for i in 0..rows {
            let id = i as i32;
            let c1 = (i % distinct) as i32;
            let c2 = ((i * 7) % 1000) as i32;
            writeln!(file, "{}|{}|{}", id, c1, c2)?;
        }
    }

    Ok(())
}
