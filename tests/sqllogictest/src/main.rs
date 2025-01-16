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
