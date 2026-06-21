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

use crate::backend::dual::DualBackend;
use crate::backend::kitesql_lmdb::KiteSqlLmdbBackend;
use crate::backend::kitesql_rocksdb::{KiteSqlOptimisticRocksDbBackend, KiteSqlRocksDbBackend};
use crate::backend::sqlite::{SqliteBackend, SqliteProfile};
use crate::backend::{BackendControl, BackendTransaction, ColumnType, StatementSpec};
use crate::delivery::DeliveryTest;
use crate::load::Load;
use crate::new_ord::NewOrdTest;
use crate::order_stat::OrderStatTest;
use crate::payment::PaymentTest;
use crate::rt_hist::RtHist;
use crate::slev::SlevTest;
use crate::utils::SeqGen;
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use kite_sql::errors::DatabaseError;
#[cfg(all(unix, feature = "pprof"))]
use pprof::ProfilerGuard;
use rand::prelude::ThreadRng;
use rand::Rng;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;
#[cfg(any(feature = "pprof", test))]
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod backend;
mod delivery;
mod load;
mod new_ord;
mod order_stat;
mod payment;
mod rt_hist;
mod slev;
mod utils;

pub(crate) const ALLOW_MULTI_WAREHOUSE_TX: bool = true;
pub(crate) const CHECK_POINT_COUNT: usize = 1000;
pub(crate) const RT_LIMITS: [Duration; 5] = [
    Duration::from_millis(500),
    Duration::from_millis(500),
    Duration::from_millis(500),
    Duration::from_secs(8),
    Duration::from_secs(2),
];
const TX_NAMES: [&str; 5] = [
    "New-Order",
    "Payment",
    "Order-Status",
    "Delivery",
    "Stock-Level",
];
pub(crate) const STOCK_LEVEL_DISTINCT_SQL: &str = "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id < $3 AND ol_o_id >= ($4 - 20)";
pub(crate) const STOCK_LEVEL_DISTINCT_SQLITE: &str = "SELECT DISTINCT ol_i_id FROM (SELECT ol_i_id FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id < $3 AND ol_o_id >= ($4 - 20) ORDER BY ol_w_id, ol_d_id, ol_o_id)";

pub(crate) trait TpccTransaction {
    type Args;

    fn run<T: BackendTransaction>(
        tx: &mut T,
        args: &Self::Args,
        statements: &mut [T::PreparedStatement],
    ) -> Result<(), TpccError>;
}

pub(crate) trait TpccTest {
    fn do_transaction<T: BackendTransaction>(
        &self,
        rng: &mut ThreadRng,
        tx: &mut T,
        num_ware: usize,
        args: &TpccArgs,
        statements: &mut [T::PreparedStatement],
    ) -> Result<(), TpccError>;
}

struct TpccArgs {
    joins: bool,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "false")]
    joins: bool,
    #[clap(long, default_value = "kite_sql_tpcc")]
    path: String,
    #[clap(long, value_enum, default_value = "kitesql-lmdb")]
    backend: BackendKind,
    #[clap(long, default_value = "5")]
    max_retry: usize,
    #[clap(long, default_value = "720")]
    measure_time: u64,
    #[clap(long, default_value = "1")]
    num_ware: usize,
    #[clap(long, default_value = "false")]
    rocksdb_stats: bool,
    #[clap(long, value_enum, default_value = "balanced")]
    sqlite_profile: SqliteProfile,
    #[cfg(feature = "pprof")]
    #[clap(long)]
    pprof_output: Option<PathBuf>,
    #[cfg(feature = "pprof")]
    #[clap(long, default_value = "100")]
    pprof_frequency: i32,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum BackendKind {
    KitesqlRocksdb,
    KitesqlOptimisticRocksdb,
    KitesqlLmdb,
    Sqlite,
    Dual,
}

// TODO: Support multi-threaded TPCC
fn main() -> Result<(), TpccError> {
    let args = Args::parse();
    let mut rng = rand::thread_rng();

    match args.backend {
        BackendKind::KitesqlRocksdb => {
            reset_db_path(Path::new(&args.path))?;
            let mut backend = KiteSqlRocksDbBackend::new(&args.path, args.rocksdb_stats)?;
            run_tpcc(&mut backend, &args, &mut rng)
        }
        BackendKind::KitesqlOptimisticRocksdb => {
            reset_db_path(Path::new(&args.path))?;
            let mut backend = KiteSqlOptimisticRocksDbBackend::new(&args.path, args.rocksdb_stats)?;
            run_tpcc(&mut backend, &args, &mut rng)
        }
        BackendKind::KitesqlLmdb => {
            reset_db_path(Path::new(&args.path))?;
            let mut backend = KiteSqlLmdbBackend::new(&args.path)?;
            run_tpcc(&mut backend, &args, &mut rng)
        }
        BackendKind::Sqlite => {
            reset_db_path(Path::new(&args.path))?;
            let mut backend = SqliteBackend::new(&args.path, args.sqlite_profile)?;
            run_tpcc(&mut backend, &args, &mut rng)
        }
        BackendKind::Dual => {
            reset_db_path(Path::new(&args.path))?;
            let mut backend = DualBackend::new(&args.path, args.rocksdb_stats)?;
            run_tpcc(&mut backend, &args, &mut rng)
        }
    }
}

fn reset_db_path(path: &Path) -> Result<(), TpccError> {
    if !path.exists() {
        return Ok(());
    }

    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }

    Ok(())
}

fn run_tpcc<B: BackendControl>(
    backend: &mut B,
    args: &Args,
    rng: &mut ThreadRng,
) -> Result<(), TpccError> {
    Load::load_items(rng, backend)?;
    Load::load_warehouses(rng, backend, args.num_ware)?;
    Load::load_custs(rng, backend, args.num_ware)?;
    Load::load_ord(rng, backend, args.num_ware)?;

    let statement_specs = statement_specs();
    let mut test_statements = backend.prepare_statements(&statement_specs)?;

    let mut rt_hist = RtHist::new();
    let mut success = [0usize; 5];
    let mut late = [0usize; 5];
    let mut failure = [0usize; 5];
    let tpcc_args = TpccArgs { joins: args.joins };

    let duration = Duration::new(args.measure_time, 0);
    let mut round_count = 0;
    let mut seq_gen = SeqGen::new(10, 10, 1, 1, 1);
    let tpcc_start = Instant::now();
    #[cfg(all(unix, feature = "pprof"))]
    let pprof = PprofSession::start(args)?;
    let progress = ProgressBar::new_spinner();
    progress.set_style(ProgressStyle::with_template("{spinner:.green} [TPCC] {msg}").unwrap());
    progress.enable_steady_tick(Duration::from_millis(120));
    update_progress_bar(
        &progress,
        round_count,
        &success,
        &late,
        &failure,
        tpcc_start,
    );

    while tpcc_start.elapsed() < duration {
        let i = seq_gen.get();
        let statement = &mut test_statements[i];

        let mut is_succeed = false;
        let mut last_error = None;
        for _attempt in 0..=args.max_retry {
            let transaction_start = Instant::now();
            let mut tx = backend.new_transaction()?;

            if let Err(err) =
                do_tpcc_transaction::<B>(i, rng, &mut tx, args.num_ware, &tpcc_args, statement)
            {
                failure[i] += 1;
                last_error = Some(err);
            } else {
                let rt = transaction_start.elapsed();
                rt_hist.hist_inc(i, rt);
                is_succeed = true;

                if rt <= RT_LIMITS[i] {
                    success[i] += 1;
                } else {
                    late[i] += 1;
                }
                tx.commit()?;
                break;
            }
        }
        if !is_succeed {
            if let Some(err) = last_error {
                eprintln!(
                    "[{}] Error after {} retries: {}",
                    tpcc_test_name(i),
                    args.max_retry,
                    err
                );
            }
            return Err(TpccError::MaxRetry);
        }
        update_progress_bar(
            &progress,
            round_count,
            &success,
            &late,
            &failure,
            tpcc_start,
        );

        if round_count != 0 && round_count % CHECK_POINT_COUNT == 0 {
            let p90 = rt_hist.hist_ckp(i);
            print_checkpoint(
                round_count / CHECK_POINT_COUNT,
                round_count,
                tpcc_test_name(i),
                p90,
                &success,
                &late,
                &failure,
                tpcc_start,
                &progress,
            );
        }
        round_count += 1;
    }
    progress.finish_and_clear();
    let actual_tpcc_time = tpcc_start.elapsed();
    update_progress_bar(
        &progress,
        round_count,
        &success,
        &late,
        &failure,
        tpcc_start,
    );
    print_summary_table(&success, &late, &failure, actual_tpcc_time);
    print_constraint_checks(&success, &late);
    print_response_checks(&success, &late);
    println!();
    rt_hist.finalize();
    rt_hist.hist_report();
    println!("<TpmC>");
    let tpmc = ((success[0] + late[0]) as f64 / (actual_tpcc_time.as_secs_f64() / 60.0)).round();
    println!("{} Tpmc", tpmc);
    if let Some(metrics) = backend.storage_metrics() {
        println!();
        println!("{metrics}");
    }
    #[cfg(all(unix, feature = "pprof"))]
    if let Some(pprof) = pprof {
        pprof.finish()?;
    }

    Ok(())
}

fn tpcc_test_name(index: usize) -> &'static str {
    TX_NAMES[index]
}

fn do_tpcc_transaction<'a, B: BackendControl>(
    index: usize,
    rng: &mut ThreadRng,
    tx: &mut B::Transaction<'a>,
    num_ware: usize,
    args: &TpccArgs,
    statements: &mut [B::PreparedStatement<'a>],
) -> Result<(), TpccError> {
    match index {
        0 => NewOrdTest.do_transaction(rng, tx, num_ware, args, statements),
        1 => PaymentTest.do_transaction(rng, tx, num_ware, args, statements),
        2 => OrderStatTest.do_transaction(rng, tx, num_ware, args, statements),
        3 => DeliveryTest.do_transaction(rng, tx, num_ware, args, statements),
        4 => SlevTest.do_transaction(rng, tx, num_ware, args, statements),
        _ => Err(TpccError::InvalidTransaction),
    }
}

#[cfg(all(unix, feature = "pprof"))]
struct PprofSession {
    guard: ProfilerGuard<'static>,
    output: PathBuf,
}

#[cfg(all(unix, feature = "pprof"))]
impl PprofSession {
    fn start(args: &Args) -> Result<Option<Self>, TpccError> {
        let Some(output) = args.pprof_output.clone() else {
            return Ok(None);
        };
        let guard = ProfilerGuard::new(args.pprof_frequency)
            .map_err(|err| TpccError::Profile(err.to_string()))?;
        Ok(Some(Self { guard, output }))
    }

    fn finish(self) -> Result<(), TpccError> {
        let report = self
            .guard
            .report()
            .build()
            .map_err(|err| TpccError::Profile(err.to_string()))?;
        let file = fs::File::create(&self.output)?;
        report
            .flamegraph(file)
            .map_err(|err| TpccError::Profile(err.to_string()))?;
        println!();
        println!("[pprof] flamegraph written to {}", self.output.display());
        Ok(())
    }
}

fn statement_specs() -> Vec<Vec<StatementSpec>> {
    vec![
        vec![
            stmt(
                "SELECT c.c_discount, c.c_last, c.c_credit, w.w_tax FROM customer AS c JOIN warehouse AS w ON c.c_w_id = w_id AND w.w_id = $1 AND c.c_w_id = $2 AND c.c_d_id = $3 AND c.c_id = $4",
                &[ColumnType::Decimal, ColumnType::Utf8, ColumnType::Utf8, ColumnType::Decimal],
            ),
            stmt(
                "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
                &[ColumnType::Decimal, ColumnType::Utf8, ColumnType::Utf8],
            ),
            stmt(
                "SELECT w_tax FROM warehouse WHERE w_id = $1",
                &[ColumnType::Decimal],
            ),
            stmt(
                "SELECT d_next_o_id, d_tax FROM district WHERE d_id = $1 AND d_w_id = $2",
                &[ColumnType::Int32, ColumnType::Decimal],
            ),
            stmt(
                "UPDATE district SET d_next_o_id = $1 + 1 WHERE d_id = $2 AND d_w_id = $3",
                &[],
            ),
            stmt(
                "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES($1, $2, $3, $4, $5, $6, $7)",
                &[],
            ),
            stmt(
                "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES ($1,$2,$3)",
                &[],
            ),
            stmt(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = $1",
                &[ColumnType::Decimal, ColumnType::Utf8, ColumnType::Utf8],
            ),
            stmt(
                "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = $1 AND s_w_id = $2",
                &[
                    ColumnType::Int16,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            stmt(
                "UPDATE stock SET s_quantity = $1 WHERE s_i_id = $2 AND s_w_id = $3",
                &[],
            ),
            stmt(
                "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                &[],
            ),
        ],
        vec![
            stmt(
                "UPDATE warehouse SET w_ytd = w_ytd + $1 WHERE w_id = $2",
                &[],
            ),
            stmt(
                "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = $1",
                &[
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            stmt(
                "UPDATE district SET d_ytd = d_ytd + $1 WHERE d_w_id = $2 AND d_id = $3",
                &[],
            ),
            stmt(
                "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = $1 AND d_id = $2",
                &[
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            stmt(
                "SELECT count(c_id) FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3",
                &[ColumnType::Int32],
            ),
            stmt(
                "SELECT c_id FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 ORDER BY c_first",
                &[ColumnType::Int32],
            ),
            stmt(
                "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
                &[
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Int64,
                    ColumnType::Decimal,
                    ColumnType::Decimal,
                    ColumnType::DateTime,
                ],
            ),
            stmt(
                "SELECT c_data FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
                &[ColumnType::Utf8],
            ),
            stmt(
                "UPDATE customer SET c_balance = $1, c_data = $2 WHERE c_w_id = $3 AND c_d_id = $4 AND c_id = $5",
                &[],
            ),
            stmt(
                "UPDATE customer SET c_balance = $1 WHERE c_w_id = $2 AND c_d_id = $3 AND c_id = $4",
                &[],
            ),
            stmt(
                "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES($1, $2, $3, $4, $5, $6, $7, $8)",
                &[],
            ),
        ],
        vec![
            // "SELECT count(c_id) FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3"
            stmt(
                "SELECT count(c_id) FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3",
                &[ColumnType::Int32],
            ),
            // "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE ... ORDER BY c_first"
            stmt(
                "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3 ORDER BY c_first",
                &[
                    ColumnType::Decimal,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            // "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3"
            stmt(
                "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
                &[
                    ColumnType::Decimal,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            // "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders ..."
            stmt(
                "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3 AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = $4 AND o_d_id = $5 AND o_c_id = $6)",
                &[ColumnType::Int32, ColumnType::DateTime, ColumnType::Int32],
            ),
            // "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line ..."
            stmt(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id = $3",
                &[
                    ColumnType::Int32,
                    ColumnType::Int16,
                    ColumnType::Int8,
                    ColumnType::Decimal,
                    ColumnType::NullableDateTime,
                ],
            ),
        ],
        vec![
            // "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = $1 AND no_w_id = $2"
            stmt(
                "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = $1 AND no_w_id = $2",
                &[ColumnType::Int32],
            ),
            // "DELETE FROM new_orders WHERE no_o_id = $1 AND no_d_id = $2 AND no_w_id = $3"
            stmt(
                "DELETE FROM new_orders WHERE no_o_id = $1 AND no_d_id = $2 AND no_w_id = $3",
                &[],
            ),
            // "SELECT o_c_id FROM orders WHERE o_id = $1 AND o_d_id = $2 AND o_w_id = $3"
            stmt(
                "SELECT o_c_id FROM orders WHERE o_id = $1 AND o_d_id = $2 AND o_w_id = $3",
                &[ColumnType::Int32],
            ),
            // "UPDATE orders SET o_carrier_id = $1 WHERE o_id = $2 AND o_d_id = $3 AND o_w_id = $4"
            stmt(
                "UPDATE orders SET o_carrier_id = $1 WHERE o_id = $2 AND o_d_id = $3 AND o_w_id = $4",
                &[],
            ),
            // "UPDATE order_line SET ol_delivery_d = $1 WHERE ol_o_id = $2 AND ol_d_id = $3 AND ol_w_id = $4"
            stmt(
                "UPDATE order_line SET ol_delivery_d = $1 WHERE ol_o_id = $2 AND ol_d_id = $3 AND ol_w_id = $4",
                &[],
            ),
            // "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = $1 AND ol_d_id = $2 AND ol_w_id = $3"
            stmt(
                "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = $1 AND ol_d_id = $2 AND ol_w_id = $3",
                &[ColumnType::Decimal],
            ),
            // "UPDATE customer SET c_balance = c_balance + $1 , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = $2 ..."
            stmt(
                "UPDATE customer SET c_balance = c_balance + $1 , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = $2 AND c_d_id = $3 AND c_w_id = $4",
                &[],
            ),
        ],
        vec![
            // "SELECT d_next_o_id FROM district WHERE d_id = $1 AND d_w_id = $2"
            stmt(
                "SELECT d_next_o_id FROM district WHERE d_id = $1 AND d_w_id = $2",
                &[ColumnType::Int32],
            ),
            stmt(STOCK_LEVEL_DISTINCT_SQL, &[ColumnType::Int32]),
            // "SELECT count(*) FROM stock WHERE s_w_id = $1 AND s_i_id = $2 AND s_quantity < $3"
            stmt(
                "SELECT count(*) FROM stock WHERE s_w_id = $1 AND s_i_id = $2 AND s_quantity < $3",
                &[ColumnType::Int32],
            ),
        ],
    ]
}

fn stmt(sql: &'static str, result_types: &'static [ColumnType]) -> StatementSpec {
    StatementSpec { sql, result_types }
}

fn print_summary_table(success: &[usize], late: &[usize], failure: &[usize], elapsed: Duration) {
    println!("---------------------------------------------------");
    println!(
        "Transaction Summary (elapsed {:.1}s)",
        elapsed.as_secs_f32()
    );
    println!("+--------------+---------+------+---------+-------+");
    println!("| Transaction  | Success | Late | Failure | Total |");
    println!("+--------------+---------+------+---------+-------+");
    for (idx, name) in TX_NAMES.iter().enumerate() {
        let total = success[idx] + late[idx] + failure[idx];
        println!(
            "| {:<12} | {:>7} | {:>4} | {:>7} | {:>5} |",
            name, success[idx], late[idx], failure[idx], total
        );
    }
    println!("+--------------+---------+------+---------+-------+");
}

fn print_constraint_checks(success: &[usize], late: &[usize]) {
    println!("<Constraint Check> (all must be [OK])");
    println!("[transaction percentage]");
    let total: f64 = success
        .iter()
        .zip(late.iter())
        .map(|(s, l)| (s + l) as f64)
        .sum();
    let checks = [
        (1, "Payment", 43.0),
        (2, "Order-Status", 4.0),
        (3, "Delivery", 4.0),
        (4, "Stock-Level", 4.0),
    ];
    for (idx, name, threshold) in checks {
        let pct = if total > 0.0 {
            ((success[idx] + late[idx]) as f64 / total) * 100.0
        } else {
            0.0
        };
        let status = if pct >= threshold { "OK" } else { "NG" };
        println!("   {name}: {pct:>5.1}% (>={threshold:.1}%)  [{status}]");
    }
}

fn print_response_checks(success: &[usize], late: &[usize]) {
    println!("[response time (at least 90% passed)]");
    for (idx, name) in TX_NAMES.iter().enumerate() {
        let total = success[idx] + late[idx];
        if total == 0 {
            println!("   {name}:  n/a  [NG]");
            continue;
        }
        let pct = (success[idx] as f64 / total as f64) * 100.0;
        let status = if pct >= 90.0 { "OK" } else { "NG" };
        println!("   {name}: {pct:>5.1}%  [{status}]");
    }
}

fn update_progress_bar(
    pb: &ProgressBar,
    round: usize,
    success: &[usize],
    late: &[usize],
    failure: &[usize],
    start: Instant,
) {
    let elapsed = start.elapsed();
    let total_success: usize = success.iter().sum();
    let total_late: usize = late.iter().sum();
    let total_failure: usize = failure.iter().sum();
    let total_mix: f64 = success
        .iter()
        .zip(late.iter())
        .map(|(s, l)| (s + l) as f64)
        .sum();
    let mix_str = if total_mix > 0.0 {
        let share = |idx: usize| ((success[idx] + late[idx]) as f64 / total_mix) * 100.0;
        format!(
            "mix NO {:>4.1}% P {:>4.1}% OS {:>4.1}% D {:>4.1}% SL {:>4.1}%",
            share(0),
            share(1),
            share(2),
            share(3),
            share(4)
        )
    } else {
        "mix n/a".to_string()
    };
    let est_tpmc = if elapsed.as_secs_f64() > 0.0 {
        ((success[0] + late[0]) as f64) / (elapsed.as_secs_f64() / 60.0)
    } else {
        0.0
    };
    pb.set_message(format!(
        "round {:>6} | succ {:>6} late {:>5} fail {:>5} | est TpmC {:>6.0} | {}",
        round, total_success, total_late, total_failure, est_tpmc, mix_str
    ));
}

fn print_checkpoint(
    checkpoint_idx: usize,
    round: usize,
    test_name: &str,
    p90: f64,
    success: &[usize],
    late: &[usize],
    failure: &[usize],
    start: Instant,
    progress: &ProgressBar,
) {
    let elapsed = start.elapsed();
    let total_failure: usize = failure.iter().sum();
    let est_tpmc = if elapsed.as_secs_f64() > 0.0 {
        ((success[0] + late[0]) as f64) / (elapsed.as_secs_f64() / 60.0)
    } else {
        0.0
    };

    progress.println(format!(
        "[CP {checkpoint_idx:>3} | round {round:>6} | {test_name} p90={p90:.3}s | \
est TpmC {:>6.0} | total fail {:>6}]",
        est_tpmc, total_failure
    ));
}

fn other_ware(rng: &mut ThreadRng, home_ware: usize, num_ware: usize) -> usize {
    if num_ware == 1 {
        return home_ware;
    }

    loop {
        let tmp = rng.gen_range(1..num_ware);
        if tmp != home_ware {
            return tmp;
        }
    }
}

#[derive(Debug)]
pub enum TpccError {
    Database(DatabaseError),
    Sqlite(sqlite::Error),
    Decimal(rust_decimal::Error),
    Chrono(chrono::ParseError),
    Io(std::io::Error),
    EmptyTuples,
    MaxRetry,
    InvalidBackend,
    InvalidTransaction,
    InvalidParameter,
    InvalidDateTime,
    BackendMismatch(String),
    Profile(String),
}

impl fmt::Display for TpccError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Database(err) => write!(f, "kite_sql: {err}"),
            Self::Sqlite(err) => write!(f, "sqlite: {err}"),
            Self::Decimal(err) => write!(f, "decimal parse error: {err}"),
            Self::Chrono(err) => write!(f, "datetime parse error: {err}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::EmptyTuples => f.write_str("tuples is empty"),
            Self::MaxRetry => f.write_str("maximum retries reached"),
            Self::InvalidBackend => f.write_str("invalid backend usage"),
            Self::InvalidTransaction => f.write_str("invalid transaction index"),
            Self::InvalidParameter => f.write_str("invalid parameter name"),
            Self::InvalidDateTime => f.write_str("invalid datetime value"),
            Self::BackendMismatch(value) => write!(f, "backend mismatch: {value}"),
            Self::Profile(value) => write!(f, "profile error: {value}"),
        }
    }
}

impl Error for TpccError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Database(err) => Some(err),
            Self::Sqlite(err) => Some(err),
            Self::Decimal(err) => Some(err),
            Self::Chrono(err) => Some(err),
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

macro_rules! impl_from_tpcc_error {
    ($source:ty, $variant:ident) => {
        impl From<$source> for TpccError {
            fn from(value: $source) -> Self {
                Self::$variant(value)
            }
        }
    };
}

impl_from_tpcc_error!(DatabaseError, Database);
impl_from_tpcc_error!(sqlite::Error, Sqlite);
impl_from_tpcc_error!(rust_decimal::Error, Decimal);
impl_from_tpcc_error!(chrono::ParseError, Chrono);
impl_from_tpcc_error!(std::io::Error, Io);

#[ignore]
#[test]
fn explain_tpcc() -> Result<(), DatabaseError> {
    use kite_sql::db::{DataBaseBuilder, ResultIter};

    fn create_table<I: ResultIter>(mut iter: I) -> Result<String, DatabaseError> {
        let mut output = iter.schema(|schema| {
            schema
                .iter()
                .map(|column| column.full_name().to_string())
                .collect::<Vec<_>>()
                .join("\t")
        });
        if !output.is_empty() {
            output.push('\n');
        }
        for tuple in iter.by_ref() {
            let tuple = tuple?;
            output.push_str(
                &tuple
                    .values
                    .iter()
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
                    .join("\t"),
            );
            output.push('\n');
        }
        iter.done()?;
        Ok(output)
    }

    let database = DataBaseBuilder::path(tpcc_db_path()).build_lmdb()?;
    let mut tx = database.new_transaction()?;

    let customer_tuple = tx
        .run("SELECT c_w_id, c_d_id, c_id, c_last, c_balance, c_data FROM customer limit 1")?
        .next()
        .unwrap()?;
    let district_tuple = tx
        .run("SELECT d_id, d_w_id, d_next_o_id FROM district limit 1")?
        .next()
        .unwrap()?;
    let item_tuple = tx.run("SELECT i_id FROM item limit 1")?.next().unwrap()?;
    let stock_tuple = tx
        .run("SELECT s_i_id, s_w_id, s_quantity FROM stock limit 1")?
        .next()
        .unwrap()?;
    let orders_tuple = tx
        .run("SELECT o_w_id, o_d_id, o_c_id, o_id, o_carrier_id FROM orders limit 1")?
        .next()
        .unwrap()?;
    let order_line_tuple = tx
        .run("SELECT ol_w_id, ol_d_id, ol_o_id, ol_delivery_d FROM order_line limit 1")?
        .next()
        .unwrap()?;
    let new_order_tuple = tx
        .run("SELECT no_d_id, no_w_id, no_o_id FROM new_orders limit 1")?
        .next()
        .unwrap()?;

    let c_w_id = customer_tuple.values[0].clone();
    let c_d_id = customer_tuple.values[1].clone();
    let c_id = customer_tuple.values[2].clone();
    let c_last = customer_tuple.values[3].clone();
    let c_balance = customer_tuple.values[4].clone();
    let c_data = customer_tuple.values[5].clone();

    let d_id = district_tuple.values[0].clone();
    let d_w_id = district_tuple.values[1].clone();
    let d_next_o_id = district_tuple.values[2].clone();

    let i_id = item_tuple.values[0].clone();

    let s_i_id = stock_tuple.values[0].clone();
    let s_w_id = stock_tuple.values[1].clone();
    let s_quantity = stock_tuple.values[2].clone();

    let o_w_id = orders_tuple.values[0].clone();
    let o_d_id = orders_tuple.values[1].clone();
    let o_c_id = orders_tuple.values[2].clone();
    let o_id = orders_tuple.values[3].clone();
    let o_carrier_id = orders_tuple.values[4].clone();

    let ol_w_id = order_line_tuple.values[0].clone();
    let ol_d_id = order_line_tuple.values[1].clone();
    let ol_o_id = order_line_tuple.values[2].clone();
    let ol_delivery_d = order_line_tuple.values[3].clone();

    let no_d_id = new_order_tuple.values[0].clone();
    let no_w_id = new_order_tuple.values[1].clone();
    let no_o_id = new_order_tuple.values[2].clone();
    // ORDER
    {
        println!("========Explain on Order");
        {
            println!("{}", format!("explain SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_w_id, c_d_id, c_id));
            let iter = tx.run(format!("explain SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_w_id, c_d_id, c_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!(
                "explain SELECT d_next_o_id, d_tax FROM district WHERE d_id = {} AND d_w_id = {}",
                d_id, d_w_id
            ))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!(
                "explain UPDATE district SET d_next_o_id = {} + 1 WHERE d_id = {} AND d_w_id = {}",
                d_next_o_id, d_id, d_w_id
            ))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!(
                "explain SELECT i_price, i_name, i_data FROM item WHERE i_id = {}",
                i_id
            ))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = {} AND s_w_id = {}", s_i_id, s_w_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!(
                "explain UPDATE stock SET s_quantity = {} WHERE s_i_id = {} AND s_w_id = {}",
                s_quantity, s_i_id, s_w_id
            ))?;

            println!("{}", create_table(iter)?);
        }
    }

    // Payment
    {
        println!("========Explain on Payment");
        {
            let iter = tx.run(format!(
                "explain UPDATE stock SET s_quantity = {} WHERE s_i_id = {} AND s_w_id = {}",
                s_quantity, s_i_id, s_w_id
            ))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {} AND d_id = {}", d_w_id, d_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT count(c_id) FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}'", c_w_id, c_d_id, c_last))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT c_id FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first", c_w_id, c_d_id, c_last))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_w_id, c_d_id, c_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT c_data FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_w_id, c_d_id, c_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain UPDATE customer SET c_balance = {}, c_data = '{}' WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_balance, c_data, c_w_id, c_d_id, c_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain UPDATE customer SET c_balance = {} WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_balance, c_w_id, c_d_id, c_id))?;

            println!("{}", create_table(iter)?);
        }
    }

    // Order-Stat
    {
        println!("========Explain on Order-Stat");
        {
            let iter = tx.run(format!("explain SELECT count(c_id) FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}'", c_w_id, c_d_id, c_last))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_last = '{}' ORDER BY c_first", c_w_id, c_d_id, c_last))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}", c_w_id, c_d_id, c_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {})", o_w_id, o_d_id, o_c_id, o_w_id, o_d_id, o_c_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id = {}", ol_w_id, ol_d_id, ol_o_id))?;

            println!("{}", create_table(iter)?);
        }
    }

    // Deliver
    {
        println!("========Explain on Deliver");
        {
            let iter = tx.run(format!("explain SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = {} AND no_w_id = {}", no_d_id, no_w_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain DELETE FROM new_orders WHERE no_o_id = {} AND no_d_id = {} AND no_w_id = {}", no_o_id, no_d_id, no_w_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!(
                "explain SELECT o_c_id FROM orders WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}",
                o_id, o_d_id, o_w_id
            ))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain UPDATE orders SET o_carrier_id = {} WHERE o_id = {} AND o_d_id = {} AND o_w_id = {}", o_carrier_id, o_id, o_d_id, o_w_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain UPDATE order_line SET ol_delivery_d = '{}' WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}", ol_delivery_d, ol_o_id, ol_d_id, ol_w_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {}", ol_o_id, ol_d_id, ol_w_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain UPDATE customer SET c_balance = c_balance + 1 , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}", c_id, c_d_id, c_w_id))?;

            println!("{}", create_table(iter)?);
        }
    }

    // Stock-Level
    {
        println!("========Explain on Stock-Level");
        {
            let iter = tx.run(format!(
                "explain SELECT d_next_o_id FROM district WHERE d_id = {} AND d_w_id = {}",
                d_id, d_w_id
            ))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id < {} AND ol_o_id >= ({} - 20)", ol_w_id, ol_d_id, ol_o_id, ol_o_id))?;

            println!("{}", create_table(iter)?);
        }
        {
            let iter = tx.run(format!("explain SELECT count(*) FROM stock WHERE s_w_id = {} AND s_i_id = {} AND s_quantity < {}", s_w_id, s_i_id, s_quantity))?;

            println!("{}", create_table(iter)?);
        }
    }

    Ok(())
}

#[cfg(test)]
fn tpcc_db_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("kite_sql_tpcc")
}
