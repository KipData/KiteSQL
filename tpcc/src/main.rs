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
use crate::backend::kite::KiteBackend;
use crate::backend::{
    BackendControl, BackendTransaction, ColumnType, PreparedStatement, StatementSpec,
};
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
use rand::prelude::ThreadRng;
use rand::Rng;
use std::fs;
use std::path::Path;
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
pub(crate) const STOCK_LEVEL_DISTINCT_SQL: &str = "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ?1 AND ol_d_id = ?2 AND ol_o_id < ?3 AND ol_o_id >= (?4 - 20)";
pub(crate) const STOCK_LEVEL_DISTINCT_SQLITE: &str = "SELECT DISTINCT ol_i_id FROM (SELECT ol_i_id FROM order_line WHERE ol_w_id = ?1 AND ol_d_id = ?2 AND ol_o_id < ?3 AND ol_o_id >= (?4 - 20) ORDER BY ol_w_id, ol_d_id, ol_o_id)";

pub(crate) trait TpccTransaction {
    type Args;

    fn run(
        tx: &mut dyn BackendTransaction,
        args: &Self::Args,
        statements: &[PreparedStatement],
    ) -> Result<(), TpccError>;
}

pub(crate) trait TpccTest {
    fn name(&self) -> &'static str;

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut dyn BackendTransaction,
        num_ware: usize,
        args: &TpccArgs,
        statements: &[PreparedStatement],
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
    #[clap(long, value_enum, default_value = "kite")]
    backend: BackendKind,
    #[clap(long, default_value = "5")]
    max_retry: usize,
    #[clap(long, default_value = "720")]
    measure_time: u64,
    #[clap(long, default_value = "1")]
    num_ware: usize,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum BackendKind {
    Kite,
    Dual,
}

// TODO: Support multi-threaded TPCC
fn main() -> Result<(), TpccError> {
    let args = Args::parse();
    let mut rng = rand::thread_rng();

    match args.backend {
        BackendKind::Kite => {
            let db_path = Path::new(&args.path);
            if db_path.exists() {
                fs::remove_dir_all(db_path)?;
            }
            let backend = KiteBackend::new(&args.path)?;
            run_tpcc(&backend, &args, &mut rng)?;
        }
        BackendKind::Dual => {
            let db_path = Path::new(&args.path);
            if db_path.exists() {
                fs::remove_dir_all(db_path)?;
            }
            let backend = DualBackend::new(&args.path)?;
            run_tpcc(&backend, &args, &mut rng)?;
        }
    }

    Ok(())
}

fn run_tpcc<B: BackendControl>(
    backend: &B,
    args: &Args,
    rng: &mut ThreadRng,
) -> Result<(), TpccError> {
    Load::load_items(rng, backend)?;
    Load::load_warehouses(rng, backend, args.num_ware)?;
    Load::load_custs(rng, backend, args.num_ware)?;
    Load::load_ord(rng, backend, args.num_ware)?;

    let statement_specs = statement_specs();
    let test_statements = backend.prepare_statements(&statement_specs)?;

    let mut rt_hist = RtHist::new();
    let mut success = [0usize; 5];
    let mut late = [0usize; 5];
    let mut failure = [0usize; 5];
    let tests: Vec<Box<dyn TpccTest>> = vec![
        Box::new(NewOrdTest),
        Box::new(PaymentTest),
        Box::new(OrderStatTest),
        Box::new(DeliveryTest),
        Box::new(SlevTest),
    ];
    let tpcc_args = TpccArgs { joins: args.joins };

    let duration = Duration::new(args.measure_time, 0);
    let mut round_count = 0;
    let mut seq_gen = SeqGen::new(10, 10, 1, 1, 1);
    let tpcc_start = Instant::now();
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
        let tpcc_test = &tests[i];
        let statement = &test_statements[i];

        let mut is_succeed = false;
        let mut last_error = None;
        for _attempt in 0..=args.max_retry {
            let transaction_start = Instant::now();
            let mut tx = backend.new_transaction()?;

            if let Err(err) =
                tpcc_test.do_transaction(rng, &mut tx, args.num_ware, &tpcc_args, statement)
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
                    tpcc_test.name(),
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
                tpcc_test.name(),
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
    rt_hist.hist_report();
    println!("<TpmC>");
    let tpmc = ((success[0] + late[0]) as f64 / (actual_tpcc_time.as_secs_f64() / 60.0)).round();
    println!("{} Tpmc", tpmc);

    Ok(())
}

fn statement_specs() -> Vec<Vec<StatementSpec>> {
    vec![
        vec![
            stmt(
                "SELECT c.c_discount, c.c_last, c.c_credit, w.w_tax FROM customer AS c JOIN warehouse AS w ON c.c_w_id = w_id AND w.w_id = ?1 AND c.c_w_id = ?2 AND c.c_d_id = ?3 AND c.c_id = ?4",
                &[ColumnType::Decimal, ColumnType::Utf8, ColumnType::Utf8, ColumnType::Decimal],
            ),
            stmt(
                "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_id = ?3",
                &[ColumnType::Decimal, ColumnType::Utf8, ColumnType::Utf8],
            ),
            stmt(
                "SELECT w_tax FROM warehouse WHERE w_id = ?1",
                &[ColumnType::Decimal],
            ),
            stmt(
                "SELECT d_next_o_id, d_tax FROM district WHERE d_id = ?1 AND d_w_id = ?2",
                &[ColumnType::Int32, ColumnType::Decimal],
            ),
            stmt(
                "UPDATE district SET d_next_o_id = ?1 + 1 WHERE d_id = ?2 AND d_w_id = ?3",
                &[],
            ),
            stmt(
                "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                &[],
            ),
            stmt(
                "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?1,?2,?3)",
                &[],
            ),
            stmt(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?1",
                &[ColumnType::Decimal, ColumnType::Utf8, ColumnType::Utf8],
            ),
            stmt(
                "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ?1 AND s_w_id = ?2",
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
                "UPDATE stock SET s_quantity = ?1 WHERE s_i_id = ?2 AND s_w_id = ?3",
                &[],
            ),
            stmt(
                "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                &[],
            ),
        ],
        vec![
            stmt(
                "UPDATE warehouse SET w_ytd = w_ytd + ?1 WHERE w_id = ?2",
                &[],
            ),
            stmt(
                "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?1",
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
                "UPDATE district SET d_ytd = d_ytd + ?1 WHERE d_w_id = ?2 AND d_id = ?3",
                &[],
            ),
            stmt(
                "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ?1 AND d_id = ?2",
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
                "SELECT count(c_id) FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_last = ?3",
                &[ColumnType::Int32],
            ),
            stmt(
                "SELECT c_id FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_last = ?3 ORDER BY c_first",
                &[ColumnType::Int32],
            ),
            stmt(
                "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_id = ?3",
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
                "SELECT c_data FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_id = ?3",
                &[ColumnType::Utf8],
            ),
            stmt(
                "UPDATE customer SET c_balance = ?1, c_data = ?2 WHERE c_w_id = ?3 AND c_d_id = ?4 AND c_id = ?5",
                &[],
            ),
            stmt(
                "UPDATE customer SET c_balance = ?1 WHERE c_w_id = ?2 AND c_d_id = ?3 AND c_id = ?4",
                &[],
            ),
            stmt(
                "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                &[],
            ),
        ],
        vec![
            // "SELECT count(c_id) FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_last = ?3"
            stmt(
                "SELECT count(c_id) FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_last = ?3",
                &[ColumnType::Int32],
            ),
            // "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE ... ORDER BY c_first"
            stmt(
                "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_last = ?3 ORDER BY c_first",
                &[
                    ColumnType::Decimal,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            // "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_id = ?3"
            stmt(
                "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ?1 AND c_d_id = ?2 AND c_id = ?3",
                &[
                    ColumnType::Decimal,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                    ColumnType::Utf8,
                ],
            ),
            // "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders ..."
            stmt(
                "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ?1 AND o_d_id = ?2 AND o_c_id = ?3 AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ?4 AND o_d_id = ?5 AND o_c_id = ?6)",
                &[ColumnType::Int32, ColumnType::DateTime, ColumnType::Int32],
            ),
            // "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line ..."
            stmt(
                "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ?1 AND ol_d_id = ?2 AND ol_o_id = ?3",
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
            // "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ?1 AND no_w_id = ?2"
            stmt(
                "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ?1 AND no_w_id = ?2",
                &[ColumnType::Int32],
            ),
            // "DELETE FROM new_orders WHERE no_o_id = ?1 AND no_d_id = ?2 AND no_w_id = ?3"
            stmt(
                "DELETE FROM new_orders WHERE no_o_id = ?1 AND no_d_id = ?2 AND no_w_id = ?3",
                &[],
            ),
            // "SELECT o_c_id FROM orders WHERE o_id = ?1 AND o_d_id = ?2 AND o_w_id = ?3"
            stmt(
                "SELECT o_c_id FROM orders WHERE o_id = ?1 AND o_d_id = ?2 AND o_w_id = ?3",
                &[ColumnType::Int32],
            ),
            // "UPDATE orders SET o_carrier_id = ?1 WHERE o_id = ?2 AND o_d_id = ?3 AND o_w_id = ?4"
            stmt(
                "UPDATE orders SET o_carrier_id = ?1 WHERE o_id = ?2 AND o_d_id = ?3 AND o_w_id = ?4",
                &[],
            ),
            // "UPDATE order_line SET ol_delivery_d = ?1 WHERE ol_o_id = ?2 AND ol_d_id = ?3 AND ol_w_id = ?4"
            stmt(
                "UPDATE order_line SET ol_delivery_d = ?1 WHERE ol_o_id = ?2 AND ol_d_id = ?3 AND ol_w_id = ?4",
                &[],
            ),
            // "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ?1 AND ol_d_id = ?2 AND ol_w_id = ?3"
            stmt(
                "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ?1 AND ol_d_id = ?2 AND ol_w_id = ?3",
                &[ColumnType::Decimal],
            ),
            // "UPDATE customer SET c_balance = c_balance + ?1 , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ?2 ..."
            stmt(
                "UPDATE customer SET c_balance = c_balance + ?1 , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ?2 AND c_d_id = ?3 AND c_w_id = ?4",
                &[],
            ),
        ],
        vec![
            // "SELECT d_next_o_id FROM district WHERE d_id = ?1 AND d_w_id = ?2"
            stmt(
                "SELECT d_next_o_id FROM district WHERE d_id = ?1 AND d_w_id = ?2",
                &[ColumnType::Int32],
            ),
            stmt(STOCK_LEVEL_DISTINCT_SQL, &[ColumnType::Int32]),
            // "SELECT count(*) FROM stock WHERE s_w_id = ?1 AND s_i_id = ?2 AND s_quantity < ?3"
            stmt(
                "SELECT count(*) FROM stock WHERE s_w_id = ?1 AND s_i_id = ?2 AND s_quantity < ?3",
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

#[derive(thiserror::Error, Debug)]
pub enum TpccError {
    #[error("kite_sql: {0}")]
    Database(
        #[source]
        #[from]
        DatabaseError,
    ),
    #[error("sqlite: {0}")]
    Sqlite(
        #[source]
        #[from]
        sqlite::Error,
    ),
    #[error("decimal parse error: {0}")]
    Decimal(
        #[source]
        #[from]
        rust_decimal::Error,
    ),
    #[error("datetime parse error: {0}")]
    Chrono(
        #[source]
        #[from]
        chrono::ParseError,
    ),
    #[error("io error: {0}")]
    Io(
        #[source]
        #[from]
        std::io::Error,
    ),
    #[error("tuples is empty")]
    EmptyTuples,
    #[error("maximum retries reached")]
    MaxRetry,
    #[error("invalid backend usage")]
    InvalidBackend,
    #[error("invalid parameter name")]
    InvalidParameter,
    #[error("invalid datetime value")]
    InvalidDateTime,
    #[error("backend mismatch: {0}")]
    BackendMismatch(String),
}
