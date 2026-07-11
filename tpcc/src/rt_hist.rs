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

use hdrhistogram::Histogram;
use std::time::Duration;

pub(crate) const NUM_TRANSACTIONS: usize = 5;
const SIGNIFICANT_FIGURES: u8 = 3;

const TX_NAMES: [&str; NUM_TRANSACTIONS] = [
    "New-Order",
    "Payment",
    "Order-Status",
    "Delivery",
    "Stock-Level",
];

pub(crate) struct RtHist {
    total: Vec<Histogram<u64>>,
    current_total: Vec<Histogram<u64>>,
}

impl RtHist {
    pub(crate) fn new() -> Self {
        Self {
            total: new_histograms(),
            current_total: new_histograms(),
        }
    }

    pub fn hist_inc(&mut self, transaction: usize, total: Duration) {
        record(&mut self.total[transaction], total);
        record(&mut self.current_total[transaction], total);
    }

    pub fn hist_ckp(&mut self, transaction: usize) -> u64 {
        let histogram = &mut self.current_total[transaction];
        let p90 = percentile(histogram, 0.90);
        histogram.clear();
        p90
    }

    pub fn hist_report(&self) {
        println!("\n<Latency Percentile RT in us (MaxRT)>");
        println!("| Transaction | p90 | Max |");
        println!("| --- | ---: | ---: |");

        for (transaction, name) in TX_NAMES.iter().enumerate() {
            print_percentiles(name, &self.total[transaction]);
        }
    }
}

fn new_histograms() -> Vec<Histogram<u64>> {
    (0..NUM_TRANSACTIONS)
        .map(|_| Histogram::new(SIGNIFICANT_FIGURES).expect("valid histogram precision"))
        .collect()
}

fn duration_micros(duration: Duration) -> u64 {
    let micros = duration.as_nanos().div_ceil(1_000).max(1);
    micros.min(u64::MAX as u128) as u64
}

fn record(histogram: &mut Histogram<u64>, duration: Duration) {
    histogram
        .record(duration_micros(duration))
        .expect("duration fits in histogram");
}

fn percentile(histogram: &Histogram<u64>, quantile: f64) -> u64 {
    if histogram.is_empty() {
        0
    } else {
        histogram.value_at_quantile(quantile)
    }
}

fn print_percentiles(name: &str, histogram: &Histogram<u64>) {
    println!(
        "| {name} | {} | {} |",
        percentile(histogram, 0.90),
        histogram.max(),
    );
}
