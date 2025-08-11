use ordered_float::OrderedFloat;
use std::time::Duration;

pub(crate) const MAX_REC: usize = 20;
pub(crate) const REC_PER_SEC: usize = 1000;
pub(crate) const NUM_TRANSACTIONS: usize = 5;

#[derive(Clone)]
pub(crate) struct RtHist {
    total_hist: Vec<Vec<i32>>,
    cur_hist: Vec<Vec<i32>>,
    max_rt: [f64; 5],
    cur_max_rt: [f64; 5],
}

impl RtHist {
    pub(crate) fn new() -> Self {
        let total_hist = vec![vec![0; MAX_REC * REC_PER_SEC]; 5];
        let cur_hist = vec![vec![0; MAX_REC * REC_PER_SEC]; 5];

        let max_rt = [0.0; 5];
        let cur_max_rt = [0.0; 5];

        Self {
            total_hist,
            cur_hist,
            max_rt,
            cur_max_rt,
        }
    }

    // Increment matched one
    pub fn hist_inc(&mut self, transaction: usize, rtclk: Duration) {
        let i = (rtclk.as_secs_f64() * REC_PER_SEC as f64) as usize;
        let i = if i >= (MAX_REC * REC_PER_SEC) {
            (MAX_REC * REC_PER_SEC) - 1
        } else {
            i
        };

        if rtclk.as_secs_f64() > self.cur_max_rt[transaction] {
            self.cur_max_rt[transaction] = rtclk.as_secs_f64();
        }

        self.cur_hist[transaction][i] += 1;
    }

    // Merge local histogram into this histogram
    pub fn merge(&mut self, other: &RtHist) {
        for transaction in 0..5 {
            // Merge current histograms
            for i in 0..(MAX_REC * REC_PER_SEC) {
                self.cur_hist[transaction][i] += other.cur_hist[transaction][i];
            }

            // Update max response time
            if other.cur_max_rt[transaction] > self.cur_max_rt[transaction] {
                self.cur_max_rt[transaction] = other.cur_max_rt[transaction];
            }
        }
    }

    // Checkpoint and add to the total histogram
    pub fn hist_ckp(&mut self, transaction: usize) -> f64 {
        let mut total = 0;
        let mut tmp = 0;
        let mut line = MAX_REC * REC_PER_SEC;
        let mut line_set = false;

        for i in 0..(MAX_REC * REC_PER_SEC) {
            total += self.cur_hist[transaction][i];
        }

        for i in 0..(MAX_REC * REC_PER_SEC) {
            tmp += self.cur_hist[transaction][i];
            self.total_hist[transaction][i] += self.cur_hist[transaction][i];
            self.cur_hist[transaction][i] = 0;

            if tmp >= total * 99 / 100 && !line_set {
                line = i;
                line_set = true;
            }
        }
        for i in 0..5 {
            self.max_rt[i] = *OrderedFloat(self.cur_max_rt[i]).max(OrderedFloat(self.max_rt[i]));
            self.cur_max_rt[i] = 0.0;
        }

        line as f64 / REC_PER_SEC as f64
    }

    // Report histograms
    pub fn hist_report(&self) {
        let mut total = [0; NUM_TRANSACTIONS];
        let mut tmp = [0; NUM_TRANSACTIONS];
        let mut line = [MAX_REC * REC_PER_SEC; NUM_TRANSACTIONS];

        for j in 0..NUM_TRANSACTIONS {
            for i in 0..(MAX_REC * REC_PER_SEC) {
                total[j] += self.total_hist[j][i];
            }

            for i in (0..(MAX_REC * REC_PER_SEC)).rev() {
                tmp[j] += self.total_hist[j][i];
                if tmp[j] * 10 <= total[j] {
                    line[j] = i;
                }
            }
        }

        println!("\n<RT Histogram>");
        for j in 0..NUM_TRANSACTIONS {
            match j {
                0 => println!("\n1.New-Order\n"),
                1 => println!("\n2.Payment\n"),
                2 => println!("\n3.Order-Status\n"),
                3 => println!("\n4.Delivery\n"),
                4 => println!("\n5.Stock-Level\n"),
                _ => (),
            }

            for i in 0..(MAX_REC * REC_PER_SEC) {
                if i <= line[j] * 4 && self.total_hist[j][i] > 0 {
                    println!(
                        "{:.3}, {:6}",
                        (i + 1) as f64 / REC_PER_SEC as f64,
                        self.total_hist[j][i]
                    );
                }
            }
        }

        println!("\n<90th Percentile RT (MaxRT)>");
        for j in 0..NUM_TRANSACTIONS {
            match j {
                0 => print!("   New-Order : "),
                1 => print!("     Payment : "),
                2 => print!("Order-Status : "),
                3 => print!("    Delivery : "),
                4 => print!(" Stock-Level : "),
                _ => (),
            }
            println!(
                "{:.3}  ({:.3})",
                line[j] as f64 / REC_PER_SEC as f64,
                self.max_rt[j]
            );
        }
    }
}
