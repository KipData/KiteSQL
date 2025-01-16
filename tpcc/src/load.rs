use crate::TpccError;
use chrono::Utc;
use indicatif::{ProgressBar, ProgressStyle};
use kite_sql::db::{Database, ResultIter};
use kite_sql::storage::Storage;
use rand::rngs::ThreadRng;
use rand::Rng;
use rust_decimal::Decimal;
use std::marker::PhantomData;
use std::ops::Add;
// https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Load.java

pub(crate) const MAX_ITEMS: usize = 100_000;
pub(crate) const CUST_PER_DIST: usize = 3_000;
pub(crate) const DIST_PER_WARE: usize = 10;
pub(crate) const ORD_PER_DIST: usize = 3000;

pub(crate) static MAX_NUM_ITEMS: usize = 15;

fn generate_string(rng: &mut ThreadRng, min: usize, max: usize) -> String {
    let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        .chars()
        .collect();

    let max = if min == max {
        min
    } else {
        rng.gen_range(min..max)
    };
    (0..max)
        .map(|_| chars[rng.gen_range(0..chars.len())])
        .collect()
}

pub(crate) fn nu_rand(rng: &mut ThreadRng, a: usize, x: usize, y: usize) -> usize {
    let c = match a {
        255 => 255,
        1023 => 1023,
        8191 => 8191,
        _ => unreachable!(),
    };

    (((rng.gen_range(0..a) | rng.gen_range(x..y)) + c) % (y - x + 1)) + x
}

pub(crate) fn last_name(num: usize) -> String {
    let n = [
        "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
    ];

    let mut name = n[num / 100].to_string();
    name = name.add(n[(num / 10) % 10]);
    name = name.add(n[num % 10]);
    name
}

fn init_permutation(rng: &mut ThreadRng) -> [usize; CUST_PER_DIST] {
    let mut nums = [0; CUST_PER_DIST];
    let mut temp_nums = [0; CUST_PER_DIST];

    for i in 0..ORD_PER_DIST {
        nums[i] = i + 1;
        temp_nums[i] = i + 1;
    }
    for i in 0..ORD_PER_DIST - 1 {
        let j = if i + 1 >= ORD_PER_DIST - 1 {
            i + 1
        } else {
            rng.gen_range(i + 1..ORD_PER_DIST - 1)
        };
        nums[j] = temp_nums[i];
    }
    nums
}

pub struct Load<S> {
    phantom: PhantomData<S>,
}

impl<S: Storage> Load<S> {
    /// table: item
    ///
    /// i_id int not null
    /// i_im_id int
    /// i_name varchar(24),
    /// i_price decimal(5, 2)
    /// i_data varchar(50)
    ///
    /// primary key (i_id)
    pub fn load_items(rng: &mut ThreadRng, db: &Database<S>) -> Result<(), TpccError> {
        db.run("drop table if exists item;")?.done()?;
        db.run(
            "create table item (
                      i_id int not null,
                      i_im_id int,
                      i_name varchar(24),
                      i_price decimal(5,2),
                      i_data varchar(50),
                      PRIMARY KEY(i_id) );",
        )?
        .done()?;
        let pb = ProgressBar::new(MAX_ITEMS as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[loading items: {elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}",
                )
                .unwrap(),
        );
        let orig = Self::gen_orig(rng);

        for i_id in 1..MAX_ITEMS + 1 {
            let i_im_id = rng.gen_range(1..10000);
            let i_name = generate_string(rng, 14, 24);
            let i_price = Decimal::from_f64_retain(rng.gen_range(1.0..1000.0))
                .unwrap()
                .round_dp(2);
            let mut i_data = generate_string(rng, 26, 50);
            if orig[i_id] == 0 {
                let pos = rng.gen_range(0..i_data.len() - 8);
                let (prefix, suffix) = i_data.split_at(pos);
                let (_, remainder) = suffix.split_at(8);

                i_data = format!("{}original{}", prefix, remainder);
            }

            db.run(format!(
                "insert into item values ({i_id}, {i_im_id}, '{i_name}', {i_price}, '{i_data}')"
            ))?
            .done()?;
            pb.set_position(i_id as u64);
        }
        pb.finish_with_message("load completed!");
        println!("[Analyze Table: item]");
        db.run("analyze table item")?.done()?;
        Ok(())
    }

    /// table: warehouse
    ///
    /// w_id smallint not null
    /// w_name varchar(10)
    /// w_street_1 varchar(20)
    /// w_street_2 varchar(20)
    /// w_city varchar(20)
    /// w_state char(2)
    /// w_zip char(9)
    /// w_tax decimal(4, 2)
    /// w_ytd decimal(12, 2)
    ///
    /// primary key (w_id)
    pub fn load_warehouses(
        rng: &mut ThreadRng,
        db: &Database<S>,
        num_ware: usize,
    ) -> Result<(), TpccError> {
        db.run("drop table if exists warehouse;")?.done()?;
        db.run(
            "create table warehouse (
                           w_id smallint not null,
                           w_name varchar(10),
                           w_street_1 varchar(20),
                           w_street_2 varchar(20),
                           w_city varchar(20),
                           w_state char(2),
                           w_zip char(9),
                           w_tax decimal(4,2),
                           w_ytd decimal(12,2),
                           PRIMARY KEY(w_id) );",
        )?
        .done()?;
        db.run("drop table if exists stock;")?.done()?;
        db.run(
            "create table stock (
                       s_i_id int not null,
                       s_w_id smallint not null,
                       s_quantity smallint,
                       s_dist_01 char(24),
                       s_dist_02 char(24),
                       s_dist_03 char(24),
                       s_dist_04 char(24),
                       s_dist_05 char(24),
                       s_dist_06 char(24),
                       s_dist_07 char(24),
                       s_dist_08 char(24),
                       s_dist_09 char(24),
                       s_dist_10 char(24),
                       s_ytd decimal(8,0),
                       s_order_cnt smallint,
                       s_remote_cnt smallint,
                       s_data varchar(50),
                       PRIMARY KEY(s_w_id, s_i_id) );",
        )?
        .done()?;
        db.run("CREATE INDEX fkey_stock_2 ON stock (s_i_id);")?
            .done()?;
        db.run("drop table if exists district;")?.done()?;
        db.run(
            "create table district (
                          d_id tinyint not null,
                          d_w_id smallint not null,
                          d_name varchar(10),
                          d_street_1 varchar(20),
                          d_street_2 varchar(20),
                          d_city varchar(20),
                          d_state char(2),
                          d_zip char(9),
                          d_tax decimal(4,2),
                          d_ytd decimal(12,2),
                          d_next_o_id int,
                          primary key (d_w_id, d_id) );",
        )?
        .done()?;
        let pb = ProgressBar::new(num_ware as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[loading warehouses: {elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}",
                )
                .unwrap(),
        );
        for w_id in 1..num_ware + 1 {
            let w_name = generate_string(rng, 6, 10);
            let w_street_1 = generate_string(rng, 10, 20);
            let w_street_2 = generate_string(rng, 10, 20);
            let w_city = generate_string(rng, 10, 20);
            let w_state = generate_string(rng, 2, 2);
            let w_zip = generate_string(rng, 9, 9);

            let w_tax = Decimal::from_f64_retain(rng.gen_range(1.0..100.0))
                .unwrap()
                .round_dp(2);
            let w_ytd = Decimal::from_f64_retain(3000000.00).unwrap().round_dp(2);

            db.run(format!(
                "insert into warehouse values({}, '{}', '{}', '{}', '{}', '{}', '{}', {}, {})",
                w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd,
            ))?
            .done()?;
            Self::stock(rng, db, w_id)?;
            Self::district(rng, db, w_id)?;

            pb.set_position(w_id as u64);
        }
        pb.finish_with_message("load completed!");
        println!("[Analyze Table: stock]");
        db.run("analyze table stock")?.done()?;
        Ok(())
    }

    pub fn load_custs(
        rng: &mut ThreadRng,
        db: &Database<S>,
        num_ware: usize,
    ) -> Result<(), TpccError> {
        db.run("drop table if exists customer;")?.done()?;
        db.run(
            "create table customer (
                          c_id int not null,
                          c_d_id tinyint not null,
                          c_w_id smallint not null,
                          c_first varchar(16),
                          c_middle char(2),
                          c_last varchar(16),
                          c_street_1 varchar(20),
                          c_street_2 varchar(20),
                          c_city varchar(20),
                          c_state char(2),
                          c_zip char(9),
                          c_phone char(16),
                          c_since datetime,
                          c_credit char(2),
                          c_credit_lim bigint,
                          c_discount decimal(4,2),
                          c_balance decimal(12,2),
                          c_ytd_payment decimal(12,2),
                          c_payment_cnt smallint,
                          c_delivery_cnt smallint,
                          c_data text,
                          PRIMARY KEY(c_w_id, c_d_id, c_id) );",
        )?
        .done()?;
        db.run("CREATE INDEX idx_customer ON customer (c_w_id,c_d_id,c_last,c_first);")?
            .done()?;
        db.run("drop table if exists history;")?.done()?;
        db.run(
            "create table history (
                         h_c_id int,
                         h_c_d_id tinyint,
                         h_c_w_id smallint,
                         h_d_id tinyint,
                         h_w_id smallint,
                         h_date datetime,
                         h_amount decimal(6,2),
                         h_data varchar(24),
                         PRIMARY KEY(h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id) );",
        )?
        .done()?;
        for w_id in 1..num_ware + 1 {
            for d_id in 1..DIST_PER_WARE + 1 {
                Self::load_customers(rng, db, d_id, w_id)?;
            }
        }
        println!("[Analyze Table: customer]");
        db.run("analyze table customer")?.done()?;

        Ok(())
    }

    pub fn load_ord(
        rng: &mut ThreadRng,
        db: &Database<S>,
        num_ware: usize,
    ) -> Result<(), TpccError> {
        db.run("drop table if exists orders;")?.done()?;
        db.run(
            "create table orders (
                        o_id int not null,
                        o_d_id tinyint not null,
                        o_w_id smallint not null,
                        o_c_id int,
                        o_entry_d datetime,
                        o_carrier_id tinyint,
                        o_ol_cnt tinyint,
                        o_all_local tinyint,
                        PRIMARY KEY(o_w_id, o_d_id, o_id) );",
        )?
        .done()?;
        db.run("CREATE INDEX idx_orders ON orders (o_w_id,o_d_id,o_c_id,o_id);")?
            .done()?;
        db.run("drop table if exists new_orders;")?.done()?;
        db.run(
            "create table new_orders (
                            no_o_id int not null,
                            no_d_id tinyint not null,
                            no_w_id smallint not null,
                            PRIMARY KEY(no_w_id, no_d_id, no_o_id));",
        )?
        .done()?;
        db.run("drop table if exists order_line;")?.done()?;
        db.run(
            "create table order_line (
                            ol_o_id int not null,
                            ol_d_id tinyint not null,
                            ol_w_id smallint not null,
                            ol_number tinyint not null,
                            ol_i_id int,
                            ol_supply_w_id smallint,
                            ol_delivery_d datetime,
                            ol_quantity tinyint,
                            ol_amount decimal(6,2),
                            ol_dist_info char(24),
                            PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) );",
        )?
        .done()?;
        db.run("CREATE INDEX fkey_order_line_1 ON order_line (ol_o_id, ol_d_id, ol_w_id);")?
            .done()?;
        db.run("CREATE INDEX fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);")?
            .done()?;
        for w_id in 1..num_ware + 1 {
            for d_id in 1..DIST_PER_WARE + 1 {
                Self::load_orders(rng, db, d_id, w_id)?;
            }
        }
        println!("[Analyze Table: orders & order_line & new_order]");
        db.run("analyze table orders")?.done()?;
        db.run("analyze table order_line")?.done()?;
        db.run("analyze table new_orders")?.done()?;

        Ok(())
    }

    /// table: stock
    ///
    /// s_i_id int not null
    /// s_w_id smallint not null
    /// s_quantity smallint
    /// s_dist_01 char(24)
    /// s_dist_02 char(24)
    /// s_dist_03 char(24)
    /// s_dist_04 char(24)
    /// s_dist_05 char(24)
    /// s_dist_06 char(24)
    /// s_dist_07 char(24)
    /// s_dist_08 char(24)
    /// s_dist_09 char(24)
    /// s_dist_10 char(24)
    /// s_ytd decimal(8,0)
    /// s_order_cnt smallint
    /// s_remote_cnt smallint
    /// s_data varchar(50)
    ///
    /// primary key(s_w_id, s_i_id)
    pub fn stock(rng: &mut ThreadRng, db: &Database<S>, w_id: usize) -> Result<(), TpccError> {
        let pb = ProgressBar::new(MAX_ITEMS as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[loading stock: {elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}",
                )
                .unwrap(),
        );
        let s_w_id = w_id;
        let orig = Self::gen_orig(rng);

        for s_i_id in 1..MAX_ITEMS + 1 {
            let s_quantity = rng.gen_range(10..100);
            let s_dist_01 = generate_string(rng, 24, 24);
            let s_dist_02 = generate_string(rng, 24, 24);
            let s_dist_03 = generate_string(rng, 24, 24);
            let s_dist_04 = generate_string(rng, 24, 24);
            let s_dist_05 = generate_string(rng, 24, 24);
            let s_dist_06 = generate_string(rng, 24, 24);
            let s_dist_07 = generate_string(rng, 24, 24);
            let s_dist_08 = generate_string(rng, 24, 24);
            let s_dist_09 = generate_string(rng, 24, 24);
            let s_dist_10 = generate_string(rng, 24, 24);

            let s_data = if orig[s_i_id] != 0 {
                "original".to_string()
            } else {
                generate_string(rng, 26, 50)
            };
            db.run(format!(
                "insert into stock values({}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, '{}')",
                s_i_id,
                s_w_id,
                s_quantity,
                s_dist_01,
                s_dist_02,
                s_dist_03,
                s_dist_04,
                s_dist_05,
                s_dist_06,
                s_dist_07,
                s_dist_08,
                s_dist_09,
                s_dist_10,
                0,
                0,
                0,
                s_data,
            ))?.done()?;
            pb.set_position(s_i_id as u64);
        }
        pb.finish_with_message("load completed!");

        Ok(())
    }

    #[allow(unused_assignments)]
    fn gen_orig(rng: &mut ThreadRng) -> Vec<i32> {
        let mut orig = vec![0; MAX_ITEMS + 1];

        for _ in 0..MAX_ITEMS / 10 {
            let mut pos = 0;
            loop {
                pos = rng.gen_range(0..MAX_ITEMS);

                if orig[pos] == 0 {
                    break;
                }
            }
            orig[pos] = 1;
        }
        orig
    }

    /// table: district
    ///
    /// d_id tinyint not null
    /// d_w_id smallint not null
    /// d_name varchar(10)
    /// d_street_1 varchar(20)
    /// d_street_2 varchar(20)
    /// d_city varchar(20)
    /// d_state char(2)
    /// d_zip char(9)
    /// d_tax decimal(4,2)
    /// d_ytd decimal(12,2)
    /// d_next_o_id int
    ///
    ///
    /// primary key (d_w_id, d_id)
    pub fn district(rng: &mut ThreadRng, db: &Database<S>, w_id: usize) -> Result<(), TpccError> {
        let pb = ProgressBar::new(DIST_PER_WARE as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[loading district: {elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}",
                )
                .unwrap(),
        );
        let d_w_id = w_id;
        let d_ytd = Decimal::from_f64_retain(30000.0).unwrap().round_dp(2);
        let d_next_o_id = 3001;

        for d_id in 1..DIST_PER_WARE + 1 {
            let d_name = generate_string(rng, 6, 10);
            let d_street_1 = generate_string(rng, 10, 20);
            let d_street_2 = generate_string(rng, 10, 20);
            let d_city = generate_string(rng, 10, 20);
            let d_state = generate_string(rng, 2, 2);
            let d_zip = generate_string(rng, 9, 9);

            let d_tax = Decimal::from_f64_retain(rng.gen_range(0.1..0.2))
                .unwrap()
                .round_dp(2);

            db.run(format!(
                "insert into district values({}, {}, '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {})",
                d_id,
                d_w_id,
                d_name,
                d_street_1,
                d_street_2,
                d_city,
                d_state,
                d_zip,
                d_tax,
                d_ytd,
                d_next_o_id,
            ))?.done()?;
            pb.set_position(d_id as u64);
        }
        pb.finish_with_message("load completed!");

        Ok(())
    }

    /// table: customer
    ///
    /// c_id int not null
    /// c_d_id tinyint not null
    /// c_w_id smallint not null
    /// c_first varchar(16)
    /// c_middle char(2),
    /// c_last varchar(16)
    /// c_street_1 varchar(20)
    /// c_street_2 varchar(20)
    /// c_city varchar(20)
    /// c_state char(2)
    /// c_zip char(9)
    /// c_phone char(16)
    /// c_since datetime
    /// c_credit char(2)
    /// c_credit_lim bigint
    /// c_discount decimal(4, 2)
    /// c_balance decimal(12, 2)
    /// c_ytd_payment decimal(12, 2)
    /// c_payment_cnt smallint
    /// c_delivery_cnt smallint
    /// c_date text,
    ///
    /// primary key(c_w_id, c_d_id, c_id)
    ///
    ///
    /// table: history
    ///
    /// h_c_id int
    /// h_c_d_id tinyint
    /// h_c_w_id smallint
    /// h_d_id tinyint
    /// h_w_id smallint
    /// h_date datetime
    /// h_amount decimal(6, 2)
    /// h_data varchar(24)
    pub fn load_customers(
        rng: &mut ThreadRng,
        db: &Database<S>,
        d_id: usize,
        w_id: usize,
    ) -> Result<(), TpccError> {
        let pb = ProgressBar::new(CUST_PER_DIST as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[loading customers: {elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}",
                )
                .unwrap(),
        );
        let date = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for c_id in 1..CUST_PER_DIST + 1 {
            let c_d_id = d_id;
            let c_w_id = w_id;

            let c_first = generate_string(rng, 8, 16);
            let c_middle = "OE";

            let num = if c_id <= 1000 {
                c_id - 1
            } else {
                nu_rand(rng, 255, 0, 999)
            };
            let c_last = last_name(num);
            let c_street_1 = generate_string(rng, 10, 20);
            let c_street_2 = generate_string(rng, 10, 20);
            let c_city = generate_string(rng, 10, 20);
            let c_state = generate_string(rng, 2, 2);
            let c_zip = generate_string(rng, 9, 9);

            let c_phone = generate_string(rng, 16, 16);
            let c_since = &date;
            let c_credit = if rng.gen_range(0..1) == 1 { "GC" } else { "BC" };
            let c_credit_lim = 50000;
            let c_discount = Decimal::from_f64_retain(rng.gen_range(0.0..0.5))
                .unwrap()
                .round_dp(2);
            let c_balance = "-10.00";

            let c_ytd_payment = "10.00";
            let c_payment_cnt = 1;
            let c_delivery_cnt = 0;

            let c_data = generate_string(rng, 300, 500);

            db.run(format!(
                "insert into customer values({}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}')",
                c_id,
                c_d_id,
                c_w_id,
                c_first,
                c_middle,
                c_last,
                c_street_1,
                c_street_2,
                c_city,
                c_state,
                c_zip,
                c_phone,
                c_since,
                c_credit,
                c_credit_lim,
                c_discount,
                c_balance,
                c_ytd_payment,
                c_payment_cnt,
                c_delivery_cnt,
                c_data,
            ))?.done()?;

            let h_date = &date;
            let h_amount = Decimal::from_f64_retain(10.0).unwrap().round_dp(2);
            let h_data = generate_string(rng, 12, 24);

            db.run(format!(
                "insert into history values({}, {}, {}, {}, {}, '{}', {}, '{}')",
                c_id, c_d_id, c_w_id, c_d_id, c_w_id, h_date, h_amount, h_data,
            ))?
            .done()?;
            pb.set_position(c_id as u64);
        }
        pb.finish_with_message("load completed!");

        Ok(())
    }

    /// table: order
    ///
    /// o_id int not null
    /// o_d_id tinyint not null
    /// o_w_id smallint not null
    /// o_c_id int
    /// o_entry_d datetime
    /// o_carrier_id tinyint
    /// o_ol_cnt tinyint
    /// o_all_local tinyint
    ///
    /// primary key(o_w_id, o_d_id, o_id)
    ///
    ///
    /// table: new_order
    ///
    /// no_o_id int not null,
    /// no_d_id tinyint not null,
    /// no_w_id smallint not null,
    ///
    /// primary key(no_w_id, no_d_id, no_o_id)
    ///
    ///
    /// table: order_line
    ///
    /// ol_o_id int not null
    /// ol_d_id tinyint not null
    /// ol_w_id smallint not null
    /// ol_number tinyint not null
    /// ol_i_id int
    /// ol_supply_w_id smallint
    /// ol_delivery_d datetime
    /// ol_quantity tinyint
    /// ol_amount decimal(6,2)
    /// ol_dist_info char(24)
    ///
    /// primary key(ol_w_id, ol_d_id, ol_o_id, ol_number)
    pub fn load_orders(
        rng: &mut ThreadRng,
        db: &Database<S>,
        d_id: usize,
        w_id: usize,
    ) -> Result<(), TpccError> {
        let pb = ProgressBar::new(ORD_PER_DIST as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[loading orders: {elapsed_precise}] {bar:40.cyan/white} {pos}/{len} {msg}",
                )
                .unwrap(),
        );
        let o_d_id = d_id;
        let o_w_id = w_id;

        let nums = init_permutation(rng);

        for o_id in 1..ORD_PER_DIST + 1 {
            let o_c_id = nums[o_id - 1];
            let o_carrier_id = rng.gen_range(1..10);
            let o_ol_cnt = rng.gen_range(5..15);

            let date = format!("'{}'", Utc::now().format("%Y-%m-%d %H:%M:%S"));

            let o_carrier_id = if o_id > 2100 {
                db.run(format!(
                    "insert into new_orders values({}, {}, {})",
                    o_id, o_d_id, o_w_id,
                ))?
                .done()?;
                "null".to_string()
            } else {
                o_carrier_id.to_string()
            };
            db.run(format!(
                "insert into orders values({}, {}, {}, {}, {}, {}, {}, {})",
                o_id, o_d_id, o_w_id, o_c_id, date, o_carrier_id, o_ol_cnt, "1",
            ))?
            .done()?;

            for ol in 1..o_ol_cnt + 1 {
                let ol_i_id = rng.gen_range(1..MAX_ITEMS);
                let ol_supply_w_id = o_w_id;
                let ol_quantity = 4;
                let ol_amount = 0.0;

                let ol_dist_info = generate_string(rng, 24, 24);
                let (ol_delivery_d, ol_amount) = if o_id > 2100 {
                    ("null", ol_amount)
                } else {
                    (date.as_str(), rng.gen_range(0.1..100.0))
                };
                db.run(format!(
                    "insert into order_line values({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')",
                    o_id,
                    o_d_id,
                    o_w_id,
                    ol,
                    ol_i_id,
                    ol_supply_w_id,
                    ol_delivery_d,
                    ol_quantity,
                    ol_amount,
                    ol_dist_info,
                ))?
                .done()?;
            }
            pb.set_position((o_id - 1) as u64);
        }
        pb.finish_with_message("load completed!");

        Ok(())
    }
}
