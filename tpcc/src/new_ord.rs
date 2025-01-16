use crate::load::{nu_rand, CUST_PER_DIST, DIST_PER_WARE, MAX_ITEMS, MAX_NUM_ITEMS};
use crate::{other_ware, TpccArgs, TpccError, TpccTest, TpccTransaction, ALLOW_MULTI_WAREHOUSE_TX};
use chrono::Utc;
use kite_sql::db::{DBTransaction, ResultIter, Statement};
use kite_sql::storage::Storage;
use kite_sql::types::value::DataValue;
use rand::prelude::ThreadRng;
use rand::Rng;
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct NewOrdArgs {
    joins: bool,
    w_id: usize,
    d_id: usize,
    c_id: usize,
    o_ol_cnt: usize,
    o_all_local: u8,
    item_id: Vec<usize>,
    supware: Vec<usize>,
    qty: Vec<u8>,
}

impl NewOrdArgs {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        joins: bool,
        w_id: usize,
        d_id: usize,
        c_id: usize,
        o_ol_cnt: usize,
        o_all_local: u8,
        item_id: Vec<usize>,
        supware: Vec<usize>,
        qty: Vec<u8>,
    ) -> Self {
        Self {
            joins,
            w_id,
            d_id,
            c_id,
            o_ol_cnt,
            o_all_local,
            item_id,
            supware,
            qty,
        }
    }
}

pub(crate) struct NewOrd;
pub(crate) struct NewOrdTest;

impl<S: Storage> TpccTransaction<S> for NewOrd {
    type Args = NewOrdArgs;

    fn run(
        tx: &mut DBTransaction<S>,
        args: &Self::Args,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let mut price = vec![Decimal::default(); MAX_NUM_ITEMS];
        let mut iname = vec![String::new(); MAX_NUM_ITEMS];
        let mut stock = vec![0; MAX_NUM_ITEMS];
        let mut bg = vec![String::new(); MAX_NUM_ITEMS];
        let mut amt = vec![Decimal::default(); MAX_NUM_ITEMS];
        let now = Utc::now().naive_utc();

        let (c_discount, c_last, c_credit, w_tax) = if args.joins {
            // "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?"
            let tuple = tx
                .execute(
                    &statements[0],
                    &[
                        ("?1", DataValue::Int16(args.w_id as i16)),
                        ("?2", DataValue::Int16(args.w_id as i16)),
                        ("?3", DataValue::Int8(args.d_id as i8)),
                        ("?4", DataValue::Int64(args.c_id as i64)),
                    ],
                )?
                .next()
                .unwrap()?;
            let c_discount = tuple.values[0].decimal().unwrap();
            let c_last = tuple.values[1].utf8().unwrap().to_string();
            let c_credit = tuple.values[2].utf8().unwrap().to_string();
            let w_tax = tuple.values[3].decimal().unwrap();

            (c_discount, c_last, c_credit, w_tax)
        } else {
            // "SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
            let tuple = tx
                .execute(
                    &statements[1],
                    &[
                        ("?1", DataValue::Int16(args.w_id as i16)),
                        ("?2", DataValue::Int8(args.d_id as i8)),
                        ("?3", DataValue::Int32(args.c_id as i32)),
                    ],
                )?
                .next()
                .unwrap()?;
            let c_discount = tuple.values[0].decimal().unwrap();
            let c_last = tuple.values[1].utf8().unwrap().to_string();
            let c_credit = tuple.values[2].utf8().unwrap().to_string();
            // "SELECT w_tax FROM warehouse WHERE w_id = ?"
            let tuple = tx
                .execute(
                    &statements[2],
                    &[("?1", DataValue::Int16(args.w_id as i16))],
                )?
                .next()
                .unwrap()?;
            let w_tax = tuple.values[0].decimal().unwrap();

            (c_discount, c_last, c_credit, w_tax)
        };
        // "SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE"
        let tuple = tx
            .execute(
                &statements[3],
                &[
                    ("?1", DataValue::Int8(args.d_id as i8)),
                    ("?2", DataValue::Int16(args.w_id as i16)),
                ],
            )?
            .next()
            .unwrap()?;
        let d_next_o_id = tuple.values[0].i32().unwrap();
        let d_tax = tuple.values[1].decimal().unwrap();
        // "UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?"
        tx.execute(
            &statements[4],
            &[
                ("?1", DataValue::Int32(d_next_o_id)),
                ("?2", DataValue::Int8(args.d_id as i8)),
                ("?3", DataValue::Int16(args.w_id as i16)),
            ],
        )?
        .done()?;
        let o_id = d_next_o_id;
        // "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES(?, ?, ?, ?, ?, ?, ?)"
        tx.execute(
            &statements[5],
            &[
                ("?1", DataValue::Int32(o_id)),
                ("?2", DataValue::Int8(args.d_id as i8)),
                ("?3", DataValue::Int16(args.w_id as i16)),
                ("?4", DataValue::Int32(args.c_id as i32)),
                ("?5", DataValue::from(&now)),
                ("?6", DataValue::Int8(args.o_ol_cnt as i8)),
                ("?7", DataValue::Int8(args.o_all_local as i8)),
            ],
        )?
        .done()?;
        // "INSERT INTO new_orders (no_o_id, no_d_id, no_w_id) VALUES (?,?,?)
        tx.execute(
            &statements[6],
            &[
                ("?1", DataValue::Int32(o_id)),
                ("?2", DataValue::Int8(args.d_id as i8)),
                ("?3", DataValue::Int16(args.w_id as i16)),
            ],
        )?
        .done()?;
        let mut ol_num_seq = vec![0; MAX_NUM_ITEMS];

        for i in 0..args.o_ol_cnt {
            ol_num_seq[i] = i;
        }
        for i in 0..args.o_ol_cnt - 1 {
            let mut tmp_0 =
                (MAX_ITEMS + 1) * args.supware[ol_num_seq[i]] + args.item_id[ol_num_seq[i]];
            let mut min_num = i;
            for j in i + 1..args.o_ol_cnt {
                let tmp_1 =
                    (MAX_ITEMS + 1) * args.supware[ol_num_seq[j]] + args.item_id[ol_num_seq[j]];
                if tmp_1 < tmp_0 {
                    tmp_0 = tmp_1;
                    min_num = j;
                }
            }
            if min_num != i {
                let swp = ol_num_seq[min_num];
                ol_num_seq[min_num] = ol_num_seq[i];
                ol_num_seq[i] = swp;
            }
        }
        for ol_number in 1..args.o_ol_cnt + 1 {
            let ol_supply_w_id = args.supware[ol_num_seq[ol_number - 1]];
            let ol_i_id = args.item_id[ol_num_seq[ol_number - 1]];
            let ol_quantity = args.qty[ol_num_seq[ol_number - 1]];
            // "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?"
            let tuple = tx
                .execute(
                    &statements[7],
                    vec![("?1", DataValue::Int32(ol_i_id as i32))],
                )?
                .next();
            let Some(tuple) = tuple else {
                return Err(TpccError::EmptyTuples);
            };
            let tuple = tuple?;
            let i_price = tuple.values[0].decimal().unwrap();
            let i_name = tuple.values[1].utf8().unwrap();
            let i_data = tuple.values[2].utf8().unwrap();

            price[ol_num_seq[ol_number - 1]] = i_price;
            iname[ol_num_seq[ol_number - 1]] = i_name.to_string();

            // "SELECT s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE s_i_id = ? AND s_w_id = ? FOR UPDATE"
            let tuple = tx
                .execute(
                    &statements[8],
                    vec![
                        ("?1", DataValue::Int32(ol_i_id as i32)),
                        ("?2", DataValue::Int16(ol_supply_w_id as i16)),
                    ],
                )?
                .next()
                .unwrap()?;
            let mut s_quantity = tuple.values[0].i16().unwrap();
            let s_data = tuple.values[1].utf8().unwrap();
            let s_dist_01 = tuple.values[2].utf8().unwrap();
            let s_dist_02 = tuple.values[3].utf8().unwrap();
            let s_dist_03 = tuple.values[4].utf8().unwrap();
            let s_dist_04 = tuple.values[5].utf8().unwrap();
            let s_dist_05 = tuple.values[6].utf8().unwrap();
            let s_dist_06 = tuple.values[7].utf8().unwrap();
            let s_dist_07 = tuple.values[8].utf8().unwrap();
            let s_dist_08 = tuple.values[9].utf8().unwrap();
            let s_dist_09 = tuple.values[10].utf8().unwrap();
            let s_dist_10 = tuple.values[11].utf8().unwrap();

            let ol_dist_info = pick_dist_info(
                args.d_id, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
                s_dist_07, s_dist_08, s_dist_09, s_dist_10,
            );
            stock[ol_num_seq[ol_number - 1]] = s_quantity;
            bg[ol_num_seq[ol_number - 1]] =
                if i_data.contains("original") && s_data.contains("original") {
                    "B"
                } else {
                    "C"
                }
                .to_string();
            s_quantity = if s_quantity > ol_quantity as i16 {
                s_quantity - ol_quantity as i16
            } else {
                s_quantity - ol_quantity as i16 + 91
            };
            // "UPDATE stock SET s_quantity = ? WHERE s_i_id = ? AND s_w_id = ?"
            tx.execute(
                &statements[9],
                vec![
                    ("?1", DataValue::Int16(s_quantity)),
                    ("?2", DataValue::Int32(ol_i_id as i32)),
                    ("?3", DataValue::Int16(ol_supply_w_id as i16)),
                ],
            )?
            .done()?;

            // Tips: Integers always have 7 digits, so divide by 10 here
            let mut ol_amount = Decimal::from(ol_quantity)
                * i_price
                * (Decimal::from(1) + w_tax + d_tax)
                * (Decimal::from(1) - c_discount).round_dp(2);
            while ol_amount.mantissa() > 4 {
                ol_amount = ol_amount / Decimal::from(10);
            }

            amt[ol_num_seq[ol_number - 1]] = ol_amount;
            // "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            tx.execute(
                &statements[10],
                vec![
                    ("?1", DataValue::Int32(o_id)),
                    ("?2", DataValue::Int8(args.d_id as i8)),
                    ("?3", DataValue::Int16(args.w_id as i16)),
                    ("?4", DataValue::Int8(ol_number as i8)),
                    ("?5", DataValue::Int32(ol_i_id as i32)),
                    ("?6", DataValue::Int16(ol_supply_w_id as i16)),
                    ("?7", DataValue::Int8(ol_quantity as i8)),
                    ("?8", DataValue::Decimal(ol_amount.round_dp(2))),
                    ("?9", DataValue::from(ol_dist_info)),
                ],
            )?
            .done()?;
        }

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for NewOrdTest {
    fn name(&self) -> &'static str {
        "New-Order"
    }

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut DBTransaction<S>,
        num_ware: usize,
        args: &TpccArgs,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let mut all_local = 1;
        let notfound = MAX_ITEMS + 1;

        let mut itemid = vec![0; MAX_NUM_ITEMS];
        let mut supware = vec![0; MAX_NUM_ITEMS];
        let mut qty = vec![0; MAX_NUM_ITEMS];

        let w_id = rng.gen_range(0..num_ware) + 1;
        let d_id = rng.gen_range(1..DIST_PER_WARE);
        let c_id = nu_rand(rng, 1023, 1, CUST_PER_DIST);
        let ol_cnt = rng.gen_range(5..15);
        let rbk = rng.gen_range(1..100);

        for i in 0..ol_cnt {
            itemid[i] = nu_rand(rng, 8191, 1, MAX_ITEMS);
            if (i == ol_cnt - 1) && (rbk == 1) {
                itemid[i] = notfound;
            }
            if ALLOW_MULTI_WAREHOUSE_TX {
                if rng.gen_range(1..100) != 1 {
                    supware[i] = w_id;
                } else {
                    supware[i] = other_ware(rng, w_id, num_ware);
                    all_local = 0;
                }
            } else {
                supware[i] = w_id;
            }
            qty[i] = rng.gen_range(1..10);
        }
        let args = NewOrdArgs::new(
            args.joins, w_id, d_id, c_id, ol_cnt, all_local, itemid, supware, qty,
        );
        NewOrd::run(tx, &args, statements)?;

        Ok(())
    }
}

fn pick_dist_info(
    ol_supply_w_id: usize,
    s_dist_01: &str,
    s_dist_02: &str,
    s_dist_03: &str,
    s_dist_04: &str,
    s_dist_05: &str,
    s_dist_06: &str,
    s_dist_07: &str,
    s_dist_08: &str,
    s_dist_09: &str,
    s_dist_10: &str,
) -> String {
    match ol_supply_w_id {
        1 => s_dist_01,
        2 => s_dist_02,
        3 => s_dist_03,
        4 => s_dist_04,
        5 => s_dist_05,
        6 => s_dist_06,
        7 => s_dist_07,
        8 => s_dist_08,
        9 => s_dist_09,
        10 => s_dist_10,
        _ => unreachable!(),
    }
    .to_string()
}
