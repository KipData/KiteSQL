use crate::load::{last_name, nu_rand, CUST_PER_DIST, DIST_PER_WARE};
use crate::{other_ware, TpccArgs, TpccError, TpccTest, TpccTransaction, ALLOW_MULTI_WAREHOUSE_TX};
use chrono::Utc;
use kite_sql::db::{DBTransaction, ResultIter, Statement};
use kite_sql::storage::Storage;
use kite_sql::types::value::DataValue;
use rand::prelude::ThreadRng;
use rand::Rng;
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct PaymentArgs {
    w_id: usize,
    d_id: usize,
    by_name: bool,
    c_w_id: usize,
    c_d_id: usize,
    c_id: usize,
    c_last: String,
    h_amount: Decimal,
}

impl PaymentArgs {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        w_id: usize,
        d_id: usize,
        by_name: bool,
        c_w_id: usize,
        c_d_id: usize,
        c_id: usize,
        c_last: String,
        h_amount: Decimal,
    ) -> Self {
        Self {
            w_id,
            d_id,
            by_name,
            c_w_id,
            c_d_id,
            c_id,
            c_last,
            h_amount,
        }
    }
}

pub(crate) struct Payment;
pub(crate) struct PaymentTest;

impl<S: Storage> TpccTransaction<S> for Payment {
    type Args = PaymentArgs;

    #[allow(unused_variables)]
    fn run(
        tx: &mut DBTransaction<S>,
        args: &Self::Args,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let now = Utc::now();
        // "UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?"
        tx.execute(
            &statements[0],
            &[
                ("?1", DataValue::Decimal(args.h_amount)),
                ("?2", DataValue::Int16(args.w_id as i16)),
            ],
        )?
        .done()?;
        // "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?"
        let tuple = tx
            .execute(
                &statements[1],
                &[("?1", DataValue::Int16(args.w_id as i16))],
            )?
            .next()
            .unwrap()?;
        let w_street_1 = tuple.values[0].utf8().unwrap();
        let w_street_2 = tuple.values[1].utf8().unwrap();
        let w_city = tuple.values[2].utf8().unwrap();
        let w_state = tuple.values[3].utf8().unwrap();
        let w_zip = tuple.values[4].utf8().unwrap();
        let w_name = tuple.values[5].utf8().unwrap();

        // "UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?"
        tx.execute(
            &statements[2],
            &[
                ("?1", DataValue::Decimal(args.h_amount)),
                ("?2", DataValue::Int16(args.w_id as i16)),
                ("?3", DataValue::Int8(args.d_id as i8)),
            ],
        )?
        .done()?;

        // "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?"
        let tuple = tx
            .execute(
                &statements[3],
                &[
                    ("?1", DataValue::Int16(args.w_id as i16)),
                    ("?2", DataValue::Int8(args.d_id as i8)),
                ],
            )?
            .next()
            .unwrap()?;
        let d_street_1 = tuple.values[0].utf8().unwrap();
        let d_street_2 = tuple.values[1].utf8().unwrap();
        let d_city = tuple.values[2].utf8().unwrap();
        let d_state = tuple.values[3].utf8().unwrap();
        let d_zip = tuple.values[4].utf8().unwrap();
        let d_name = tuple.values[5].utf8().unwrap();

        let mut c_id = args.c_id as i32;
        if args.by_name {
            // "SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"
            let tuple = tx
                .execute(
                    &statements[4],
                    &[
                        ("?1", DataValue::Int16(args.c_w_id as i16)),
                        ("?2", DataValue::Int8(args.c_d_id as i8)),
                        ("?3", DataValue::from(args.c_last.clone())),
                    ],
                )?
                .next()
                .unwrap()?;
            let mut name_cnt = tuple.values[0].i32().unwrap();

            // "SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first"
            let mut tuple_iter = tx.execute(
                &statements[5],
                &[
                    ("?1", DataValue::Int16(args.c_w_id as i16)),
                    ("?2", DataValue::Int8(args.c_d_id as i8)),
                    ("?3", DataValue::from(args.c_last.clone())),
                ],
            )?;
            if name_cnt % 2 == 1 {
                name_cnt += 1;
            }
            for _ in 0..name_cnt / 2 {
                let result = tuple_iter.next().unwrap()?;
                c_id = result.values[0].i32().unwrap();
            }
        }
        // "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE"
        let tuple = tx
            .execute(
                &statements[6],
                &[
                    ("?1", DataValue::Int16(args.c_w_id as i16)),
                    ("?2", DataValue::Int8(args.c_d_id as i8)),
                    ("?3", DataValue::Int32(c_id)),
                ],
            )?
            .next()
            .unwrap()?;
        let c_first = tuple.values[0].utf8().unwrap();
        let c_middle = tuple.values[1].utf8().unwrap();
        let c_last = tuple.values[2].utf8().unwrap();
        let c_street_1 = tuple.values[3].utf8().unwrap();
        let c_street_2 = tuple.values[4].utf8().unwrap();
        let c_city = tuple.values[5].utf8().unwrap();
        let c_state = tuple.values[6].utf8().unwrap();
        let c_zip = tuple.values[7].utf8().unwrap();
        let c_phone = tuple.values[8].utf8().unwrap();
        let c_credit = tuple.values[9].utf8();
        let c_credit_lim = tuple.values[10].i64().unwrap();
        let c_discount = tuple.values[11].decimal().unwrap();
        let mut c_balance = tuple.values[12].decimal().unwrap();
        let c_since = tuple.values[13].datetime().unwrap();

        c_balance += args.h_amount;
        if let Some(c_credit) = c_credit {
            if c_credit.contains("BC") {
                // "SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                let tuple = tx
                    .execute(
                        &statements[7],
                        &[
                            ("?1", DataValue::Int16(args.c_w_id as i16)),
                            ("?2", DataValue::Int8(args.c_d_id as i8)),
                            ("?3", DataValue::Int32(c_id)),
                        ],
                    )?
                    .next()
                    .unwrap()?;
                let c_data = tuple.values[0].utf8().unwrap();

                // https://github.com/AgilData/tpcc/blob/dfbabe1e35cc93b2bf2e107fc699eb29c2097e24/src/main/java/com/codefutures/tpcc/Payment.java#L284
                // let c_new_data = format!("| {} {} {} {} {} {} {}", c_id, args.c_d_id, args.c_w_id, args.d_id, args.w_id, args.h_amount, )

                // "UPDATE customer SET c_balance = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                tx.execute(
                    &statements[8],
                    &[
                        ("?1", DataValue::Decimal(c_balance)),
                        ("?2", DataValue::from(c_data.to_string())),
                        ("?3", DataValue::Int16(args.c_w_id as i16)),
                        ("?4", DataValue::Int8(args.c_d_id as i8)),
                        ("?5", DataValue::Int32(c_id)),
                    ],
                )?
                .done()?;
            } else {
                // "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
                tx.execute(
                    &statements[9],
                    &[
                        ("?1", DataValue::Decimal(c_balance)),
                        ("?2", DataValue::Int16(args.c_w_id as i16)),
                        ("?3", DataValue::Int8(args.c_d_id as i8)),
                        ("?4", DataValue::Int32(c_id)),
                    ],
                )?
                .done()?;
            }
        } else {
            // "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
            tx.execute(
                &statements[9],
                &[
                    ("?1", DataValue::Decimal(c_balance)),
                    ("?2", DataValue::Int16(args.c_w_id as i16)),
                    ("?3", DataValue::Int8(args.c_d_id as i8)),
                    ("?4", DataValue::Int32(c_id)),
                ],
            )?
            .done()?;
        }
        let h_data = format!("\\0{d_name}    \\0");
        // "INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
        tx.execute(
            &statements[10],
            &[
                ("?1", DataValue::Int8(args.c_d_id as i8)),
                ("?2", DataValue::Int16(args.c_w_id as i16)),
                ("?3", DataValue::Int32(c_id)),
                ("?4", DataValue::Int8(args.d_id as i8)),
                ("?5", DataValue::Int16(args.w_id as i16)),
                ("?6", DataValue::from(&now.naive_utc())),
                ("?7", DataValue::Decimal(args.h_amount)),
                ("?8", DataValue::from(h_data)),
            ],
        )?
        .done()?;

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for PaymentTest {
    fn name(&self) -> &'static str {
        "Payment"
    }

    fn do_transaction(
        &self,
        rng: &mut ThreadRng,
        tx: &mut DBTransaction<S>,
        num_ware: usize,
        _: &TpccArgs,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let w_id = rng.gen_range(0..num_ware) + 1;
        let d_id = rng.gen_range(1..DIST_PER_WARE);
        let c_id = nu_rand(rng, 1023, 1, CUST_PER_DIST);
        let c_last = last_name(nu_rand(rng, 255, 0, 999));
        let h_amount = rng.gen_range(1..5000);
        let by_name = rng.gen_range(1..100) < 60;
        let (c_w_id, c_d_id) = if ALLOW_MULTI_WAREHOUSE_TX {
            if rng.gen_range(1..100) < 85 {
                (w_id, d_id)
            } else {
                (
                    other_ware(rng, w_id, num_ware),
                    rng.gen_range(1..DIST_PER_WARE),
                )
            }
        } else {
            (w_id, d_id)
        };
        let args = PaymentArgs::new(
            w_id,
            d_id,
            by_name,
            c_w_id,
            c_d_id,
            c_id,
            c_last,
            Decimal::from(h_amount),
        );
        Payment::run(tx, &args, statements)?;

        Ok(())
    }
}
