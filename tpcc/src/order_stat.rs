use crate::load::{last_name, nu_rand, CUST_PER_DIST, DIST_PER_WARE};
use crate::{TpccArgs, TpccError, TpccTest, TpccTransaction};
use kite_sql::db::{DBTransaction, Statement};
use kite_sql::storage::Storage;
use kite_sql::types::value::DataValue;
use rand::prelude::ThreadRng;
use rand::Rng;
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct OrderStatArgs {
    w_id: usize,
    d_id: usize,
    by_name: bool,
    c_id: usize,
    c_last: String,
}

impl OrderStatArgs {
    pub(crate) fn new(
        w_id: usize,
        d_id: usize,
        by_name: bool,
        c_id: usize,
        c_last: String,
    ) -> Self {
        Self {
            w_id,
            d_id,
            by_name,
            c_id,
            c_last,
        }
    }
}

pub(crate) struct OrderStat;
pub(crate) struct OrderStatTest;

impl<S: Storage> TpccTransaction<S> for OrderStat {
    type Args = OrderStatArgs;

    fn run(
        tx: &mut DBTransaction<S>,
        args: &Self::Args,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let (c_balance, c_first, c_middle, c_last) = if args.by_name {
            // SELECT count(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?"
            let tuple = tx
                .execute(
                    &statements[0],
                    &[
                        ("?1", DataValue::Int16(args.w_id as i16)),
                        ("?2", DataValue::Int8(args.d_id as i8)),
                        ("?3", DataValue::from(args.c_last.clone())),
                    ],
                )?
                .next()
                .unwrap()?;
            let mut name_cnt = tuple.values[0].i32().unwrap() as usize;
            // SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first"
            let mut tuple_iter = tx.execute(
                &statements[1],
                &[
                    ("?1", DataValue::Int16(args.w_id as i16)),
                    ("?2", DataValue::Int8(args.d_id as i8)),
                    ("?3", DataValue::from(args.c_last.clone())),
                ],
            )?;

            if name_cnt % 2 == 1 {
                name_cnt += 1;
            }
            let mut c_balance = Decimal::default();
            let mut c_first = String::new();
            let mut c_middle = String::new();
            let mut c_last = String::new();

            for _ in 0..name_cnt / 2 {
                let tuple = tuple_iter.next().unwrap()?;

                c_balance = tuple.values[0].decimal().unwrap();
                c_first = tuple.values[1].utf8().unwrap().to_string();
                c_middle = tuple.values[2].utf8().unwrap().to_string();
                c_last = tuple.values[3].utf8().unwrap().to_string();
            }
            (c_balance, c_first, c_middle, c_last)
        } else {
            // "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
            let tuple = tx
                .execute(
                    &statements[2],
                    &[
                        ("?1", DataValue::Int16(args.w_id as i16)),
                        ("?2", DataValue::Int8(args.d_id as i8)),
                        ("?3", DataValue::Int32(args.c_id as i32)),
                    ],
                )?
                .next()
                .unwrap()?;
            let c_balance = tuple.values[0].decimal().unwrap();
            let c_first = tuple.values[1].utf8().unwrap().to_string();
            let c_middle = tuple.values[2].utf8().unwrap().to_string();
            let c_last = tuple.values[3].utf8().unwrap().to_string();
            (c_balance, c_first, c_middle, c_last)
        };
        // TODO: Join Eq
        // "SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)"
        let tuple = tx
            .execute(
                &statements[3],
                &[
                    ("?1", DataValue::Int16(args.w_id as i16)),
                    ("?2", DataValue::Int8(args.d_id as i8)),
                    ("?3", DataValue::Int32(args.c_id as i32)),
                    ("?4", DataValue::Int16(args.w_id as i16)),
                    ("?5", DataValue::Int8(args.d_id as i8)),
                    ("?6", DataValue::Int32(args.c_id as i32)),
                ],
            )?
            .next();
        let Some(tuple) = tuple else {
            return Err(TpccError::EmptyTuples);
        };
        let tuple = tuple?;
        let o_id = tuple.values[0].i32().unwrap();
        // let o_entry_d = tuple.values[1].datetime().unwrap();
        // let o_carrier_id = tuple.values[2].i32().unwrap();
        // "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
        let tuple = tx
            .execute(
                &statements[4],
                &[
                    ("?1", DataValue::Int16(args.w_id as i16)),
                    ("?2", DataValue::Int8(args.d_id as i8)),
                    ("?3", DataValue::Int32(o_id)),
                ],
            )?
            .next()
            .unwrap()?;
        // let ol_i_id = tuple.values[0].i32();
        // let ol_supply_w_id = tuple.values[1].i16();
        // let ol_quantity = tuple.values[2].i8();
        // let ol_amount = tuple.values[3].decimal();
        // let ol_delivery_d = tuple.values[4].datetime();

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for OrderStatTest {
    fn name(&self) -> &'static str {
        "Order-Status"
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
        let by_name = rng.gen_range(1..100) <= 60;

        let args = OrderStatArgs::new(w_id, d_id, by_name, c_id, c_last);
        OrderStat::run(tx, &args, statements)?;

        Ok(())
    }
}
