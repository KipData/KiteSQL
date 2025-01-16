use crate::load::DIST_PER_WARE;
use crate::{TpccArgs, TpccError, TpccTest, TpccTransaction};
use chrono::Utc;
use kite_sql::db::{DBTransaction, ResultIter, Statement};
use kite_sql::storage::Storage;
use kite_sql::types::value::DataValue;
use rand::prelude::ThreadRng;
use rand::Rng;

#[derive(Debug)]
pub(crate) struct DeliveryArgs {
    w_id: usize,
    o_carrier_id: usize,
}

impl DeliveryArgs {
    pub(crate) fn new(w_id: usize, o_carrier_id: usize) -> Self {
        Self { w_id, o_carrier_id }
    }
}

pub(crate) struct Delivery;
pub(crate) struct DeliveryTest;

impl<S: Storage> TpccTransaction<S> for Delivery {
    type Args = DeliveryArgs;

    fn run(
        tx: &mut DBTransaction<S>,
        args: &Self::Args,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        let now = Utc::now().naive_utc();

        for d_id in 1..DIST_PER_WARE + 1 {
            // "SELECT COALESCE(MIN(no_o_id),0) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?"
            let tuple = tx
                .execute(
                    &statements[0],
                    &[
                        ("?1", DataValue::Int8(d_id as i8)),
                        ("?2", DataValue::Int16(args.w_id as i16)),
                    ],
                )?
                .next()
                .unwrap()?;
            let no_o_id = tuple.values[0].i32().unwrap();

            if no_o_id == 0 {
                continue;
            }
            // "DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?"
            tx.execute(
                &statements[1],
                &[
                    ("?1", DataValue::Int32(no_o_id)),
                    ("?2", DataValue::Int8(d_id as i8)),
                    ("?3", DataValue::Int16(args.w_id as i16)),
                ],
            )?
            .done()?;
            // "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?"
            let tuple = tx
                .execute(
                    &statements[2],
                    &[
                        ("?1", DataValue::Int32(no_o_id)),
                        ("?2", DataValue::Int8(d_id as i8)),
                        ("?3", DataValue::Int16(args.w_id as i16)),
                    ],
                )?
                .next()
                .unwrap()?;
            let c_id = tuple.values[0].i32().unwrap();
            // "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?"
            tx.execute(
                &statements[3],
                &[
                    ("?1", DataValue::Int8(args.o_carrier_id as i8)),
                    ("?2", DataValue::Int32(no_o_id)),
                    ("?3", DataValue::Int8(d_id as i8)),
                    ("?4", DataValue::Int16(args.w_id as i16)),
                ],
            )?
            .done()?;
            // "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
            tx.execute(
                &statements[4],
                &[
                    ("?1", DataValue::from(&now)),
                    ("?2", DataValue::Int32(no_o_id)),
                    ("?3", DataValue::Int8(d_id as i8)),
                    ("?4", DataValue::Int16(args.w_id as i16)),
                ],
            )?
            .done()?;
            // "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?"
            let tuple = tx
                .execute(
                    &statements[5],
                    &[
                        ("?1", DataValue::Int32(no_o_id)),
                        ("?2", DataValue::Int8(d_id as i8)),
                        ("?3", DataValue::Int16(args.w_id as i16)),
                    ],
                )?
                .next()
                .unwrap()?;
            let ol_total = tuple.values[0].decimal().unwrap();
            // "UPDATE customer SET c_balance = c_balance + ? , c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?"
            tx.execute(
                &statements[6],
                &[
                    ("?1", DataValue::Decimal(ol_total)),
                    ("?2", DataValue::Int32(c_id)),
                    ("?3", DataValue::Int8(d_id as i8)),
                    ("?4", DataValue::Int16(args.w_id as i16)),
                ],
            )?
            .done()?;
        }

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for DeliveryTest {
    fn name(&self) -> &'static str {
        "Delivery"
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
        let o_carrier_id = rng.gen_range(1..10);

        let args = DeliveryArgs::new(w_id, o_carrier_id);
        Delivery::run(tx, &args, statements)?;

        Ok(())
    }
}
