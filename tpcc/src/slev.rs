use crate::load::DIST_PER_WARE;
use crate::{TpccArgs, TpccError, TpccTest, TpccTransaction};
use kite_sql::db::{DBTransaction, Statement};
use kite_sql::storage::Storage;
use kite_sql::types::value::DataValue;
use rand::prelude::ThreadRng;
use rand::Rng;

#[derive(Debug)]
pub(crate) struct SlevArgs {
    w_id: usize,
    d_id: usize,
    level: usize,
}

impl SlevArgs {
    pub(crate) fn new(w_id: usize, d_id: usize, level: usize) -> Self {
        Self { w_id, d_id, level }
    }
}

pub(crate) struct Slev;
pub(crate) struct SlevTest;

impl<S: Storage> TpccTransaction<S> for Slev {
    type Args = SlevArgs;

    fn run(
        tx: &mut DBTransaction<S>,
        args: &Self::Args,
        statements: &[Statement],
    ) -> Result<(), TpccError> {
        // "SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?"
        let tuple = tx
            .execute(
                &statements[0],
                &[
                    ("?1", DataValue::Int8(args.d_id as i8)),
                    ("?2", DataValue::Int16(args.w_id as i16)),
                ],
            )?
            .next()
            .unwrap()?;
        let d_next_o_id = tuple.values[0].i32().unwrap();
        // "SELECT DISTINCT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= (? - 20)"
        let tuple = tx
            .execute(
                &statements[1],
                &[
                    ("?1", DataValue::Int16(args.w_id as i16)),
                    ("?2", DataValue::Int8(args.d_id as i8)),
                    ("?3", DataValue::Int32(d_next_o_id)),
                    ("?4", DataValue::Int32(d_next_o_id)),
                ],
            )?
            .next()
            .unwrap()?;
        let ol_i_id = tuple.values[0].i32().unwrap();
        // "SELECT count(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?"
        let tuple = tx
            .execute(
                &statements[2],
                &[
                    ("?1", DataValue::Int16(args.w_id as i16)),
                    ("?2", DataValue::Int8(ol_i_id as i8)),
                    ("?3", DataValue::Int16(args.level as i16)),
                ],
            )?
            .next()
            .unwrap()?;
        // let i_count = tuple.values[0].i32().unwrap();

        Ok(())
    }
}

impl<S: Storage> TpccTest<S> for SlevTest {
    fn name(&self) -> &'static str {
        "Stock-Level"
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
        let level = rng.gen_range(10..20);

        let args = SlevArgs::new(w_id, d_id, level);
        Slev::run(tx, &args, statements)?;

        Ok(())
    }
}
