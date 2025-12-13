#![cfg(not(target_arch = "wasm32"))]

use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::errors::DatabaseError;
use kite_sql::types::tuple::Tuple;
use kite_sql::types::value::DataValue;

fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./example_data/transaction").build_optimistic()?;
    database
        .run("create table if not exists t1 (c1 int primary key, c2 int)")?
        .done()?;
    let mut transaction = database.new_transaction()?;

    transaction
        .run("insert into t1 values(0, 0), (1, 1)")?
        .done()?;

    assert!(database.run("select * from t1")?.next().is_none());

    transaction.commit()?;

    let mut iter = database.run("select * from t1")?;
    assert_eq!(
        iter.next().unwrap()?,
        Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)])
    );
    assert_eq!(
        iter.next().unwrap()?,
        Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(1)])
    );
    assert!(iter.next().is_none());

    // Scenario: another transaction updates but does not commit; changes stay invisible.
    let mut tx2 = database.new_transaction()?;
    tx2.run("update t1 set c2 = 99 where c1 = 0")?.done()?;
    assert_eq!(
        database
            .run("select c2 from t1 where c1 = 0")?
            .next()
            .unwrap()?
            .values[0]
            .i32(),
        Some(0)
    );
    // rollback
    drop(tx2);

    database.run("drop table t1")?.done()?;

    Ok(())
}
