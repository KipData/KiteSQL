#![cfg(not(target_arch = "wasm32"))]

use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::errors::DatabaseError;

fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./transaction").build_optimistic()?;
    let mut transaction = database.new_transaction()?;

    // Scenario: create table and insert rows inside a transaction; visible after commit.
    transaction
        .run("create table if not exists t1 (c1 int primary key, c2 int)")?
        .done()?;
    transaction
        .run("insert into t1 values(0, 0), (1, 1)")?
        .done()?;

    assert!(database.run("select * from t1").is_err());

    transaction.commit()?;

    assert!(database.run("select * from t1").is_ok());

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
