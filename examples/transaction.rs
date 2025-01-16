use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::errors::DatabaseError;

fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./transaction").build()?;
    let mut transaction = database.new_transaction()?;

    transaction
        .run("create table if not exists t1 (c1 int primary key, c2 int)")?
        .done()?;
    transaction
        .run("insert into t1 values(0, 0), (1, 1)")?
        .done()?;

    assert!(database.run("select * from t1").is_err());

    transaction.commit()?;

    assert!(database.run("select * from t1").is_ok());

    database.run("drop table t1")?.done()?;

    Ok(())
}
