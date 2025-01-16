use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::errors::DatabaseError;
use kite_sql::implement_from_tuple;
use kite_sql::types::value::DataValue;

#[derive(Default, Debug, PartialEq)]
struct MyStruct {
    pub c1: i32,
    pub c2: String,
}

implement_from_tuple!(
    MyStruct, (
        c1: i32 => |inner: &mut MyStruct, value| {
            if let DataValue::Int32(val) = value {
                inner.c1 = val;
            }
        },
        c2: String => |inner: &mut MyStruct, value| {
            if let DataValue::Utf8 { value, .. } = value {
                inner.c2 = value;
            }
        }
    )
);

#[cfg(feature = "macros")]
fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./hello_world").build()?;

    database
        .run("create table if not exists my_struct (c1 int primary key, c2 int)")?
        .done()?;
    database
        .run("insert into my_struct values(0, 0), (1, 1)")?
        .done()?;

    let iter = database.run("select * from my_struct")?;
    let schema = iter.schema().clone();

    for tuple in iter {
        println!("{:?}", MyStruct::from((&schema, tuple?)));
    }
    database.run("drop table my_struct")?.done()?;

    Ok(())
}
