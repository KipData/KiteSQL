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

    // 1) Create table and insert multiple rows with mixed types.
    database
        .run(
            "create table if not exists my_struct (
                c1 int primary key,
                c2 varchar,
                c3 int
            )",
        )?
        .done()?;
    database
        .run(
            r#"
        insert into my_struct values
            (0, 'zero', 0),
            (1, 'one', 1),
            (2, 'two', 2)
        "#,
        )?
        .done()?;

    // 2) Update and delete demo.
    database
        .run("update my_struct set c3 = c3 + 10 where c1 = 1")?
        .done()?;
    database.run("delete from my_struct where c1 = 2")?.done()?;

    // 3) Query and deserialize into Rust struct.
    let iter = database.run("select * from my_struct")?;
    let schema = iter.schema().clone();

    for tuple in iter {
        println!("{:?}", MyStruct::from((&schema, tuple?)));
    }

    // 4) Aggregate example.
    let mut agg = database.run("select count(*) from my_struct")?;
    if let Some(count_row) = agg.next() {
        println!("row count = {:?}", count_row?);
    }
    agg.done()?;

    database.run("drop table my_struct")?.done()?;

    Ok(())
}
