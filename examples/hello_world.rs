// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(target_arch = "wasm32"))]
mod app {
    use kite_sql::db::{DataBaseBuilder, ResultIter};
    use kite_sql::errors::DatabaseError;
    use kite_sql::implement_from_tuple;
    use kite_sql::types::value::DataValue;

    #[derive(Default, Debug, PartialEq)]
    pub struct MyStruct {
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

    pub fn run() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path("./example_data/hello_world").build()?;

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

        database
            .run("update my_struct set c3 = c3 + 10 where c1 = 1")?
            .done()?;
        database.run("delete from my_struct where c1 = 2")?.done()?;

        let iter = database.run("select * from my_struct")?;
        let schema = iter.schema().clone();

        for tuple in iter {
            println!("{:?}", MyStruct::from((&schema, tuple?)));
        }

        let mut agg = database.run("select count(*) from my_struct")?;
        if let Some(count_row) = agg.next() {
            println!("row count = {:?}", count_row?);
        }
        agg.done()?;

        database.run("drop table my_struct")?.done()?;

        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(all(not(target_arch = "wasm32"), feature = "macros"))]
fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    app::run()
}
