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

//! `hello_world` on the async API. Run with
//! `cargo run --example async_hello_world --no-default-features --features async,orm`.

#[cfg(all(not(target_arch = "wasm32"), feature = "async", feature = "orm"))]
mod app {
    use kite_sql::db::{DataBaseBuilder, Database};
    use kite_sql::errors::DatabaseError;
    use kite_sql::storage::memory::MemoryStorage;
    use kite_sql::Model;
    use std::borrow::Cow;

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "my_struct")]
    pub struct MyStruct<'a> {
        #[model(primary_key)]
        pub c1: i32,
        pub c2: Cow<'a, str>,
    }

    async fn run_with_database(mut database: Database<MemoryStorage>) -> Result<(), DatabaseError> {
        database.create_table_if_not_exists::<MyStruct>().await?;
        database
            .insert(&MyStruct {
                c1: 0,
                c2: "zero".into(),
            })
            .await?;
        database
            .insert(&MyStruct {
                c1: 1,
                c2: "one".into(),
            })
            .await?;
        database
            .insert(&MyStruct {
                c1: 2,
                c2: "two".into(),
            })
            .await?;

        database
            .bind(|ctx| {
                ctx.mutate::<MyStruct>()?
                    .filter(|e| e.column(MyStruct::c1())?.eq(1))?
                    .update(|u| u.set_value(MyStruct::c2(), "ONE"))
            })
            .await?
            .done()
            .await?;
        database
            .bind(|ctx| {
                ctx.mutate::<MyStruct>()?
                    .filter(|e| e.column(MyStruct::c1())?.eq(2))?
                    .delete()
            })
            .await?
            .done()
            .await?;

        let mut count = 0;
        let mut rows = database.fetch::<MyStruct>().await?;
        while let Some(row) = rows.next().await {
            println!("{:?}", row?);
            count += 1;
        }
        rows.done().await?;
        println!("row count = {count}");

        database.drop_table::<MyStruct>().await?;

        Ok(())
    }

    pub async fn run() -> Result<(), DatabaseError> {
        let database = DataBaseBuilder::path(".").build_in_memory()?;
        // The database and its futures are `Send`, so the work can run on any worker thread.
        tokio::spawn(run_with_database(database))
            .await
            .expect("example task panicked")
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "async", feature = "orm"))]
#[tokio::main]
async fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    app::run().await
}

#[cfg(not(all(not(target_arch = "wasm32"), feature = "async", feature = "orm")))]
fn main() {}
