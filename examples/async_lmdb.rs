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

//! The async API with LMDB storage, run on tokio.
//!
//! ```text
//! cargo run --example async_lmdb --no-default-features --features lmdb,orm,async
//! ```
//!
//! LMDB operations in async mode return immediately-ready futures. The storage
//! itself is synchronous, but the async trait implementation allows LMDB to be
//! used in async codebases alongside truly async storage engines like SlateDB.

#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "lmdb",
    feature = "async",
    feature = "orm"
))]
mod app {
    use kite_sql::db::{AsyncIter, DataBaseBuilder};
    use kite_sql::storage::lmdb::LmdbStorage;
    use kite_sql::Model;
    use std::borrow::Cow;
    use std::error::Error;
    use std::fs;
    use std::io::ErrorKind;
    use std::path::Path;

    const EXAMPLE_DB_PATH: &str = "./example_data/async_lmdb";

    fn reset_example_dir() -> Result<(), Box<dyn Error>> {
        if let Err(err) = fs::remove_dir_all(EXAMPLE_DB_PATH) {
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        if let Some(parent) = Path::new(EXAMPLE_DB_PATH).parent() {
            fs::create_dir_all(parent)?;
        }

        Ok(())
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "my_struct")]
    pub struct MyStruct<'a> {
        #[model(primary_key)]
        pub c1: i32,
        pub c2: Cow<'a, str>,
    }

    pub async fn run() -> Result<(), Box<dyn Error>> {
        reset_example_dir()?;
        let lmdb_path = Path::new(EXAMPLE_DB_PATH).join("kite_sql.lmdb");
        println!("opening LMDB at {:?}", lmdb_path);

        let storage = LmdbStorage::new(lmdb_path)?;
        let mut database = DataBaseBuilder::path(".").build_with_storage(storage)?;

        // LMDB futures are immediately ready, but the async API allows it to work
        // alongside truly async storage like SlateDB in the same codebase.
        database.drop_table_if_exists::<MyStruct>().await?;
        database.create_table_if_not_exists::<MyStruct>().await?;

        for (c1, c2) in [(0, "zero"), (1, "one"), (2, "two")] {
            database.insert(&MyStruct { c1, c2: c2.into() }).await?;
        }

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

        let mut rows = database.fetch::<MyStruct>().await?;
        let mut count = 0;
        while let Some(row) = rows.next().await {
            println!("{:?}", row?);
            count += 1;
        }
        rows.done().await?;
        println!("row count = {count}");

        database.drop_table::<MyStruct>().await?;

        Ok(())
    }
}

#[cfg(all(
    not(target_arch = "wasm32"),
    feature = "lmdb",
    feature = "async",
    feature = "orm"
))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::run().await
}

#[cfg(not(all(
    not(target_arch = "wasm32"),
    feature = "lmdb",
    feature = "async",
    feature = "orm"
)))]
fn main() {}
