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

//! The async API on Amazon S3, through SlateDB, run on tokio. It writes a table on a spawned
//! task, closes the database, opens it again from the bucket and reads the table back.
//!
//! ```text
//! KITESQL_S3_BUCKET=my-bucket AWS_REGION=us-east-1 \
//! AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//! cargo run --example async_s3 --no-default-features --features slatedb-s3,orm
//! ```
//!
//! Credentials and region come from the standard `AWS_*` environment variables or the default
//! AWS chain. For an S3-compatible store such as MinIO, also set `KITESQL_S3_ENDPOINT` (and
//! `KITESQL_S3_ALLOW_HTTP=true` for a plain `http://` endpoint). The data goes under the
//! `KITESQL_S3_PATH` prefix, `kitesql/async_s3` by default.

#[cfg(all(not(target_arch = "wasm32"), feature = "slatedb-s3", feature = "orm"))]
mod app {
    use kite_sql::db::{CatalogKind, DataBaseBuilder, Database};
    use kite_sql::errors::DatabaseError;
    use kite_sql::storage::slatedb::{S3Config, SlateDbStorage};
    use kite_sql::Model;
    use std::borrow::Cow;
    use std::env;
    use std::error::Error;

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "my_struct")]
    pub struct MyStruct<'a> {
        #[model(primary_key)]
        pub c1: i32,
        pub c2: Cow<'a, str>,
    }

    fn config() -> Result<S3Config, Box<dyn Error>> {
        let bucket = env::var("KITESQL_S3_BUCKET")
            .map_err(|_| "set KITESQL_S3_BUCKET to the bucket to store the database in")?;
        Ok(S3Config {
            endpoint: env::var("KITESQL_S3_ENDPOINT").ok(),
            allow_http: env::var("KITESQL_S3_ALLOW_HTTP").is_ok_and(|value| value == "true"),
            ..S3Config::new(bucket)
        })
    }

    async fn write(mut database: Database<SlateDbStorage>) -> Result<(), DatabaseError> {
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
            .await
    }

    async fn read(database: &Database<SlateDbStorage>) -> Result<(), DatabaseError> {
        let mut count = 0;
        let mut rows = database.fetch::<MyStruct>().await?;
        while let Some(row) = rows.next().await {
            println!("{:?}", row?);
            count += 1;
        }
        rows.done().await?;
        println!("row count = {count}");
        Ok(())
    }

    pub async fn run() -> Result<(), Box<dyn Error>> {
        let config = config()?;
        let path = env::var("KITESQL_S3_PATH").unwrap_or_else(|_| "kitesql/async_s3".to_string());
        println!("opening {path} with {config:?}");

        let storage = SlateDbStorage::open_s3(path.as_str(), &config, Default::default()).await?;
        let slatedb = storage.db().clone();
        let database = DataBaseBuilder::path(".").build_with_storage(storage)?;
        // The database and its futures are `Send`, so the work can run on any worker thread.
        tokio::spawn(write(database))
            .await
            .expect("example task panicked")?;
        // Flushes what is still in memory to S3 and releases the database.
        slatedb.close().await?;

        // A new process, or another machine, can open the same data from the bucket.
        let storage = SlateDbStorage::open_s3(path.as_str(), &config, Default::default()).await?;
        let slatedb = storage.db().clone();
        let mut database = DataBaseBuilder::path(".").build_with_storage(storage)?;
        database
            .load(CatalogKind::Table("my_struct".to_string()))
            .await?;
        read(&database).await?;
        drop(database);
        slatedb.close().await?;
        Ok(())
    }
}

#[cfg(all(not(target_arch = "wasm32"), feature = "slatedb-s3", feature = "orm"))]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::run().await
}

#[cfg(not(all(not(target_arch = "wasm32"), feature = "slatedb-s3", feature = "orm")))]
fn main() {}
