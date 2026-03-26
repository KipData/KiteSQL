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

#[cfg(all(not(target_arch = "wasm32"), feature = "orm"))]
mod app {
    use kite_sql::db::{DataBaseBuilder, Database};
    use kite_sql::errors::DatabaseError;
    use kite_sql::storage::Storage;
    use kite_sql::Model;
    use std::env;
    use std::fs;
    use std::io::ErrorKind;
    use std::path::Path;

    const EXAMPLE_DB_PATH: &str = "./example_data/hello_world";

    fn reset_example_dir() -> Result<(), DatabaseError> {
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
    pub struct MyStruct {
        #[model(primary_key)]
        pub c1: i32,
        pub c2: String,
    }

    fn run_with_database<S: Storage>(database: Database<S>) -> Result<(), DatabaseError> {
        database.create_table_if_not_exists::<MyStruct>()?;
        database.insert(&MyStruct {
            c1: 0,
            c2: "zero".to_string(),
        })?;
        database.insert(&MyStruct {
            c1: 1,
            c2: "one".to_string(),
        })?;
        database.insert(&MyStruct {
            c1: 2,
            c2: "two".to_string(),
        })?;

        database
            .from::<MyStruct>()
            .eq(MyStruct::c1(), 1)
            .update()
            .set(MyStruct::c2(), "ONE")
            .execute()?;
        database.from::<MyStruct>().eq(MyStruct::c1(), 2).delete()?;

        for row in database.fetch::<MyStruct>()? {
            println!("{:?}", row?);
        }

        let mut agg = database.run("select count(*) from my_struct")?;
        if let Some(count_row) = agg.next() {
            println!("row count = {:?}", count_row?);
        }
        agg.done()?;

        database.drop_table::<MyStruct>()?;

        Ok(())
    }

    pub fn run() -> Result<(), DatabaseError> {
        reset_example_dir()?;
        let backend = env::var("KITESQL_BACKEND").unwrap_or_else(|_| "rocksdb".to_string());

        match backend.to_ascii_lowercase().as_str() {
            "rocksdb" => run_with_database(DataBaseBuilder::path(EXAMPLE_DB_PATH).build_rocksdb()?),
            "lmdb" => run_with_database(DataBaseBuilder::path(EXAMPLE_DB_PATH).build_lmdb()?),
            other => Err(DatabaseError::InvalidValue(format!(
                "unsupported example backend '{other}', expected 'rocksdb' or 'lmdb'"
            ))),
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(all(not(target_arch = "wasm32"), feature = "orm"))]
fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    app::run()
}

#[cfg(all(not(target_arch = "wasm32"), not(feature = "orm")))]
fn main() {}
