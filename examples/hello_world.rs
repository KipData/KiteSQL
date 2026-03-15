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
    use kite_sql::db::{DataBaseBuilder, ResultIter};
    use kite_sql::errors::DatabaseError;
    use kite_sql::Model;
    use std::fs;
    use std::io::ErrorKind;

    const EXAMPLE_DB_PATH: &str = "./example_data/hello_world";

    fn reset_example_dir() -> Result<(), DatabaseError> {
        match fs::remove_dir_all(EXAMPLE_DB_PATH) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    #[derive(Default, Debug, PartialEq, Model)]
    #[model(table = "my_struct")]
    pub struct MyStruct {
        #[model(primary_key)]
        pub c1: i32,
        pub c2: String,
    }

    pub fn run() -> Result<(), DatabaseError> {
        reset_example_dir()?;
        let database = DataBaseBuilder::path(EXAMPLE_DB_PATH).build()?;

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

        let mut row = database.get::<MyStruct>(&1)?.expect("row should exist");
        row.c2 = "ONE".to_string();
        database.update(&row)?;
        database.delete_by_id::<MyStruct>(&2)?;

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
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(all(not(target_arch = "wasm32"), feature = "orm"))]
fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    app::run()
}

#[cfg(all(not(target_arch = "wasm32"), not(feature = "orm")))]
fn main() {}
