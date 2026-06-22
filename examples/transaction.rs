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
    use kite_sql::db::DataBaseBuilder;
    use kite_sql::errors::DatabaseError;
    use kite_sql::types::tuple::Tuple;
    use kite_sql::types::value::DataValue;
    use std::fs;
    use std::io::ErrorKind;
    use std::path::Path;

    const EXAMPLE_DB_PATH: &str = "./example_data/transaction";

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

    pub fn run() -> Result<(), DatabaseError> {
        reset_example_dir()?;
        // Optimistic transactions are currently backed by RocksDB.
        let mut database = DataBaseBuilder::path(EXAMPLE_DB_PATH).build_optimistic()?;
        database.ddl("create table if not exists t1 (c1 int primary key, c2 int)")?;
        let mut transaction = database.new_transaction()?;

        transaction
            .run("insert into t1 values(0, 0), (1, 1)")?
            .done()?;

        let mut hidden_iter = database.run("select * from t1")?;
        assert!(hidden_iter.next_tuple(|_, _| ())?.is_none());
        hidden_iter.done()?;

        transaction.commit()?;

        let mut iter = database.run("select * from t1")?;
        iter.next_tuple(|_, tuple| {
            assert_eq!(
                tuple,
                &Tuple::new(None, vec![DataValue::Int32(0), DataValue::Int32(0)])
            );
        })?
        .unwrap();
        iter.next_tuple(|_, tuple| {
            assert_eq!(
                tuple,
                &Tuple::new(None, vec![DataValue::Int32(1), DataValue::Int32(1)])
            );
        })?
        .unwrap();
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;

        let mut tx2 = database.new_transaction()?;
        tx2.run("update t1 set c2 = 99 where c1 = 0")?.done()?;
        let mut c2_iter = database.run("select c2 from t1 where c1 = 0")?;
        assert_eq!(
            c2_iter
                .next_tuple(|_, tuple| tuple.values[0].i32())?
                .unwrap(),
            Some(0)
        );
        c2_iter.done()?;
        drop(tx2);

        database.ddl("drop table t1")?;

        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    app::run()
}
