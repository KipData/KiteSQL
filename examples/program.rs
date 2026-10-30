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
    use kite_sql::types::value::DataValue;
    use kite_sql::types::LogicalType;
    use std::fs;
    use std::io::ErrorKind;
    use std::path::Path;

    const EXAMPLE_DB_PATH: &str = "./example_data/program";

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
        let mut database = DataBaseBuilder::path(EXAMPLE_DB_PATH).build_rocksdb()?;
        database.ddl("create table t1 (c1 int primary key, c2 int)")?;
        database
            .run("insert into t1 values(0, 0), (1, 1), (2, 2)")?
            .done()?;

        // A program is parsed, optimized and built into executors once. Every execution
        // binds new parameters and reuses those executors, in any transaction of the
        // database. It runs one execution at a time: the iterator borrows it and puts
        // it back when it ends.
        let mut update = database.program_sql(
            "update t1 set c2 = $2 where c1 = $1",
            &[(1, LogicalType::Integer), (2, LogicalType::Integer)],
        )?;
        let mut select = database.program_sql(
            "select c2 from t1 where c1 = $1",
            &[(1, LogicalType::Integer)],
        )?;
        for round in 1..=2 {
            let mut transaction = database.new_transaction(false)?;
            for c1 in 0..3 {
                update
                    .execute(
                        &mut transaction,
                        [
                            (1, DataValue::Int32(c1)),
                            (2, DataValue::Int32(c1 * 10 * round)),
                        ],
                    )?
                    .done()?;

                let mut iter = select.execute(&mut transaction, [(1, DataValue::Int32(c1))])?;
                let c2 = iter.next_tuple(|_, tuple| tuple[0].i32())?.unwrap();
                assert_eq!(c2, Some(c1 * 10 * round));
            }
            transaction.commit()?;
        }

        // A later transaction sees what the earlier ones committed.
        let mut transaction = database.new_transaction(true)?;
        let mut iter = select.execute(&mut transaction, [(1, DataValue::Int32(2))])?;
        let c2 = iter.next_tuple(|_, tuple| tuple[0].i32())?.unwrap();
        assert_eq!(c2, Some(40));
        drop(iter);
        drop(transaction);

        drop((update, select));
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
