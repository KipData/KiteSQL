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

use kite_sql::db::DataBaseBuilder;
use kite_sql::errors::DatabaseError;
use std::env;

const DEFAULT_ITERS: usize = 20_000;

fn main() -> Result<(), DatabaseError> {
    let iters = env::args()
        .nth(1)
        .and_then(|arg| arg.parse::<usize>().ok())
        .unwrap_or(DEFAULT_ITERS);

    let mut database = DataBaseBuilder::path(".").build_in_memory()?;
    database.ddl("create table t1 (c1 int primary key, c2 int, c3 varchar)")?;

    let sql = "\
        select \
            c1 + c2 * 3 as score, \
            case when c2 > 10 then upper(c3) else lower(c3) end as label, \
            substring(c3 from 1 for 2) as prefix \
        from t1 \
        where \
            (c1 > 10 and c2 < 100) \
            or c3 like 'ab%' \
            or c1 in (1, 2, 3, 4, 5) \
        order by score desc, label \
        limit 10";

    for _ in 0..iters {
        database.run(sql)?.done()?;
    }

    Ok(())
}
