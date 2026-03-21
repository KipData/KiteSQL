<p align="center">
  <picture>
    <source srcset="./static/images/kite_sql_dark.png" media="(prefers-color-scheme: dark)">
    <source srcset="./static/images/kite_sql_light.png" media="(prefers-color-scheme: light)">
    <img src="./static/images/kite_sql_light.png" alt="KiteSQL Logo" width="400px">
  </picture>    
</p>

<h3 align="center">
    SQL as a Function for Rust
</h3>

<p align="center">
    <a href="https://summer-ospp.ac.cn/org/orgdetail/0b09d23d-2510-4537-aa9d-45158bb6bdc2"><img src="https://img.shields.io/badge/OSPP-KipData-3DA639?logo=opensourceinitiative"></a>
    <a href="https://github.com/KipData/KiteSQL/blob/main/LICENSE"><img src="https://img.shields.io/github/license/KipData/KiteSQL"></a>
    &nbsp;
    <a href="https://www.rust-lang.org/community"><img src="https://img.shields.io/badge/Rust_Community%20-Join_us-brightgreen?style=plastic&logo=rust"></a>
</p>
<p align="center">
    <a href="https://github.com/KipData/KiteSQL/actions/workflows/ci.yml"><img src="https://github.com/KipData/KiteSQL/actions/workflows/ci.yml/badge.svg" alt="CI"></img></a>
    <a href="https://crates.io/crates/kite_sql/"><img src="https://img.shields.io/crates/v/kite_sql.svg"></a>
    <a href="https://github.com/KipData/KiteSQL" target="_blank">
    <img src="https://img.shields.io/github/stars/KipData/KiteSQL.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/KipData/KiteSQL.svg?style=social" alt="github fork"/>
  </a>
</p>

## Introduction
**KiteSQL** is a lightweight embedded relational database for Rust, inspired by **MyRocks** and **SQLite** and fully written in Rust. It is designed to work not only as a SQL engine, but also as a Rust-native data API that can be embedded directly into applications without relying on external services or heavyweight infrastructure.

KiteSQL supports direct SQL execution, typed ORM models, schema migration, and builder-style queries, so you can combine relational power with an API surface that feels natural in Rust.

## Key Features
- A lightweight embedded SQL database fully rewritten in Rust
- A Rust-native relational API alongside direct SQL execution
- Typed ORM models with migrations and a lightweight typed query/mutation builder
- Higher write speed with an application-friendly embedding model
- All metadata and actual data in KV storage, with no intermediate stateful service layer
- Extensible storage integration for customized workloads
- Supports most of the SQL 2016 syntax
- Ships a WebAssembly build for JavaScript runtimes

#### 👉[check more](docs/features.md)

## ORM
KiteSQL includes a built-in ORM behind the `orm` feature flag. With `#[derive(Model)]`, you can define typed models and get tuple mapping, schema creation, migration support, projections, set queries, and builder-style query/mutation workflows.

### Schema Migration
Model changes are part of the normal workflow. KiteSQL ORM can help evolve tables for common schema updates, including adding, dropping, renaming, and changing columns, so many migrations can stay close to the Rust model definition instead of being managed as hand-written SQL.

For the full ORM guide, see [`src/orm/README.md`](src/orm/README.md).

## Examples

```rust
use kite_sql::db::DataBaseBuilder;
use kite_sql::errors::DatabaseError;
use kite_sql::{Model, Projection};

#[derive(Default, Debug, PartialEq, Model)]
#[model(table = "users")]
#[model(index(name = "users_name_age_idx", columns = "name, age"))]
struct User {
    #[model(primary_key)]
    id: i32,
    #[model(unique, varchar = 128)]
    email: String,
    #[model(rename = "user_name", varchar = 64)]
    name: String,
    #[model(default = "18", index)]
    age: Option<i32>,
}

#[derive(Default, Debug, PartialEq, Projection)]
struct UserSummary {
    id: i32,
    #[projection(rename = "user_name")]
    display_name: String,
}

fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./data").build()?;

    database.migrate::<User>()?;

    database.insert_many([
        User {
            id: 1,
            email: "alice@example.com".to_string(),
            name: "Alice".to_string(),
            age: Some(18),
        },
        User {
            id: 2,
            email: "bob@example.com".to_string(),
            name: "Bob".to_string(),
            age: Some(24),
        },
    ])?;

    database
        .from::<User>()
        .eq(User::id(), 1)
        .update()
        .set(User::age(), Some(19))
        .execute()?;

    database
        .from::<User>()
        .eq(User::id(), 2)
        .delete()?;

    let users = database
        .from::<User>()
        .gte(User::age(), 18)
        .project::<UserSummary>()
        .asc(User::name())
        .limit(10)
        .fetch()?;

    for user in users {
        println!("{:?}", user?);
    }

    // For ad-hoc or more SQL-shaped workloads, `run(...)` is still available.

    Ok(())
}
```

👉**more examples**
- [hello_word](examples/hello_world.rs)
- [transaction](examples/transaction.rs)


## WebAssembly
- Build: `wasm-pack build --release --target nodejs` (outputs to `./pkg`; use `--target web` or `--target bundler` for browser/bundler setups).
- Usage:
```js
import { WasmDatabase } from "./pkg/kite_sql.js";

const db = new WasmDatabase();
await db.execute("create table demo(id int primary key, v int)");
await db.execute("insert into demo values (1, 2), (2, 4)");
const rows = db.run("select * from demo").rows();
console.log(rows.map((r) => r.values.map((v) => v.Int32 ?? v)));
```
- In Node.js, provide a small `localStorage` shim if you enable statistics-related features (see `examples/wasm_index_usage.test.mjs`).

## Python (PyO3)
- Enable bindings with Cargo feature `python`.
- Constructor is explicit: `Database(path)`; in-memory usage is `Database.in_memory()`.
- Minimal usage:
```python
import kite_sql

db = kite_sql.Database.in_memory()
db.execute("create table demo(id int primary key, v int)")
db.execute("insert into demo values (1, 2), (2, 4)")
for row in db.run("select * from demo"):
    print(row["values"])
```

## TPC-C
Run `make tpcc` (or `cargo run -p tpcc --release`) to execute the benchmark against the default KiteSQL storage.  
Run `make tpcc-dual` to mirror every TPCC statement to an in-memory SQLite database alongside KiteSQL and assert the two engines return identical results; this target runs for 60 seconds (`--measure-time 60`). Use `cargo run -p tpcc --release -- --backend dual --measure-time <secs>` for a custom duration.

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPC-C currently only supports single thread

All cases have been fully optimized.
```shell
<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.005)
     Payment : 0.001  (0.013)
Order-Status : 0.002  (0.006)
    Delivery : 0.010  (0.023)
 Stock-Level : 0.002  (0.017)
<TpmC>
27226 Tpmc
```
#### 👉[check more](tpcc/README.md)

## Roadmap
- Get [SQL 2016](https://github.com/KipData/KiteSQL/issues/130) mostly supported
- LLVM JIT: [Perf: TPCC](https://github.com/KipData/KiteSQL/issues/247)

## License

KiteSQL uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/KipData/KiteSQL/blob/main/LICENSE>

## Contributors
[![](https://opencollective.com/kitesql/contributors.svg?width=890&button=false)](https://github.com/KipData/KiteSQL/graphs/contributors)
