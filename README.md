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
    <a href="https://codecov.io/github/KipData/KiteSQL"><img src="https://codecov.io/github/KipData/KiteSQL/branch/main/graph/badge.svg" alt="Codecov"></img></a>
    <a href="https://deepwiki.com/KipData/KiteSQL"><img src="https://deepwiki.com/badge.svg" alt="Ask DeepWiki"></img></a>
    <a href="https://discord.gg/dU8eVGpJ8h"><img src="https://img.shields.io/badge/Discord-Join_us-5865F2?logo=discord&logoColor=white" alt="Discord"></a>
    <a href="https://crates.io/crates/kite_sql/"><img src="https://img.shields.io/crates/v/kite_sql.svg"></a>
    <a href="https://github.com/KipData/KiteSQL" target="_blank">
    <img src="https://img.shields.io/github/stars/KipData/KiteSQL.svg?style=social" alt="github star"/>
    <img src="https://img.shields.io/github/forks/KipData/KiteSQL.svg?style=social" alt="github fork"/>
  </a>
</p>

## Introduction
**KiteSQL** is a lightweight embedded relational database written in Rust, inspired by **MyRocks** and **SQLite**. It runs inside your application with no external service: execute SQL directly, or use typed ORM models, migrations, and builder-style queries.

- Most of the SQL 2016 syntax
- All metadata and data stored in KV storage (RocksDB, LMDB, or in-memory)
- Typed ORM with schema migration (`orm` feature), see [`src/orm/README.md`](src/orm/README.md)
- WebAssembly and Python bindings

👉 [More features](docs/features.md)

## Example

```rust
use kite_sql::db::DataBaseBuilder;
use kite_sql::errors::DatabaseError;
use kite_sql::orm::OrmQueryResultExt;
use kite_sql::Model;

#[derive(Default, Debug, PartialEq, Model)]
#[model(table = "users")]
struct User {
    #[model(primary_key)]
    id: i32,
    #[model(varchar = 64)]
    name: String,
    #[model(default = "18", index)]
    age: Option<i32>,
}

fn main() -> Result<(), DatabaseError> {
    let mut database = DataBaseBuilder::path("./data").build_rocksdb()?;
    database.migrate::<User>()?;

    database.insert_many([
        User { id: 1, name: "Alice".to_string(), age: Some(18) },
        User { id: 2, name: "Bob".to_string(), age: Some(24) },
    ])?;

    let users = database
        .bind(|ctx| {
            ctx.from::<User>()?
                .filter(|e| e.column(User::age())?.gte(18))?
                .project_scalars((User::id(), User::name()))?
                .order_by(User::name())?
                .finish()
        })?
        .project_tuple::<(i32, String)>();
    for user in users {
        println!("{:?}", user?);
    }

    // Plain SQL works too.
    database.run("select count(*) from users")?.done()?;
    Ok(())
}
```

More: [hello_world](examples/hello_world.rs), [transaction](examples/transaction.rs).

## Storage Backends
| Builder | Storage |
| --- | --- |
| `build_rocksdb()` | RocksDB (default feature `rocksdb`), stronger for write-heavy workloads |
| `build_lmdb()` | LMDB (feature `lmdb`), stronger for read-heavy workloads |
| `build_in_memory()` | In-memory, for tests and temporary data |
| `build_optimistic()` | RocksDB with optimistic transactions |

Feature flags, checkpoints, and transaction isolation: [docs/features.md](docs/features.md), [docs/transaction-isolation.md](docs/transaction-isolation.md).

## Shell, WebAssembly, Python

### Shell
```bash
cargo run --bin kitesql-shell
cargo run --bin kitesql-shell -- -e "select 1"
```
Type `.help` for metacommands.

### WebAssembly
```bash
wasm-pack build --release --target nodejs
```
```js
import { WasmDatabase } from "./pkg/kite_sql.js";

const db = new WasmDatabase();
await db.ddl("create table demo(id int primary key, v int)");
console.log(db.run("select * from demo").rows());
```

### Python
Requires the `python` feature.
```python
import kite_sql

db = kite_sql.Database.in_memory()  # or Database(path, backend="lmdb")
db.execute("create table demo(id int primary key, v int)")
print(list(db.run("select * from demo")))
```

## TPC-C
`make tpcc` runs the benchmark (`--backend rocksdb|lmdb`); `make tpcc-dual` cross-checks every statement against SQLite.

720-second single-thread run on an i9-13900HX (32 GB, KIOXIA EXCERIA PLUS G3), every backend pinned to the same P-core. Latencies are p90 in µs, including commit.

| Backend | TpmC | New-Order | Payment | Order-Status | Delivery | Stock-Level |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| KiteSQL LMDB | 159616 | 315 | 81 | 44 | 435 | 332 |
| KiteSQL RocksDB | 48035 | 459 | 258 | 200 | 11055 | 704 |
| SQLite balanced | 67102 | 283 | 67 | 48 | 338 | 473 |
| SQLite practical | 67153 | 336 | 66 | 41 | 427 | 339 |

👉 [Details and how to reproduce](tpcc/README.md)

## Roadmap
- Get [SQL 2016](https://github.com/KipData/KiteSQL/issues/130) mostly supported
- LLVM JIT: [Perf: TPCC](https://github.com/KipData/KiteSQL/issues/247)

## License

KiteSQL uses the [Apache 2.0 license][1] to strike a balance between
open contributions and allowing you to use the software however you want.

[1]: <https://github.com/KipData/KiteSQL/blob/main/LICENSE>

## Contributors
[![](https://opencollective.com/kitesql/contributors.svg?width=890&button=false)](https://github.com/KipData/KiteSQL/graphs/contributors)
