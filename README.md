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
**KiteSQL** is a lightweight embedded database inspired by **MyRocks** and **SQLite** and completely coded in Rust. It aims to provide a more user-friendly, lightweight, and low-loss RDBMS for Rust programming so that the APP does not rely on other complex components. It can perform complex relational data operations.

## Key Features
- A lightweight embedded SQL database fully rewritten in Rust
- Higher write speed, more user-friendly API
- All metadata and actual data in KV Storage, and there is no state component (e.g. system table) in the middle
- Supports extending storage for customized workloads
- Supports most of the SQL 2016 syntax
- Ships a WebAssembly build for JavaScript runtimes

#### 👉[check more](docs/features.md)

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

## Examples

```rust
use kite_sql::db::{DataBaseBuilder, ResultIter};

let kite_sql = DataBaseBuilder::path("./data").build()?;

kite_sql
    .run("create table if not exists t1 (c1 int primary key, c2 int)")?
    .done()?;
kite_sql
    .run("insert into t1 values(0, 0), (1, 1)")?
    .done()?;

let mut iter = kite_sql.run("select * from t1")?;

// Query schema is available on every result iterator.
let column_names: Vec<_> = iter.schema().iter().map(|c| c.name()).collect();
println!("columns: {column_names:?}");

for tuple in iter {
    println!("{:?}", tuple?);
}
```

👉**more examples**
- [hello_word](examples/hello_world.rs)
- [transaction](examples/transaction.rs)

## TPC-C
run `cargo run -p tpcc --release` to run tpcc

- i9-13900HX
- 32.0 GB
- KIOXIA-EXCERIA PLUS G3 SSD
- Tips: TPC-C currently only supports single thread
```shell
<90th Percentile RT (MaxRT)>
   New-Order : 0.002  (0.005)
     Payment : 0.001  (0.003)
Order-Status : 0.057  (0.088)
    Delivery : 0.001  (0.001)
 Stock-Level : 0.002  (0.006)
<TpmC>
11125 Tpmc
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
