## Features
### Shell

Run the local interactive shell with:

```bash
cargo run --bin kitesql-shell
```

Use a custom data directory:

```bash
cargo run --bin kitesql-shell -- --path ./tmp/kitesql-shell-data
```

Run a one-shot SQL check:

```bash
cargo run --bin kitesql-shell -- -e "select current_timestamp"
```

In interactive mode, end SQL statements with `;`; an empty line also executes the buffered statement.

Built-in metacommands:

- `.help`
- `.quit`
- `.tables`
- `.views`
- `.schema <name>`

### PG Wire: 

run `cargo run --features="net"` to start service

### ORM Mapping: `features = ["orm"]`
See [the ORM guide](../src/orm/README.md) for the full ORM guide, including:

- `#[derive(Model)]` usage and supported attributes
- CRUD, DDL, and migration helpers
- typed query builder APIs
- public ORM structs, enums, and traits
- related `ResultIter::orm::<M>()` integration

### Spill-backed Aggregation: `features = ["spill"]`

The native-only `spill` feature bounds memory usage for large grouped
aggregations. It is not enabled by default and cannot be combined with `wasm`.

For SQL, place the optimizer hint immediately after `SELECT`:

```sql
SELECT /*+ FORCE_AGG_SPILL */ user_id, COUNT(*)
FROM orders
GROUP BY user_id
ORDER BY user_id;
```

For ORM queries, enable both `orm` and `spill`, then call `force_spill()` before
building the projection and aggregate plan:

```rust,ignore
let grouped = database.bind(|ctx| {
    ctx.from::<Order>()?
        .force_spill()?
        .project_tuple(|e| {
            Ok(vec![e.column(Order::user_id())?, e.count_all()?])
        })?
        .group_by(|e| e.column(Order::user_id()))?
        .order_by(Order::user_id())?
        .finish()
})?;
```

KiteSQL still reuses an already ordered input when possible. Otherwise it adds
an external sort on the group keys and executes a streaming aggregate. The
hint is intended for grouped aggregates or `DISTINCT`; an aggregate without
group keys already uses constant-size accumulator state.

### Nested-loop Join Hint

Use `FORCE_NEST_LOOP_JOIN` to select the nested-loop implementation for joins
in the current `SELECT` query block:

```sql
SELECT /*+ FORCE_NEST_LOOP_JOIN */ orders.id, users.name
FROM orders
JOIN users ON orders.user_id = users.id;
```

ORM queries can select the same implementation before adding joins:

```rust,ignore
let joined = database.bind(|ctx| {
    ctx.from::<Order>()?
        .force_nested_loop()
        .inner_join::<User, _>(|e| {
            e.column(Order::user_id())?.eq(e.column(User::id())?)
        })?
        .finish()
})?;
```

It avoids Hash Join's build-side tuple materialization, but may perform
substantially more work on large inputs.

### User-Defined Function: `features = ["macros"]`
```rust
scala_function!(TestFunction::test(LogicalType::Integer, LogicalType::Integer) -> LogicalType::Integer => |v1: DataValue, v2: DataValue| {
    let plus_binary_evaluator = EvaluatorFactory::binary_create(LogicalType::Integer, BinaryOperator::Plus)?;
    let value = plus_binary_evaluator.binary_eval(&v1, &v2);

    let plus_unary_evaluator = EvaluatorFactory::unary_create(LogicalType::Integer, UnaryOperator::Minus)?;
    Ok(plus_unary_evaluator.unary_eval(&value))
});

let kite_sql = DataBaseBuilder::path("./data")
    .register_scala_function(TestFunction::new())
    .build()?;
```

### User-Defined Table Function: `features = ["macros"]`
```rust
table_function!(MyTableFunction::test_numbers(LogicalType::Integer) -> [c1: LogicalType::Integer, c2: LogicalType::Integer] => (|v1: DataValue| {
    let num = v1.i32().unwrap();

    Ok(Box::new((0..num)
        .into_iter()
        .map(|i| Ok(Tuple {
            id: None,
            values: vec![
                DataValue::Int32(Some(i)),
                DataValue::Int32(Some(i)),
            ]
        }))) as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>)
}));
let kite_sql = DataBaseBuilder::path("./data")
   .register_table_function(MyTableFunction::new())
   .build()?;
```

### Optimizer
- RBO
- CBO based on RBO(Physical Selection)

### Executor
- Volcano

### MVCC Transaction
- Pessimistic (Default)
- Optimistic
- Isolation levels: see [Transaction Isolation](transaction-isolation.md)

### Checkpoint
KiteSQL exposes checkpoint as a storage capability rather than a full backup workflow. A checkpoint only creates a consistent local snapshot directory; compressing, uploading, retaining, and pruning backups should stay in application code.

Support matrix:
- `build_optimistic()` supports `Database::checkpoint(...)` through RocksDB's safe checkpoint API.
- `build_rocksdb()` requires Cargo feature `unsafe_txdb_checkpoint` because upstream `rocksdb` does not currently expose a safe `TransactionDB` checkpoint API.
- `build_lmdb()` and `build_in_memory()` do not currently expose checkpoint support.

Opt in for `TransactionDB` checkpoint support:
```bash
cargo check --features unsafe_txdb_checkpoint
```

Minimal usage:
```rust
use kite_sql::db::DataBaseBuilder;
use kite_sql::errors::DatabaseError;

fn main() -> Result<(), DatabaseError> {
    let database = DataBaseBuilder::path("./data").build_rocksdb()?;

    database.checkpoint("./backup/checkpoint-2026-03-29")?;

    Ok(())
}
```

If `unsafe_txdb_checkpoint` is not enabled, `build_rocksdb()` returns an explicit error instead of attempting the experimental implementation.

### Field options
- [not] null
- unique
- primary key

### Supports index type
- PrimaryKey
- Unique
- Normal
- Composite

### Supports multiple primary key types
- Tinyint
- UTinyint
- Smallint
- USmallint
- Integer
- UInteger
- Bigint
- UBigint
- Char
- Varchar

### DDL
- Begin (Server only)
- Commit (Server only)
- Rollback (Server only)
- Create
    - [x] Table
    - [x] Index: Unique\Normal\Composite
    - [x] View
- Drop
    - [x] Table
    - [x] Index
      - Tips: `Drop Index table_name.index_name`
    - [x] View
- Alert
    - [x] Add Column
    - [x] Drop Column
- [x] Truncate

### DQL
- [x] Select
    - SeqScan
    - IndexScan
    - FunctionScan
- [x] Where
- [x] Distinct
- [x] Alias
- [x] Aggregation: 
  - count()
  - sum()
  - avg()
  - min()
  - max()
- [x] SubQuery[select/from/where]
- [x] Join: 
  - Inner
  - Left
  - Right
  - Full
  - Cross (Natural\Using)
- [x] Exists
- [x] Group By
- [x] Having
- [x] Window functions:
  - `row_number()`, `rank()`, `dense_rank()`
  - `count()`, `sum()`, `avg()`, `min()`, `max()` with `OVER`
  - `PARTITION BY` and window `ORDER BY` with the default frame
  - Explicit frames, named windows, and `QUALIFY` are not yet supported
- [x] Order By
- [x] Limit
- [x] Show Tables
- [x] Explain
- [x] Describe
- [x] Union
- [x] EXCEPT
- [x] INTERSECT

### DML
- [x] Insert
- [x] Insert Overwrite
- [x] Update
- [x] Delete
- [x] Analyze
- [x] Copy To
- [x] Copy From

### DataTypes
- Invalid
- SqlNull
- Boolean
- Tinyint
- UTinyint
- Smallint
- USmallint
- Integer
- UInteger
- Bigint
- UBigint
- Float
- Double
- Char
- Varchar
- Date
- DateTime
- Time
- TimeStamp
- Tuple
