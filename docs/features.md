## Features
### PG Wire: 

run `cargo run --features="net"` to start service

### ORM Mapping: `features = ["orm"]`
See [the ORM guide](../src/orm/README.md) for the full ORM guide, including:

- `#[derive(Model)]` usage and supported attributes
- CRUD, DDL, and migration helpers
- typed query builder APIs
- public ORM structs, enums, and traits
- related `ResultIter::orm::<M>()` integration

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
- [x] Order By
- [x] Limit
- [x] Show Tables
- [x] Explain
- [x] Describe
- [x] Union
- [x] EXCEPT

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
