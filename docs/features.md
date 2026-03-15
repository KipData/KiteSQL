## Features
### PG Wire: 

run `cargo run --features="net"` to start service

### ORM Mapping: `features = ["orm"]`
```rust
use kite_sql::Model;

#[derive(Default, Debug, PartialEq, Model)]
#[model(table = "users")]
#[model(index(name = "users_name_age_index", columns = "name, age"))]
struct User {
  #[model(primary_key)]
  id: i32,
  #[model(rename = "user_name", varchar = 64)]
  name: String,
  #[model(char = 2)]
  country: String,
  #[model(default = "18", index)]
  age: Option<i32>,
  #[model(skip)]
  cache: String,
}
```

`Model` generates both tuple-to-struct mapping and CRUD metadata for the model.

Supported field attributes:

- `#[model(primary_key)]`
- `#[model(unique)]`
- `#[model(varchar = 64)]`
- `#[model(char = 2)]`
- `#[model(default = "18")]`
- `#[model(decimal_precision = 10, decimal_scale = 2)]`
- `#[model(index)]`
- `#[model(index(name = "users_name_age_index", columns = "name, age"))]`
- `#[model(rename = "column_name")]`
- `#[model(skip)]`

Supported CRUD helpers:

- `database.insert(&model)?`
- `database.get::<User>(&id)?`
- `database.list::<User>()?`
- `database.update(&model)?`
- `database.delete_by_id::<User>(&id)?`

Supported DDL helpers (table creation also creates any `#[model(index)]` secondary indexes):

- `database.create_table::<User>()?`
- `database.create_table_if_not_exists::<User>()?`
- `database.drop_table::<User>()?`
- `database.drop_table_if_exists::<User>()?`
- `database.drop_index::<User>("users_age_index")?`
- `database.drop_index_if_exists::<User>("users_name_age_index")?`
- `database.analyze::<User>()?`

`Model` exposes the primary-key type as an associated type, so lookup and delete-by-id APIs infer the key type directly from the model. Indexes declared by the model can be managed directly by name. The primary-key index is managed by the table definition and cannot be dropped independently.

Query results can still be converted directly into an ORM iterator:

```rust
use kite_sql::db::ResultIter;

let iter = database.run("select id, user_name, age from users")?;

for user in iter.orm::<User>() {
    println!("{:?}", user?);
}
```

If you need manual conversion logic, use the lower-level `from_tuple!` macro with `features = ["macros"]`.

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
