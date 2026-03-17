# ORM

KiteSQL provides a built-in ORM behind `features = ["orm"]`.

The ORM is centered around `#[derive(Model)]`. It generates:

- tuple-to-struct mapping
- cached CRUD statements
- cached DDL statements
- migration metadata
- typed field accessors for query building

## Enabling the feature

```toml
kite_sql = { version = "*", features = ["orm"] }
```

If you also want to derive the model macro, enable `macros` as well:

```toml
kite_sql = { version = "*", features = ["orm", "macros"] }
```

## Quick start

```rust
use kite_sql::db::DataBaseBuilder;
use kite_sql::Model;

#[derive(Default, Debug, PartialEq, Model)]
#[model(table = "users")]
#[model(index(name = "users_name_age_index", columns = "name, age"))]
struct User {
    #[model(primary_key)]
    id: i32,
    #[model(rename = "user_name", varchar = 64)]
    name: String,
    #[model(default = "18", index)]
    age: Option<i32>,
}

let database = DataBaseBuilder::path(".").build_in_memory()?;
database.create_table::<User>()?;

database.insert(&User {
    id: 1,
    name: "Alice".to_string(),
    age: Some(18),
})?;

let user = database.get::<User>(&1)?.unwrap();
assert_eq!(user.name, "Alice");

let adults = database
    .select::<User>()
    .gte(User::age(), 18)
    .asc(User::name())
    .fetch()?;

for user in adults {
    println!("{:?}", user?);
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Derive macro

`#[derive(Model)]` is the intended entry point for ORM models.

### Struct attributes

- `#[model(table = "users")]`: sets the backing table name
- `#[model(index(name = "idx", columns = "a, b"))]`: declares a secondary index at the model level

### Field attributes

- `#[model(primary_key)]`
- `#[model(unique)]`
- `#[model(index)]`
- `#[model(rename = "column_name")]`
- `#[model(default = "18")]`
- `#[model(varchar = 64)]`
- `#[model(char = 2)]`
- `#[model(decimal_precision = 10, decimal_scale = 2)]`
- `#[model(skip)]`

### Generated helpers

The derive macro generates:

- the `Model` trait implementation
- tuple mapping from query results into the Rust struct
- cached statements for DDL and CRUD
- static column metadata for migrations
- typed field getters such as `User::id()` and `User::name()`

## Database ORM APIs

The following ORM helpers are available on `Database`.

### DDL

- `create_table::<M>()`: creates the table and any declared secondary indexes
- `create_table_if_not_exists::<M>()`: idempotent table and index creation
- `migrate::<M>()`: aligns an existing table with the current model definition
- `drop_index::<M>(index_name)`
- `drop_index_if_exists::<M>(index_name)`
- `drop_table::<M>()`
- `drop_table_if_exists::<M>()`

### DML

- `analyze::<M>()`: refreshes optimizer statistics for the model table
- `insert::<M>(&model)`
- `update::<M>(&model)`
- `delete_by_id::<M>(&key)`

### DQL

- `get::<M>(&key) -> Result<Option<M>, DatabaseError>`
- `fetch::<M>() -> Result<OrmIter<...>, DatabaseError>`
- `select::<M>() -> SelectBuilder<...>`

## Transaction ORM APIs

The following ORM helpers are available on `DBTransaction`.

### DML

- `analyze::<M>()`
- `insert::<M>(&model)`
- `update::<M>(&model)`
- `delete_by_id::<M>(&key)`

### DQL

- `get::<M>(&key) -> Result<Option<M>, DatabaseError>`
- `fetch::<M>() -> Result<OrmIter<...>, DatabaseError>`
- `select::<M>() -> SelectBuilder<...>`

`DBTransaction` does not currently expose the ORM DDL convenience methods.

## Query builder API

`Database::select::<M>()` and `DBTransaction::select::<M>()` return a typed `SelectBuilder`.

### Field expressions

Generated field accessors return `Field<M, T>`. A field supports:

- `eq(value)`
- `ne(value)`
- `gt(value)`
- `gte(value)`
- `lt(value)`
- `lte(value)`
- `is_null()`
- `is_not_null()`
- `like(pattern)`
- `not_like(pattern)`

### Function calls

Use `func(name, args)` to build scalar function calls, including registered UDFs.
Function calls can be used anywhere a `QueryValue` is accepted, such as filters and sorting.

### Boolean composition

`QueryExpr` supports:

- `and(rhs)`
- `or(rhs)`
- `not()`

### Builder methods

`SelectBuilder` supports:

- `filter(expr)`
- `and(left, right)`
- `or(left, right)`
- `not(expr)`
- `eq(left, right)`
- `ne(left, right)`
- `gt(left, right)`
- `gte(left, right)`
- `lt(left, right)`
- `lte(left, right)`
- `is_null(value)`
- `is_not_null(value)`
- `like(value, pattern)`
- `not_like(value, pattern)`
- `asc(value)`
- `desc(value)`
- `limit(n)`
- `offset(n)`
- `raw()`
- `fetch()`
- `get()`
- `exists()`
- `count()`

### Example

```rust
use kite_sql::orm::{func, QueryValue};

let exists = database
    .select::<User>()
    .and(User::name().like("A%"), User::age().gte(18))
    .exists()?;

let count = database
    .select::<User>()
    .is_not_null(User::age())
    .count()?;

let top = database
    .select::<User>()
    .or(User::id().eq(1), User::id().eq(2))
    .desc(User::age())
    .get()?;

let normalized = database
    .select::<User>()
    .eq(
        func(
            "add_one",
            [QueryValue::from(User::id())],
        ),
        2,
    )
    .get()?;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

## Public structs and enums

### `OrmField`

Static metadata for one persisted field.

Fields:

- `column`
- `placeholder`
- `primary_key`
- `unique`

### `OrmColumn`

Static metadata for one persisted column used by table creation and migration.

Fields:

- `name`
- `ddl_type`
- `nullable`
- `primary_key`
- `unique`
- `default_expr`

Methods:

- `definition_sql()`

### `Field<M, T>`

A typed model field handle used by the query builder.

### `QueryValue`

A query-side value node used by expressions.

Variants:

- `Column { table, column }`
- `Param(DataValue)`
- `Function { name, args }`

Helpers:

- `func(name, args)`

### `CompareOp`

Comparison operator used by `QueryExpr`.

Variants:

- `Eq`
- `Ne`
- `Gt`
- `Gte`
- `Lt`
- `Lte`

### `QueryExpr`

Boolean query expression used in `where` clauses.

Variants:

- `Compare`
- `IsNull`
- `Like`
- `And`
- `Or`
- `Not`

Methods:

- `and(rhs)`
- `or(rhs)`
- `not()`

### `SelectBuilder<Q, M>`

A lightweight single-table ORM query builder.

### `DatabaseSelectSource<'a, S>`

Internal source adapter used by `Database::select::<M>()`.
This type is public for generic completeness, but it is not usually used directly.

### `TransactionSelectSource<'a, 'tx, S>`

Internal source adapter used by `DBTransaction::select::<M>()`.
This type is public for generic completeness, but it is not usually used directly.

## Public traits

### `Model`

The core ORM trait implemented by `#[derive(Model)]`.

Important associated items:

- `type PrimaryKey`
- `table_name()`
- `fields()`
- `columns()`
- `params(&self)`
- `primary_key(&self)`
- cached statement getters such as `select_statement()` and `insert_statement()`

In most cases, you should derive this trait instead of implementing it manually.

### `FromDataValue`

Converts a `DataValue` into a Rust value during ORM mapping.

### `ToDataValue`

Converts a Rust value into a `DataValue` for ORM parameters and query expressions.

### `ModelColumnType`

Maps a Rust type to the SQL DDL type used by ORM table creation and migration.

### `StringType`

Marker trait for string-like fields that support `#[model(varchar = N)]` and `#[model(char = N)]`.

### `DecimalType`

Marker trait for decimal-like fields that support precision and scale annotations.

### `StatementSource`

Execution abstraction shared by `Database` and `DBTransaction` for prepared ORM statements.
This is mostly framework infrastructure.

### `SelectSource`

Execution abstraction used by `SelectBuilder`.
This is mostly framework infrastructure.

## Public helper function

### `try_get`

`try_get` extracts a named field from a tuple and converts it into a Rust value.
It is primarily intended for derive-generated code and low-level integrations.

## Migration behavior

`migrate::<M>()` is intended to preserve existing data whenever possible.

Current behavior:

- creates the table if it does not exist
- adds missing columns
- drops removed columns
- applies supported `CHANGE COLUMN` updates for compatible existing columns
- can infer safe renames in straightforward cases
- can update type, default, and nullability when supported by the engine

Current limitations:

- primary key changes are rejected
- unique-constraint changes are rejected
- the primary-key index is managed by the table and cannot be dropped independently
- changing the type of an indexed column is intentionally conservative

## Related APIs outside `crate::orm`

ORM models can also be created from arbitrary query results through `ResultIter::orm::<M>()`:

```rust
let iter = database.run("select id, user_name, age from users")?;
let users = iter.orm::<User>();
# let _ = users;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

If you need custom tuple-to-struct mapping without `#[derive(Model)]`, use the lower-level `from_tuple!` macro with `features = ["macros"]`.
