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
    .from::<User>()
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
- `from::<M>() -> FromBuilder<...>`

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
- `from::<M>() -> FromBuilder<...>`

`DBTransaction` does not currently expose the ORM DDL convenience methods.

## Query builder API

`Database::from::<M>()` and `DBTransaction::from::<M>()` start a typed query
from one ORM model table.

If you need an explicit relation alias, call `.alias("name")` on a source or
pending join, and re-qualify fields with `Field::qualify("name")` where needed.

For ordinary multi-table queries, `inner_join::<N>().on(...)` and
`left_join::<N>().on(...)` are enough. Aliases are mainly useful for self-joins
or when you want explicit qualification.

The query flow is:

- start with `from::<M>()`
- optionally add `distinct`, filters, grouping, ordering, and limits
- either fetch full `M` rows, or switch into a projection with `project::<P>()`,
  `project_value(...)`, or `project_tuple(...)`

### Field expressions

Generated field accessors return `Field<M, T>`. A field supports:

- `add(value)`
- `sub(value)`
- `mul(value)`
- `div(value)`
- `modulo(value)`
- `neg()`
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
- `in_list(values)`
- `not_in_list(values)`
- `between(low, high)`
- `not_between(low, high)`
- `cast("type")`
- `cast_to(DataType)`
- `alias(name)`
- `qualify(relation)`
- `in_subquery(query)`
- `not_in_subquery(query)`

### Function calls

Use `func(name, args)` to build scalar function calls, including registered UDFs.
Function calls can be used anywhere a `QueryValue` is accepted, such as filters and sorting.

Built-in helpers are also available for common expression shapes:

- `count(expr)`
- `count_all()`
- `sum(expr)`
- `avg(expr)`
- `min(expr)`
- `max(expr)`
- `case_when([(cond, value), ...], else_value)`
- `case_value(expr, [(when, value), ...], else_value)`

### Boolean composition

`QueryExpr` supports:

- `and(rhs)`
- `or(rhs)`
- `not()`
- `exists(query)`
- `not_exists(query)`

### Shared builder methods

`FromBuilder` supports the following methods, and the same chainable query
methods remain available after calling `project::<P>()`, `project_value(...)`,
or `project_tuple(...)`:

- `filter(expr)`
- `alias(name)`
- `inner_join::<N>().on(expr)`
- `left_join::<N>().on(expr)`
- `distinct()`
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
- `in_list(value, values)`
- `not_in_list(value, values)`
- `between(value, low, high)`
- `not_between(value, low, high)`
- `in_subquery(value, query)`
- `not_in_subquery(value, query)`
- `where_exists(query)`
- `where_not_exists(query)`
- `group_by(value)`
- `having(expr)`
- `asc(value)`
- `desc(value)`
- `limit(n)`
- `offset(n)`
- `raw()`
- `fetch()`
- `get()`
- `exists()`
- `count()`

### Struct projections

Use `Database::from::<M>().project::<P>()` or
`DBTransaction::from::<M>().project::<P>()` to project rows into a dedicated
DTO-like struct. `P` is typically a `#[derive(Projection)]` type whose field
names match the projected output names.

Field-level renaming is supported with `#[projection(rename = "...")]`, which
maps a DTO field to a different source column while still aliasing the result
back to the DTO field name.

If you need expression-based outputs, prefer `project_value(...)` or
`project_tuple(...)` and assign explicit names with `.alias(...)`.

### Join example

```rust
let rows = database
    .from::<User>()
    .inner_join::<Order>()
    .on(User::id().eq(Order::user_id()))
    .project_tuple((User::name(), Order::amount()))
    .fetch::<(String, i32)>()?;
# let _ = rows;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

Use `project::<P>()` when:

- you want a DTO-style result type instead of full `M`
- selected outputs are plain columns, optionally with renamed field mapping
- you want field-name-based decoding instead of positional tuple decoding

### Single-value queries

Use `Database::from::<M>().project_value(expr)` or
`DBTransaction::from::<M>().project_value(expr)` to project a single
expression. The resulting query still supports the same filtering, grouping,
ordering, and subquery composition, and returns typed values via
`fetch::<T>()` and `get::<T>()`.

This is also the intended entry point for scalar subqueries.

Use `project_value(...)` when:

- the query returns exactly one value per row
- you want scalar decoding such as `i32`, `String`, or `Option<T>`
- you are building scalar subqueries such as `IN (subquery)`
- the output is an expression or aggregate rather than a DTO field mapping

### Tuple queries

Use `Database::from::<M>().project_tuple(values)` or
`DBTransaction::from::<M>().project_tuple(values)` to project multiple
expressions and decode them positionally into a Rust tuple via
`fetch::<(T1, T2, ...)>()` and `get::<(T1, T2, ...)>()`.

Use `project_tuple(...)` when:

- you need multiple outputs but do not want to define a DTO type
- the projection contains expressions, aggregates, or custom aliases
- positional decoding is acceptable

In practice:

- `project::<P>()`: named-field DTO mapping
- `project_value(...)`: one expression, one decoded value
- `project_tuple(...)`: multiple expressions, positional decoding

### Example

Struct projection:

```rust
use kite_sql::Projection;

#[derive(Default, Projection)]
struct UserSummary {
    id: i32,
    #[projection(rename = "user_name")]
    display_name: String,
}

let summaries = database
    .from::<User>()
    .project::<UserSummary>()
    .asc(User::id())
    .fetch()?;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

Single-value projection:

```rust
use kite_sql::orm::{case_when, count_all};

let total_users = database
    .from::<User>()
    .project_value(count_all().alias("total_users"))
    .get::<i32>()?;

let age_bucket = database
    .from::<User>()
    .project_value(
        case_when(
            [(User::age().is_null(), "unknown"), (User::age().lt(20), "minor")],
            "adult",
        )
        .alias("age_bucket"),
    )
    .fetch::<String>()?;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

Tuple projection with aggregates:

```rust
use kite_sql::orm::{count_all, sum};

let grouped_stats = database
    .from::<EventLog>()
    .project_tuple((
        EventLog::category(),
        sum(EventLog::score()).alias("total_score"),
        count_all().alias("total_count"),
    ))
    .group_by(EventLog::category())
    .fetch::<(String, i32, i32)>()?;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

Subqueries:

```rust
use kite_sql::orm::{func, QueryValue};

let normalized = database
    .from::<User>()
    .eq(func("add_one", [QueryValue::from(User::id())]), 2)
    .get()?;

let uncorrelated = database
    .from::<User>()
    .where_exists(
        database
            .from::<User>()
            .project_value(User::id())
            .eq(User::id(), 1),
    )
    .get()?;
# Ok::<(), kite_sql::errors::DatabaseError>(())
```

For scalar subqueries such as `IN (subquery)` and `EXISTS (subquery)`, use
`Database::from::<M>().project_value(...)` or
`DBTransaction::from::<M>().project_value(...)` to build a single-column
subquery directly when the binder expects one expression to be returned.

## Key types

### `Field<M, T>`

The typed column handle returned by generated accessors such as `User::id()`.

You usually use it to build expressions:

- comparisons such as `eq`, `gt`, `lt`
- null checks such as `is_null`
- pattern matching such as `like`
- range and membership checks such as `between` and `in_list`
- `cast`, `cast_to`
- `alias`
- subquery predicates such as `in_subquery`

### `QueryValue`

The value expression type used throughout ORM query building.

Use it for:

- function calls such as `func(name, args)`
- aggregates such as `count`, `sum`, `avg`, `min`, `max`
- `CASE` expressions
- casts
- aliased projection expressions
- scalar subqueries

`QueryValue` also supports comparison and predicate helpers such as `eq`,
`ne`, `gt`, `gte`, `lt`, `lte`, `like`, `in_list`, `between`, and subquery
predicates when you need to keep composing expressions.

It also supports arithmetic composition such as `add`, `sub`, `mul`, `div`,
`modulo`, and unary `neg`.

### `QueryExpr`

The boolean expression type used for filtering and `HAVING`.

Common helpers:

- `and(rhs)`
- `or(rhs)`
- `not()`
- `exists(query)`
- `not_exists(query)`

### `FromBuilder<Q, M>`

The main single-table ORM query builder returned by `from::<M>()`.

It starts in full-model mode and can later switch into:

- `project::<P>()` for DTO structs
- `project_value(...)` for scalar results
- `project_tuple(...)` for tuple results

## Key traits

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

### `Projection`

The DTO projection trait implemented by `#[derive(Projection)]`.

It powers `Database::from::<M>().project::<P>()` and
`DBTransaction::from::<M>().project::<P>()`.

Derived projections declare their source model with
matching field names, and may use `rename` on fields.

## Extension traits

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
