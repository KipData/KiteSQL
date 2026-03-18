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

The query flow is:

- start with `from::<M>()`
- optionally add filters, grouping, ordering, and limits
- either fetch full `M` rows, or switch into a projection with `project::<P>()`,
  `project_value(...)`, or `project_tuple(...)`

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
- `in_list(values)`
- `not_in_list(values)`
- `between(low, high)`
- `not_between(low, high)`
- `cast("type")`
- `cast_to(DataType)`
- `alias(name)`
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

### AST access

`QueryValue` and `QueryExpr` are lightweight wrappers around `sqlparser::ast::Expr`.
They support:

- `from_ast(expr)`
- `as_ast()`
- `into_ast()`

This lets you mix the typed ORM helpers with lower-level AST construction when
KiteSQL already supports an expression shape that the high-level ORM helpers do
not expose yet.

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

### Single-value queries

Use `Database::from::<M>().project_value(expr)` or
`DBTransaction::from::<M>().project_value(expr)` to project a single
expression. The resulting query still supports the same filtering, grouping,
ordering, and subquery composition, and returns typed values via
`fetch::<T>()` and `get::<T>()`.

This is also the intended entry point for scalar subqueries.

### Tuple queries

Use `Database::from::<M>().project_tuple(values)` or
`DBTransaction::from::<M>().project_tuple(values)` to project multiple
expressions and decode them positionally into a Rust tuple via
`fetch::<(T1, T2, ...)>()` and `get::<(T1, T2, ...)>()`.

### Example

```rust
use kite_sql::Projection;
use kite_sql::orm::{case_when, count_all, func, sum, QueryExpr, QueryValue};
use sqlparser::ast::{BinaryOperator, Expr};

#[derive(Default, Projection)]
struct UserSummary {
    id: i32,
    #[projection(rename = "user_name")]
    display_name: String,
    age: Option<i32>,
}

let exists = database
    .from::<User>()
    .and(User::name().like("A%"), User::age().gte(18))
    .exists()?;

let count = database
    .from::<User>()
    .is_not_null(User::age())
    .count()?;

let top = database
    .from::<User>()
    .or(User::id().eq(1), User::id().eq(2))
    .desc(User::age())
    .get()?;

let normalized = database
    .from::<User>()
    .eq(
        func(
            "add_one",
            [QueryValue::from(User::id())],
        ),
        2,
    )
    .get()?;

let ranged = database
    .from::<User>()
    .between(User::id(), 1, 2)
    .fetch()?;

let typed = database
    .from::<User>()
    .eq(User::id().cast("BIGINT")?, 1_i64)
    .get()?;

let summaries = database
    .from::<User>()
    .project::<UserSummary>()
    .asc(User::id())
    .fetch()?;

let ids = database
    .from::<User>()
    .project_value(User::id())
    .asc(User::id())
    .fetch::<i32>()?;

let age_bucket = database
    .from::<User>()
    .project_value(
        case_when(
            [(User::age().is_null(), "unknown"), (User::age().lt(20), "minor")],
            "adult",
        )
        .alias("age_bucket"),
    )
    .asc(User::id())
    .fetch::<String>()?;

let total_users = database
    .from::<User>()
    .project_value(count_all().alias("total_users"))
    .get::<i32>()?;

let rows = database
    .from::<User>()
    .project_tuple((User::id(), User::name()))
    .asc(User::id())
    .fetch::<(i32, String)>()?;

let repeated_ages = database
    .from::<User>()
    .project_value(User::age())
    .group_by(User::age())
    .having(count_all().gt(1))
    .fetch::<Option<i32>>()?;

let grouped_ids = database
    .from::<User>()
    .project_tuple((User::age(), sum(User::id()).alias("total_ids")))
    .group_by(User::age())
    .fetch::<(Option<i32>, i32)>()?;

let grouped_stats = database
    .from::<EventLog>()
    .project_tuple((
        EventLog::category(),
        sum(EventLog::score()).alias("total_score"),
        count_all().alias("total_count"),
    ))
    .group_by(EventLog::category())
    .fetch::<(String, i32, i32)>()?;

let raw_ast = database
    .from::<User>()
    .filter(QueryExpr::from_ast(Expr::BinaryOp {
        left: Box::new(QueryValue::from(User::id()).into_ast()),
        op: BinaryOperator::Eq,
        right: Box::new(QueryValue::from(1).into_ast()),
    }))
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

Common methods:

- comparisons such as `eq`, `gt`, `lt`
- null checks such as `is_null`
- pattern matching such as `like`
- range and membership checks such as `between` and `in_list`
- `cast`, `cast_to`
- `alias`
- subquery predicates such as `in_subquery`

### `QueryValue`

A query-side wrapper around `sqlparser::ast::Expr` for value-producing expressions.

Helpers:

- comparisons such as `eq`, `gt`, `lt`
- `func(name, args)`
- `count(expr)`
- `count_all()`
- `sum(expr)`
- `avg(expr)`
- `min(expr)`
- `max(expr)`
- `case_when([(cond, value), ...], else_value)`
- `case_value(expr, [(when, value), ...], else_value)`
- `in_list(values)`
- `not_in_list(values)`
- `between(low, high)`
- `not_between(low, high)`
- `cast("type") -> Result<QueryValue, DatabaseError>`
- `cast_to(DataType)`
- `alias(name)`
- `subquery(query)`
- `in_subquery(query)`
- `not_in_subquery(query)`
- `from_ast(expr)`
- `as_ast()`
- `into_ast()`

### `ProjectedValue`

A projected select-list item used by `project::<P>()`, `project_value(...)`,
and `project_tuple(...)`, optionally carrying an alias.

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

Boolean-oriented wrapper around `sqlparser::ast::Expr`, typically used in `where`
clauses and builder filters.

Methods:

- `and(rhs)`
- `or(rhs)`
- `not()`
- `exists(query)`
- `not_exists(query)`
- `from_ast(expr)`
- `as_ast()`
- `into_ast()`

### `FromBuilder<Q, M>`

A lightweight single-table ORM query builder.

`FromBuilder` is the public entry point for ORM DQL construction.

By default it selects full model rows and supports `fetch()` / `get()` into `M`.
Calling `project::<P>()`, `project_value(...)`, or `project_tuple(...)` keeps
the same query chain but changes the final decoding shape to a struct, scalar,
or tuple result.

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

### `Projection`

The DTO projection trait implemented by `#[derive(Projection)]`.

It powers `Database::from::<M>().project::<P>()` and
`DBTransaction::from::<M>().project::<P>()`.

Derived projections declare their source model with
matching field names, and may use `rename` on fields.

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
