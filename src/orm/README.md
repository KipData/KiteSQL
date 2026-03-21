# ORM

KiteSQL provides a built-in ORM behind `features = ["orm"]`.

The ORM is centered around `#[derive(Model)]`. It generates:

- tuple-to-struct mapping
- cached model statements
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

## Model derive

`#[derive(Model)]` is the intended entry point for ORM models.

Struct attributes:

- `#[model(table = "users")]`: sets the backing table name
- `#[model(index(name = "idx", columns = "a, b"))]`: declares a secondary index at the model level

Field attributes:

- `#[model(primary_key)]`
- `#[model(unique)]`
- `#[model(index)]`
- `#[model(rename = "column_name")]`
- `#[model(default = "18")]`
- `#[model(varchar = 64)]`
- `#[model(char = 2)]`
- `#[model(decimal_precision = 10, decimal_scale = 2)]`
- `#[model(skip)]`

The derive macro generates the `Model` implementation, tuple decoding, cached
read/insert/DDL statements, migration metadata, and typed field getters such as
`User::id()` and `User::name()`.

## Query Builder

`Database::from::<M>()` and `DBTransaction::from::<M>()` start a typed query
from one ORM model table.

The usual flow is:

- start with `from::<M>()`
- add filters, joins, grouping, ordering, and limits
- keep full-model output, or switch into `project::<P>()`,
  `project_value(...)`, or `project_tuple(...)`
- once the output shape is fixed, compose set queries with `union(...)`,
  `except(...)`, and optional `.all()`

If you need an explicit relation alias, call `.alias("name")` on a source or
pending join, and re-qualify fields with `Field::qualify("name")` where
needed. For ordinary multi-table queries, `inner_join::<N>().on(...)`,
`left_join::<N>().on(...)`, `right_join::<N>().on(...)`,
`full_join::<N>().on(...)`, `cross_join::<N>()`, and `using(...)` cover most
cases.

Most expression building starts from generated fields such as `User::id()` and
`User::name()`. Field values support arithmetic, comparison, null checks,
pattern matching, range checks, casts, aliases, and subquery predicates. For
computed expressions, use `QueryValue` helpers such as `func`, `count`,
`count_all`, `sum`, `avg`, `min`, `max`, `case_when`, and `case_value`.

Boolean composition lives on `QueryExpr` through `and`, `or`, `not`, `exists`,
and `not_exists`.

### Projections

Use full-model fetches when the query still matches `M`, or switch to one of
the projection modes:

- `project::<P>()`: decode rows into a DTO-style struct
- `project_value(...)`: decode one expression per row into a scalar type
- `project_tuple(...)`: decode multiple expressions positionally into a tuple

For `project::<P>()`, `P` is typically a `#[derive(Projection)]` type whose
field names match the output names. Use `#[projection(rename = "...")]` to map
DTO fields to differently named source columns, and `#[projection(from = "...")]`
for join projections that need an explicit source relation.

If the output is expression-based, prefer `project_value(...)` or
`project_tuple(...)` and assign explicit names with `.alias(...)`.

### Set queries

Set operations are available after the output shape is fixed:

- model rows: `from::<User>().union(...)`
- single values: `project_value(...).union(...)`
- tuples: `project_tuple(...).except(...)`
- struct projections: `project::<P>().union(...)`

Call `.all()` after `union(...)` or `except(...)` when you want multiset
semantics instead of the default distinct result.

After a set query is formed, you can still apply result-level methods such as
`asc(...)`, `desc(...)`, `nulls_first()`, `nulls_last()`, `limit(...)`,
`offset(...)`, `fetch()`, `get()`, `exists()`, `count()`, and `explain()`.

Tips: `nulls_first()` and `nulls_last()` only affect the most recently added
sort key from `asc(...)` or `desc(...)`.

For richer combinations such as join projections, grouping, scalar subqueries,
and set queries, prefer the rustdoc on `FromBuilder`, `SetQueryBuilder`,
`Field`, `QueryValue`, and `QueryExpr`.

## Change Operations

The ORM supports both schema changes and data changes.

### Schema changes

On `Database`:

- `create_table::<M>()`
- `create_table_if_not_exists::<M>()`
- `migrate::<M>()`
- `drop_index::<M>(index_name)`
- `drop_index_if_exists::<M>(index_name)`
- `drop_table::<M>()`
- `drop_table_if_exists::<M>()`
- `truncate::<M>()`
- `create_view(name, query_builder)`
- `create_or_replace_view(name, query_builder)`
- `drop_view(name)`
- `drop_view_if_exists(name)`

`DBTransaction` does not currently expose the ORM DDL convenience methods.

Typical schema maintenance uses the same model types and query builders:
create tables from `Model`, truncate by model, and create or replace views from
ORM queries.

### Data changes

For common model-oriented writes:

- `insert::<M>(&model)`
- `insert_many::<M>(models)`

For query-driven writes, reuse the same filtered `from::<M>()` entrypoint and
finish with:

- `insert::<Target>()`
- `insert_into::<Target>(...)`
- `overwrite::<Target>()`
- `overwrite_into::<Target>(...)`
- `update().set(...).execute()`
- `delete()`

Here `overwrite*` follows the engine's `INSERT OVERWRITE` semantics, meaning
conflicting target rows are replaced rather than the whole table being cleared.

For model-oriented writes, use `insert` and `insert_many`. For query-driven
writes, compose from `from::<M>()` and finish with `insert`, `overwrite`,
`update`, or `delete`.

Query-driven writes are intentionally shaped like read queries first, so the
same filters, joins, and projections can flow into the final write operation.

## Introspection / Maintenance

The ORM also exposes light-weight introspection and maintenance helpers.

On `Database`:

- `show_tables()`
- `show_views()`
- `describe::<M>()`
- `from::<M>()...explain()`
- `analyze::<M>()`

On `DBTransaction`:

- `show_tables()`
- `show_views()`
- `describe::<M>()`

These helpers are intended for light-weight inspection around ORM-managed
tables, without dropping down to raw SQL for common metadata queries.

## Further reading

Detailed method-by-method examples live in the rustdoc for:

- `Field<M, T>`
- `QueryValue`
- `QueryExpr`
- `FromBuilder<Q, M>`
- `SetQueryBuilder<Q, M>`
- `Projection`
- `Model`
