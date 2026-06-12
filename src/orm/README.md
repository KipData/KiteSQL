# ORM

KiteSQL's ORM is available with `features = ["orm"]`. This also enables the
derive macros used by `#[derive(Model)]`; `#[derive(Projection)]` is optional
for DTO-style projections.

```toml
kite_sql = { version = "*", features = ["orm"] }
```

## Model

`#[derive(Model)]` defines the table mapping, cached model operations, typed
field accessors, and migration metadata.

```rust,ignore
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
```

Common field attributes are `primary_key`, `unique`, `index`, `rename`,
`default`, `varchar`, `char`, `decimal_precision`, `decimal_scale`, and `skip`.

## Queries

The recommended query entrypoint is `bind`. The closure receives an
`OrmContext`, drives the binder directly, and may return a plan stage; the ORM
normalizes it into a `LogicalPlan` at the boundary.

```rust,ignore
use kite_sql::db::{DataBaseBuilder, ResultIter};
use kite_sql::orm::{BoundExpressionOps, OrmQueryResultExt};

let database = DataBaseBuilder::path(".").build_in_memory()?;
database.create_table::<User>()?;
database.insert(&User {
    id: 1,
    name: "Alice".to_string(),
    age: Some(18),
})?;

let adults = database
    .bind(|ctx| {
        ctx.from::<User>()?.filter(|e| {
            let adult = e.column(User::age())?.gte(18)?;
            let named_a = e.column(User::name())?.like("A%")?;
            adult.and(named_a)
        })
    })?
    .orm::<User>()
    .collect::<Result<Vec<_>, _>>()?;

assert_eq!(adults[0].name, "Alice");
# Ok::<(), Box<dyn std::error::Error>>(())
```

Inside expression closures, `e.column(User::id())?` resolves through the core
binder and returns a bound expression. Expression methods such as `eq`, `gte`,
`like`, `and`, `or`, `is_null`, and `in_list` compose directly into core
`ScalarExpression` values; constants can be passed directly.

Prefer the compact helpers when the query shape is simple:

```rust,ignore
let rows = database
    .bind(|ctx| {
        ctx.from::<User>()?
            .filter(|e| e.column(User::age())?.gte(18))?
            .desc_by(User::age())?
            .project_scalars((User::id(), User::name()))
    })?
    .project_tuple::<(i32, String)>()
    .collect::<Result<Vec<_>, _>>()?;
# let _ = rows;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Use `project_scalar(...)` for one field, `project_scalars((...))` for simple
tuples, and `project_value/project_tuple` when the projection is an expression
or needs aliases. Use `asc_by/desc_by` for field ordering, and `asc/desc` for
computed sort expressions.

Joins and set operations use the same binder-backed style:

```rust,ignore
let joined = database
    .bind(|ctx| {
        ctx.from::<User>()?
            .inner_join::<Order>(|e| {
                e.column(User::id())?.eq(e.column(Order::user_id())?)
            })?
            .project_scalars((User::name(), Order::amount()))?
            .asc_by(Order::id())
    })?;

let ids = database.bind(|ctx| {
    ctx.union(
        true,
        |ctx| ctx.from::<User>()?.project_scalar(User::id()),
        |ctx| ctx.from::<Order>()?.project_scalar(Order::user_id()),
    )
})?;
# let _ = (joined, ids);
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Writes

For model rows, use the direct helpers:

```rust,ignore
database.insert(&user)?;
database.insert_many(users)?;
let user = database.get::<User>(&1)?;
let all = database.fetch::<User>()?;
# let _ = (user, all);
# Ok::<(), Box<dyn std::error::Error>>(())
```

For query-shaped writes, start with `ctx.mutate::<M>()` and finish with
`update` or `delete`.

```rust,ignore
database
    .bind(|ctx| {
        ctx.mutate::<User>()?
            .filter(|e| e.column(User::id())?.eq(1))?
            .update(|u| {
                u.set_value(User::name(), "Bob")?;
                u.set_value(User::age(), None::<i32>)
            })
    })?
    .done()?;

database
    .bind(|ctx| {
        ctx.mutate::<User>()?
            .filter(|e| e.column(User::id())?.eq(2))?
            .delete()
    })?
    .done()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`insert_select` and `overwrite_select` accept the same closure style for the
source plan:

```rust,ignore
database
    .bind(|ctx| {
        ctx.insert_select::<UserSnapshot, _, _>(["id", "user_name"], |ctx| {
            ctx.from::<User>()?
                .project_scalars((User::id(), User::name()))
        })
    })?
    .done()?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Schema And Maintenance

Common schema helpers are:

- `create_table::<M>()`
- `create_table_if_not_exists::<M>()`
- `migrate::<M>()`
- `drop_table::<M>()`
- `drop_table_if_exists::<M>()`
- `truncate::<M>()`
- `create_view(...)` / `create_or_replace_view(...)`
- `drop_view(...)` / `drop_view_if_exists(...)`

Introspection helpers include `show_tables()`, `show_views()`, `describe::<M>()`,
and `analyze::<M>()`.

The ORM frontend does not build SQL AST nodes. SQL parsing is the SQL frontend;
ORM queries bind directly into `ScalarExpression` and `LogicalPlan`.
