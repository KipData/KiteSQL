// Copyright 2024 KipData/KiteSQL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Structured SQL fuzzing over a fixed schema.
//!
//! The input is not SQL text: it drives the choices (rows, tables, columns,
//! expression shapes, literals) of a generator that only emits well-typed
//! queries, so nearly every query binds and reaches the optimizer and executor.
//!
//! Sizes are bounded (three tables of at most `MAX_ROWS` rows, at most 3-way
//! joins, at most one uncorrelated subquery per query, no recursive CTEs or
//! `numbers()`), so unlike `sql_exec` any panic, sanitizer report, timeout or
//! OOM is a bug. `Err` results are expected and ignored.
//!
//! The WHERE predicate is kept apart from the rest of the query so a later
//! oracle (e.g. TLP) can re-render the same query with derived predicates.
//!
//! Set `KITESQL_FUZZ_DEBUG=1` to print every statement before it runs, e.g.
//! when reproducing an artifact.

#![no_main]

use kite_sql::db::{DataBaseBuilder, Database};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::lmdb::LmdbStorage;
use kite_sql::types::value::DataValue;
use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use std::collections::HashMap;
use std::fmt::{Display, Write};
use std::sync::OnceLock;

const SCHEMA: &[&str] = &[
    "create table t0(id int primary key, a int, b bigint, c varchar, d double)",
    "create table t1(id int primary key, a int, b bigint, c varchar, d double)",
    "create table t2(id int primary key, a int, b bigint, c varchar, d double)",
    "create index t0_a on t0(a)",
    "create unique index t1_b on t1(b)",
    "create index t1_ac on t1(a, c)",
];
const TABLES: &[&str] = &["t0", "t1", "t2"];
const MAX_ROWS: usize = 8;
const MAX_QUERIES: usize = 8;
const MAX_DEPTH: u32 = 3;

#[derive(Clone, Copy, PartialEq)]
enum Ty {
    Num,
    Str,
}

// Every table has the same columns; `id` is the primary key.
const COLUMNS: &[(&str, Ty)] = &[
    ("id", Ty::Num),
    ("a", Ty::Num),
    ("b", Ty::Num),
    ("c", Ty::Str),
    ("d", Ty::Num),
];

// Small domains so predicates, joins and GROUP BY actually match rows,
// plus NULL and type boundaries.
const INT_VALUES: &[&str] = &[
    "null",
    "0",
    "1",
    "2",
    "3",
    "-1",
    "2147483647",
    "-2147483648",
];
const BIGINT_VALUES: &[&str] = &[
    "null",
    "0",
    "1",
    "2",
    "-1",
    "9223372036854775807",
    "-9223372036854775808",
];
const DOUBLE_VALUES: &[&str] = &["null", "0.0", "1.5", "-2.5", "1e300", "-0.0"];
const STR_VALUES: &[&str] = &["null", "''", "'a'", "'b'", "'ab'", "'A'", "'%'", "'a b'"];
const NUM_LITERALS: &[&str] = &["null", "0", "1", "2", "-1", "3", "2147483647", "1.5"];
const INT_LITERALS: &[&str] = &["null", "0", "1", "2", "-1", "3", "2147483647"];
const LIKE_PATTERNS: &[&str] = &["'a%'", "'%b'", "'_'", "'%'", "''", "'a_'"];

const CMP_OPS: &[&str] = &["=", "<>", "<", "<=", ">", ">="];
const ARITH_OPS: &[&str] = &["+", "-", "*", "/", "%"];
// `/` always yields a double.
const INT_ARITH_OPS: &[&str] = &["+", "-", "*", "%"];
const JOINS: &[&str] = &[
    "inner join",
    "left join",
    "right join",
    "full join",
    "cross join",
];
const AGGS: &[&str] = &["count", "sum", "min", "max", "avg"];

/// An aggregate call, kept structured so the TLP oracle can point its argument
/// at a column of the partitioned derived table.
struct Agg {
    func: &'static str,
    distinct: bool,
    // `None` is `count(*)`.
    arg: Option<String>,
}

impl Agg {
    fn render(&self, arg: Option<&str>) -> String {
        match arg {
            None => format!("{}(*)", self.func),
            Some(arg) if self.distinct => format!("{}(distinct {arg})", self.func),
            Some(arg) => format!("{}({arg})", self.func),
        }
    }
}

enum Body {
    Plain {
        distinct: bool,
        items: Vec<String>,
    },
    Grouped {
        keys: Vec<String>,
        aggs: Vec<Agg>,
        // `having <agg> <op> <literal>`
        having: Option<(Agg, &'static str, &'static str)>,
    },
}

/// A generated query. The WHERE predicate is kept apart so the TLP oracle can
/// re-render the query with derived predicates.
struct Select {
    from: String,
    predicate: Option<String>,
    body: Body,
    // (output column, descending)
    order_by: Option<(usize, bool)>,
    // (limit, offset)
    limit: Option<(usize, Option<usize>)>,
}

impl Select {
    /// The query as generated, optionally without its WHERE clause and/or its
    /// LIMIT / OFFSET.
    fn render(&self, with_predicate: bool, with_limit: bool) -> String {
        let mut sql = String::from("select ");
        let mut tail = String::new();
        match &self.body {
            Body::Plain { distinct, items } => {
                if *distinct {
                    sql.push_str("distinct ");
                }
                aliased(&mut sql, items.iter());
            }
            Body::Grouped { keys, aggs, having } => {
                let outputs = keys
                    .iter()
                    .cloned()
                    .chain(aggs.iter().map(|agg| agg.render(agg.arg.as_deref())));
                aliased(&mut sql, outputs);
                if !keys.is_empty() {
                    let _ = write!(tail, " group by {}", keys.join(", "));
                }
                if let Some((agg, op, literal)) = having {
                    let _ = write!(
                        tail,
                        " having {} {op} {literal}",
                        agg.render(agg.arg.as_deref())
                    );
                }
            }
        }
        let _ = write!(sql, " from {}", self.from);
        if let (true, Some(predicate)) = (with_predicate, &self.predicate) {
            let _ = write!(sql, " where {predicate}");
        }
        sql.push_str(&tail);
        sql.push_str(&self.order_limit(with_limit));
        sql
    }

    /// ORDER BY / LIMIT over the output columns `c0, c1, ...`.
    fn order_limit(&self, with_limit: bool) -> String {
        let mut sql = String::new();
        if let Some((column, desc)) = self.order_by {
            let _ = write!(
                sql,
                " order by c{column}{}",
                if desc { " desc" } else { "" }
            );
        }
        if let (true, Some((limit, offset))) = (with_limit, self.limit) {
            let _ = write!(sql, " limit {limit}");
            if let Some(offset) = offset {
                let _ = write!(sql, " offset {offset}");
            }
        }
        sql
    }
    /// TLP form: rows are split by `p`, `not p` and `p is null` (exactly one
    /// holds per row), recombined with UNION ALL in a derived table, and the
    /// rest of the query (DISTINCT, GROUP BY, aggregates, HAVING, ORDER BY,
    /// LIMIT) is applied on top. It must return what the query without the
    /// WHERE clause returns.
    fn render_tlp(&self, predicate: &str, with_limit: bool) -> String {
        // Columns each partition exposes, and the outer select over them.
        let mut inner = Vec::new();
        let mut outer = String::from("select ");
        let mut tail = String::new();
        match &self.body {
            Body::Plain { distinct, items } => {
                if *distinct {
                    outer.push_str("distinct ");
                }
                for (i, item) in items.iter().enumerate() {
                    inner.push(format!("{item} as x{i}"));
                }
                aliased(&mut outer, (0..items.len()).map(|i| format!("u.x{i}")));
            }
            Body::Grouped { keys, aggs, having } => {
                let mut outputs = Vec::new();
                for (i, key) in keys.iter().enumerate() {
                    inner.push(format!("{key} as x{i}"));
                    outputs.push(format!("u.x{i}"));
                }
                let key_refs = outputs.clone();
                // Moves an aggregate's argument into the partitions and
                // aggregates the matching derived-table column instead.
                let mut retarget = |agg: &Agg| match &agg.arg {
                    None => agg.render(None),
                    Some(arg) => {
                        let column = format!("x{}", inner.len());
                        inner.push(format!("{arg} as {column}"));
                        agg.render(Some(&format!("u.{column}")))
                    }
                };
                for agg in aggs {
                    outputs.push(retarget(agg));
                }
                if !key_refs.is_empty() {
                    let _ = write!(tail, " group by {}", key_refs.join(", "));
                }
                if let Some((agg, op, literal)) = having {
                    let _ = write!(tail, " having {} {op} {literal}", retarget(agg));
                }
                aliased(&mut outer, outputs.iter());
            }
        }
        // e.g. `select count(*) ...`: the partitions still need a column.
        if inner.is_empty() {
            inner.push("1 as x0".to_string());
        }
        let columns = inner.join(", ");
        let parts = [
            predicate.to_string(),
            format!("not ({predicate})"),
            format!("({predicate}) is null"),
        ]
        .map(|p| format!("select {columns} from {} where {p}", self.from))
        .join(" union all ");
        format!(
            "{outer} from ({parts}) as u{tail}{}",
            self.order_limit(with_limit)
        )
    }
}

/// `e0 as c0, e1 as c1, ...`
fn aliased(sql: &mut String, exprs: impl Iterator<Item = impl Display>) {
    for (i, expr) in exprs.enumerate() {
        let sep = if i == 0 { "" } else { ", " };
        let _ = write!(sql, "{sep}{expr} as c{i}");
    }
}

struct Gen<'a, 'b> {
    u: &'b mut Unstructured<'a>,
    // Remaining subqueries for the current query (KiteSQL rejects some mixes).
    subqueries: u32,
    // KiteSQL only accepts EXISTS / IN subqueries in WHERE.
    in_where: bool,
    // Numeric expressions stay integral (no `d`, float literals or `/`).
    ints_only: bool,
    next_alias: u32,
}

impl<'a, 'b> Gen<'a, 'b> {
    fn pick<'x, T: Copy + 'x>(
        &mut self,
        items: impl IntoIterator<Item = &'x T, IntoIter: Clone>,
    ) -> arbitrary::Result<T> {
        let mut items = items.into_iter();
        let i = self.below(items.clone().count())?;
        Ok(*items.nth(i).expect("index below the item count"))
    }

    fn below(&mut self, n: usize) -> arbitrary::Result<usize> {
        // Exhausted input yields 0, which is always the simplest choice.
        self.u.choose_index(n)
    }

    fn alias(&mut self) -> String {
        self.next_alias += 1;
        format!("q{}", self.next_alias)
    }

    fn flag(&mut self, one_in: usize) -> arbitrary::Result<bool> {
        // Exhausted input yields false (`Unstructured::ratio` would yield true).
        Ok(self.below(one_in)? == 1)
    }

    fn column(&mut self, scope: &[String], ty: Ty) -> arbitrary::Result<String> {
        let alias = &scope[self.below(scope.len())?];
        let ints_only = self.ints_only;
        let name = self.pick(
            COLUMNS
                .iter()
                .filter(|(name, t)| *t == ty && !(ints_only && *name == "d"))
                .map(|(name, _)| name),
        )?;
        Ok(format!("{alias}.{name}"))
    }

    /// Runs `f` with subqueries disabled (e.g. inside aggregates or ON).
    fn without_subqueries<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> arbitrary::Result<T>,
    ) -> arbitrary::Result<T> {
        let saved = std::mem::replace(&mut self.subqueries, 0);
        let result = f(self);
        self.subqueries = saved;
        result
    }

    fn num_expr(&mut self, scope: &[String], depth: u32) -> arbitrary::Result<String> {
        let choices = if depth == 0 { 2 } else { 8 };
        let next = depth.saturating_sub(1);
        Ok(match self.below(choices)? {
            0 => self.column(scope, Ty::Num)?,
            1 => {
                let literals = if self.ints_only {
                    INT_LITERALS
                } else {
                    NUM_LITERALS
                };
                self.pick(literals)?.to_string()
            }
            2 => {
                let l = self.num_expr(scope, next)?;
                let ops = if self.ints_only {
                    INT_ARITH_OPS
                } else {
                    ARITH_OPS
                };
                let op = self.pick(ops)?;
                let r = self.num_expr(scope, next)?;
                format!("({l} {op} {r})")
            }
            // The space matters: `--1` would start a comment.
            3 => format!("(- {})", self.num_expr(scope, next)?),
            4 => {
                let c = self.bool_expr(scope, next)?;
                let t = self.num_expr(scope, next)?;
                let e = self.num_expr(scope, next)?;
                format!("(case when {c} then {t} else {e} end)")
            }
            5 => {
                let f = self.pick(&["coalesce", "nullif"])?;
                let l = self.num_expr(scope, next)?;
                let r = self.num_expr(scope, next)?;
                format!("{f}({l}, {r})")
            }
            6 => format!("char_length({})", self.str_expr(scope, next)?),
            _ => match self.scalar_subquery()? {
                Some(subquery) => subquery,
                None => self.column(scope, Ty::Num)?,
            },
        })
    }

    fn str_expr(&mut self, scope: &[String], depth: u32) -> arbitrary::Result<String> {
        let choices = if depth == 0 { 2 } else { 5 };
        let next = depth.saturating_sub(1);
        Ok(match self.below(choices)? {
            0 => self.column(scope, Ty::Str)?,
            1 => self.pick(STR_VALUES)?.to_string(),
            2 => {
                let f = self.pick(&["lower", "upper"])?;
                format!("{f}({})", self.str_expr(scope, next)?)
            }
            3 => {
                let c = self.bool_expr(scope, next)?;
                let t = self.str_expr(scope, next)?;
                let e = self.str_expr(scope, next)?;
                format!("(case when {c} then {t} else {e} end)")
            }
            _ => {
                let l = self.str_expr(scope, next)?;
                let r = self.str_expr(scope, next)?;
                format!("coalesce({l}, {r})")
            }
        })
    }

    fn bool_expr(&mut self, scope: &[String], depth: u32) -> arbitrary::Result<String> {
        let choices = if depth == 0 { 3 } else { 10 };
        let next = depth.saturating_sub(1);
        Ok(match self.below(choices)? {
            0 => {
                let l = self.num_expr(scope, next)?;
                let op = self.pick(CMP_OPS)?;
                let r = self.num_expr(scope, next)?;
                format!("({l} {op} {r})")
            }
            1 => {
                let l = self.str_expr(scope, next)?;
                let op = self.pick(CMP_OPS)?;
                let r = self.str_expr(scope, next)?;
                format!("({l} {op} {r})")
            }
            2 => {
                let ty = if self.flag(2)? { Ty::Str } else { Ty::Num };
                let not = if self.flag(2)? { " not" } else { "" };
                format!("({} is{not} null)", self.column(scope, ty)?)
            }
            3 => format!("(not {})", self.bool_expr(scope, next)?),
            4 | 5 => {
                let l = self.bool_expr(scope, next)?;
                let op = self.pick(&["and", "or"])?;
                let r = self.bool_expr(scope, next)?;
                format!("({l} {op} {r})")
            }
            6 => {
                let e = self.num_expr(scope, next)?;
                let not = if self.flag(2)? { " not" } else { "" };
                let lo = self.pick(NUM_LITERALS)?;
                let hi = self.pick(NUM_LITERALS)?;
                format!("({e}{not} between {lo} and {hi})")
            }
            7 => {
                let e = self.num_expr(scope, next)?;
                let not = if self.flag(2)? { " not" } else { "" };
                let len = self.below(3)? + 1;
                let mut list = Vec::with_capacity(len);
                for _ in 0..len {
                    list.push(self.pick(NUM_LITERALS)?);
                }
                format!("({e}{not} in ({}))", list.join(", "))
            }
            8 => {
                let e = self.str_expr(scope, next)?;
                let not = if self.flag(2)? { " not" } else { "" };
                format!("({e}{not} like {})", self.pick(LIKE_PATTERNS)?)
            }
            _ => match self.subquery_predicate(scope)? {
                Some(predicate) => predicate,
                None => format!("({} is not null)", self.column(scope, Ty::Num)?),
            },
        })
    }

    /// Consumes one unit of the subquery budget; `false` if none is left.
    fn take_subquery(&mut self) -> bool {
        if self.subqueries == 0 {
            return false;
        }
        self.subqueries -= 1;
        true
    }

    fn single_table(&mut self) -> arbitrary::Result<(String, String)> {
        let table = self.pick(TABLES)?;
        let alias = self.alias();
        Ok((format!("{table} as {alias}"), alias))
    }

    /// Uncorrelated `(select agg(expr) from t where p)`.
    fn scalar_subquery(&mut self) -> arbitrary::Result<Option<String>> {
        if !self.take_subquery() {
            return Ok(None);
        }
        let (from, alias) = self.single_table()?;
        let scope = [alias];
        let agg = self.pick(&["count", "sum", "min", "max"])?;
        let arg = self.num_expr(&scope, 1)?;
        let mut sql = format!("(select {agg}({arg}) from {from}");
        if self.flag(2)? {
            let _ = write!(sql, " where {}", self.bool_expr(&scope, 1)?);
        }
        sql.push(')');
        Ok(Some(sql))
    }

    /// Uncorrelated `[not] exists (...)` or `expr [not] in (select ...)`.
    fn subquery_predicate(&mut self, outer: &[String]) -> arbitrary::Result<Option<String>> {
        if !self.in_where || !self.take_subquery() {
            return Ok(None);
        }
        let (from, alias) = self.single_table()?;
        let scope = [alias];
        let predicate = self.bool_expr(&scope, 1)?;
        let not = if self.flag(2)? { "not " } else { "" };
        Ok(Some(if self.flag(2)? {
            format!("({not}exists (select 1 from {from} where {predicate}))")
        } else {
            let e = self.num_expr(outer, 1)?;
            let column = self.column(&scope, Ty::Num)?;
            format!("({e} {not}in (select {column} from {from} where {predicate}))")
        }))
    }

    /// `t as q1 [join t as q2 on ...] [join ...]`, at most 3 tables.
    fn from(&mut self) -> arbitrary::Result<(String, Vec<String>)> {
        let (mut from, alias) = self.single_table()?;
        let mut scope = vec![alias];
        for _ in 0..self.below(3)? {
            let join = self.pick(JOINS)?;
            let (table, alias) = self.single_table()?;
            let left = scope[self.below(scope.len())?].clone();
            scope.push(alias.clone());
            let _ = write!(from, " {join} {table}");
            if join != "cross join" {
                // Bias towards equi-joins so hash joins are exercised.
                let on = if self.flag(3)? {
                    self.without_subqueries(|g| g.bool_expr(&scope, 1))?
                } else {
                    let l = self.pick(&["id", "a", "b"])?;
                    let r = self.pick(&["id", "a", "b"])?;
                    format!("{left}.{l} = {alias}.{r}")
                };
                let _ = write!(from, " on {on}");
            }
        }
        Ok((from, scope))
    }

    fn aggregate(&mut self, scope: &[String]) -> arbitrary::Result<Agg> {
        Ok(match self.below(3)? {
            0 => Agg {
                func: "count",
                distinct: false,
                arg: None,
            },
            1 => Agg {
                func: "count",
                distinct: true,
                arg: Some(self.column(scope, Ty::Num)?),
            },
            _ => {
                let func = self.pick(AGGS)?;
                // Integral arguments keep `sum` / `avg` exact whatever the row
                // order, which differs between a query and its TLP form.
                let saved = std::mem::replace(&mut self.ints_only, matches!(func, "sum" | "avg"));
                let arg = self.without_subqueries(|g| g.num_expr(scope, 1));
                self.ints_only = saved;
                Agg {
                    func,
                    distinct: false,
                    arg: Some(arg?),
                }
            }
        })
    }

    fn select(&mut self) -> arbitrary::Result<Select> {
        self.subqueries = 1;
        let (from, scope) = self.from()?;
        // Exhausted input yields no predicate.
        let predicate = match self.below(4)? {
            0 => None,
            _ => {
                self.in_where = true;
                let predicate = self.bool_expr(&scope, MAX_DEPTH);
                self.in_where = false;
                Some(predicate?)
            }
        };

        let body = if self.flag(3)? {
            let mut keys = Vec::new();
            for _ in 0..self.below(3)? {
                let ty = if self.flag(3)? { Ty::Str } else { Ty::Num };
                keys.push(self.column(&scope, ty)?);
            }
            let mut aggs = Vec::new();
            for _ in 0..=self.below(2)? {
                aggs.push(self.aggregate(&scope)?);
            }
            let having = if self.flag(3)? {
                let agg = self.aggregate(&scope)?;
                Some((agg, self.pick(CMP_OPS)?, self.pick(NUM_LITERALS)?))
            } else {
                None
            };
            Body::Grouped { keys, aggs, having }
        } else {
            let distinct = self.flag(4)?;
            let mut items = Vec::new();
            for _ in 0..=self.below(3)? {
                items.push(match self.below(3)? {
                    0 => self.num_expr(&scope, MAX_DEPTH - 1)?,
                    1 => self.str_expr(&scope, MAX_DEPTH - 1)?,
                    _ => self.bool_expr(&scope, MAX_DEPTH - 1)?,
                });
            }
            Body::Plain { distinct, items }
        };
        let outputs = match &body {
            Body::Plain { items, .. } => items.len(),
            Body::Grouped { keys, aggs, .. } => keys.len() + aggs.len(),
        };
        let order_by = if self.flag(2)? {
            Some((self.below(outputs)?, self.flag(2)?))
        } else {
            None
        };
        let limit = if self.flag(3)? {
            let limit = self.below(5)?;
            Some((
                limit,
                if self.flag(3)? {
                    Some(self.below(5)?)
                } else {
                    None
                },
            ))
        } else {
            None
        };
        Ok(Select {
            from,
            predicate,
            body,
            order_by,
            limit,
        })
    }

    /// One `insert` per row, so a rejected row (e.g. unique violation) only
    /// drops itself.
    fn inserts(&mut self) -> arbitrary::Result<Vec<String>> {
        let mut statements = Vec::new();
        for table in TABLES {
            for id in 0..self.below(MAX_ROWS + 1)? {
                let a = self.pick(INT_VALUES)?;
                let b = self.pick(BIGINT_VALUES)?;
                let c = self.pick(STR_VALUES)?;
                let d = self.pick(DOUBLE_VALUES)?;
                statements.push(format!(
                    "insert into {table} values ({id}, {a}, {b}, {c}, {d})"
                ));
            }
        }
        Ok(statements)
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let mut g = Gen {
        u: &mut u,
        subqueries: 0,
        in_where: false,
        ints_only: false,
        next_alias: 0,
    };
    let Ok(inserts) = g.inserts() else {
        return;
    };
    let mut selects = Vec::new();
    for _ in 0..g.below(MAX_QUERIES).unwrap_or(0) + 1 {
        match g.select() {
            Ok(select) => selects.push(select),
            Err(_) => break,
        }
    }

    DB.with(|db| {
        for table in TABLES {
            run(db, &format!("delete from {table}")).expect("reset fuzz table");
        }
        for sql in &inserts {
            let _ = run(db, sql);
        }
        for select in &selects {
            let _ = run(db, &select.render(true, true));
            if let Some(predicate) = &select.predicate {
                check_tlp(db, select, predicate);
            }
        }
    });
});

thread_local! {
    // One database per process, reused by every input (tables are emptied
    // before each input).
    static DB: Database<LmdbStorage> = open_db();
}

fn open_db() -> Database<LmdbStorage> {
    let path = std::env::temp_dir().join(format!("kitesql-fuzz-sql_gen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let mut db = DataBaseBuilder::path(path)
        .lmdb_no_sync(true)
        .build_lmdb()
        .expect("lmdb database");
    for ddl in SCHEMA {
        db.ddl(ddl).expect("fixed fuzz schema");
    }
    db
}

/// TLP oracle (ternary logic partitioning), see [`Select::render_tlp`]: the
/// query without its WHERE clause and its TLP form must return the same rows,
/// in the same ORDER BY key order, and LIMIT must pick consistent rows.
/// Skipped when either side errors, e.g. on overflow while evaluating the
/// predicate, which the query without WHERE never does.
///
/// Only the base result is kept; the other queries are checked against it as
/// they are read.
fn check_tlp(db: &Database<LmdbStorage>, select: &Select, predicate: &str) {
    let base_sql = select.render(false, false);
    let mut base = Vec::new();
    if for_each_row(db, &base_sql, |row| base.push(row.to_vec())).is_err() {
        return;
    }
    let order_by = select.order_by.map(|(column, _)| column);
    let tlp_sql = select.render_tlp(predicate, false);
    if let Ok(false) = rows_match(db, &tlp_sql, &base, order_by, 0, base.len()) {
        mismatch("rows or ORDER BY keys", &[&base_sql, &tlp_sql]);
    }

    let Some((limit, offset)) = select.limit else {
        return;
    };
    // Rows within ORDER BY ties (or without ORDER BY) may be picked in any
    // order, so check what is determined: the count, that the rows come from
    // the full result, and the ORDER BY key slice.
    let offset = offset.unwrap_or(0).min(base.len());
    let expected = limit.min(base.len() - offset);
    for sql in [
        select.render(false, true),
        select.render_tlp(predicate, true),
    ] {
        if let Ok(false) = rows_match(db, &sql, &base, order_by, offset, expected) {
            mismatch("LIMIT", &[&base_sql, &sql]);
        }
    }
}

/// Whether `sql` returns `len` rows taken from `base` (as a multiset) whose
/// ORDER BY keys equal those of `base[offset..]`, in order.
fn rows_match(
    db: &Database<LmdbStorage>,
    sql: &str,
    base: &[Vec<DataValue>],
    order_by: Option<usize>,
    offset: usize,
    len: usize,
) -> Result<bool, DatabaseError> {
    let mut remaining: HashMap<&[DataValue], usize> = HashMap::new();
    for row in base {
        *remaining.entry(row.as_slice()).or_default() += 1;
    }
    let mut matched = true;
    let mut i = offset;
    let rows = for_each_row(db, sql, |row| {
        let in_base = match remaining.get_mut(row) {
            Some(count) if *count > 0 => {
                *count -= 1;
                true
            }
            _ => false,
        };
        let key_ok =
            order_by.is_none_or(|column| base.get(i).is_some_and(|b| b[column] == row[column]));
        matched &= in_base && key_ok;
        i += 1;
    })?;
    Ok(matched && rows == len)
}

fn mismatch(what: &str, sqls: &[&str]) -> ! {
    panic!("TLP mismatch: {what}\n  {};", sqls.join(";\n  "))
}

/// Runs `sql` to completion.
fn run(db: &Database<LmdbStorage>, sql: &str) -> Result<usize, DatabaseError> {
    for_each_row(db, sql, |_| {})
}

/// Runs `sql` to completion, passing every row to `f`; returns the row count.
fn for_each_row(
    db: &Database<LmdbStorage>,
    sql: &str,
    mut f: impl FnMut(&[DataValue]),
) -> Result<usize, DatabaseError> {
    // Printed before running so a crashing statement is still visible.
    if debug() {
        eprintln!("{sql};");
    }
    let mut iter = db.run(sql)?;
    let mut rows = 0;
    while iter.next_tuple(|_, tuple| f(&tuple.values))?.is_some() {
        rows += 1;
    }
    iter.done()?;
    Ok(rows)
}

fn debug() -> bool {
    static DEBUG: OnceLock<bool> = OnceLock::new();
    *DEBUG.get_or_init(|| std::env::var_os("KITESQL_FUZZ_DEBUG").is_some())
}
