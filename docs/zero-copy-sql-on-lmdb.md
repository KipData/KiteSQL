# Zero-Copy SQL Computation on LMDB

English | [中文](zero-copy-sql-on-lmdb.zh-CN.md)

> By the time a database hands a row to your application, its strings have usually been copied several times: from the file into a cache, from the cache into the executor, and from the executor into your struct. This article explores another option. If the storage layer is an mmap-based KV engine such as LMDB, the pages a read-only transaction reads keep their addresses until the transaction ends. Can a SQL engine then let query results borrow those pages all the way through, so that the whole SQL computation runs without copying?

## Summary

SQLite is the most common in-process SQL database, with stable bindings in most languages. This example uses Rust's rusqlite. A very common task: fetch all comments under a thread, map them to structs, assemble them into a reply tree by `parent_id`, and serialize the tree as nested JSON for the caller:

```rust
use rusqlite::{params, Connection};
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;

#[derive(Serialize)]
struct Post {
    id: i64,
    thread_id: i64,
    parent_id: i64, // 0 means a top-level comment
    user: String,
    body: String,
}

#[derive(Serialize)]
struct Comment<'a> {
    #[serde(flatten)]
    post: &'a Post,
    replies: Vec<Comment<'a>>,
}

fn build<'a>(children: &'a HashMap<i64, Vec<Post>>, parent: i64) -> Vec<Comment<'a>> {
    let Some(posts) = children.get(&parent) else { return Vec::new() };
    posts.iter()
        .map(|post| Comment { post, replies: build(children, post.id) })
        .collect()
}

fn write_thread(conn: &Connection, thread_id: i64, out: impl Write) -> Result<(), Box<dyn Error>> {
    let mut stmt = conn.prepare_cached("select id, thread_id, parent_id, user, body from posts where thread_id = ?1")?;
    let mut rows = stmt.query(params![thread_id])?;
    // Building the tree needs every row, but a row only lives until the next call to next, so Post must own its Strings
    let mut children: HashMap<i64, Vec<Post>> = HashMap::new();
    while let Some(row) = rows.next()? {
        let post = Post {
            id: row.get(0)?,
            thread_id: row.get(1)?,
            parent_id: row.get(2)?,
            user: row.get(3)?,
            body: row.get(4)?,
        };
        children.entry(post.parent_id).or_default().push(post);
    }
    serde_json::to_writer(out, &build(&children, 0))?;
    Ok(())
}
```

For this utterly ordinary "fetch rows, map them to structs" code, the data goes through these steps before it becomes the `Post` in your hands:

```text
 Database file
     │
     │  Default: read() copies whole pages into SQLite's page cache   [copy]
     │  - With mmap_size: read the mapped file pages directly
     ▼
 B-tree page (page cache or file mapping)
     │
     │  sqlite3_step → OP_Column decodes user / body from the record
     │    - Within a page: copy into a register buffer                  [copy]
     │    - Across overflow pages: malloc a buffer, join page by page   [alloc][copy]
     │  With ORDER BY / GROUP BY: write into the sorter and read back   [copy]
     ▼
 Register buffer
     │
     │  sqlite3_column_text → rusqlite row.get()
     │  The pointer only lives until the next call to next, so Post must own a copy
     │  user / body each become a String                                [alloc][copy] × 2
     ▼
 Post { user: String, body: String, .. }
     │
     │  Build the tree by parent_id and write it out recursively
     │  serde_json::to_writer writes the JSON
     ▼
 JSON
```

Along this path, both SQLite and its API contract require strings to be moved somewhere else. The more fields, the longer the strings and the more complex the query, the more copies and allocations happen along the way. This is in fact the standard design of most databases: manage your own buffer pool, control memory in user space, and don't let the operating system decide which pages stay in memory. The cost is that a page in the pool can be evicted at any moment, so data must be copied before it is handed out. If, as DBOS [1] envisions, the database moved into the operating system kernel and managed the OS state itself, the user-space buffer pool and the kernel page cache might no longer need to be two separate layers.

Yet in typical web applications, the main work of many queries is just shaping and passing data along: take data out of SQL, convert it into the corresponding structs (POJOs in Java) with some light processing, serialize it to JSON, and return it over HTTP. GraphQL [2] turned this into an interface specification: the caller describes the fields and nesting it wants, and the server takes the columns out of the database in that shape, assembles them and serializes them, doing almost nothing else. This kind of nearly pure read-and-reshape work makes up a growing share of web backends.

Here, every `String` field of `Post` costs one allocation, and more fields mean more allocations. Look back at the flow above and you'll see each string is copied twice. The first copy happens when SQLite's `step` reads the column and copies it from the page into a register buffer; even with mmap enabled, this copy can't be avoided. The second happens when mapping to `Post`, copying from the register buffer into each field. But `row.get_ref` can lend out a `&str` directly. Why not let `Post`'s fields borrow it and skip the second copy? Unfortunately, `row` only lives until the next call to `next`, so a `Post` borrowing from it can't outlive the row, and the compiler rejects it outright. Building the tree needs all rows at once, and neither SQL sorting nor JSON functions can produce nesting of arbitrary depth, so these rows must all be held at the same time. This copy comes from SQLite's API contract, and code on top of it can hardly avoid it.

## Zero-Copy Computation from Storage to Results

### MMAP Storage

Andy Pavlo once said that MMAP will ruin your database and your life [3]. That was one of the rejected title candidates for his CIDR 2022 paper with Crotty and Leis, finally titled *Are You Sure You Want to Use MMAP in Your Database Management System?*. The paper lists four classes of problems with using mmap instead of a buffer pool in a database: transactional safety, I/O stalls, error handling and performance. As the title suggests, this work does not call mmap directly to build its own storage. It uses LMDB as the storage backend instead. LMDB is a mature embedded KV storage engine whose design avoids the most serious problem in the paper, transactional safety [4]:

- **Read-only mapping by default.** The file is mapped read-only, so the process cannot modify it through pointers. Stray pointer writes in the host program can't corrupt the database, and the mapping holds no dirty pages the kernel could write back on its own. Trying to modify a value returned by a read-only transaction triggers SIGSEGV immediately.
- **Writes bypass the mapping.** A write transaction modifies its pages in heap memory first, writes them back to the file with `pwrite` at commit, then calls `fsync` as configured. LMDB alone decides when and in what order data reaches disk.
- **Copy-on-write B+ tree (shadow paging).** Committed pages are never modified in place. Changing a leaf writes a new page and copies every page on the path up to the root.
- **Two alternating meta pages.** The last step of a commit writes a meta page that records the new root. The two copies are written alternately, so after a crash one of them is always complete, and the database never stops in a half-written state.
- **A reader table protects snapshots.** Every read-only transaction registers the version it sees, and old pages it might read are never reclaimed for reuse. So a value read by a read-only transaction stays valid at the same address until the transaction ends. This is exactly the guarantee that **zero-copy SQL computation** relies on.
- **A single writer.** Only one write transaction runs at a time, and the paper notes that this is how LMDB avoids conflicts between transactions.

LMDB does not solve the paper's other problems, of course: a page fault blocks the thread synchronously, I/O errors turn into SIGBUS, and the kernel decides which pages stay in memory. LMDB assumes the hot data fits in memory, and in the embedded scenarios SQLite targets, that assumption usually holds.

### Data Encode

The first step in building SQL on a KV store is deciding how a row becomes a key and a value. TiDB's approach is representative [5]:

```text
Row:   t{TableID}_r{RowID}                                    →  [col1, col2, col3, ...]
Index: t{TableID}_i{IndexID}_{indexed column values}_{RowID}  →  null
```

Rows of the same table share a prefix and are sorted by primary key, so both full table scans and index lookups become range scans over contiguous keys. The implementation here uses the same structure: the key is "table ID + type tag + primary key", and the value holds the row's columns.

For zero copy, what matters is how strings are stored in the value. Take the `orders` table used in the benchmarks below. After inserting the row `(1, 7, 'paid', 42, 'hello')`, its value looks like this:

```text
  00                               null bitmap
  01 00 00 00                      id = 1
  07 00 00 00                      customer_id = 7
  04 00 00 00 70 61 69 64          status: length 4 + "paid"
  2a 00 00 00                      amount = 42
  05 00 00 00 68 65 6c 6c 6f       note: length 5 + "hello"
  00 00 00 00 00 00 00 00          statement stamp: the id of the statement that wrote this row, used by updates to skip rows the same statement just wrote
```

Strings are stored contiguously as "length + raw bytes". Decoding `note` only needs to read its 4-byte length to get a (pointer, length) pair pointing into an LMDB page, which is exactly a `&str` with no conversion at all. LMDB also guarantees that the overflow pages holding a large value are contiguous, so however long the string is, its bytes form one contiguous block in the mapping and can be lent out directly. Note that all columns of a row form a single LMDB value. LMDB places a value that doesn't fit in a leaf on a contiguous run of overflow pages, so the whole row is contiguous, not just one field: `status` and `note` are simply two offsets into the same bytes.

Putting large values on separate pages follows the same idea as PostgreSQL's TOAST and InnoDB's external pages. Those mechanisms solve row bloat, so queries that read only small columns don't touch large values. They don't solve copying, though: values are still copied when fetched into the application. The truly copy-free approach is something like FILESTREAM, handing the value out as a file handle, but then it is no longer a SQL value. The engine can no longer filter, sort or measure it, and it no longer lines up with the row's transaction snapshot. The difference here is that overflow pages stay in the same mapping and the same read-only transaction, so operators and the caller see the same bytes.

### Execution

Lending strings out of storage is only the first step. A SQL statement passes through a chain of operators, such as scan, filter, sort, group and join, before its results reach the caller, and if any layer copies a string, zero copy breaks there. The approach is a discipline: storage provides a lifetime, operators don't break it, and Rust's borrow checker lets this be enforced at compile time.

**Value representation: either owned or borrowed.** Strings and binary values use the same type (simplified):

```rust
pub struct MaybeMapped<B: ?Sized>(Repr<B>);

enum Repr<B: ?Sized> {
    Owned(B::Owned),     // memory it owns: literals in SQL, new values computed by expressions
    Mapped { ptr, len }, // a (pointer, length) pair pointing into a page read by a read-only transaction
}
```

Both states appear in the same query. In `where note = 'hello'`, for instance, the constant `'hello'` is `Owned` and the column value `note` is `Mapped`.

When a column value is `Mapped` depends on the transaction. A read-only transaction declares at its start that "the bytes it reads stay valid at the same address until it ends", so row decoding produces `Mapped` directly. A write transaction can't promise this, so decoding has to copy and produces `Owned`.

**Materializing operators copy only pointers.** A materializing operator must collect rows from upstream: sort gathers all rows, grouping keeps group keys, hash join builds a hash table, and window functions hold an entire partition. When they keep values, a `Mapped` string copies only its (pointer, length), not its content. The whole query runs in one read-only transaction and those pages never become invalid, so as long as nothing spills, materializing operators can keep values in their in-memory containers as borrows. New strings appear only in a few cases: values computed by expressions (such as `upper(note)`), and values serialized out and read back when the data is large enough to spill. In principle, spilled files could also be read back through mmap and borrowed again, but that is extra work.

**How long borrows last: enforced by the compiler outside, by convention inside.** The risk of borrowing is a pointer outliving its page. This is handled in two layers:

- Results handed to the caller are tied to the transaction's lifetime. The `Post` in the conclusion works this way: it can outlive any single row and be used until the transaction ends, but trying to carry it out of the transaction is rejected by the compiler.
- Borrows inside the executor rely on convention. The same prepared statement can run repeatedly in different transactions, so before each execution, operators must clear the borrowed values left from the previous run. The compiler can't check this layer; a liveness check in debug builds backs it up, panicking on any read of a borrowed value whose transaction has ended.

## Benchmarking SQL Execution: SQLite with mmap vs. LMDB SQL

In the benchmarks below, both sides load identical data, build the same indexes and run the same SQL. Each statement is prepared once, and all executions run in one read-only transaction. SQLite has `PRAGMA mmap_size` enabled (1 GiB, larger than the database file, so the whole file is mapped), so both sides read the database file through a memory mapping, and the I/O conditions are the same.

- **LMDB SQL**: the SQL engine running on LMDB read-only transactions, with strings borrowed from storage all the way to the caller.
- **LMDB SQL (forced copy)**: the same query run in a write transaction, so strings are copied. This is the control group.
- **SQLite**: SQLite 3.45.1 with mmap enabled.

Allocations are counted at libc's `malloc`. The machine is an i9-13900HX, with the program pinned to one P-core (`cpu8`).

The goal is not to prove which side is faster, but to find the string length at which the cost of copying starts to show. So string lengths range from 64 B up to 256 KiB. At 64 B SQLite leads, and 256 KiB is far larger than a typical column in a business table; the tiers in between are the common document, log and JSON fields.

Tables 1 to 3 use the `orders` table below. `note` is loaded at lengths from 64 B to 256 KiB, each tier in its own database, and each query hits about 10 rows by `(customer_id, status)`.

```sql
create table orders (
    id          int primary key,
    customer_id int,
    status      varchar,  -- 'new' / 'paid' / 'shipped' / 'done' / 'void'
    amount      int,
    note        varchar
);
create index orders_customer_status on orders (customer_id, status);
```

**Table 1: Plain index lookup**

Fetch about 10 rows through the index, and the caller reads every string. Reproduce with `examples/alloc_trace.rs` [6].

```sql
select id, status, amount, note from orders
where customer_id = $1 and status = $2 and amount > $3;
```

| note length | LMDB SQL | LMDB SQL (forced copy) | SQLite |
| ---: | ---: | ---: | ---: |
| 64 B | 3.74 / 0.4 | 5.22 / 1.1 | 2.99 / 0 |
| 1 KiB | 3.70 / 0.4 | 5.33 / 10 | 3.44 / 0 |
| 4 KiB | 4.37 / 0.4 | 8.00 / 40 | 6.44 / 40 |
| 16 KiB | 3.65 / 0.4 | 13.07 / 160 | 11.60 / 158 |
| 64 KiB | 2.56 / 0.4 | 18.57 / 640 | 18.05 / 628 |
| 256 KiB | 2.38 / 0.4 | 61.58 / 2560 | 66.14 / 2458 |

Unit: each cell is "p50 latency µs / KiB allocated per execution".

**Table 2: Length functions**

Same predicate and data as Table 1, with only the projection replaced by a length function. The first column fetches `note` and has the caller call `len()` on the string; `char_length` corresponds to `length()` in SQLite.

| note length | `select note`, caller calls `len()` | `octet_length(note)` | `char_length(note)` |
| ---: | ---: | ---: | ---: |
| 64 B | 3.56 / 2.52 | 3.81 / 2.51 | 3.93 / 2.77 |
| 4 KiB | 4.33 / 6.87 | 4.77 / 2.93 | 7.34 / 16.07 |
| 64 KiB | 2.35 / 20.35 | 2.75 / 1.95 | 26.07 / 164.01 |
| 256 KiB | 2.14 / 69.03 | 2.35 / 1.82 | 89.25 / 627.00 |

Unit: each cell is "LMDB SQL / SQLite p50 latency µs".

**Table 3: Allocation and performance across several materializing operators**

Read the same rows as Table 1, but pass the strings through three materializing operators in turn: grouping, a window function and a sort. Reproduce with the `multi-op` variant in `examples/alloc_trace.rs` [6].

```sql
select status, note, peak, max(peak) over (partition by status)
from (select status, note, max(amount) as peak from orders
      where customer_id = $1 and status = $2 and amount > $3
      group by status, note) t
order by note desc;
```

| note length | LMDB SQL | SQLite |
| ---: | ---: | ---: |
| 64 B | 7.91 / 4.2 | 12.74 / 109 |
| 4 KiB | 13.82 / 4.2 | 33.19 / 757 |
| 64 KiB | 84.74 / 4.1 | 328.34 / 10846 |
| 256 KiB | 320.24 / 4.0 | 7479.64 / 32945 |

Unit: each cell is "p50 latency µs / KiB allocated per execution".

**Table 4: Parsing 100 JSON / CSV documents**

Each execution fetches 100 documents by `grp`, keeps the results past the end of the query, then parses them into structs. On LMDB SQL the struct fields borrow the text in the transaction directly; on SQLite they follow the common pattern of one `String` per field. The CSV case uses a `csv_docs` table with the same structure, where `body` holds one CSV line. Reproduce with `examples/borrowed_parse.rs` [7].

```sql
create table json_docs (
    id   int primary key,
    grp  int,      -- 100 documents per group
    body varchar
);
create index json_docs_grp on json_docs (grp);

select id, body from json_docs where grp = $1;
```

| Document size | Case | LMDB SQL | SQLite |
| ---: | --- | ---: | ---: |
| 256 B | Keep raw rows only | 28.34 / 3.5 | 24.64 / 28 |
| 256 B | Parse JSON | 43.27 / 12 | 60.17 / 35 |
| 2 KiB | Parse JSON | 68.80 / 12 | 97.20 / 214 |
| 16 KiB | Keep raw rows only | 28.60 / 3.5 | 137.69 / 3205 |
| 16 KiB | Parse JSON | 187.33 / 12 | 292.65 / 3212 |
| 16 KiB | Parse CSV | 35.15 / 12 | 172.15 / 3206 |

Unit: each cell is "p50 latency µs / KiB allocated per execution", with 100 rows per execution.

### Analysis

**LMDB SQL's allocations don't depend on data size; SQLite's follow the data it returns.** In Table 1, LMDB SQL always allocates 0.4 KiB. From 4 KiB on, SQLite allocates once per row, and the total roughly equals the bytes returned. SQLite shows 0 at 64 B and 1 KiB, but that doesn't mean nothing was copied: short strings are copied into a reused buffer, so no allocation is counted. Checking the returned pointers shows that every LMDB SQL pointer falls inside the mapping of the database file, and none of SQLite's do.

**SQLite is fast on short data, but its latency grows with length; LMDB SQL's latency doesn't depend on length.** At 64 B SQLite needs only 2.99 µs, faster than LMDB SQL, because copying a few dozen bytes costs next to nothing. As `note` grows, SQLite's latency and allocations grow together, reaching 66.14 µs at 256 KiB, where copying becomes the main cost. LMDB SQL stays between 2.4 and 4.4 µs throughout. It doesn't read the string content, so length doesn't affect it.

**The gap comes from copying, not I/O.** When LMDB SQL is forced to copy strings, its latency and allocations land in the same range as SQLite's (61.6 µs vs. 66.1 µs at 256 KiB).

**`octet_length` is constant time on both sides, so it is no argument for zero copy.** LMDB SQL calls `len()` on the borrowed `&str`. For a column used only as the argument of a length function, SQLite reads the length straight from the record header without loading the content or reading overflow pages (`OPFLAG_BYTELENARG` on `OP_Column`), so SQLite is actually faster here.

**`char_length` scans every byte on both sides, and from 64 KiB up LMDB SQL is 6 to 7 times faster.** LMDB SQL counts directly on the mapped pages (`chars().count()`, about 28 GB/s at 256 KiB). SQLite first joins the overflow page content into a `malloc`ed buffer, then counts characters one by one. This is the typical "read all the content, process it lightly" case: the computation is cheap, copying dominates, and zero copy pays off the most. Recording the character count or an "ASCII only" flag at write time would make `char_length` constant time too; this is not implemented yet.

**Every materializing operator widens the gap again.** At 64 KiB and 256 KiB in Table 3, SQLite allocates 13 to 17 times the returned data, because every operator writes records in and reads them back out. LMDB SQL allocates only about 4 KiB for its hash table and sort array. LMDB SQL's latency still grows with `note`, because grouping hashes the whole `note`.

**Parsing itself is equally fast on both sides.** For 16 KiB JSON, after subtracting the fetch time, pure parsing takes about the same on both sides (159 µs vs. 155 µs). Nearly all of LMDB SQL's advantage comes from not copying during the fetch.

## Conclusion

Back to the code example from the beginning. The same functionality can be written like this:

```rust
use kite_sql::db::{Transaction, TxnResultIter};
use kite_sql::storage::lmdb::LmdbStorage;
use kite_sql::Model;
use serde::Serialize;
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;

#[derive(Default, Model, Serialize)]
#[model(table = "posts")]
struct Post<'a> {
    #[model(primary_key)]
    id: i32,
    thread_id: i32,
    parent_id: i32, // 0 means a top-level comment
    user: Cow<'a, str>,
    body: Cow<'a, str>,
}

#[derive(Serialize)]
struct Comment<'a> {
    #[serde(flatten)]
    post: &'a Post<'a>,
    replies: Vec<Comment<'a>>,
}

fn build<'a>(children: &'a HashMap<i32, Vec<Post<'a>>>, parent: i32) -> Vec<Comment<'a>> {
    let Some(posts) = children.get(&parent) else { return Vec::new() };
    posts.iter()
        .map(|post| Comment { post, replies: build(children, post.id) })
        .collect()
}

fn write_thread(tx: &mut Transaction<'_, LmdbStorage>, thread_id: i32, out: impl Write) -> Result<(), Box<dyn Error>> {
    // Post borrows LMDB pages and lives until the transaction ends, so all rows can be collected before building the tree
    let mut children: HashMap<i32, Vec<Post>> = HashMap::new();
    let posts = tx
        .bind(|ctx| {
            ctx.from::<Post>()?
                .filter(|e| e.column(Post::thread_id())?.eq(thread_id))?
                .finish()
        })?
        // Every string field is Cow::Borrowed and points straight into an LMDB page
        .txn_orm::<Post>();
    for post in posts {
        let post = post?;
        children.entry(post.parent_id).or_default().push(post);
    }
    serde_json::to_writer(out, &build(&children, 0))?;
    Ok(())
}
```

Here the same process looks like this:

```text
 Database file (LMDB mapping)
     │
     │  Read-only transaction: pages stay valid at the same address until it ends
     ▼
 B+ tree page / overflow pages (a large value is one contiguous block in the mapping)
     │
     │  Row decoding: read the 4-byte length, get (pointer, length)
     ▼
 Cow::Borrowed(&str), pointing straight into the page
     │
     │  txn_orm: put into the fields of Post
     ▼
 Post { user: Cow::Borrowed, body: Cow::Borrowed, .. } (lives until the transaction ends)
     │
     │  Build the tree by parent_id: only pointers move
     │  serde_json::to_writer writes the JSON
     ▼
 JSON
```

Apart from the types, the two programs are nearly identical: read `Post`s row by row, put them into a `HashMap` by `parent_id`, then build the tree recursively and serialize it. The difference is that this map holds only pointers. `Post`'s string fields are `Cow<'_, str>`, and in a read-only transaction they are all `Cow::Borrowed`, pointing straight into LMDB pages. Their lifetime is tied to the transaction rather than to a row, so the first row's `Post` can wait in the map alongside the last one until the tree is built and the JSON is written, and no string is copied even once. The fields are `Cow` rather than `&str` because pages may change in a write transaction, in which case the fields fall back to `Cow::Owned` copies. It's not that rusqlite can't borrow: `row.get_ref` lends out a `&str`, but only until the next call to `next`. Building the tree needs every row, and a `Post` borrowing from `row` can't survive the next `next`, so only copies can go into the map.

This advantage has its limits. On workloads with short rows processed one at a time, the two sides are about even, with SQLite slightly faster. Strings are still copied in write transactions. Workloads like TPC-C show no change. When an aggregation operator holds borrowed rows, the pages behind them must stay in memory, but they are clean file pages that SQLite would also load into its page cache to read the same data, and the kernel can reclaim them at any time. Heap memory that can't be reclaimed is actually lower on the zero-copy side (Table 3). A read-only transaction has to stay open as long as the borrowed results live, and LMDB can't reclaim old pages while readers reference them, so holding results for a long time under heavy writes makes the database file grow.

Thanks for reading. The implementation and benchmark code are in the KiteSQL repository: <https://github.com/KipData/KiteSQL>.

## References

[1] Athinagoras Skiadopoulos, Qian Li, Peter Kraft, Kostis Kaffes, Daniel Hong, Shana Mathew, David Bestor, Michael Cafarella, Vijay Gadepally, Goetz Graefe, Jeremy Kepner, Christos Kozyrakis, Tim Kraska, Michael Stonebraker, Lalith Suresh, Matei Zaharia. *DBOS: A DBMS-oriented Operating System*. PVLDB, 15(1): 21–30, 2022. doi:[10.14778/3485450.3485454](https://doi.org/10.14778/3485450.3485454). PDF: <https://www.vldb.org/pvldb/vol15/p21-skiadopoulos.pdf>

[2] GraphQL Specification, September 2025 Edition. GraphQL Foundation. <https://spec.graphql.org/September2025/>. Project site: <https://graphql.org/>

[3] Andrew Crotty, Viktor Leis, Andrew Pavlo. *Are You Sure You Want to Use MMAP in Your Database Management System?* CIDR 2022. Project page: <https://db.cs.cmu.edu/mmap-cidr2022/>. PDF: <https://db.cs.cmu.edu/papers/2022/cidr2022-p13-crotty.pdf>. The candidate title is mentioned at 00:52 in Andy Pavlo's lecture recording: <https://www.youtube.com/watch?v=1BRGU_AS25c>.

[4] LMDB API documentation, Symas Corporation: <http://www.lmdb.tech/doc/group__mdb.html>. The `MDB_val` section says returned values are "valid only until a subsequent update operation, or the end of the transaction" and "commonly point into the database itself"; the `MDB_WRITEMAP` section explains that the default read-only mapping protects the database from stray pointer writes.

[5] PingCAP. *TiDB Computing*, section "Mapping table data to Key-Value": <https://docs.pingcap.com/tidb/stable/tidb-computing/>

[6] `examples/alloc_trace.rs`: <https://github.com/KipData/KiteSQL/blob/main/examples/alloc_trace.rs>. Run: `cargo run --release --example alloc_trace --features lmdb,orm`

[7] `examples/borrowed_parse.rs`: <https://github.com/KipData/KiteSQL/blob/main/examples/borrowed_parse.rs>. Run: `cargo run --release --example borrowed_parse --features lmdb,orm`
