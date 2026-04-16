# Transaction Isolation

KiteSQL currently exposes two transaction isolation levels:

- `ReadCommitted`
- `RepeatableRead`

The isolation level is selected through `DataBaseBuilder` and is validated by
the chosen storage backend.

```rust
use kite_sql::db::DataBaseBuilder;
use kite_sql::errors::DatabaseError;
use kite_sql::storage::TransactionIsolationLevel;

fn main() -> Result<(), DatabaseError> {
    let db = DataBaseBuilder::path("./data")
        .transaction_isolation(TransactionIsolationLevel::RepeatableRead)
        .build_rocksdb()?;

    assert_eq!(
        db.transaction_isolation(),
        TransactionIsolationLevel::RepeatableRead
    );
    Ok(())
}
```

## Support Matrix

Current storage support is:

| Storage | Default | Supported Levels |
| --- | --- | --- |
| RocksDB `build_rocksdb()` | `ReadCommitted` | `ReadCommitted`, `RepeatableRead` |
| Optimistic RocksDB `build_optimistic()` | `ReadCommitted` | `ReadCommitted`, `RepeatableRead` |
| LMDB `build_lmdb()` | `RepeatableRead` | `RepeatableRead` only |
| Memory `build_in_memory()` | `ReadCommitted` | `ReadCommitted` only |

If a storage backend does not support the requested level, builder creation
fails with an explicit error.

## Semantics

KiteSQL defines isolation in terms of the read snapshot used by ordinary SQL
statements.

### Read Committed

`ReadCommitted` uses one snapshot per statement.

That means:

- every statement sees only data committed before that statement starts
- a later statement in the same transaction may see changes committed by other
  transactions after the earlier statement completed
- the transaction still sees its own writes because reads go through the
  storage transaction object rather than bypassing it

Example:

1. Transaction `T1` starts.
2. `T1` runs `SELECT ...` and reads snapshot `S1`.
3. Transaction `T2` commits an update.
4. `T1` runs another `SELECT ...` and reads a new snapshot `S2`.
5. The second statement may see `T2`'s committed update.

### Repeatable Read

`RepeatableRead` uses one fixed snapshot per transaction.

That means:

- the first read view chosen for the transaction is reused by all statements
- re-running the same query inside the same transaction returns the same
  committed view unless the transaction itself modified the rows
- range reads also stay stable because they are evaluated against the same
  snapshot

Example:

1. Transaction `T1` starts and receives snapshot `S`.
2. `T1` runs `SELECT ...`.
3. Transaction `T2` commits an update.
4. `T1` runs the same `SELECT ...` again.
5. `T1` still reads snapshot `S`, so it does not see `T2`'s newly committed
   row versions.

## How KiteSQL Implements RC and RR

The implementation is storage-driven, but the public API is storage-agnostic.

The common abstraction lives in `Storage::transaction_with_isolation(...)` and
the statement hooks `Transaction::begin_statement_scope()` and
`Transaction::end_statement_scope()`.

### RocksDB

RocksDB is where both levels are currently implemented.

KiteSQL does not rely on RocksDB transaction options to define read visibility.
Instead, it explicitly attaches a database snapshot to `ReadOptions` for each
read operation.

The key idea is:

- `ReadCommitted`: create a database snapshot at statement start, attach it to
  all reads in that statement, and drop it when the statement finishes
- `RepeatableRead`: create a database snapshot when the transaction starts and
  attach it to all reads in all statements of that transaction

This is why the isolation difference is concentrated in the statement-scope
hooks and the current snapshot field inside the RocksDB transaction wrapper.

Reads still execute through `rocksdb::Transaction`, not through the raw
database handle. That keeps "read your own writes" behavior intact while also
ensuring a statement or transaction uses one consistent committed view for
index scans and table lookups.

### LMDB

LMDB already provides a natural fixed snapshot view for a transaction, so
KiteSQL currently exposes only `RepeatableRead` there.

KiteSQL intentionally does not emulate `ReadCommitted` on LMDB with extra
plumbing because that would complicate the storage contract and diverge from the
minimal implementation model used today.

### Memory

The in-memory storage currently exposes only `ReadCommitted`.

This backend mainly exists for tests, examples, and temporary workloads, so the
implementation stays intentionally simple.

## Conflict Detection in Current KiteSQL

KiteSQL's current conflict detection is primarily key-based.

In the RocksDB-backed implementation, table rows are stored under concrete KV
keys derived from the primary key, and write operations such as `INSERT`,
`UPDATE`, and `DELETE` ultimately modify those concrete keys through the storage
transaction.

That means KiteSQL already has a solid baseline conflict detection capability
for cases like:

- two transactions writing the same primary-key row
- two transactions rewriting the same concrete storage entry

This is the most important transactional conflict detection foundation in the
current design: conflicts are naturally detected at the physical key level by
the underlying storage transaction mechanism.

What KiteSQL does not currently provide is predicate-level or range-level
conflict detection such as:

- "I read `a > 10`, so inserts into that range must now conflict"
- "I evaluated this SQL predicate, so future writes matching the predicate must
  be blocked or rejected"

Those stronger behaviors require explicit range locking, predicate locking, or
other higher-level concurrency control beyond today's key-based model.

## Why This Matches RC and RR

For ordinary SQL reads, the difference between `ReadCommitted` and
`RepeatableRead` is exactly the lifetime of the read snapshot:

- `ReadCommitted`: snapshot lifetime is one statement
- `RepeatableRead`: snapshot lifetime is one transaction

That is the rule KiteSQL enforces.

As a result:

- `ReadCommitted` prevents dirty reads because every statement reads from a
  committed snapshot
- `ReadCommitted` allows non-repeatable reads across statements because a later
  statement may use a newer snapshot
- `RepeatableRead` prevents non-repeatable reads because every statement uses
  the same snapshot
- `RepeatableRead` also keeps repeated range reads stable because the visible
  key space is evaluated against the same snapshot

This is why KiteSQL's current key-level conflict detection is sufficient for
its present `ReadCommitted` and `RepeatableRead` support:

- the basic write/write correctness still comes from the storage transaction's
  concrete key conflict handling
- the `RC` / `RR` distinction itself comes from snapshot lifetime, not from
  extra predicate conflict detection
- ordinary `RC` / `RR` reads do not require "I read this range, therefore other
  transactions may not insert into it" semantics

In other words, KiteSQL's current implementation satisfies the normal MVCC
definition of RC and RR for plain reads.

## Scope and Non-Goals

This document describes the guarantees for ordinary statement reads.

It does not claim support for:

- `Serializable`
- `SELECT ... FOR UPDATE`
- explicit row-lock or range-lock APIs
- lock-based writer scheduling semantics beyond what the underlying storage
  already provides

Those features can be added later without changing the core visibility model
used here for `ReadCommitted` and `RepeatableRead`.
