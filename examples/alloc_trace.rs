//! Runs one prepared, index-driven query on KiteSQL (LMDB) and on SQLite, with `note` strings
//! from 64 B to 256 KiB, and compares latency, allocations and memory. A second query (`multi-op`)
//! reads the same rows but passes them through grouping, a window and a sort first, each of which
//! keeps the strings while it runs.
//!
//! ```shell
//! cargo run --release --example alloc_trace --features lmdb,orm
//! ```
//!
//! What keeps the comparison fair:
//! - Both engines load the same rows with the same index on `(customer_id, status)`, reopen the
//!   database, and run the same SQL, prepared once, with the same parameters, inside one read
//!   transaction. Every execution returns about 10 rows.
//! - Both hand each string over as a pointer and a length (`&str` / `sqlite3_column_text`), and
//!   the caller reads that string. Whatever an engine copies to produce the pointer is its cost.
//! - Allocations are counted at libc `malloc`, which KiteSQL's Rust code, LMDB and SQLite all
//!   call. SQLite serves small allocations from its lookaside slab, which it allocates when the
//!   connection opens, without calling `malloc`; those come from `SQLITE_DBSTATUS_LOOKASIDE_HIT`
//!   and have their own column.
//! - Memory is the live heap, measured the same way for both: after opening, held by one
//!   prepared statement after its first execution, and after the measured executions. SQLite's
//!   lookaside slab and page cache are heap and count. LMDB reads pages through a file mapping
//!   that the OS page cache backs, so they do not count; but the LMDB environment preallocates
//!   page lists for write transactions when it opens, which do. `of which` shows each engine's
//!   share: the page cache for SQLite, the LMDB environment alone for KiteSQL.
//! - SQLite also runs with `PRAGMA mmap_size`, which reads pages through a memory map like LMDB.
//! - `notes in file map` checks where each engine's `note` pointer points, from
//!   `/proc/self/maps`: into the mapping of the database file, or elsewhere. SQLite copies even
//!   a string that fits in its page into a buffer the statement reuses, so it allocates nothing
//!   for short strings but still copies them.
//! - Neither engine checks again that a stored string is UTF-8: both encoded it from text.
//! - `prepare+1st` prepares from SQL text on both engines. Most of KiteSQL's share is parsing:
//!   KiteSQL parses with `sqlparser`, which supports far more syntax than SQLite's own parser
//!   but runs far slower. Programs built with the ORM builder (`Database::program`) skip parsing
//!   entirely.
//!
//! Linux with glibc only: counting replaces `malloc` and forwards to glibc's `__libc_malloc`.

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    bench::main()
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn main() {
    eprintln!("alloc_trace counts allocations through glibc, so it runs on Linux only");
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[path = "support/counting.rs"]
mod counting;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[path = "support/sqlite3.rs"]
mod sqlite3;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
mod bench {
    use crate::counting;
    use crate::sqlite3::*;
    use kite_sql::db::{CatalogKind, DataBaseBuilder, Param, Program, Transaction, TxnResultIter};
    use kite_sql::errors::DatabaseError;
    use kite_sql::storage::lmdb::LmdbStorage;
    use kite_sql::types::value::DataValue;
    use kite_sql::types::{CharLengthUnits, LogicalType};
    use kite_sql::{Model, Projection};
    use std::borrow::Cow;
    use std::ffi::{c_int, c_void, CStr, CString};
    use std::path::Path;
    use std::ptr;
    use std::time::Instant;

    #[derive(Default, Debug, Model)]
    #[model(table = "orders")]
    #[model(index(name = "orders_customer_status", columns = "customer_id, status"))]
    struct Order<'a> {
        #[model(primary_key)]
        id: i32,
        customer_id: i32,
        status: Cow<'a, str>,
        amount: i32,
        note: Cow<'a, str>,
    }

    /// A result row whose strings borrow the read transaction when they can.
    #[derive(Debug, Projection)]
    struct OrderView<'a> {
        id: i32,
        status: Cow<'a, str>,
        amount: i32,
        note: Cow<'a, str>,
    }

    // `$n` is a numbered parameter in KiteSQL and, numbered by first use, in SQLite.
    const QUERY: &str = "select id, status, amount, note from orders \
                         where customer_id = $1 and status = $2 and amount > $3";
    /// The same rows as `QUERY`, grouped, ranked by a window and sorted before they are returned.
    const MULTI_OP_QUERY: &str = "select status, note, peak, max(peak) over (partition by status) \
                                  from (select status, note, max(amount) as peak from orders \
                                  where customer_id = $1 and status = $2 and amount > $3 \
                                  group by status, note) t \
                                  order by note desc";
    const STATUSES: [&str; 5] = ["new", "paid", "shipped", "done", "void"];
    /// Every customer has 10 orders per status, so every execution returns about 10 rows.
    const ORDERS_PER_CUSTOMER: usize = 10 * STATUSES.len();
    const NOTE_LENS: [usize; 6] = [64, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10];
    /// Roughly the note bytes loaded per note length, and read per variant.
    const LOAD_BYTES: usize = 64 << 20;
    const READ_BYTES: usize = 1 << 30;
    /// Fresh statements prepared and run once, for `prepare+1st`.
    const FIRST_SAMPLES: usize = 200;
    /// Executions before measuring, to warm caches and grow reused buffers.
    const WARMUP: usize = 200;

    type Args = (i32, &'static str, i32);

    struct Workload {
        note_len: usize,
        customers: usize,
        runs: usize,
    }

    impl Workload {
        fn new(note_len: usize) -> Self {
            Self {
                note_len,
                customers: (LOAD_BYTES / (note_len * ORDERS_PER_CUSTOMER)).clamp(5, 400),
                runs: (READ_BYTES / (note_len * 10)).clamp(500, 20_000),
            }
        }

        fn orders(&self) -> usize {
            self.customers * ORDERS_PER_CUSTOMER
        }

        /// `(customer_id, status, amount, note)` of order `id`.
        fn order(&self, id: usize) -> (i32, &'static str, i32, String) {
            let mut note = format!("note-{id:06}-");
            note.push_str(&"n".repeat(self.note_len.saturating_sub(note.len())));
            (
                (id % self.customers) as i32,
                STATUSES[id / self.customers % STATUSES.len()],
                (id % 997) as i32,
                note,
            )
        }

        /// Parameters of execution `i`: a customer, a status and a minimum amount.
        fn args(&self, i: usize) -> Args {
            (
                (i % self.customers) as i32,
                STATUSES[i % STATUSES.len()],
                10,
            )
        }
    }

    /// What a variant's executions read. Every variant must read the same rows and text bytes.
    #[derive(Default, Clone, Copy, Debug)]
    struct Totals {
        rows: usize,
        text_bytes: usize,
        /// Notes that borrowed the transaction, counted by variants that decode into `OrderView`.
        borrowed: usize,
    }

    /// One variant's measured executions.
    struct Run {
        label: &'static str,
        /// Median of preparing a fresh statement and running it once, for variants that prepare.
        first_ns: Option<u64>,
        ns: Vec<u64>,
        totals: Totals,
        mallocs: usize,
        malloc_bytes: usize,
        lookaside: Option<usize>,
        /// Whether `totals.borrowed` was counted.
        decodes_views: bool,
    }

    /// Live heap of one engine, in bytes.
    struct Memory {
        label: &'static str,
        open: isize,
        statement: isize,
        after_runs: isize,
        /// The part of `after_runs` that the engine's storage holds by design.
        of_which: (&'static str, isize),
        /// Share of returned `note` pointers inside a mapping of the database file.
        notes_in_mapping: f64,
    }

    fn warm(
        exec: &mut impl FnMut(usize, &mut Totals) -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError> {
        let mut totals = Totals::default();
        (0..WARMUP).try_for_each(|i| exec(i, &mut totals))
    }

    /// Times every execution into `ns`, allocated beforehand, and counts the allocations of all
    /// executions together.
    fn measure(
        label: &'static str,
        runs: usize,
        mut ns: Vec<u64>,
        exec: &mut impl FnMut(usize, &mut Totals) -> Result<(), DatabaseError>,
    ) -> Result<Run, DatabaseError> {
        assert!(ns.capacity() >= runs);
        let mut totals = Totals::default();
        counting::start();
        for i in 0..runs {
            let start = Instant::now();
            let result = exec(i, &mut totals);
            ns.push(start.elapsed().as_nanos() as u64);
            if let Err(err) = result {
                counting::stop();
                return Err(err);
            }
        }
        let (mallocs, malloc_bytes) = counting::stop();
        Ok(Run {
            label,
            first_ns: None,
            ns,
            totals,
            mallocs,
            malloc_bytes,
            lookaside: None,
            decodes_views: false,
        })
    }

    fn median(samples: &mut [u64]) -> u64 {
        samples.sort_unstable();
        samples[samples.len() / 2]
    }

    fn timing_buffers(w: &Workload, count: usize) -> Vec<Vec<u64>> {
        (0..count).map(|_| Vec::with_capacity(w.runs)).collect()
    }

    /// Share of `ptrs` that point into a mapping of `file`, from `/proc/self/maps`.
    fn share_in_mapping(file: &Path, ptrs: &[usize]) -> f64 {
        let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
        let file = file.to_str().unwrap();
        let ranges: Vec<(usize, usize)> = maps
            .lines()
            .filter(|line| line.ends_with(file))
            .filter_map(|line| {
                let (start, end) = line.split_whitespace().next()?.split_once('-')?;
                let parse = |hex| usize::from_str_radix(hex, 16).ok();
                Some((parse(start)?, parse(end)?))
            })
            .collect();
        let inside = ptrs
            .iter()
            .filter(|&&ptr| {
                ranges
                    .iter()
                    .any(|&(start, end)| (start..end).contains(&ptr))
            })
            .count();
        inside as f64 / ptrs.len() as f64
    }

    fn param_types() -> [(usize, LogicalType); 3] {
        [
            (1, LogicalType::Integer),
            (2, LogicalType::Varchar(None, CharLengthUnits::Characters)),
            (3, LogicalType::Integer),
        ]
    }

    fn kitesql_params((customer_id, status, amount): Args) -> [(usize, Param<'static>); 3] {
        [
            (1, DataValue::Int32(customer_id).into()),
            (2, Param::Str(status)),
            (3, DataValue::Int32(amount).into()),
        ]
    }

    /// Reads every string of every row as the `&str` the callback lends.
    fn kitesql_read<'p, 'txn: 'p>(
        program: &mut Program<'p>,
        tx: &mut Transaction<'txn, LmdbStorage>,
        args: Args,
        totals: &mut Totals,
    ) -> Result<(), DatabaseError> {
        let mut iter = program.execute(tx, kitesql_params(args))?;
        while iter
            .next_tuple(|_, row| {
                for value in row.iter() {
                    if let DataValue::Utf8 { value, .. } = value {
                        let text: &str = value;
                        totals.text_bytes += text.len();
                    }
                }
            })?
            .is_some()
        {
            totals.rows += 1;
        }
        Ok(())
    }

    /// Where the `note` of every row returned points.
    fn kitesql_note_ptrs<'p, 'txn: 'p>(
        program: &mut Program<'p>,
        tx: &mut Transaction<'txn, LmdbStorage>,
        args: Args,
    ) -> Result<Vec<usize>, DatabaseError> {
        let mut ptrs = Vec::new();
        let mut iter = program.execute(tx, kitesql_params(args))?;
        while iter
            .next_tuple(|_, row| {
                if let Some(DataValue::Utf8 { value, .. }) = row.iter().nth(3) {
                    let text: &str = value;
                    ptrs.push(text.as_ptr() as usize);
                }
            })?
            .is_some()
        {}
        Ok(ptrs)
    }

    /// Decodes every row into an `OrderView`, whose strings borrow the transaction when they can.
    fn kitesql_views<'p, 'txn: 'p>(
        program: &mut Program<'p>,
        tx: &mut Transaction<'txn, LmdbStorage>,
        args: Args,
        totals: &mut Totals,
    ) -> Result<(), DatabaseError> {
        for view in program
            .execute(tx, kitesql_params(args))?
            .txn_orm::<OrderView<'_>>()
        {
            let view = view?;
            std::hint::black_box((view.id, view.amount));
            totals.rows += 1;
            totals.text_bytes += view.status.len() + view.note.len();
            totals.borrowed += usize::from(matches!(view.note, Cow::Borrowed(_)));
        }
        Ok(())
    }

    fn kitesql(
        dir: &Path,
        w: &Workload,
        lmdb_env: isize,
    ) -> Result<(Vec<Run>, Memory), DatabaseError> {
        {
            let mut db = DataBaseBuilder::path(dir).sync(false).build_lmdb()?;
            db.create_table::<Order>()?;
            db.insert_many((0..w.orders()).map(|id| {
                let (customer_id, status, amount, note) = w.order(id);
                Order {
                    id: id as i32,
                    customer_id,
                    status: status.into(),
                    amount,
                    note: note.into(),
                }
            }))?;
            db.analyze_model::<Order>()?;
        }

        // Allocated before opening, so the live heap measured below holds only the engine's.
        let mut first = Vec::with_capacity(FIRST_SAMPLES);
        let mut buffers = timing_buffers(w, 4);
        let mut runs = Vec::with_capacity(4);

        let before = counting::live();
        let mut db = DataBaseBuilder::path(dir).build_lmdb()?;
        let open = counting::live() - before;
        // A reopened database loads table catalogs on request; SQLite loads its schema on the
        // first prepare.
        db.load(CatalogKind::Table("orders".to_string()))?;
        let types = param_types();

        let mut tx = db.new_transaction(true)?;
        // Mostly `sqlparser` parsing `QUERY`, kept for its broad SQL syntax; the ORM builder
        // (`Database::program`) builds the same program without parsing.
        for i in 0..FIRST_SAMPLES {
            let start = Instant::now();
            let mut program = tx.program_sql(QUERY, &types)?;
            kitesql_read(&mut program, &mut tx, w.args(i), &mut Totals::default())?;
            first.push(start.elapsed().as_nanos() as u64);
        }
        let before_statement = counting::live();
        let mut program = tx.program_sql(QUERY, &types)?;
        kitesql_read(&mut program, &mut tx, w.args(0), &mut Totals::default())?;
        let statement = counting::live() - before_statement;
        let note_ptrs = kitesql_note_ptrs(&mut program, &mut tx, w.args(0))?;
        let notes_in_mapping = share_in_mapping(&dir.join("data.mdb"), &note_ptrs);
        drop(note_ptrs);

        let mut read =
            |i, totals: &mut Totals| kitesql_read(&mut program, &mut tx, w.args(i), totals);
        warm(&mut read)?;
        let ns = buffers.pop().unwrap();
        let mut run = measure("KiteSQL  next_tuple", w.runs, ns, &mut read)?;
        run.first_ns = Some(median(&mut first));
        runs.push(run);

        let mut views =
            |i, totals: &mut Totals| kitesql_views(&mut program, &mut tx, w.args(i), totals);
        warm(&mut views)?;
        let ns = buffers.pop().unwrap();
        let mut run = measure("KiteSQL  txn_orm", w.runs, ns, &mut views)?;
        run.decodes_views = true;
        runs.push(run);
        let after_runs = counting::live() - before;

        let mut multi_op = tx.program_sql(MULTI_OP_QUERY, &types)?;
        let mut read =
            |i, totals: &mut Totals| kitesql_read(&mut multi_op, &mut tx, w.args(i), totals);
        warm(&mut read)?;
        let ns = buffers.pop().unwrap();
        runs.push(measure("KiteSQL  multi-op", w.runs, ns, &mut read)?);
        tx.commit()?;

        // A write transaction is not zero-copy: its pages can move before it ends, so strings are
        // copied out.
        let mut tx = db.new_transaction(false)?;
        let mut views =
            |i, totals: &mut Totals| kitesql_views(&mut program, &mut tx, w.args(i), totals);
        warm(&mut views)?;
        let ns = buffers.pop().unwrap();
        let mut run = measure("KiteSQL  txn_orm, write txn", w.runs, ns, &mut views)?;
        run.decodes_views = true;
        runs.push(run);
        tx.commit()?;

        let memory = Memory {
            label: "KiteSQL",
            open,
            statement,
            after_runs,
            of_which: ("LMDB env", lmdb_env),
            notes_in_mapping,
        };
        Ok((runs, memory))
    }

    unsafe fn sqlite_load(path: &CStr, w: &Workload) {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(path.as_ptr(), &mut db), 0);
        sqlite_exec(
            db,
            "create table orders(id integer primary key, customer_id int, status text, \
             amount int, note text); \
             create index orders_customer_status on orders(customer_id, status); \
             begin",
        );
        let insert = sqlite_prepare(db, c"insert into orders values (?, ?, ?, ?, ?)");
        for id in 0..w.orders() {
            let (customer_id, status, amount, note) = w.order(id);
            sqlite3_reset(insert);
            sqlite3_bind_int64(insert, 1, id as i64);
            sqlite3_bind_int64(insert, 2, customer_id as i64);
            sqlite3_bind_text(
                insert,
                3,
                status.as_ptr().cast(),
                status.len() as c_int,
                None,
            );
            sqlite3_bind_int64(insert, 4, amount as i64);
            sqlite3_bind_text(insert, 5, note.as_ptr().cast(), note.len() as c_int, None);
            assert_eq!(sqlite3_step(insert), DONE);
        }
        sqlite3_finalize(insert);
        sqlite_exec(db, "commit; analyze");
        sqlite3_close(db);
    }

    unsafe fn sqlite_bind(stmt: *mut c_void, (customer_id, status, amount): Args) {
        sqlite3_reset(stmt);
        sqlite3_bind_int64(stmt, 1, customer_id as i64);
        // A null destructor is `SQLITE_STATIC`: SQLite borrows the text.
        sqlite3_bind_text(stmt, 2, status.as_ptr().cast(), status.len() as c_int, None);
        sqlite3_bind_int64(stmt, 3, amount as i64);
    }

    /// Where the `note` of every row returned points.
    unsafe fn sqlite_note_ptrs(stmt: *mut c_void, args: Args) -> Vec<usize> {
        sqlite_bind(stmt, args);
        let mut ptrs = Vec::new();
        while sqlite3_step(stmt) == ROW {
            ptrs.push(sqlite3_column_text(stmt, 3) as usize);
        }
        ptrs
    }

    /// Reads the strings in `columns` of every row through the pointer `sqlite3_column_text`
    /// lends.
    unsafe fn sqlite_read(stmt: *mut c_void, columns: &[c_int], args: Args, totals: &mut Totals) {
        sqlite_bind(stmt, args);
        loop {
            match sqlite3_step(stmt) {
                ROW => {
                    totals.rows += 1;
                    for &column in columns {
                        let text = sqlite3_column_text(stmt, column);
                        let len = sqlite3_column_bytes(stmt, column) as usize;
                        let text =
                            std::str::from_utf8_unchecked(std::slice::from_raw_parts(text, len));
                        totals.text_bytes += text.len();
                    }
                }
                DONE => break,
                code => panic!("sqlite3_step failed: {code}"),
            }
        }
    }

    unsafe fn sqlite(path: &CStr, w: &Workload, mmap: bool) -> (Vec<Run>, Memory) {
        let (label, multi_op_label) = if mmap {
            ("SQLite   mmap_size", "SQLite   mmap_size, multi-op")
        } else {
            ("SQLite", "SQLite   multi-op")
        };
        // Allocated before opening, so the live heap measured below holds only the engine's.
        let mut first = Vec::with_capacity(FIRST_SAMPLES);
        let mut buffers = timing_buffers(w, 2);
        let query = CString::new(QUERY).unwrap();
        let multi_op_query = CString::new(MULTI_OP_QUERY).unwrap();

        let before = counting::live();
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(path.as_ptr(), &mut db), 0);
        let open = counting::live() - before;
        if mmap {
            sqlite_exec(db, "pragma mmap_size = 1073741824");
        }

        // One read transaction for every execution, as KiteSQL has.
        sqlite_exec(db, "begin");
        for i in 0..FIRST_SAMPLES {
            let start = Instant::now();
            let stmt = sqlite_prepare(db, &query);
            sqlite_read(stmt, &[1, 3], w.args(i), &mut Totals::default());
            first.push(start.elapsed().as_nanos() as u64);
            sqlite3_finalize(stmt);
        }
        let stmt = sqlite_prepare(db, &query);
        sqlite_read(stmt, &[1, 3], w.args(0), &mut Totals::default());
        let statement = sqlite3_stmt_status(stmt, STMTSTATUS_MEMUSED, 0) as isize;
        let note_ptrs = sqlite_note_ptrs(stmt, w.args(0));
        let notes_in_mapping = share_in_mapping(Path::new(path.to_str().unwrap()), &note_ptrs);
        drop(note_ptrs);

        let mut runs = Vec::with_capacity(2);
        let mut read = |i, totals: &mut Totals| {
            sqlite_read(stmt, &[1, 3], w.args(i), totals);
            Ok(())
        };
        warm(&mut read).unwrap();
        db_status(db, DBSTATUS_LOOKASIDE_HIT);
        let ns = buffers.pop().unwrap();
        let mut run = measure(label, w.runs, ns, &mut read).unwrap();
        run.lookaside = Some(db_status(db, DBSTATUS_LOOKASIDE_HIT).1 as usize);
        run.first_ns = Some(median(&mut first));
        runs.push(run);
        let after_runs = counting::live() - before;
        let page_cache = db_status(db, DBSTATUS_CACHE_USED).0 as isize;
        sqlite3_reset(stmt);
        sqlite3_finalize(stmt);

        let stmt = sqlite_prepare(db, &multi_op_query);
        let mut read = |i, totals: &mut Totals| {
            sqlite_read(stmt, &[0, 1], w.args(i), totals);
            Ok(())
        };
        warm(&mut read).unwrap();
        db_status(db, DBSTATUS_LOOKASIDE_HIT);
        let ns = buffers.pop().unwrap();
        let mut run = measure(multi_op_label, w.runs, ns, &mut read).unwrap();
        run.lookaside = Some(db_status(db, DBSTATUS_LOOKASIDE_HIT).1 as usize);
        runs.push(run);
        sqlite3_reset(stmt);
        sqlite3_finalize(stmt);
        sqlite_exec(db, "commit");
        sqlite3_close(db);
        let memory = Memory {
            label,
            open,
            statement,
            after_runs,
            of_which: ("page cache", page_cache),
            notes_in_mapping,
        };
        (runs, memory)
    }

    /// Heap the LMDB environment holds after opening, outside KiteSQL.
    fn lmdb_env_bytes(dir: &Path) -> Result<isize, DatabaseError> {
        let before = counting::live();
        let env = lmdb::Environment::new()
            .set_flags(lmdb::EnvironmentFlags::NO_TLS)
            .open(dir)?;
        let bytes = counting::live() - before;
        drop(env);
        Ok(bytes)
    }

    fn print(w: &Workload, runs: &mut [Run], memory: &[Memory]) {
        let rows = runs[0].totals.rows as f64 / w.runs as f64;
        println!(
            "\nnote {} B: {} orders; {} executions per variant, {rows:.1} rows each",
            w.note_len,
            w.orders(),
            w.runs
        );
        println!(
            "{:<28} | {:>16} | {:>8} | {:>8} | {:>11} | {:>14} | {:>15} | {:>14}",
            "variant",
            "prepare+1st µs",
            "p50 µs",
            "p99 µs",
            "malloc/exec",
            "lookaside/exec",
            "malloc KiB/exec",
            "borrowed notes"
        );
        let per_exec = |count: usize| count as f64 / w.runs as f64;
        for run in runs.iter_mut() {
            let first = run
                .first_ns
                .map_or("-".to_string(), |ns| format!("{:.2}", ns as f64 / 1e3));
            let p99_index = (run.ns.len() - 1) * 99 / 100;
            let p50 = median(&mut run.ns) as f64 / 1e3;
            let p99 = run.ns[p99_index] as f64 / 1e3;
            let lookaside = run
                .lookaside
                .map_or("-".to_string(), |hits| format!("{:.2}", per_exec(hits)));
            let borrowed = if run.decodes_views {
                format!(
                    "{:.0}%",
                    run.totals.borrowed as f64 * 100.0 / run.totals.rows as f64
                )
            } else {
                "-".to_string()
            };
            println!(
                "{:<28} | {first:>16} | {p50:>8.2} | {p99:>8.2} | {:>11.2} | {lookaside:>14} | {:>15.2} | {borrowed:>14}",
                run.label,
                per_exec(run.mallocs),
                per_exec(run.malloc_bytes) / 1024.0,
            );
        }
        let kib = |bytes: isize| format!("{:.1}", bytes as f64 / 1024.0);
        println!(
            "{:<28} | {:>16} | {:>14} | {:>16} | {:>20} | {:>17}",
            "live heap (KiB)", "open", "statement", "after runs", "of which", "notes in file map"
        );
        for memory in memory {
            println!(
                "{:<28} | {:>16} | {:>14} | {:>16} | {:>20} | {:>17}",
                memory.label,
                kib(memory.open),
                kib(memory.statement),
                kib(memory.after_runs),
                format!("{} {}", kib(memory.of_which.1), memory.of_which.0),
                format!("{:.0}%", memory.notes_in_mapping * 100.0),
            );
        }
    }

    pub fn main() -> Result<(), DatabaseError> {
        let version = unsafe { CStr::from_ptr(sqlite3_libversion()) };
        let scratch = tempfile::TempDir::new()?;
        let lmdb_env = lmdb_env_bytes(scratch.path())?;
        println!("SQLite {}", version.to_string_lossy());
        for note_len in NOTE_LENS {
            let w = Workload::new(note_len);
            let dir = tempfile::TempDir::new()?;
            let (mut runs, kitesql_memory) = kitesql(&dir.path().join("kitesql"), &w, lmdb_env)?;
            let mut memory = vec![kitesql_memory];
            let path = dir.path().join("orders.sqlite");
            let path = CString::new(path.to_str().unwrap()).unwrap();
            unsafe { sqlite_load(&path, &w) };
            for mmap in [false, true] {
                let (sqlite_runs, sqlite_memory) = unsafe { sqlite(&path, &w, mmap) };
                runs.extend(sqlite_runs);
                memory.push(sqlite_memory);
            }
            // `multi-op` returns the same rows and strings as `QUERY`, in another order.
            let read = |run: &Run| (run.totals.rows, run.totals.text_bytes);
            for run in &runs {
                assert_eq!(read(run), read(&runs[0]), "{} read other rows", run.label);
            }
            print(&w, &mut runs, &memory);
        }
        Ok(())
    }
}
