//! Collects a whole result set and keeps it after the query: as raw rows, parsed from JSON, and
//! parsed from CSV. KiteSQL (LMDB) borrows every string from its read transaction; SQLite hands
//! a string out only until the next `sqlite3_step`, so keeping one means owning it.
//!
//! ```shell
//! cargo run --release --example borrowed_parse --features lmdb,orm
//! ```
//!
//! Every execution selects one group of 100 documents by an index on `grp`, and builds a `Vec` of
//! records that outlives the loop over the rows; afterwards their fields are summed, and both
//! engines must arrive at the same sums.
//! - `collect`: `(id, body)` pairs. KiteSQL keeps `Cow::Borrowed` bodies; SQLite copies each
//!   body into a `String`.
//! - `json`: `serde_json` into a struct of `&str` fields borrowed from the stored text on
//!   KiteSQL, and of `String` fields on SQLite.
//! - `csv`: the same fields split from a comma-separated line, without quoting.
//!
//! SQLite runs the common way: records own their strings. Its cheapest alternative copies each
//! row's text once into a `String` and borrows the fields from that, one allocation per row
//! instead of one per field.
//!
//! SQLite reads pages through `PRAGMA mmap_size`. Allocations are counted at libc `malloc`, as in
//! `alloc_trace`, with SQLite's lookaside allocations in their own column.
//!
//! Linux with glibc only: counting replaces `malloc` and forwards to glibc's `__libc_malloc`.

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn main() -> Result<(), kite_sql::errors::DatabaseError> {
    bench::main()
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn main() {
    eprintln!("borrowed_parse counts allocations through glibc, so it runs on Linux only");
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
    use kite_sql::db::{CatalogKind, DataBaseBuilder, Program, Transaction, TxnResultIter};
    use kite_sql::errors::DatabaseError;
    use kite_sql::storage::lmdb::LmdbStorage;
    use kite_sql::types::value::DataValue;
    use kite_sql::types::LogicalType;
    use kite_sql::{Model, Projection};
    use serde::Deserialize;
    use std::borrow::Cow;
    use std::ffi::{c_int, c_void, CStr, CString};
    use std::path::Path;
    use std::ptr;
    use std::time::Instant;

    #[derive(Default, Debug, Model)]
    #[model(table = "json_docs")]
    #[model(index(name = "json_docs_grp", columns = "grp"))]
    struct JsonDoc<'a> {
        #[model(primary_key)]
        id: i32,
        grp: i32,
        body: Cow<'a, str>,
    }

    #[derive(Default, Debug, Model)]
    #[model(table = "csv_docs")]
    #[model(index(name = "csv_docs_grp", columns = "grp"))]
    struct CsvDoc<'a> {
        #[model(primary_key)]
        id: i32,
        grp: i32,
        body: Cow<'a, str>,
    }

    /// A selected row, whose body borrows the read transaction.
    #[derive(Debug, Projection)]
    struct Raw<'a> {
        id: i32,
        body: Cow<'a, str>,
    }

    /// A parsed document whose fields borrow the text they were parsed from.
    #[derive(Deserialize)]
    struct Record<'a> {
        id: i64,
        user: &'a str,
        email: &'a str,
        title: &'a str,
        tags: [&'a str; 3],
        body: &'a str,
    }

    /// The same document owning its strings, as SQLite has to keep it.
    #[derive(Deserialize)]
    struct OwnedRecord {
        id: i64,
        user: String,
        email: String,
        title: String,
        tags: [String; 3],
        body: String,
    }

    impl Record<'_> {
        fn parse_csv(line: &str) -> Record<'_> {
            let mut fields = line.splitn(8, ',');
            let mut next = || fields.next().expect("eight fields");
            Record {
                id: next().parse().expect("numeric id"),
                user: next(),
                email: next(),
                title: next(),
                tags: [next(), next(), next()],
                body: next(),
            }
        }

        fn to_owned_record(&self) -> OwnedRecord {
            OwnedRecord {
                id: self.id,
                user: self.user.to_owned(),
                email: self.email.to_owned(),
                title: self.title.to_owned(),
                tags: self.tags.map(str::to_owned),
                body: self.body.to_owned(),
            }
        }

        fn fields(&self) -> (i64, [&str; 7]) {
            let [t0, t1, t2] = self.tags;
            let fields = [self.user, self.email, self.title, t0, t1, t2, self.body];
            (self.id, fields)
        }
    }

    impl OwnedRecord {
        fn fields(&self) -> (i64, [&str; 7]) {
            let [t0, t1, t2] = &self.tags;
            let fields = [&self.user, &self.email, &self.title, t0, t1, t2, &self.body];
            (self.id, fields.map(String::as_str))
        }
    }

    const PER_GROUP: usize = 100;
    const DOC_LENS: [usize; 3] = [256, 2 << 10, 16 << 10];
    /// Roughly the document bytes loaded per table, and read per scenario and engine.
    const LOAD_BYTES: usize = 64 << 20;
    const READ_BYTES: usize = 1 << 30;
    /// Executions before measuring, to warm caches and grow reused buffers.
    const WARMUP: usize = 100;
    const JSON_QUERY: &str = "select id, body from json_docs where grp = $1";
    const CSV_QUERY: &str = "select id, body from csv_docs where grp = $1";

    struct Workload {
        doc_len: usize,
        groups: usize,
        runs: usize,
    }

    impl Workload {
        fn new(doc_len: usize) -> Self {
            Self {
                doc_len,
                groups: (LOAD_BYTES / (doc_len * PER_GROUP)).clamp(4, 200),
                runs: (READ_BYTES / (doc_len * PER_GROUP)).clamp(300, 5_000),
            }
        }

        fn docs(&self) -> usize {
            self.groups * PER_GROUP
        }

        /// Document `id` as JSON and as CSV, with the same fields, about `doc_len` bytes long.
        fn doc(&self, id: usize) -> (String, String) {
            let user = format!("user-{id:06}");
            let email = format!("{user}@example.com");
            let title = format!("document number {id}");
            let tags = [id % 7, id % 11, id % 13].map(|n| format!("tag-{n}"));
            let [t0, t1, t2] = &tags;
            let fill = self.doc_len.saturating_sub(120 + 2 * user.len());
            let body = "b".repeat(fill);
            let json = format!(
                r#"{{"id":{id},"user":"{user}","email":"{email}","title":"{title}","tags":["{t0}","{t1}","{t2}"],"body":"{body}"}}"#
            );
            let csv = format!("{id},{user},{email},{title},{t0},{t1},{t2},{body}");
            (json, csv)
        }

        fn group(&self, i: usize) -> i32 {
            (i % self.groups) as i32
        }
    }

    /// What the kept records add up to. Every scenario on both engines must arrive at the same.
    #[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
    struct Totals {
        rows: usize,
        ids: i64,
        bytes: usize,
    }

    impl Totals {
        fn add(&mut self, id: i64, fields: &[&str]) {
            self.rows += 1;
            self.ids += id;
            self.bytes += fields.iter().map(|field| field.len()).sum::<usize>();
        }
    }

    /// One engine's measured executions of one scenario.
    struct Run {
        ns: Vec<u64>,
        totals: Totals,
        mallocs: usize,
        malloc_bytes: usize,
        lookaside: Option<usize>,
    }

    type Exec<'a> = dyn FnMut(usize, &mut Totals) -> Result<(), DatabaseError> + 'a;

    fn warm(exec: &mut Exec<'_>) -> Result<(), DatabaseError> {
        let mut totals = Totals::default();
        (0..WARMUP).try_for_each(|i| exec(i, &mut totals))
    }

    /// Times every execution, and counts the allocations of all executions together.
    fn measure(runs: usize, exec: &mut Exec<'_>) -> Result<Run, DatabaseError> {
        let mut ns = Vec::with_capacity(runs);
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
            ns,
            totals,
            mallocs,
            malloc_bytes,
            lookaside: None,
        })
    }

    fn kite_params(grp: i32) -> [(usize, kite_sql::db::Param<'static>); 1] {
        [(1, DataValue::Int32(grp).into())]
    }

    /// The text a read-only transaction lent.
    fn lent(body: Cow<'_, str>) -> &str {
        match body {
            Cow::Borrowed(text) => text,
            Cow::Owned(_) => unreachable!("a read-only LMDB transaction lends its strings"),
        }
    }

    /// Keeps every row as `(id, body)`, with the body borrowed from the transaction.
    fn kite_collect<'p, 'txn: 'p>(
        program: &mut Program<'p>,
        tx: &mut Transaction<'txn, LmdbStorage>,
        grp: i32,
        totals: &mut Totals,
    ) -> Result<(), DatabaseError> {
        let mut rows = Vec::with_capacity(PER_GROUP);
        for raw in program.execute(tx, kite_params(grp))?.txn_orm::<Raw<'_>>() {
            rows.push(raw?);
        }
        for raw in &rows {
            totals.add(raw.id as i64, &[&raw.body]);
        }
        Ok(())
    }

    /// Parses every row into a `Record` whose fields borrow the transaction.
    fn kite_parse<'p, 'txn: 'p>(
        program: &mut Program<'p>,
        tx: &mut Transaction<'txn, LmdbStorage>,
        grp: i32,
        totals: &mut Totals,
        parse: for<'a> fn(&'a str) -> Record<'a>,
    ) -> Result<(), DatabaseError> {
        let mut records = Vec::with_capacity(PER_GROUP);
        for raw in program.execute(tx, kite_params(grp))?.txn_orm::<Raw<'_>>() {
            records.push(parse(lent(raw?.body)));
        }
        for record in &records {
            let (id, fields) = record.fields();
            totals.add(id, &fields);
        }
        Ok(())
    }

    fn parse_json(text: &str) -> Record<'_> {
        serde_json::from_str(text).expect("valid JSON without escapes")
    }

    /// Runs `stmt` for group `grp`, lending each row's id and text to `f` until the next step.
    unsafe fn sqlite_rows(stmt: *mut c_void, grp: i32, mut f: impl FnMut(i64, &str)) {
        sqlite3_reset(stmt);
        sqlite3_bind_int64(stmt, 1, grp as i64);
        loop {
            match sqlite3_step(stmt) {
                ROW => {
                    let id = sqlite3_column_int64(stmt, 0);
                    let text = sqlite3_column_text(stmt, 1);
                    let len = sqlite3_column_bytes(stmt, 1) as usize;
                    f(
                        id,
                        std::str::from_utf8_unchecked(std::slice::from_raw_parts(text, len)),
                    );
                }
                DONE => break,
                code => panic!("sqlite3_step failed: {code}"),
            }
        }
    }

    /// Keeps every row as `(id, body)`, copying the body out before the next step.
    unsafe fn sqlite_collect(
        stmt: *mut c_void,
        grp: i32,
        totals: &mut Totals,
    ) -> Result<(), DatabaseError> {
        let mut rows = Vec::with_capacity(PER_GROUP);
        sqlite_rows(stmt, grp, |id, text| rows.push((id, text.to_owned())));
        for (id, body) in &rows {
            totals.add(*id, &[body]);
        }
        Ok(())
    }

    /// Parses every row into an `OwnedRecord`, the common way to keep it past the next step.
    unsafe fn sqlite_parse(
        stmt: *mut c_void,
        grp: i32,
        totals: &mut Totals,
        parse: fn(&str) -> OwnedRecord,
    ) -> Result<(), DatabaseError> {
        let mut records = Vec::with_capacity(PER_GROUP);
        sqlite_rows(stmt, grp, |_, text| records.push(parse(text)));
        for record in &records {
            let (id, fields) = record.fields();
            totals.add(id, &fields);
        }
        Ok(())
    }

    fn parse_json_owned(text: &str) -> OwnedRecord {
        serde_json::from_str(text).expect("valid JSON")
    }

    fn parse_csv_owned(text: &str) -> OwnedRecord {
        Record::parse_csv(text).to_owned_record()
    }

    type Docs = [(String, String)];

    fn kitesql(dir: &Path, w: &Workload, docs: &Docs) -> Result<[Run; 3], DatabaseError> {
        {
            let mut db = DataBaseBuilder::path(dir).sync(false).build_lmdb()?;
            db.create_table::<JsonDoc>()?;
            db.create_table::<CsvDoc>()?;
            let grp = |id: usize| (id % w.groups) as i32;
            db.insert_many(docs.iter().enumerate().map(|(id, (json, _))| JsonDoc {
                id: id as i32,
                grp: grp(id),
                body: json.as_str().into(),
            }))?;
            db.insert_many(docs.iter().enumerate().map(|(id, (_, csv))| CsvDoc {
                id: id as i32,
                grp: grp(id),
                body: csv.as_str().into(),
            }))?;
            db.analyze_model::<JsonDoc>()?;
            db.analyze_model::<CsvDoc>()?;
        }
        let mut db = DataBaseBuilder::path(dir).build_lmdb()?;
        db.load(CatalogKind::Table("json_docs".to_string()))?;
        db.load(CatalogKind::Table("csv_docs".to_string()))?;
        let types = [(1, LogicalType::Integer)];
        let mut tx = db.new_transaction(true)?;
        let mut json = tx.program_sql(JSON_QUERY, &types)?;
        let mut csv = tx.program_sql(CSV_QUERY, &types)?;

        let mut collect =
            |i: usize, t: &mut Totals| kite_collect(&mut json, &mut tx, w.group(i), t);
        warm(&mut collect)?;
        let collected = measure(w.runs, &mut collect)?;
        let mut parse =
            |i: usize, t: &mut Totals| kite_parse(&mut json, &mut tx, w.group(i), t, parse_json);
        warm(&mut parse)?;
        let parsed_json = measure(w.runs, &mut parse)?;
        let mut parse = |i: usize, t: &mut Totals| {
            kite_parse(&mut csv, &mut tx, w.group(i), t, Record::parse_csv)
        };
        warm(&mut parse)?;
        let parsed_csv = measure(w.runs, &mut parse)?;
        tx.commit()?;
        Ok([collected, parsed_json, parsed_csv])
    }

    unsafe fn sqlite(path: &CStr, w: &Workload, docs: &Docs) -> [Run; 3] {
        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(path.as_ptr(), &mut db), 0);
        sqlite_exec(
            db,
            "create table json_docs(id integer primary key, grp int, body text); \
             create index json_docs_grp on json_docs(grp); \
             create table csv_docs(id integer primary key, grp int, body text); \
             create index csv_docs_grp on csv_docs(grp); \
             begin",
        );
        for (table, csv) in [("json_docs", false), ("csv_docs", true)] {
            let sql = CString::new(format!("insert into {table} values (?, ?, ?)")).unwrap();
            let insert = sqlite_prepare(db, &sql);
            for (id, (json_body, csv_body)) in docs.iter().enumerate() {
                let body = if csv { csv_body } else { json_body };
                sqlite3_reset(insert);
                sqlite3_bind_int64(insert, 1, id as i64);
                sqlite3_bind_int64(insert, 2, (id % w.groups) as i64);
                sqlite3_bind_text(insert, 3, body.as_ptr().cast(), body.len() as c_int, None);
                assert_eq!(sqlite3_step(insert), DONE);
            }
            sqlite3_finalize(insert);
        }
        sqlite_exec(db, "commit; analyze");
        sqlite3_close(db);

        let mut db = ptr::null_mut();
        assert_eq!(sqlite3_open(path.as_ptr(), &mut db), 0);
        sqlite_exec(db, "pragma mmap_size = 1073741824; begin");
        let json = sqlite_prepare(db, &CString::new(JSON_QUERY).unwrap());
        let csv = sqlite_prepare(db, &CString::new(CSV_QUERY).unwrap());
        let run = |exec: &mut Exec<'_>| {
            warm(exec).unwrap();
            db_status(db, DBSTATUS_LOOKASIDE_HIT);
            let mut run = measure(w.runs, exec).unwrap();
            run.lookaside = Some(db_status(db, DBSTATUS_LOOKASIDE_HIT).1 as usize);
            run
        };
        let runs = [
            run(&mut |i, t| sqlite_collect(json, w.group(i), t)),
            run(&mut |i, t| sqlite_parse(json, w.group(i), t, parse_json_owned)),
            run(&mut |i, t| sqlite_parse(csv, w.group(i), t, parse_csv_owned)),
        ];
        for stmt in [json, csv] {
            sqlite3_reset(stmt);
            sqlite3_finalize(stmt);
        }
        sqlite_exec(db, "commit");
        sqlite3_close(db);
        runs
    }

    fn print(w: &Workload, kite: &mut [Run; 3], lite: &mut [Run; 3]) {
        println!(
            "\ndoc {} B: {} docs per table; {} executions of {PER_GROUP} rows per scenario",
            w.doc_len,
            w.docs(),
            w.runs
        );
        let columns = |engine: &str| {
            format!(
                "{engine:>11} | {:>8} | {:>10} | {:>8}",
                "parse", "malloc/row", "KiB/exec"
            )
        };
        println!(
            "{:<8} | {} | {} | {:>13}",
            "scenario",
            columns("KiteSQL p50"),
            columns("SQLite p50"),
            "lookaside/row"
        );
        let rows = (w.runs * PER_GROUP) as f64;
        let p50 = |run: &mut Run| {
            run.ns.sort_unstable();
            run.ns[run.ns.len() / 2] as f64 / 1e3
        };
        let collect = [p50(&mut kite[0]), p50(&mut lite[0])];
        for (i, scenario) in ["collect", "json", "csv"].iter().enumerate() {
            let mut line = format!("{scenario:<8} |");
            for (run, collect) in [(&mut kite[i], collect[0]), (&mut lite[i], collect[1])] {
                let p50 = p50(run);
                let parse = if i == 0 {
                    "-".to_string()
                } else {
                    format!("{:.2}", p50 - collect)
                };
                line += &format!(
                    " {p50:>11.2} | {parse:>8} | {:>10.2} | {:>8.2} |",
                    run.mallocs as f64 / rows,
                    run.malloc_bytes as f64 / w.runs as f64 / 1024.0,
                );
            }
            let hits = lite[i].lookaside.unwrap_or_default();
            println!("{line} {:>13.2}", hits as f64 / rows);
        }
    }

    pub fn main() -> Result<(), DatabaseError> {
        let version = unsafe { CStr::from_ptr(sqlite3_libversion()) };
        println!("SQLite {} with mmap_size", version.to_string_lossy());
        println!(
            "parse: p50 minus the same engine's `collect` p50. malloc/row: malloc calls per row, \
             SQLite's lookaside allocations aside."
        );
        for doc_len in DOC_LENS {
            let w = Workload::new(doc_len);
            let docs: Vec<_> = (0..w.docs()).map(|id| w.doc(id)).collect();
            let dir = tempfile::TempDir::new()?;
            let mut kite = kitesql(&dir.path().join("kitesql"), &w, &docs)?;
            let path = CString::new(dir.path().join("docs.sqlite").to_str().unwrap()).unwrap();
            let mut lite = unsafe { sqlite(&path, &w, &docs) };
            for (kite, lite) in kite.iter().zip(&lite) {
                assert_eq!(
                    kite.totals, lite.totals,
                    "the engines kept different records"
                );
                assert_eq!(kite.totals.rows, w.runs * PER_GROUP);
            }
            assert_eq!(
                kite[1].totals, kite[2].totals,
                "JSON and CSV hold the same fields"
            );
            print(&w, &mut kite, &mut lite);
        }
        Ok(())
    }
}
