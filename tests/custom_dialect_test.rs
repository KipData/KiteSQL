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

// These tests use the sync API, which needs the SQL parser and excludes `async` and wasm32.
#![cfg(all(
    feature = "parser",
    not(feature = "async"),
    not(target_arch = "wasm32")
))]

use kite_sql::db::{DataBaseBuilder, Database};
use kite_sql::dialect::{DynDialect, GenericSqlDialect, SqlDialect, DIALECT_GENERIC};
use kite_sql::errors::DatabaseError;
use kite_sql::storage::memory::MemoryStorage;
use kite_sql::types::LogicalType;
use sqlparser::ast::{DataType, Expr, LimitClause, Statement, Value};
use sqlparser::dialect::{Dialect as SqlParserDialect, GenericDialect};

const LIMIT_CAP: usize = 2;

/// Test-only dialect: maps `TEST_INT` to `INTEGER`, rejects `TEST_UNSUPPORTED`, and caps
/// every `LIMIT` at [`LIMIT_CAP`].
#[derive(Debug, Default)]
struct TestDialect {
    parser: GenericDialect,
}

impl SqlDialect for TestDialect {
    fn name(&self) -> &str {
        "test"
    }

    fn parser_dialect(&self) -> &dyn SqlParserDialect {
        &self.parser
    }

    fn map_data_type(&self, data_type: &DataType) -> Option<Result<LogicalType, DatabaseError>> {
        let DataType::Custom(name, _) = data_type else {
            return None;
        };
        match name.to_string().to_ascii_lowercase().as_str() {
            "test_int" => Some(Ok(LogicalType::Integer)),
            "test_unsupported" => Some(Err(DatabaseError::UnsupportedStmt(
                "test_unsupported".to_string(),
            ))),
            _ => None,
        }
    }

    fn rewrite(&self, stmt: &mut Statement) -> Result<(), DatabaseError> {
        if let Statement::Query(query) = stmt {
            if let Some(LimitClause::LimitOffset {
                limit: Some(limit), ..
            }) = &mut query.limit_clause
            {
                *limit = Expr::Value(Value::Number(LIMIT_CAP.to_string(), false).into());
            }
        }
        Ok(())
    }
}

fn database_with<D: SqlDialect>(
    dialect: D,
) -> Result<Database<MemoryStorage, DynDialect>, DatabaseError> {
    let name = dialect.name().to_string();
    let mut dynamic = DynDialect::new();
    dynamic.register(dialect);
    assert!(dynamic.switch(&name));

    DataBaseBuilder::path(".")
        .dialect(dynamic)
        .build_in_memory()
}

fn test_database() -> Result<Database<MemoryStorage, DynDialect>, DatabaseError> {
    database_with(TestDialect::default())
}

fn first_column_type<D: SqlDialect>(
    db: &Database<MemoryStorage, D>,
    sql: &str,
) -> Result<LogicalType, DatabaseError> {
    let iter = db.run(sql)?;
    let ty = iter.schema(|schema| schema.get(0).map(|column| column.datatype().clone()));
    iter.done()?;
    Ok(ty.expect("result has no columns"))
}

fn count_rows<D: SqlDialect>(
    db: &Database<MemoryStorage, D>,
    sql: &str,
) -> Result<usize, DatabaseError> {
    let mut iter = db.run(sql)?;
    let mut rows = 0;
    while iter.next_tuple(|_, _| ())?.is_some() {
        rows += 1;
    }
    iter.done()?;
    Ok(rows)
}

#[test]
fn default_dialect_is_generic() -> Result<(), DatabaseError> {
    let mut db: Database<MemoryStorage, GenericSqlDialect> =
        DataBaseBuilder::path(".").build_in_memory()?;
    db.ddl("create table t1 (id int primary key)")?;
    db.run("insert into t1 values (1), (2), (3)")?.done()?;
    assert_eq!(count_rows(&db, "select * from t1 limit 10")?, 3);
    assert!(db.ddl("create table t2 (id test_int primary key)").is_err());

    Ok(())
}

#[test]
fn custom_dialect_maps_column_types() -> Result<(), DatabaseError> {
    let mut db = test_database()?;
    db.ddl("create table t1 (id test_int primary key)")?;
    db.run("insert into t1 values (1)")?.done()?;
    assert_eq!(
        first_column_type(&db, "select id from t1")?,
        LogicalType::Integer
    );

    db.ddl("alter table t1 add column v test_int")?;
    assert_eq!(
        first_column_type(&db, "select v from t1")?,
        LogicalType::Integer
    );

    let err = db
        .ddl("create table t2 (id test_unsupported primary key)")
        .unwrap_err();
    assert!(err.to_string().contains("test_unsupported"), "{err}");

    Ok(())
}

#[test]
fn custom_dialect_maps_cast_types() -> Result<(), DatabaseError> {
    let db = test_database()?;
    assert_eq!(
        first_column_type(&db, "select cast('7' as test_int)")?,
        LogicalType::Integer
    );

    Ok(())
}

#[test]
fn custom_dialect_rewrites_statements() -> Result<(), DatabaseError> {
    let mut db = test_database()?;
    db.ddl("create table t1 (id int primary key)")?;
    db.run("insert into t1 values (1), (2), (3), (4), (5)")?
        .done()?;

    assert_eq!(count_rows(&db, "select * from t1")?, 5);
    assert_eq!(count_rows(&db, "select * from t1 limit 100")?, LIMIT_CAP);

    Ok(())
}

#[test]
fn static_dialect_behaves_like_dyn_dialect() -> Result<(), DatabaseError> {
    let mut db = DataBaseBuilder::path(".")
        .dialect(TestDialect::default())
        .build_in_memory()?;
    db.ddl("create table t1 (id test_int primary key)")?;
    db.run("insert into t1 values (1), (2), (3)")?.done()?;
    assert_eq!(
        first_column_type(&db, "select id from t1")?,
        LogicalType::Integer
    );
    assert_eq!(count_rows(&db, "select * from t1 limit 100")?, LIMIT_CAP);

    Ok(())
}

#[test]
fn dyn_dialect_switches_between_registered_dialects() {
    let mut dialect = DynDialect::new();
    assert!(!dialect.switch("test"));
    assert_eq!(dialect.current().name(), DIALECT_GENERIC);

    dialect.register(TestDialect::default());
    assert!(dialect.switch("test"));
    assert_eq!(dialect.name(), "test");
    assert!(dialect
        .map_data_type(&DataType::Custom(
            sqlparser::ast::ObjectName::from(vec![sqlparser::ast::Ident::new("test_int")]),
            vec![]
        ))
        .is_some());

    assert!(dialect.switch(DIALECT_GENERIC));
    assert_eq!(dialect.name(), DIALECT_GENERIC);
}

#[test]
fn generic_dialect_is_noop() -> Result<(), DatabaseError> {
    let dialect = GenericSqlDialect::new();
    assert_eq!(dialect.name(), DIALECT_GENERIC);
    assert!(dialect.map_data_type(&DataType::Integer(None)).is_none());
    assert!(dialect.map_column_type(&DataType::Integer(None)).is_none());

    let original = sqlparser::parser::Parser::new(dialect.parser_dialect())
        .try_with_sql("select 1 limit 10")?
        .parse_statement()?;
    let mut stmt = original.clone();
    dialect.rewrite(&mut stmt)?;
    assert_eq!(stmt, original);

    Ok(())
}

mod upsert {
    use super::*;
    use kite_sql::storage::Storage;

    /// Test-only dialect: accepts `ON DUPLICATE KEY UPDATE` and keeps the default affected rows.
    #[derive(Debug, Default)]
    struct UpsertDialect {
        parser: GenericDialect,
    }

    impl SqlDialect for UpsertDialect {
        fn name(&self) -> &str {
            "test_upsert"
        }

        fn parser_dialect(&self) -> &dyn SqlParserDialect {
            &self.parser
        }

        fn supports_on_duplicate_key_update(&self) -> bool {
            true
        }
    }

    /// Test-only dialect: accepts `ON DUPLICATE KEY UPDATE` and reports 1 for an inserted row,
    /// 2 for a changed row and 0 for an unchanged one.
    #[derive(Debug, Default)]
    struct UpsertCountsDialect {
        parser: GenericDialect,
    }

    impl SqlDialect for UpsertCountsDialect {
        fn name(&self) -> &str {
            "test_upsert_counts"
        }

        fn parser_dialect(&self) -> &dyn SqlParserDialect {
            &self.parser
        }

        fn supports_on_duplicate_key_update(&self) -> bool {
            true
        }

        fn upsert_affected_rows(&self, inserted: bool, updated: bool) -> u64 {
            match (inserted, updated) {
                (true, _) => 1,
                (false, true) => 2,
                (false, false) => 0,
            }
        }
    }

    fn upsert_database() -> Result<Database<MemoryStorage, DynDialect>, DatabaseError> {
        database_with(UpsertDialect::default())
    }

    fn rows<S: Storage, D: SqlDialect>(
        db: &Database<S, D>,
        sql: &str,
    ) -> Result<Vec<Vec<String>>, DatabaseError> {
        let mut iter = db.run(sql)?;
        let mut rows = Vec::new();
        while let Some(row) = iter.next_tuple(|_, row| {
            row.take_values()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })? {
            rows.push(row);
        }
        iter.done()?;
        Ok(rows)
    }

    fn affected<S: Storage, D: SqlDialect>(
        db: &Database<S, D>,
        sql: &str,
    ) -> Result<String, DatabaseError> {
        let mut result = rows(db, sql)?;
        assert_eq!(result.len(), 1, "{sql}");
        Ok(result.remove(0).remove(0))
    }

    fn run_err<S: Storage, D: SqlDialect>(db: &Database<S, D>, sql: &str) -> DatabaseError {
        match db.run(sql).and_then(|iter| iter.done()) {
            Ok(()) => panic!("expected `{sql}` to fail"),
            Err(err) => err,
        }
    }

    fn table<S: Storage, D: SqlDialect>(
        db: &Database<S, D>,
        sql: &str,
    ) -> Result<Vec<String>, DatabaseError> {
        Ok(rows(db, sql)?
            .into_iter()
            .map(|row| row.join(" "))
            .collect())
    }

    #[test]
    fn generic_dialect_rejects_on_duplicate_key_update() -> Result<(), DatabaseError> {
        let mut db = DataBaseBuilder::path(".").build_in_memory()?;
        db.ddl("create table t (id int primary key, v int)")?;
        db.run("insert into t values (1, 10)")?.done()?;

        let err = run_err(
            &db,
            "insert into t values (1, 20) on duplicate key update v = 20",
        );
        assert!(
            err.to_string()
                .contains("INSERT ON DUPLICATE KEY UPDATE is not supported"),
            "{err}"
        );
        assert_eq!(table(&db, "select * from t")?, ["1 10"]);

        Ok(())
    }

    #[test]
    fn on_conflict_stays_rejected() -> Result<(), DatabaseError> {
        let mut db = upsert_database()?;
        db.ddl("create table t (id int primary key, v int)")?;

        let err = run_err(
            &db,
            "insert into t values (1, 20) on conflict (id) do update set v = 20",
        );
        assert!(
            err.to_string()
                .contains("INSERT ON CONFLICT is not supported"),
            "{err}"
        );

        Ok(())
    }

    #[test]
    fn upsert_updates_primary_key_conflicts() -> Result<(), DatabaseError> {
        let mut db = upsert_database()?;
        db.ddl("create table t (id int primary key, v int, c int)")?;
        db.run("insert into t values (1, 10, 0), (2, 20, 0), (3, 30, 0)")?
            .done()?;

        db.run(
        "insert into t values (1, 100, 0), (2, 200, 0), (4, 40, 0) on duplicate key update v = 999",
    )?
    .done()?;
        assert_eq!(
            table(&db, "select * from t order by id")?,
            ["1 999 0", "2 999 0", "3 30 0", "4 40 0"]
        );

        db.run(
            "insert into t values (1, 3, 0) on duplicate key update v = v + values(v), c = c + 1",
        )?
        .done()?;
        assert_eq!(table(&db, "select * from t where id = 1")?, ["1 1002 1"]);

        // A later row of the same statement conflicts with an earlier one.
        db.run(
            "insert into t values (5, 1, 0), (5, 5, 0) on duplicate key update v = v + values(v)",
        )?
        .done()?;
        assert_eq!(table(&db, "select * from t where id = 5")?, ["5 6 0"]);

        db.run(
        "insert into t select id, 7, 7 from t where id < 3 on duplicate key update c = values(c)",
    )?
    .done()?;
        assert_eq!(
            table(&db, "select id, c from t where id < 3 order by id")?,
            ["1 7", "2 7"]
        );

        Ok(())
    }

    #[test]
    fn upsert_updates_unique_key_conflicts() -> Result<(), DatabaseError> {
        let mut db = upsert_database()?;
        db.ddl("create table u (id int primary key, email varchar(50) unique, n int)")?;
        db.run("insert into u values (1, 'a', 0)")?.done()?;

        db.run("insert into u values (2, 'a', 0) on duplicate key update n = n + 1")?
            .done()?;
        assert_eq!(table(&db, "select * from u")?, ["1 a 1"]);

        // A NULL in a unique key never matches, so the row is inserted.
        db.run("insert into u values (3, null, 5) on duplicate key update n = n + 1")?
            .done()?;
        db.run("insert into u values (4, null, 6) on duplicate key update n = n + 1")?
            .done()?;
        assert_eq!(
            table(&db, "select * from u order by id")?,
            ["1 a 1", "3 NULL 5", "4 NULL 6"]
        );

        db.ddl("create table cu (id varchar(10) primary key, a varchar(10), b varchar(10), v varchar(10), unique (a, b))")?;
        db.run("insert into cu values ('k1', 'x', 'y', 'old')")?
            .done()?;
        db.run(
            "insert into cu values ('k2', 'x', 'y', 'new') on duplicate key update v = values(v)",
        )?
        .done()?;
        db.run("insert into cu values ('k3', 'x', null, 'n') on duplicate key update v = 'never'")?
            .done()?;
        assert_eq!(
            table(&db, "select * from cu order by id")?,
            ["k1 x y new", "k3 x NULL n"]
        );

        Ok(())
    }

    #[test]
    fn upsert_reads_the_row_alias() -> Result<(), DatabaseError> {
        let mut db = upsert_database()?;
        db.ddl("create table t (id int primary key, v int)")?;
        db.run("insert into t values (1, 1)")?.done()?;

        db.run("insert into t values (1, 7) as new on duplicate key update v = new.v")?
            .done()?;
        assert_eq!(table(&db, "select * from t")?, ["1 7"]);

        db.run("insert into t values (1, 8) as New on duplicate key update v = NEW.v + values(V)")?
            .done()?;
        assert_eq!(table(&db, "select * from t")?, ["1 16"]);

        let err = run_err(
            &db,
            "insert into t values (1, 1) as n (a, b) on duplicate key update v = n.b",
        );
        assert!(err.to_string().contains("column aliases"), "{err}");

        Ok(())
    }

    // Memory storage writes in place, so a failed statement needs a storage that rolls back.
    #[cfg(feature = "rocksdb")]
    #[test]
    fn upsert_moves_the_primary_key() -> Result<(), DatabaseError> {
        let dir = tempfile::TempDir::new().unwrap();
        let mut dialect = DynDialect::new();
        dialect.register(UpsertDialect::default());
        assert!(dialect.switch("test_upsert"));
        let mut db = DataBaseBuilder::path(dir.path())
            .dialect(dialect)
            .build_rocksdb()?;
        db.ddl("create table t (id int primary key, v int)")?;
        db.run("insert into t values (1, 10), (2, 20)")?.done()?;

        db.run("insert into t values (1, 0) on duplicate key update id = 50")?
            .done()?;
        assert_eq!(
            table(&db, "select * from t order by id")?,
            ["2 20", "50 10"]
        );
        assert!(table(&db, "select * from t where id = 1")?.is_empty());
        assert_eq!(table(&db, "select * from t where id = 50")?, ["50 10"]);

        // Moving onto an existing key fails like any other duplicate primary key, and the
        // statement's writes are rolled back.
        let err = run_err(
            &db,
            "insert into t values (2, 0) on duplicate key update id = 50",
        );
        assert!(matches!(err, DatabaseError::DuplicatePrimaryKey), "{err}");
        assert_eq!(
            table(&db, "select * from t order by id")?,
            ["2 20", "50 10"]
        );

        Ok(())
    }

    #[test]
    fn upsert_affected_rows_default_to_one_per_row() -> Result<(), DatabaseError> {
        let mut db = upsert_database()?;
        db.ddl("create table t (id int primary key, v int)")?;
        db.run("insert into t values (1, 10)")?.done()?;

        let upsert =
            "insert into t values (1, 10), (1, 20), (2, 5) on duplicate key update v = values(v)";
        assert_eq!(affected(&db, upsert)?, "3");
        assert_eq!(table(&db, "select * from t order by id")?, ["1 20", "2 5"]);

        Ok(())
    }

    #[test]
    fn upsert_affected_rows_follow_the_dialect() -> Result<(), DatabaseError> {
        let mut db = database_with(UpsertCountsDialect::default())?;
        db.ddl("create table t (id int primary key, v int)")?;
        db.run("insert into t values (1, 10)")?.done()?;

        let upsert = |values: &str| {
            format!("insert into t values {values} on duplicate key update v = values(v)")
        };
        assert_eq!(affected(&db, &upsert("(1, 10)"))?, "0");
        assert_eq!(affected(&db, &upsert("(1, 20)"))?, "2");
        assert_eq!(affected(&db, &upsert("(2, 5)"))?, "1");
        assert_eq!(affected(&db, &upsert("(3, 1), (3, 2)"))?, "3");
        // A plain insert is not an upsert and keeps counting one per row.
        assert_eq!(affected(&db, "insert into t values (4, 1), (5, 1)")?, "2");

        Ok(())
    }
}
