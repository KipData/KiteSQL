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
use kite_sql::types::value::DataValue;
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

/// Test-only dialect: compares numbers with strings as `DOUBLE` and casts booleans to
/// `'1'`/`'0'`.
#[derive(Debug, Default)]
struct CoercionDialect {
    parser: GenericDialect,
}

impl SqlDialect for CoercionDialect {
    fn name(&self) -> &str {
        "test_coercion"
    }

    fn parser_dialect(&self) -> &dyn SqlParserDialect {
        &self.parser
    }

    fn comparison_type(&self, left: &LogicalType, right: &LogicalType) -> Option<LogicalType> {
        let is_text =
            |ty: &LogicalType| matches!(ty, LogicalType::Char(..) | LogicalType::Varchar(..));
        ((left.is_numeric() && is_text(right)) || (is_text(left) && right.is_numeric()))
            .then_some(LogicalType::Double)
    }

    fn boolean_text(&self, value: bool) -> Option<&'static str> {
        Some(if value { "1" } else { "0" })
    }
}

fn first_value<D: SqlDialect>(
    db: &Database<MemoryStorage, D>,
    sql: &str,
) -> Result<DataValue, DatabaseError> {
    let mut iter = db.run(sql)?;
    let value = iter
        .next_tuple(|_, row| row.take_value(0))?
        .flatten()
        .expect("query returned no rows");
    iter.done()?;
    Ok(value)
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
fn custom_dialect_sets_comparison_type() -> Result<(), DatabaseError> {
    let generic = DataBaseBuilder::path(".").build_in_memory()?;
    let custom = database_with(CoercionDialect::default())?;

    // (query, result with the built-in coercion, result with `CoercionDialect`)
    let cases = [
        ("select 10 < '9'", true, false),
        ("select '9' > 10", true, false),
        ("select 10 = '10.0'", false, true),
        ("select 10 in ('10.0', '11')", false, true),
        ("select 10 between '9' and '11'", false, true),
        ("select 1 = 1", true, true),
        ("select 'a' < 'b'", true, true),
    ];
    for (sql, expected_generic, expected_custom) in cases {
        assert_eq!(
            first_value(&generic, sql)?,
            DataValue::Boolean(expected_generic),
            "generic: {sql}"
        );
        assert_eq!(
            first_value(&custom, sql)?,
            DataValue::Boolean(expected_custom),
            "custom: {sql}"
        );
    }

    let mut custom = custom;
    custom.ddl("create table t1 (id int primary key, v varchar(10))")?;
    custom
        .run("insert into t1 values (1, '10'), (2, '9'), (3, '8.5')")?
        .done()?;
    assert_eq!(count_rows(&custom, "select id from t1 where v > 9")?, 1);
    assert_eq!(count_rows(&custom, "select id from t1 where v = 8.5")?, 1);
    assert_eq!(
        count_rows(&custom, "select id from t1 where v in (9, 10)")?,
        2
    );

    Ok(())
}

#[test]
fn custom_dialect_sets_boolean_text() -> Result<(), DatabaseError> {
    let mut generic = DataBaseBuilder::path(".").build_in_memory()?;
    let mut custom = database_with(CoercionDialect::default())?;

    let create = "create table t1 (id int primary key, b boolean null)";
    let insert = "insert into t1 values (1, true), (2, false), (3, null)";
    generic.ddl(create)?;
    generic.run(insert)?.done()?;
    custom.ddl(create)?;
    custom.run(insert)?.done()?;
    for (sql, expected_generic, expected_custom) in [
        ("select cast(true as varchar)", "true", "1"),
        ("select cast(false as varchar)", "false", "0"),
        ("select cast(1 = 1 as char(4))", "true", "1"),
        (
            "select cast(b as varchar) from t1 where id = 1",
            "true",
            "1",
        ),
        (
            "select cast(b as varchar) from t1 where id = 2",
            "false",
            "0",
        ),
    ] {
        assert_eq!(
            first_value(&generic, sql)?.to_string(),
            expected_generic,
            "generic: {sql}"
        );
        assert_eq!(
            first_value(&custom, sql)?.to_string(),
            expected_custom,
            "custom: {sql}"
        );
    }
    let sql = "select cast(b as varchar) from t1 where id = 3";
    assert_eq!(first_value(&generic, sql)?, DataValue::Null);
    assert_eq!(first_value(&custom, sql)?, DataValue::Null);

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
    assert!(dialect
        .comparison_type(
            &LogicalType::Integer,
            &LogicalType::Varchar(None, kite_sql::types::CharLengthUnits::Characters)
        )
        .is_none());
    assert!(dialect.boolean_text(true).is_none());

    let original = sqlparser::parser::Parser::new(dialect.parser_dialect())
        .try_with_sql("select 1 limit 10")?
        .parse_statement()?;
    let mut stmt = original.clone();
    dialect.rewrite(&mut stmt)?;
    assert_eq!(stmt, original);

    Ok(())
}
