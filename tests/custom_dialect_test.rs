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
