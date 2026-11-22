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

use crate::errors::DatabaseError;
use sqlparser::ast::Statement;
use sqlparser::dialect::{Dialect as SqlParserDialect, GenericDialect};
use std::fmt::Debug;

pub const DIALECT_GENERIC: &str = "generic";

/// SQL dialect trait for customizing parsing, type mapping, and statement rewriting.
///
/// Implement this trait to provide dialect-specific behavior. All methods have
/// default implementations that preserve standard SQL behavior.
pub trait SqlDialect: Debug + Send + Sync + 'static {
    /// Returns the unique name of this dialect.
    fn name(&self) -> &str;

    /// Returns the sqlparser dialect used for parsing SQL statements.
    fn parser_dialect(&self) -> &dyn SqlParserDialect;

    /// Rewrites a parsed SQL statement before it is bound.
    ///
    /// Called once for every statement parsed with this dialect. The default
    /// implementation performs no rewriting.
    fn rewrite(&self, _stmt: &mut Statement) -> Result<(), DatabaseError> {
        Ok(())
    }

    /// Maps a SQL data type to KiteSQL's internal logical type.
    ///
    /// Used for data types inside expressions, such as `CAST(.. AS type)` and typed
    /// string literals. Return `Some(Ok(type))` to override the default type mapping, `Some(Err(...))`
    /// to reject the type, or `None` to use the default mapping.
    ///
    /// The default implementation returns `None` for all types.
    fn map_data_type(
        &self,
        _data_type: &sqlparser::ast::DataType,
    ) -> Option<Result<crate::types::LogicalType, DatabaseError>> {
        None
    }

    /// Maps the data type of a column definition to KiteSQL's internal logical type.
    ///
    /// Used by `CREATE TABLE`, `ALTER TABLE .. ADD COLUMN` and column type changes; the
    /// return value follows the same convention as [`map_data_type`](Self::map_data_type).
    ///
    /// The default implementation delegates to [`map_data_type`](Self::map_data_type).
    fn map_column_type(
        &self,
        data_type: &sqlparser::ast::DataType,
    ) -> Option<Result<crate::types::LogicalType, DatabaseError>> {
        self.map_data_type(data_type)
    }
}

/// Standard SQL behavior with no customizations; the default dialect.
#[derive(Debug)]
pub struct GenericSqlDialect {
    parser: GenericDialect,
}

impl GenericSqlDialect {
    pub fn new() -> Self {
        Self {
            parser: GenericDialect {},
        }
    }
}

impl Default for GenericSqlDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDialect for GenericSqlDialect {
    fn name(&self) -> &str {
        DIALECT_GENERIC
    }

    fn parser_dialect(&self) -> &dyn SqlParserDialect {
        &self.parser
    }
}

/// A dialect chosen at runtime from a set of registered dialects.
///
/// Pass it to [`DataBaseBuilder::dialect`](crate::db::DataBaseBuilder::dialect) when the dialect
/// is only known at runtime; the default stays [`GenericSqlDialect`]. It starts with
/// [`GenericSqlDialect`] registered and current; [`register`](Self::register) adds dialects and
/// [`switch`](Self::switch) changes the current one. Every [`SqlDialect`] hook forwards to
/// [`current`](Self::current).
#[derive(Debug)]
pub struct DynDialect {
    dialects: Vec<Box<dyn SqlDialect>>,
    current: usize,
}

impl DynDialect {
    /// Creates a set containing only the generic dialect, which is current.
    pub fn new() -> Self {
        Self {
            dialects: vec![Box::new(GenericSqlDialect::new())],
            current: 0,
        }
    }

    /// Registers a dialect under its [`SqlDialect::name`], replacing any with the same name.
    pub fn register<D: SqlDialect + Send + Sync + 'static>(&mut self, dialect: D) {
        let dialect = Box::new(dialect);
        match self
            .dialects
            .iter()
            .position(|registered| registered.name() == dialect.name())
        {
            Some(index) => self.dialects[index] = dialect,
            None => self.dialects.push(dialect),
        }
    }

    /// Makes the dialect registered as `name` current; returns `false` if there is none.
    pub fn switch(&mut self, name: &str) -> bool {
        match self
            .dialects
            .iter()
            .position(|dialect| dialect.name() == name)
        {
            Some(index) => {
                self.current = index;
                true
            }
            None => false,
        }
    }

    /// Returns the current dialect.
    pub fn current(&self) -> &dyn SqlDialect {
        self.dialects[self.current].as_ref()
    }
}

impl Default for DynDialect {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlDialect for DynDialect {
    fn name(&self) -> &str {
        self.current().name()
    }

    fn parser_dialect(&self) -> &dyn SqlParserDialect {
        self.current().parser_dialect()
    }

    fn rewrite(&self, stmt: &mut Statement) -> Result<(), DatabaseError> {
        self.current().rewrite(stmt)
    }

    fn map_data_type(
        &self,
        data_type: &sqlparser::ast::DataType,
    ) -> Option<Result<crate::types::LogicalType, DatabaseError>> {
        self.current().map_data_type(data_type)
    }

    fn map_column_type(
        &self,
        data_type: &sqlparser::ast::DataType,
    ) -> Option<Result<crate::types::LogicalType, DatabaseError>> {
        self.current().map_column_type(data_type)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[maybe_async]
mod tests {
    use super::*;
    use crate::db::{DataBaseBuilder, Database};
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use sqlparser::ast::{DataType, Expr, LimitClause, Value};

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

        fn map_data_type(
            &self,
            data_type: &DataType,
        ) -> Option<Result<LogicalType, DatabaseError>> {
            matches!(data_type, DataType::Custom(name, _) if name.to_string() == "test_int")
                .then_some(Ok(LogicalType::Integer))
        }

        fn rewrite(&self, stmt: &mut Statement) -> Result<(), DatabaseError> {
            if let Statement::Query(query) = stmt {
                if let Some(LimitClause::LimitOffset {
                    limit: Some(limit), ..
                }) = &mut query.limit_clause
                {
                    *limit = Expr::Value(Value::Number("1".to_string(), false).into());
                }
            }
            Ok(())
        }
    }

    pub(super) async fn query<S: Storage, D: SqlDialect>(
        db: &Database<S, D>,
        sql: &str,
    ) -> Result<Vec<Vec<DataValue>>, DatabaseError> {
        let mut iter = db.run(sql).await?;
        let mut rows = Vec::new();
        while let Some(row) = iter.next_tuple(|_, row| row.take_values()).await? {
            rows.push(row);
        }
        iter.done().await?;
        Ok(rows)
    }

    #[test]
    async fn custom_dialect_maps_types_and_rewrites() -> Result<(), DatabaseError> {
        let mut generic = DataBaseBuilder::path(".").build_in_memory().await?;
        assert!(generic
            .ddl("create table t1 (id test_int primary key)")
            .await
            .is_err());

        let mut db = DataBaseBuilder::path(".")
            .dialect(TestDialect::default())
            .build_in_memory()
            .await?;
        db.ddl("create table t1 (id test_int primary key)").await?;
        db.run("insert into t1 values (1), (2)")
            .await?
            .done()
            .await?;
        let iter = db.run("select id from t1").await?;
        iter.schema(|schema| assert_eq!(schema.get(0).unwrap().datatype(), &LogicalType::Integer));
        iter.done().await?;
        assert_eq!(query(&db, "select id from t1 limit 10").await?.len(), 1);

        Ok(())
    }

    #[test]
    async fn dyn_dialect_switches_at_runtime() -> Result<(), DatabaseError> {
        let mut dialect = DynDialect::new();
        assert_eq!(dialect.name(), DIALECT_GENERIC);
        assert!(!dialect.switch("test"));
        dialect.register(TestDialect::default());
        assert!(dialect.switch("test"));

        let db = DataBaseBuilder::path(".")
            .dialect(dialect)
            .build_in_memory()
            .await?;
        assert_eq!(
            query(&db, "select cast(7 as test_int) limit 10").await?,
            vec![vec![DataValue::Int32(7)]]
        );

        Ok(())
    }

    #[test]
    async fn database_switches_dialect_after_build() -> Result<(), DatabaseError> {
        let mut dialect = DynDialect::new();
        dialect.register(TestDialect::default());
        let mut db = DataBaseBuilder::path(".")
            .dialect(dialect)
            .build_in_memory()
            .await?;

        let sql = "select cast(7 as test_int)";
        assert!(query(&db, sql).await.is_err());
        assert!(db.switch_dialect("test"));
        assert_eq!(query(&db, sql).await?, vec![vec![DataValue::Int32(7)]]);

        Ok(())
    }
}
