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

use crate::ast::Statement;
use crate::errors::DatabaseError;
#[cfg(feature = "parser")]
use crate::parser::generic::SqlParser;
use crate::types::LogicalType;
#[cfg(feature = "parser")]
use sqlparser::dialect::GenericDialect;
use std::fmt::Debug;

pub const DIALECT_GENERIC: &str = "generic";

/// SQL dialect trait: how SQL text becomes KiteSQL statements, and how they are typed.
///
/// [`parse`](Self::parse) owns the syntax: a dialect may use any parser and rewrite the
/// statements before returning them. The other methods customize binding and have
/// default implementations that keep the built-in behavior.
pub trait SqlDialect: Debug + Send + Sync + 'static {
    /// Returns the unique name of this dialect.
    fn name(&self) -> &str;

    /// Parses SQL text into KiteSQL statements.
    ///
    /// A dialect built on sqlparser can delegate to [`SqlParser`].
    fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError>;

    /// Returns the type both operands of a comparison are cast to before comparing.
    ///
    /// Called by the binder for `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN` lists and `BETWEEN`
    /// with the operand types. Return `Some(type)` to cast every operand to `type`, or
    /// `None` to keep the built-in coercion.
    ///
    /// The default implementation returns `None` for all types.
    fn comparison_type(&self, _left: &LogicalType, _right: &LogicalType) -> Option<LogicalType> {
        None
    }

    /// Returns the text of a boolean in `CAST(.. AS CHAR)` or `CAST(.. AS VARCHAR)`.
    ///
    /// Called by the binder, so the text is fixed when the statement is bound. Return
    /// `Some(text)` to override the text, or `None` to keep the built-in `'true'`/`'false'`.
    ///
    /// The default implementation returns `None`.
    fn boolean_text(&self, _value: bool) -> Option<&'static str> {
        None
    }
}

/// Standard SQL behavior with no customizations; the default dialect.
///
/// Parses with sqlparser's generic dialect, so it needs the `parser` feature.
#[derive(Debug, Default)]
pub struct GenericSqlDialect {
    recursion_limit: Option<usize>,
}

impl GenericSqlDialect {
    pub fn new() -> Self {
        Self::default()
    }

    /// Limits how deeply nested the parsed SQL may be; unset uses sqlparser's default.
    /// Higher limits may require a larger thread stack for deeply nested SQL.
    pub fn with_recursion_limit(mut self, limit: usize) -> Self {
        self.recursion_limit = Some(limit);
        self
    }
}

impl SqlDialect for GenericSqlDialect {
    fn name(&self) -> &str {
        DIALECT_GENERIC
    }

    #[cfg(feature = "parser")]
    fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError> {
        let mut parser = SqlParser::new(&GenericDialect {});
        if let Some(limit) = self.recursion_limit {
            if limit == 0 {
                return Err(DatabaseError::InvalidValue(
                    "parser recursion limit must be >= 1".to_string(),
                ));
            }
            parser = parser.with_recursion_limit(limit);
        }
        parser.parse(sql)
    }

    #[cfg(not(feature = "parser"))]
    fn parse(&self, _: &str) -> Result<Vec<Statement>, DatabaseError> {
        Err(DatabaseError::UnsupportedStmt(
            "parsing SQL with `GenericSqlDialect` requires the `parser` feature".to_string(),
        ))
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

    fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError> {
        self.current().parse(sql)
    }

    fn comparison_type(&self, left: &LogicalType, right: &LogicalType) -> Option<LogicalType> {
        self.current().comparison_type(left, right)
    }

    fn boolean_text(&self, value: bool) -> Option<&'static str> {
        self.current().boolean_text(value)
    }
}

#[cfg(all(test, feature = "parser", not(target_arch = "wasm32")))]
#[maybe_async]
mod tests {
    use super::*;
    use crate::ast::{Expr, Value, ValueWithSpan};
    use crate::db::{DataBaseBuilder, Database};
    use crate::parser::generic::DataTypeMapper;
    use crate::storage::Storage;
    use crate::types::value::DataValue;
    use sqlparser::ast::DataType;

    #[derive(Debug)]
    struct TestDialect;

    impl DataTypeMapper for TestDialect {
        fn map_data_type(
            &self,
            data_type: &DataType,
        ) -> Option<Result<LogicalType, DatabaseError>> {
            matches!(data_type, DataType::Custom(name, _) if name.to_string() == "test_int")
                .then_some(Ok(LogicalType::Integer))
        }
    }

    impl SqlDialect for TestDialect {
        fn name(&self) -> &str {
            "test"
        }

        fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError> {
            let mut statements = SqlParser::new(&GenericDialect {})
                .with_type_mapper(self)
                .parse(sql)?;
            // Rewrites every `LIMIT n` to `LIMIT 1`.
            for statement in statements.iter_mut() {
                if let Statement::Query(query) = statement {
                    if let Some(limit) = query.limit.as_mut().and_then(|limit| limit.limit.as_mut())
                    {
                        *limit = Expr::Value(ValueWithSpan {
                            value: Value::Number("1".to_string()),
                            span: Default::default(),
                        });
                    }
                }
            }
            Ok(statements)
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
            .dialect(TestDialect)
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
        dialect.register(TestDialect);
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
        dialect.register(TestDialect);
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

    #[derive(Debug)]
    struct CoercionDialect;

    impl SqlDialect for CoercionDialect {
        fn name(&self) -> &str {
            "test_coercion"
        }

        fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError> {
            GenericSqlDialect::new().parse(sql)
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

    #[test]
    async fn comparison_type_and_boolean_text() -> Result<(), DatabaseError> {
        let generic = DataBaseBuilder::path(".").build_in_memory().await?;
        let custom = DataBaseBuilder::path(".")
            .dialect(CoercionDialect)
            .build_in_memory()
            .await?;
        let sql = "select 10 < '9', 10 = '10.0', 10 in ('9', '10.0'), 10 between '9' and '11', \
                   cast(true as varchar), cast(1 = 0 as char(5)), \
                   cast(cast(null as boolean) as varchar) is null";
        let text = |rows: Vec<Vec<DataValue>>| -> Vec<String> {
            rows[0].iter().map(ToString::to_string).collect()
        };

        assert_eq!(
            text(query(&generic, sql).await?),
            ["true", "false", "false", "false", "true", "false", "true"]
        );
        assert_eq!(
            text(query(&custom, sql).await?),
            ["false", "true", "true", "true", "1", "0", "true"]
        );

        Ok(())
    }
}
