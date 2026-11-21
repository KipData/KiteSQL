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
use std::sync::Arc;

pub const DIALECT_GENERIC: &str = "generic";

/// Returned by [`SqlDialect::map_column_type`]: the logical type used for storage and
/// computation, plus an optional display name (e.g. `VARCHAR(100)`) that is not stored in the
/// catalog.
#[derive(Debug, Clone)]
pub struct ColumnTypeSpec {
    pub logical: crate::types::LogicalType,
    pub display: Option<Arc<str>>,
}

impl ColumnTypeSpec {
    pub fn new(logical: crate::types::LogicalType) -> Self {
        Self {
            logical,
            display: None,
        }
    }

    pub fn with_display(mut self, display: impl Into<Arc<str>>) -> Self {
        self.display = Some(display.into());
        self
    }
}

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

    /// Maps the data type of a column definition to a column type specification.
    ///
    /// Used by `CREATE TABLE`, `ALTER TABLE .. ADD COLUMN` and column type changes; the
    /// return value follows the same convention as [`map_data_type`](Self::map_data_type).
    /// The binder uses [`ColumnTypeSpec::logical`]; `display` is for callers that keep
    /// their own type names and is not stored in the catalog.
    ///
    /// The default implementation delegates to [`map_data_type`](Self::map_data_type)
    /// and wraps the result in a [`ColumnTypeSpec`] without display metadata.
    fn map_column_type(
        &self,
        data_type: &sqlparser::ast::DataType,
    ) -> Option<Result<ColumnTypeSpec, DatabaseError>> {
        self.map_data_type(data_type)
            .map(|result| result.map(ColumnTypeSpec::new))
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
    ) -> Option<Result<ColumnTypeSpec, DatabaseError>> {
        self.current().map_column_type(data_type)
    }
}
