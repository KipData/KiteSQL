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

//! Parses SQL with [`sqlparser`] and converts the statements into KiteSQL's [`ast`].
//!
//! [`GenericSqlDialect`](crate::dialect::GenericSqlDialect) parses this way. A custom
//! dialect can use [`SqlParser`] with another sqlparser dialect or its own type mapping.

mod expr;
mod query;
mod statement;
mod types;

pub use types::DataTypeMapper;

use crate::ast::{self, Statement};
use crate::errors::DatabaseError;
use crate::types::LogicalType;
use sqlparser::ast::DataType;
use sqlparser::dialect::{Dialect as SqlParserDialect, GenericDialect};
use sqlparser::parser::Parser;
use sqlparser::tokenizer;

/// Parses SQL with a sqlparser dialect into KiteSQL statements.
pub struct SqlParser<'a> {
    dialect: &'a dyn SqlParserDialect,
    recursion_limit: Option<usize>,
    types: &'a dyn DataTypeMapper,
}

impl<'a> SqlParser<'a> {
    /// A parser with the built-in type mapping and sqlparser's default recursion limit.
    pub fn new(dialect: &'a dyn SqlParserDialect) -> Self {
        Self {
            dialect,
            recursion_limit: None,
            types: &(),
        }
    }

    /// Limits how deeply nested the parsed SQL may be.
    pub fn with_recursion_limit(mut self, limit: usize) -> Self {
        self.recursion_limit = Some(limit);
        self
    }

    /// Maps data types with `types` before the built-in mapping.
    pub fn with_type_mapper(mut self, types: &'a dyn DataTypeMapper) -> Self {
        self.types = types;
        self
    }

    pub fn parse(&self, sql: &str) -> Result<Vec<Statement>, DatabaseError> {
        let mut parser = Parser::new(self.dialect);
        if let Some(limit) = self.recursion_limit {
            parser = parser.with_recursion_limit(limit);
        }
        let lower = Lower { types: self.types };
        parser
            .try_with_sql(sql)?
            .parse_statements()?
            .into_iter()
            .map(|statement| lower.statement(statement))
            .collect()
    }
}

/// Parses SQL with sqlparser's generic dialect.
///
/// # Example
/// ```rust
/// use kite_sql::parser::generic::parse_sql;
/// let sql = "SELECT a, b, 123, myfunc(b) \
///            FROM table_1 \
///            WHERE a > b AND b < 100 \
///            ORDER BY a DESC, b";
/// let ast = parse_sql(sql).unwrap();
/// println!("{:?}", ast);
/// ```
pub fn parse_sql<S: AsRef<str>>(sql: S) -> Result<Vec<Statement>, DatabaseError> {
    SqlParser::new(&GenericDialect {}).parse(sql.as_ref())
}

/// Converts sqlparser statements into KiteSQL statements.
struct Lower<'a> {
    types: &'a dyn DataTypeMapper,
}

impl Lower<'_> {
    fn data_type(&self, data_type: DataType) -> Result<LogicalType, DatabaseError> {
        match self.types.map_data_type(&data_type) {
            Some(result) => result,
            None => types::logical_type(data_type),
        }
    }

    fn column_type(&self, data_type: DataType) -> Result<LogicalType, DatabaseError> {
        match self.types.map_column_type(&data_type) {
            Some(result) => result,
            None => types::logical_type(data_type),
        }
    }
}

fn span(span: tokenizer::Span) -> ast::Span {
    let location = |location: tokenizer::Location| ast::Location {
        line: location.line,
        column: location.column,
    };
    ast::Span::new(location(span.start), location(span.end))
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    DatabaseError::UnsupportedStmt(message.into())
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::parse_sql;
    use crate::ast::{SetExpr, Statement};

    #[test]
    fn parses_optimizer_hint() {
        let statements = parse_sql("SELECT /*+ FORCE_AGG_SPILL */ a FROM t").unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query statement");
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select query");
        };

        assert_eq!(
            select.optimizer_hint.as_deref().map(str::trim),
            Some("FORCE_AGG_SPILL")
        );
    }
}
// GRCOV_EXCL_STOP
