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

//! Parses SQL as SQLite does, with [`sqlite3_parser`], and converts the statements into
//! KiteSQL's [`ast`](crate::ast).
//!
//! It covers queries, `INSERT`/`UPDATE`/`DELETE`, `CREATE`/`DROP` of tables, indexes and
//! views, `ALTER TABLE` column changes, `ANALYZE` and `EXPLAIN`. SQLite syntax KiteSQL does
//! not support is rejected. Its syntax tree records no source positions, so errors carry no
//! span.

mod expr;
mod query;
mod statement;
mod types;

use crate::ast::{self, Statement};
use crate::errors::DatabaseError;
use sqlite3_parser::ast::{Name, QualifiedName};
use sqlite3_parser::lexer::sql::Parser;
use sqlite3_parser::{Bump, FallibleIterator};

/// Parses SQLite SQL into KiteSQL statements.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, DatabaseError> {
    let bump = Bump::new();
    let mut parser = Parser::new(&bump, sql.as_bytes());
    let mut statements = Vec::new();
    while let Some(cmd) = parser
        .next()
        .map_err(|err| DatabaseError::ParserSql(err.to_string()))?
    {
        statements.push(statement::cmd(&cmd)?);
    }
    Ok(statements)
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    DatabaseError::UnsupportedStmt(message.into())
}

/// Removes the quotes of `"name"`, `[name]`, `` `name` `` or `'name'`, with a doubled quote
/// standing for one.
fn unquote(text: &str) -> (String, Option<char>) {
    let mut chars = text.chars();
    let (Some(open), Some(close)) = (chars.next(), chars.next_back()) else {
        return (text.to_string(), None);
    };
    let expected = match open {
        '"' | '`' | '\'' => open,
        '[' => ']',
        _ => return (text.to_string(), None),
    };
    if close != expected {
        return (text.to_string(), None);
    }
    let inner = chars.as_str();
    let value = if open == '[' {
        inner.to_string()
    } else {
        inner.replace(&format!("{open}{open}"), &open.to_string())
    };
    (value, Some(open))
}

fn ident(text: &str) -> ast::Ident {
    let (value, quote_style) = unquote(text);
    ast::Ident {
        value,
        quote_style,
        span: ast::Span::empty(),
    }
}

fn name(name: &Name<'_>) -> ast::Ident {
    ident(name.0)
}

fn object_name(name: &Name<'_>) -> ast::ObjectName {
    ast::ObjectName(vec![self::name(name)])
}

fn qualified_name(name: &QualifiedName<'_>) -> ast::ObjectName {
    let mut parts = Vec::with_capacity(2);
    if let Some(db_name) = &name.db_name {
        parts.push(self::name(db_name));
    }
    parts.push(self::name(&name.name));
    ast::ObjectName(parts)
}
