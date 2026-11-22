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

//! KiteSQL's SQL syntax tree.
//!
//! A [`SqlDialect`](crate::dialect::SqlDialect) parses SQL text into these statements, and
//! the binder turns them into logical plans. The tree holds only the syntax KiteSQL
//! supports: a dialect rejects anything else while building it. Type names are already
//! resolved to [`LogicalType`](crate::types::LogicalType)s.

mod expr;
mod query;
mod statement;

pub use expr::*;
pub use query::*;
pub use statement::*;

use crate::errors::{DatabaseError, SqlErrorSpan};
use std::fmt;

/// A position in the SQL text; lines and columns start at 1, and `0` means unknown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Location {
    pub line: u64,
    pub column: u64,
}

/// The source range of a syntax node, used to point errors at the SQL text.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Span {
    pub start: Location,
    pub end: Location,
}

impl Span {
    pub const fn new(start: Location, end: Location) -> Self {
        Self { start, end }
    }

    /// A span that points nowhere.
    pub const fn empty() -> Self {
        Self {
            start: Location { line: 0, column: 0 },
            end: Location { line: 0, column: 0 },
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::empty()
    }

    /// The smallest span covering both spans; an empty span covers nothing.
    pub fn union(self, other: Self) -> Self {
        let key = |location: Location| (location.line, location.column);
        match (self.is_empty(), other.is_empty()) {
            (true, _) => other,
            (_, true) => self,
            _ => Self {
                start: std::cmp::min_by_key(self.start, other.start, |l| key(*l)),
                end: std::cmp::max_by_key(self.end, other.end, |l| key(*l)),
            },
        }
    }

    fn sql_error_span(self) -> Option<SqlErrorSpan> {
        if self.is_empty() {
            return None;
        }
        let start = self.start.column as usize;
        let mut end = self.end.column as usize;
        if end <= start {
            end = start.saturating_add(1);
        }
        Some(SqlErrorSpan {
            start,
            end,
            line: self.start.line as usize,
            highlight: None,
        })
    }
}

/// Points `err` at `span` unless it already points somewhere.
pub(crate) fn attach_span_if_absent(err: DatabaseError, span: Span) -> DatabaseError {
    if err.sql_error_span().is_some() {
        return err;
    }
    match span.sql_error_span() {
        Some(span) => err.with_span(span),
        None => err,
    }
}

/// An identifier as written; the binder decides how its case folds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Ident {
    pub value: String,
    /// The opening quote, if the identifier was quoted.
    pub quote_style: Option<char>,
    pub span: Span,
}

impl Ident {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            quote_style: None,
            span: Span::empty(),
        }
    }
}

impl fmt::Display for Ident {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.quote_style {
            Some(quote) => {
                let close = if quote == '[' { ']' } else { quote };
                write!(f, "{quote}{}{close}", self.value)
            }
            None => f.write_str(&self.value),
        }
    }
}

/// A possibly qualified name, such as `table` or `schema.table`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectName(pub Vec<Ident>);

impl ObjectName {
    pub fn span(&self) -> Span {
        self.0
            .iter()
            .fold(Span::empty(), |span, ident| span.union(ident.span))
    }
}

impl fmt::Display for ObjectName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, ident) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(".")?;
            }
            write!(f, "{ident}")?;
        }
        Ok(())
    }
}

// GRCOV_EXCL_START
#[cfg(test)]
mod tests {
    use super::{attach_span_if_absent, Location, Span};
    use crate::errors::{DatabaseError, SqlErrorSpan};

    #[test]
    fn attach_span_if_absent_keeps_an_existing_span() {
        let at = |line, column| Location { line, column };
        let err = attach_span_if_absent(
            DatabaseError::invalid_column("bad"),
            Span::new(at(3, 5), at(3, 5)),
        );
        assert_eq!(
            err.sql_error_span(),
            Some(&SqlErrorSpan {
                start: 5,
                end: 6,
                line: 3,
                highlight: None,
            })
        );

        let existing = SqlErrorSpan {
            start: 1,
            end: 2,
            line: 1,
            highlight: None,
        };
        let err = attach_span_if_absent(
            DatabaseError::invalid_column("bad").with_span(existing.clone()),
            Span::new(at(4, 8), at(4, 9)),
        );
        assert_eq!(err.sql_error_span(), Some(&existing));

        let err = attach_span_if_absent(DatabaseError::invalid_column("bad"), Span::empty());
        assert_eq!(err.sql_error_span(), None);
    }
}
// GRCOV_EXCL_STOP
