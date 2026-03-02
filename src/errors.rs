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

use crate::expression::{BinaryOperator, ScalarExpression, UnaryOperator};
use crate::types::tuple::TupleId;
use crate::types::LogicalType;
use chrono::ParseError;
use sqlparser::parser::ParserError;
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::{ParseBoolError, Utf8Error};
use std::string::FromUtf8Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlErrorSpan {
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub near: Option<String>,
}

fn format_sql_error_loc(span: &Option<SqlErrorSpan>) -> String {
    span.as_ref()
        .map(|s| {
            if let Some(near) = &s.near {
                format!(" near '{near}' at line {}", s.line)
            } else {
                format!(" at line {}, range {}..{}", s.line, s.start, s.end)
            }
        })
        .unwrap_or_default()
}

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("agg miss: {0}")]
    AggMiss(String),
    #[error("bindcode: {0}")]
    Bincode(
        #[source]
        #[from]
        Box<bincode::ErrorKind>,
    ),
    #[error("cache size overflow")]
    CacheSizeOverFlow,
    #[error(
        "cast fail: {from} -> {to}{loc}",
        loc = format_sql_error_loc(span)
    )]
    CastFail {
        from: LogicalType,
        to: LogicalType,
        span: Option<SqlErrorSpan>,
    },
    #[error("channel close")]
    ChannelClose,
    #[error("columns empty")]
    ColumnsEmpty,
    #[error("column id: {0} not found")]
    ColumnIdNotFound(String),
    #[error(
        "column: {name} not found{loc}",
        loc = format_sql_error_loc(span)
    )]
    ColumnNotFound {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    #[error("csv error: {0}")]
    Csv(
        #[from]
        #[source]
        csv::Error,
    ),
    #[error("default cannot be a column related to the table")]
    DefaultNotColumnRef,
    #[error("default does not exist")]
    DefaultNotExist,
    #[error("column: {0} already exists")]
    DuplicateColumn(String),
    #[error("table or view: {0} hash already exists")]
    DuplicateSourceHash(String),
    #[error("index: {0} already exists")]
    DuplicateIndex(String),
    #[error("duplicate primary key")]
    DuplicatePrimaryKey,
    #[error("the column has been declared unique and the value already exists")]
    DuplicateUniqueValue,
    #[error(
        "function: {name} not found{loc}",
        loc = format_sql_error_loc(span)
    )]
    FunctionNotFound {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    #[error("empty plan")]
    EmptyPlan,
    #[error("sql statement is empty")]
    EmptyStatement,
    #[error("evaluator not found")]
    EvaluatorNotFound,
    #[error("from utf8: {0}")]
    FromUtf8Error(
        #[source]
        #[from]
        FromUtf8Error,
    ),
    #[error("can not compare two types: {0} and {1}")]
    Incomparable(LogicalType, LogicalType),
    #[error(
        "invalid column: {name}{loc}",
        loc = format_sql_error_loc(span)
    )]
    InvalidColumn {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    #[error("invalid index")]
    InvalidIndex,
    #[error(
        "invalid table: {name}{loc}",
        loc = format_sql_error_loc(span)
    )]
    InvalidTable {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    #[error("invalid type")]
    InvalidType,
    #[error("invalid value: {0}")]
    InvalidValue(String),
    #[error("io: {0}")]
    IO(
        #[source]
        #[from]
        std::io::Error,
    ),
    #[error("{0} and {1} do not match")]
    MisMatch(&'static str, &'static str),
    #[error("add column must be nullable or specify a default value")]
    NeedNullAbleOrDefault,
    #[error(
        "parameter: {name} not found{loc}",
        loc = format_sql_error_loc(span)
    )]
    ParametersNotFound {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    #[error("no transaction begin")]
    NoTransactionBegin,
    #[error("cannot be null")]
    NotNull,
    #[error("over flow")]
    OverFlow,
    #[error("parser bool: {0}")]
    ParseBool(
        #[source]
        #[from]
        ParseBoolError,
    ),
    #[error("parser date: {0}")]
    ParseDate(
        #[source]
        #[from]
        ParseError,
    ),
    #[error("parser float: {0}")]
    ParseFloat(
        #[source]
        #[from]
        ParseFloatError,
    ),
    #[error("parser int: {0}")]
    ParseInt(
        #[source]
        #[from]
        ParseIntError,
    ),
    #[error("parser sql: {0}")]
    ParserSql(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("must contain primary key!")]
    PrimaryKeyNotFound,
    #[error("primaryKey only allows single or multiple values")]
    PrimaryKeyTooManyLayers,
    #[cfg(not(target_arch = "wasm32"))]
    #[error("rocksdb: {0}")]
    RocksDB(
        #[source]
        #[from]
        rocksdb::Error,
    ),
    #[error("the number of caches cannot be divisible by the number of shards")]
    SharedNotAlign,
    #[error("the table or view not found")]
    SourceNotFound,
    #[error("the table already exists")]
    TableExists,
    #[error("the table not found")]
    TableNotFound,
    #[error("transaction already exists")]
    TransactionAlreadyExists,
    #[error("try from decimal: {0}")]
    TryFromDecimal(
        #[source]
        #[from]
        rust_decimal::Error,
    ),
    #[error("try from int: {0}")]
    TryFromInt(
        #[source]
        #[from]
        TryFromIntError,
    ),
    #[error("too long")]
    TooLong,
    #[error("tuple id: {0} not found")]
    TupleIdNotFound(TupleId),
    #[error("there are more buckets: {0} than elements: {1}")]
    TooManyBuckets(usize, usize),
    #[error("this scalar expression: '{0}' unbind position")]
    UnbindExpressionPosition(ScalarExpression),
    #[error("unsupported unary operator: {0} cannot support {1} for calculations")]
    UnsupportedUnaryOperator(LogicalType, UnaryOperator),
    #[error("unsupported binary operator: {0} cannot support {1} for calculations")]
    UnsupportedBinaryOperator(LogicalType, BinaryOperator),
    #[error("unsupported statement: {0}")]
    UnsupportedStmt(String),
    #[error("utf8: {0}")]
    Utf8(
        #[source]
        #[from]
        Utf8Error,
    ),
    #[error("values length not match, expect {0}, got {1}")]
    ValuesLenMismatch(usize, usize),
    #[error("the view already exists")]
    ViewExists,
    #[error("the view not found")]
    ViewNotFound,
}

impl DatabaseError {
    pub fn invalid_column(name: impl Into<String>) -> Self {
        Self::InvalidColumn {
            name: name.into(),
            span: None,
        }
    }

    pub fn column_not_found(name: impl Into<String>) -> Self {
        Self::ColumnNotFound {
            name: name.into(),
            span: None,
        }
    }

    pub fn invalid_table(name: impl Into<String>) -> Self {
        Self::InvalidTable {
            name: name.into(),
            span: None,
        }
    }

    pub fn function_not_found(name: impl Into<String>) -> Self {
        Self::FunctionNotFound {
            name: name.into(),
            span: None,
        }
    }

    pub fn parameter_not_found(name: impl Into<String>) -> Self {
        Self::ParametersNotFound {
            name: name.into(),
            span: None,
        }
    }

    pub fn with_span(self, span: SqlErrorSpan) -> Self {
        match self {
            Self::CastFail { from, to, .. } => Self::CastFail {
                from,
                to,
                span: Some(span),
            },
            Self::InvalidColumn { name, .. } => Self::InvalidColumn {
                name,
                span: Some(span),
            },
            Self::ColumnNotFound { name, .. } => Self::ColumnNotFound {
                name,
                span: Some(span),
            },
            Self::InvalidTable { name, .. } => Self::InvalidTable {
                name,
                span: Some(span),
            },
            Self::FunctionNotFound { name, .. } => Self::FunctionNotFound {
                name,
                span: Some(span),
            },
            Self::ParametersNotFound { name, .. } => Self::ParametersNotFound {
                name,
                span: Some(span),
            },
            other => other,
        }
    }

    pub fn with_sql_context(self, sql: &str) -> Self {
        let annotate = |span: Option<SqlErrorSpan>| -> Option<SqlErrorSpan> {
            span.map(|mut span| {
                if span.near.is_none() {
                    span.near = extract_sql_near(sql, &span);
                }
                span
            })
        };

        match self {
            Self::CastFail { from, to, span } => Self::CastFail {
                from,
                to,
                span: annotate(span),
            },
            Self::InvalidColumn { name, span } => Self::InvalidColumn {
                name,
                span: annotate(span),
            },
            Self::ColumnNotFound { name, span } => Self::ColumnNotFound {
                name,
                span: annotate(span),
            },
            Self::InvalidTable { name, span } => Self::InvalidTable {
                name,
                span: annotate(span),
            },
            Self::FunctionNotFound { name, span } => Self::FunctionNotFound {
                name,
                span: annotate(span),
            },
            Self::ParametersNotFound { name, span } => Self::ParametersNotFound {
                name,
                span: annotate(span),
            },
            other => other,
        }
    }

    pub fn sql_error_span(&self) -> Option<&SqlErrorSpan> {
        match self {
            DatabaseError::CastFail { span, .. }
            | DatabaseError::InvalidColumn { span, .. }
            | DatabaseError::ColumnNotFound { span, .. }
            | DatabaseError::InvalidTable { span, .. }
            | DatabaseError::FunctionNotFound { span, .. }
            | DatabaseError::ParametersNotFound { span, .. } => span.as_ref(),
            _ => None,
        }
    }
}

fn extract_sql_near(sql: &str, span: &SqlErrorSpan) -> Option<String> {
    if span.line == 0 || span.start == 0 {
        return None;
    }

    let line = sql.lines().nth(span.line.saturating_sub(1))?;
    let line = line.trim_end_matches('\r');

    let line_char_count = line.chars().count();
    let start_idx = span.start.saturating_sub(1).min(line_char_count);
    let end_idx = span.end.min(line_char_count).max(start_idx + 1);

    let start_byte = char_to_byte_offset(line, start_idx)?;
    let end_byte = char_to_byte_offset(line, end_idx)?;
    let near = line.get(start_byte..end_byte)?.trim();

    if near.is_empty() {
        None
    } else {
        Some(near.to_string())
    }
}

fn char_to_byte_offset(s: &str, char_index: usize) -> Option<usize> {
    if char_index == s.chars().count() {
        return Some(s.len());
    }
    s.char_indices().nth(char_index).map(|(idx, _)| idx)
}
