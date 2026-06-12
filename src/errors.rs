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

use crate::expression::{BinaryOperator, UnaryOperator};
use crate::types::tuple::TupleId;
use crate::types::LogicalType;
#[cfg(feature = "time")]
use chrono::ParseError;
#[cfg(feature = "parser")]
use sqlparser::parser::ParserError;
use std::convert::Infallible;
use std::error::Error;
use std::fmt;
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::{ParseBoolError, Utf8Error};
use std::string::FromUtf8Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlErrorSpan {
    pub start: usize,
    pub end: usize,
    pub line: usize,
    pub highlight: Option<String>,
}

fn format_sql_error_loc(span: &Option<SqlErrorSpan>) -> String {
    span.as_ref()
        .map(|s| {
            if let Some(highlight) = &s.highlight {
                format!("\n{highlight}")
            } else {
                format!(" at line {}, range {}..{}", s.line, s.start, s.end)
            }
        })
        .unwrap_or_default()
}

fn format_not_null_message(column: &Option<String>, span: &Option<SqlErrorSpan>) -> String {
    match column {
        Some(column) => format!(
            "column: `{column}` cannot be null{}",
            format_sql_error_loc(span)
        ),
        None => format!("cannot be null{}", format_sql_error_loc(span)),
    }
}

#[derive(Debug)]
pub enum DatabaseError {
    AggMiss(String),
    CacheSizeOverFlow,
    CastFail {
        from: LogicalType,
        to: LogicalType,
        span: Option<SqlErrorSpan>,
    },
    ChannelClose,
    ColumnsEmpty,
    ColumnIdNotFound(String),
    ColumnNotFound {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    #[cfg(feature = "copy")]
    Csv(csv::Error),
    DefaultNotColumnRef,
    DefaultNotExist,
    DuplicateColumn(String),
    DuplicateSourceHash(String),
    DuplicateIndex(String),
    DuplicatePrimaryKey,
    DuplicateUniqueValue,
    FunctionNotFound {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    EmptyPlan,
    EmptyStatement,
    EvaluatorNotFound,
    FromUtf8Error(FromUtf8Error),
    Incomparable(LogicalType, LogicalType),
    InvalidColumn {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    InvalidIndex,
    InvalidTable {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    InvalidType,
    InvalidValue(String),
    IO(std::io::Error),
    MisMatch(&'static str, &'static str),
    NeedNullAbleOrDefault,
    ParametersNotFound {
        name: String,
        span: Option<SqlErrorSpan>,
    },
    NoTransactionBegin,
    NotNull {
        column: Option<String>,
        span: Option<SqlErrorSpan>,
    },
    OverFlow,
    ParseBool(ParseBoolError),
    #[cfg(feature = "time")]
    ParseDate(ParseError),
    ParseFloat(ParseFloatError),
    ParseInt(ParseIntError),
    #[cfg(feature = "parser")]
    ParserSql(ParserError),
    PrimaryKeyNotFound,
    PrimaryKeyTooManyLayers,
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    Lmdb(lmdb::Error),
    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    RocksDB(rocksdb::Error),
    SharedNotAlign,
    SourceNotFound,
    TableExists,
    TableNotFound,
    TransactionAlreadyExists,
    #[cfg(feature = "decimal")]
    TryFromDecimal(rust_decimal::Error),
    TryFromInt(TryFromIntError),
    TooLong,
    TupleIdNotFound(TupleId),
    TooManyBuckets(usize, usize),
    UnsupportedUnaryOperator(LogicalType, UnaryOperator),
    UnsupportedBinaryOperator(LogicalType, BinaryOperator),
    UnsupportedStmt(String),
    Utf8(Utf8Error),
    ValuesLenMismatch(usize, usize),
    ViewExists,
    ViewNotFound,
}

impl fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AggMiss(value) => write!(f, "agg miss: {value}"),
            Self::CacheSizeOverFlow => f.write_str("cache size overflow"),
            Self::CastFail { from, to, span } => {
                write!(f, "cast fail: {from} -> {to}{}", format_sql_error_loc(span))
            }
            Self::ChannelClose => f.write_str("channel close"),
            Self::ColumnsEmpty => f.write_str("columns empty"),
            Self::ColumnIdNotFound(value) => write!(f, "column id: `{value}` not found"),
            Self::ColumnNotFound { name, span } => {
                write!(
                    f,
                    "column: `{name}` not found{}",
                    format_sql_error_loc(span)
                )
            }
            #[cfg(feature = "copy")]
            Self::Csv(err) => write!(f, "csv error: {err}"),
            Self::DefaultNotColumnRef => {
                f.write_str("default cannot be a column related to the table")
            }
            Self::DefaultNotExist => f.write_str("default does not exist"),
            Self::DuplicateColumn(value) => write!(f, "column: `{value}` already exists"),
            Self::DuplicateSourceHash(value) => {
                write!(f, "table or view: `{value}` hash already exists")
            }
            Self::DuplicateIndex(value) => write!(f, "index: `{value}` already exists"),
            Self::DuplicatePrimaryKey => f.write_str("duplicate primary key"),
            Self::DuplicateUniqueValue => {
                f.write_str("the column has been declared unique and the value already exists")
            }
            Self::FunctionNotFound { name, span } => {
                write!(
                    f,
                    "function: `{name}` not found{}",
                    format_sql_error_loc(span)
                )
            }
            Self::EmptyPlan => f.write_str("empty plan"),
            Self::EmptyStatement => f.write_str("sql statement is empty"),
            Self::EvaluatorNotFound => f.write_str("evaluator not found"),
            Self::FromUtf8Error(err) => write!(f, "from utf8: {err}"),
            Self::Incomparable(left, right) => {
                write!(f, "can not compare two types: {left} and {right}")
            }
            Self::InvalidColumn { name, span } => {
                write!(f, "invalid column: `{name}`{}", format_sql_error_loc(span))
            }
            Self::InvalidIndex => f.write_str("invalid index"),
            Self::InvalidTable { name, span } => {
                write!(f, "invalid table: `{name}`{}", format_sql_error_loc(span))
            }
            Self::InvalidType => f.write_str("invalid type"),
            Self::InvalidValue(value) => write!(f, "invalid value: {value}"),
            Self::IO(err) => write!(f, "io: {err}"),
            Self::MisMatch(left, right) => write!(f, "{left} and {right} do not match"),
            Self::NeedNullAbleOrDefault => {
                f.write_str("add column must be nullable or specify a default value")
            }
            Self::ParametersNotFound { name, span } => {
                write!(
                    f,
                    "parameter: `{name}` not found{}",
                    format_sql_error_loc(span)
                )
            }
            Self::NoTransactionBegin => f.write_str("no transaction begin"),
            Self::NotNull { column, span } => f.write_str(&format_not_null_message(column, span)),
            Self::OverFlow => f.write_str("over flow"),
            Self::ParseBool(err) => write!(f, "parser bool: {err}"),
            #[cfg(feature = "time")]
            Self::ParseDate(err) => write!(f, "parser date: {err}"),
            Self::ParseFloat(err) => write!(f, "parser float: {err}"),
            Self::ParseInt(err) => write!(f, "parser int: {err}"),
            #[cfg(feature = "parser")]
            Self::ParserSql(err) => write!(f, "parser sql: {err}"),
            Self::PrimaryKeyNotFound => f.write_str("must contain primary key!"),
            Self::PrimaryKeyTooManyLayers => {
                f.write_str("primaryKey only allows single or multiple values")
            }
            #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
            Self::Lmdb(err) => write!(f, "lmdb: {err}"),
            #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
            Self::RocksDB(err) => write!(f, "rocksdb: {err}"),
            Self::SharedNotAlign => {
                f.write_str("the number of caches cannot be divisible by the number of shards")
            }
            Self::SourceNotFound => f.write_str("the table or view not found"),
            Self::TableExists => f.write_str("the table already exists"),
            Self::TableNotFound => f.write_str("the table not found"),
            Self::TransactionAlreadyExists => f.write_str("transaction already exists"),
            #[cfg(feature = "decimal")]
            Self::TryFromDecimal(err) => write!(f, "try from decimal: {err}"),
            Self::TryFromInt(err) => write!(f, "try from int: {err}"),
            Self::TooLong => f.write_str("too long"),
            Self::TupleIdNotFound(value) => write!(f, "tuple id: {value} not found"),
            Self::TooManyBuckets(buckets, elements) => {
                write!(
                    f,
                    "there are more buckets: {buckets} than elements: {elements}"
                )
            }
            Self::UnsupportedUnaryOperator(ty, op) => {
                write!(
                    f,
                    "unsupported unary operator: {ty} cannot support {op} for calculations"
                )
            }
            Self::UnsupportedBinaryOperator(ty, op) => {
                write!(
                    f,
                    "unsupported binary operator: {ty} cannot support {op} for calculations"
                )
            }
            Self::UnsupportedStmt(value) => write!(f, "unsupported statement: {value}"),
            Self::Utf8(err) => write!(f, "utf8: {err}"),
            Self::ValuesLenMismatch(expect, got) => {
                write!(f, "values length not match, expect {expect}, got {got}")
            }
            Self::ViewExists => f.write_str("the view already exists"),
            Self::ViewNotFound => f.write_str("the view not found"),
        }
    }
}

impl Error for DatabaseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            #[cfg(feature = "copy")]
            Self::Csv(err) => Some(err),
            Self::FromUtf8Error(err) => Some(err),
            Self::IO(err) => Some(err),
            Self::ParseBool(err) => Some(err),
            #[cfg(feature = "time")]
            Self::ParseDate(err) => Some(err),
            Self::ParseFloat(err) => Some(err),
            Self::ParseInt(err) => Some(err),
            #[cfg(feature = "parser")]
            Self::ParserSql(err) => Some(err),
            #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
            Self::Lmdb(err) => Some(err),
            #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
            Self::RocksDB(err) => Some(err),
            #[cfg(feature = "decimal")]
            Self::TryFromDecimal(err) => Some(err),
            Self::TryFromInt(err) => Some(err),
            Self::Utf8(err) => Some(err),
            _ => None,
        }
    }
}

macro_rules! impl_from_database_error {
    ($source:ty, $variant:ident) => {
        impl From<$source> for DatabaseError {
            fn from(value: $source) -> Self {
                Self::$variant(value)
            }
        }
    };
}

#[cfg(feature = "copy")]
impl_from_database_error!(csv::Error, Csv);
impl_from_database_error!(FromUtf8Error, FromUtf8Error);
impl_from_database_error!(std::io::Error, IO);
impl_from_database_error!(ParseBoolError, ParseBool);
#[cfg(feature = "time")]
impl_from_database_error!(ParseError, ParseDate);
impl_from_database_error!(ParseFloatError, ParseFloat);
impl_from_database_error!(ParseIntError, ParseInt);
#[cfg(feature = "parser")]
impl_from_database_error!(ParserError, ParserSql);
#[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
impl_from_database_error!(lmdb::Error, Lmdb);
#[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
impl_from_database_error!(rocksdb::Error, RocksDB);
#[cfg(feature = "decimal")]
impl_from_database_error!(rust_decimal::Error, TryFromDecimal);
impl_from_database_error!(TryFromIntError, TryFromInt);
impl_from_database_error!(Utf8Error, Utf8);

impl From<Infallible> for DatabaseError {
    fn from(value: Infallible) -> Self {
        match value {}
    }
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

    pub fn not_null() -> Self {
        Self::NotNull {
            column: None,
            span: None,
        }
    }

    pub fn not_null_column(name: impl Into<String>) -> Self {
        Self::NotNull {
            column: Some(name.into()),
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
            Self::NotNull { column, .. } => Self::NotNull {
                column,
                span: Some(span),
            },
            other => other,
        }
    }

    pub fn with_sql_context(self, sql: &str) -> Self {
        let annotate = |span: Option<SqlErrorSpan>| -> Option<SqlErrorSpan> {
            span.map(|mut span| {
                if span.highlight.is_none() {
                    span.highlight = build_sql_highlight(sql, &span);
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
            Self::NotNull { column, span } => Self::NotNull {
                column,
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
            | DatabaseError::ParametersNotFound { span, .. }
            | DatabaseError::NotNull { span, .. } => span.as_ref(),
            _ => None,
        }
    }
}

fn build_sql_highlight(sql: &str, span: &SqlErrorSpan) -> Option<String> {
    if span.line == 0 || span.start == 0 {
        return None;
    }

    let lines = sql
        .lines()
        .map(|line| line.trim_end_matches('\r').to_string())
        .collect::<Vec<_>>();
    if lines.is_empty() || span.line > lines.len() {
        return None;
    }

    let width = lines.len().to_string().len();
    let mut out = String::new();
    out.push_str(&format!("--> line {}\n", span.line));

    for (i, line) in lines.iter().enumerate() {
        let line_no = i + 1;
        out.push_str(&format!("{line_no:>width$} | {line}\n"));

        if line_no == span.line {
            let char_len = line.chars().count();
            let start = span.start.saturating_sub(1).min(char_len);
            let end = span.end.min(char_len).max(start + 1);
            let marker_len = end.saturating_sub(start).max(1);
            out.push_str(&format!(
                "{:>width$} | {}{}\n",
                "",
                " ".repeat(start),
                "^".repeat(marker_len),
                width = width
            ));
        }
    }

    Some(out.trim_end().to_string())
}
