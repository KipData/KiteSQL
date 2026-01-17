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

use super::{
    BackendControl, BackendTransaction, ColumnType, DbParam, PreparedStatement, QueryResult,
    SimpleExecutor, StatementSpec,
};
use crate::TpccError;
use chrono::{NaiveDateTime, TimeZone, Utc};
use kite_sql::types::tuple::Tuple;
use kite_sql::types::value::{DataValue, Utf8Type};
use rust_decimal::Decimal;
use sqlite::{Connection, CursorWithOwnership, Row, Statement as SqliteStatement, Value};
use sqlparser::ast::CharLengthUnits;

pub struct SqliteBackend {
    connection: Connection,
}

impl SqliteBackend {
    #[allow(dead_code)]
    pub fn new(path: &str) -> Result<Self, TpccError> {
        Ok(Self {
            connection: Connection::open(path)?,
        })
    }

    pub fn new_memory() -> Result<Self, TpccError> {
        Ok(Self {
            connection: Connection::open(":memory:")?,
        })
    }

    fn prepare_spec_groups(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<PreparedStatement>>, TpccError> {
        Ok(specs
            .iter()
            .map(|group| {
                group
                    .iter()
                    .cloned()
                    .map(|spec| PreparedStatement::Sqlite { spec })
                    .collect()
            })
            .collect())
    }

    fn start_transaction(&self) -> Result<SqliteTransaction<'_>, TpccError> {
        self.connection.execute("BEGIN IMMEDIATE")?;
        Ok(SqliteTransaction {
            connection: &self.connection,
            finished: false,
        })
    }
}

impl BackendControl for SqliteBackend {
    type Transaction<'a>
        = SqliteTransaction<'a>
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<PreparedStatement>>, TpccError> {
        self.prepare_spec_groups(specs)
    }

    fn new_transaction(&self) -> Result<Self::Transaction<'_>, TpccError> {
        self.start_transaction()
    }
}

impl SimpleExecutor for SqliteBackend {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError> {
        self.connection.execute(sql)?;
        Ok(())
    }
}

pub struct SqliteTransaction<'a> {
    connection: &'a Connection,
    finished: bool,
}

impl<'a> SqliteTransaction<'a> {
    pub(crate) fn execute_raw<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<SqliteResult<'b>, TpccError> {
        let PreparedStatement::Sqlite { spec } = statement else {
            return Err(TpccError::InvalidBackend);
        };
        let mut stmt = self.connection.prepare(spec.sql)?;
        bind_params(&mut stmt, params)?;
        SqliteResult::new(stmt.into_iter(), spec.result_types)
    }
}

impl Drop for SqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.connection.execute("ROLLBACK");
        }
    }
}

impl<'a> BackendTransaction for SqliteTransaction<'a> {
    fn execute<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<QueryResult<'b>, TpccError> {
        let iter = self.execute_raw(statement, params)?;
        Ok(QueryResult::from_sqlite(iter))
    }

    fn commit(mut self) -> Result<(), TpccError> {
        self.connection.execute("COMMIT")?;
        self.finished = true;
        Ok(())
    }
}

fn bind_params(statement: &mut SqliteStatement<'_>, params: &[DbParam]) -> Result<(), TpccError> {
    for (key, value) in params {
        let sqlite_value = convert_value(value)?;
        if let Some(index) = key.strip_prefix('?') {
            let idx: usize = index.parse().map_err(|_| TpccError::InvalidParameter)?;
            statement.bind((idx, sqlite_value.clone()))?;
        } else {
            statement.bind((key.as_ref(), sqlite_value.clone()))?;
        }
    }
    Ok(())
}

fn convert_value(value: &DataValue) -> Result<Value, TpccError> {
    Ok(match value {
        DataValue::Null => Value::Null,
        DataValue::Boolean(v) => Value::Integer(*v as i64),
        DataValue::Float32(v) => Value::Float(v.0 as f64),
        DataValue::Float64(v) => Value::Float(v.0),
        DataValue::Int8(v) => Value::Integer(*v as i64),
        DataValue::Int16(v) => Value::Integer(*v as i64),
        DataValue::Int32(v) => Value::Integer(*v as i64),
        DataValue::Int64(v) => Value::Integer(*v),
        DataValue::UInt8(v) => Value::Integer(*v as i64),
        DataValue::UInt16(v) => Value::Integer(*v as i64),
        DataValue::UInt32(v) => Value::Integer(*v as i64),
        DataValue::UInt64(v) => Value::Integer(*v as i64),
        DataValue::Utf8 { value, .. } => Value::String(value.clone()),
        DataValue::Date32(v) => Value::Integer(*v as i64),
        DataValue::Date64(v) => Value::String(
            Utc.timestamp_opt(*v, 0)
                .single()
                .ok_or(TpccError::InvalidDateTime)?
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
        ),
        DataValue::Time32(_, _) | DataValue::Time64(_, _, _) => Value::Null,
        DataValue::Decimal(v) => Value::String(v.to_string()),
        DataValue::Tuple(_, _) => Value::Null,
    })
}

pub struct SqliteResult<'a> {
    cursor: CursorWithOwnership<'a>,
    column_types: &'static [ColumnType],
}

impl<'a> SqliteResult<'a> {
    fn new(
        cursor: CursorWithOwnership<'a>,
        column_types: &'static [ColumnType],
    ) -> Result<Self, TpccError> {
        Ok(Self {
            cursor,
            column_types,
        })
    }
}

impl Iterator for SqliteResult<'_> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = match self.cursor.next()? {
            Ok(row) => row,
            Err(err) => return Some(Err(TpccError::Sqlite(err))),
        };

        Some(convert_row(row, self.column_types))
    }
}

fn convert_row(row: Row, types: &[ColumnType]) -> Result<Tuple, TpccError> {
    let mut values = Vec::with_capacity(types.len());
    for (idx, column_type) in types.iter().enumerate() {
        let value = match column_type {
            ColumnType::Int8 => DataValue::Int8(row.try_read::<i64, _>(idx)? as i8),
            ColumnType::Int16 => DataValue::Int16(row.try_read::<i64, _>(idx)? as i16),
            ColumnType::Int32 => DataValue::Int32(row.try_read::<i64, _>(idx)? as i32),
            ColumnType::Int64 => DataValue::Int64(row.try_read::<i64, _>(idx)?),
            ColumnType::Decimal => DataValue::Decimal(read_decimal(&row, idx)?),
            ColumnType::Utf8 => DataValue::Utf8 {
                value: row.try_read::<&str, _>(idx)?.to_string(),
                ty: Utf8Type::Variable(None),
                unit: CharLengthUnits::Characters,
            },
            ColumnType::DateTime => {
                let text: &str = row.try_read(idx)?;
                parse_datetime(text)?
            }
            ColumnType::NullableDateTime => {
                let text: Option<&str> = row.try_read(idx)?;
                match text {
                    Some(value) => parse_datetime(value)?,
                    None => DataValue::Null,
                }
            }
        };
        values.push(value);
    }
    Ok(Tuple::new(None, values))
}

fn parse_datetime(text: &str) -> Result<DataValue, TpccError> {
    let dt = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")?;
    Ok(DataValue::from(&dt))
}

fn read_decimal(row: &Row, idx: usize) -> Result<Decimal, TpccError> {
    if let Ok(text) = row.try_read::<&str, _>(idx) {
        return Ok(Decimal::from_str_exact(text)?);
    }
    if let Ok(value) = row.try_read::<f64, _>(idx) {
        return Ok(Decimal::from_str_exact(&value.to_string())?);
    }
    let value: i64 = row.try_read(idx)?;
    Ok(Decimal::from_str_exact(&value.to_string())?)
}
