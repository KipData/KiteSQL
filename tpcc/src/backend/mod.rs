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

pub mod dual;
pub mod kite;
pub mod sqlite;

use self::dual::DualQueryResult;
use self::kite::KiteTxnResult;
use self::sqlite::SqliteResult;
use crate::TpccError;
use kite_sql::db::Statement;
use kite_sql::types::tuple::Tuple;
use kite_sql::types::value::DataValue;

pub type DbParam = (&'static str, DataValue);

pub trait SimpleExecutor {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError>;
}

pub trait BackendControl: SimpleExecutor {
    type Transaction<'a>: BackendTransaction + 'a
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<PreparedStatement>>, TpccError>;

    fn new_transaction(&self) -> Result<Self::Transaction<'_>, TpccError>;
}

pub struct QueryResult<'a>(QueryResultKind<'a>);

enum QueryResultKind<'a> {
    Kite(KiteTxnResult<'a>),
    Sqlite(SqliteResult<'a>),
    Dual(DualQueryResult<'a>),
}

impl<'a> QueryResult<'a> {
    pub(crate) fn from_kite(iter: KiteTxnResult<'a>) -> Self {
        Self(QueryResultKind::Kite(iter))
    }

    pub(crate) fn from_sqlite(iter: SqliteResult<'a>) -> Self {
        Self(QueryResultKind::Sqlite(iter))
    }

    pub(crate) fn from_dual(iter: DualQueryResult<'a>) -> Self {
        Self(QueryResultKind::Dual(iter))
    }
}

impl<'a> Iterator for QueryResult<'a> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            QueryResultKind::Kite(iter) => iter.next(),
            QueryResultKind::Sqlite(iter) => iter.next(),
            QueryResultKind::Dual(iter) => iter.next(),
        }
    }
}

pub trait BackendTransaction {
    fn execute<'a>(
        &'a mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<QueryResult<'a>, TpccError>;

    fn commit(self) -> Result<(), TpccError>;

    fn execute_drain(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<(), TpccError> {
        let mut iter = self.execute(statement, params)?;
        while let Some(row) = iter.next() {
            row?;
        }
        Ok(())
    }
}

pub trait TransactionExt: BackendTransaction {
    fn query_one(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<Tuple, TpccError> {
        let mut iter = self.execute(statement, params)?;
        match iter.next() {
            Some(row) => row,
            None => Err(TpccError::EmptyTuples),
        }
    }
}

impl<T: BackendTransaction + ?Sized> TransactionExt for T {}

#[derive(Clone, Copy)]
pub enum ColumnType {
    Int8,
    Int16,
    Int32,
    Int64,
    Decimal,
    Utf8,
    DateTime,
    NullableDateTime,
}

#[derive(Clone)]
pub struct StatementSpec {
    pub sql: &'static str,
    pub result_types: &'static [ColumnType],
}

#[derive(Clone)]
pub enum PreparedStatement {
    Kite {
        statement: Statement,
        spec: StatementSpec,
    },
    Sqlite {
        spec: StatementSpec,
    },
}

impl PreparedStatement {
    pub fn spec(&self) -> &StatementSpec {
        match self {
            PreparedStatement::Kite { spec, .. } => spec,
            PreparedStatement::Sqlite { spec } => spec,
        }
    }
}
