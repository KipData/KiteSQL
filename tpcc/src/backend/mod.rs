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
pub mod kitesql_lmdb;
pub mod kitesql_rocksdb;
pub mod sqlite;

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

    fn storage_metrics(&self) -> Option<String> {
        None
    }
}

pub trait BackendTransaction {
    fn query_one(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<Tuple, TpccError>;

    fn query_nth(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        n: usize,
    ) -> Result<Tuple, TpccError>;

    fn execute_drain(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<(), TpccError>;

    fn with_query_one(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        let tuple = self.query_one(statement, params)?;
        visitor(&tuple)
    }

    fn with_query_nth(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        n: usize,
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        let tuple = self.query_nth(statement, params, n)?;
        visitor(&tuple)
    }

    fn commit(self) -> Result<(), TpccError>;
}

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
    KiteSql {
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
            PreparedStatement::KiteSql { spec, .. } => spec,
            PreparedStatement::Sqlite { spec } => spec,
        }
    }
}
