use std::borrow::Borrow;
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

#[cfg(feature = "kitesql-sync")]
pub mod dual;
pub mod kitesql;
pub mod sqlite;

use crate::TpccError;
use kite_sql::db::{Param, Program};
use kite_sql::types::tuple::TupleImpl;
use kite_sql::types::LogicalType;

pub type DbParam<'a> = (usize, Param<'a>);

pub type RowVisitor<'v> = &'v mut (dyn FnMut(&TupleImpl) -> Result<(), TpccError> + Send);

// Only the workload in this binary calls these, so the futures need no `Send` bound.
#[allow(async_fn_in_trait)]
pub trait SimpleExecutor {
    async fn execute_batch(&mut self, sql: &str) -> Result<(), TpccError>;
}

pub trait BackendControl: SimpleExecutor {
    type PreparedStatement<'a>: PreparedStatement + 'a
    where
        Self: 'a;
    type Transaction<'a>: BackendTransaction<PreparedStatement = Self::PreparedStatement<'a>> + 'a
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<Self::PreparedStatement<'_>>>, TpccError>;

    type Worker<'a>: Borrow<Self> + Send
    where
        Self: 'a;

    fn new_transaction(&self, read_only: bool) -> Result<Self::Transaction<'_>, TpccError>;

    fn worker(&self) -> Result<Self::Worker<'_>, TpccError>;

    fn storage_metrics(&self) -> Option<String> {
        None
    }

    /// Whether transactions wait on asynchronous storage, so that several terminals on one
    /// thread overlap their waits. Terminals of a sync storage run one after another instead.
    fn is_async(&self) -> bool {
        false
    }
}

#[allow(async_fn_in_trait)]
pub trait BackendTransaction {
    type PreparedStatement: PreparedStatement;

    async fn execute_drain(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
    ) -> Result<(), TpccError>;

    async fn with_query_one(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError>;

    async fn with_query_all(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError>;

    async fn with_query_nth(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        n: usize,
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError>;

    async fn commit(self) -> Result<(), TpccError>;
}

#[derive(Clone)]
pub struct StatementSpec {
    pub sql: &'static str,
    pub result_types: Vec<LogicalType>,
    pub parameters: Vec<(usize, LogicalType)>,
}

pub struct KiteSqlPreparedStatement<'a> {
    pub program: Program<'a>,
    #[cfg_attr(not(feature = "kitesql-sync"), allow(dead_code))]
    pub spec: StatementSpec,
}

#[cfg_attr(not(feature = "kitesql-sync"), allow(dead_code))]
pub trait PreparedStatement {
    fn spec(&self) -> &StatementSpec;
}

impl PreparedStatement for KiteSqlPreparedStatement<'_> {
    fn spec(&self) -> &StatementSpec {
        &self.spec
    }
}
