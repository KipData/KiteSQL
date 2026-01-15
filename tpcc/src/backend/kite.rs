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
    BackendControl, BackendTransaction, DbParam, PreparedStatement, QueryResult, SimpleExecutor,
    StatementSpec,
};
use crate::TpccError;
use kite_sql::db::{DBTransaction, DataBaseBuilder, Database, ResultIter, TransactionIter};
use kite_sql::storage::rocksdb::RocksStorage;
use kite_sql::types::tuple::Tuple;

pub struct KiteBackend {
    database: Database<RocksStorage>,
}

impl KiteBackend {
    pub fn new(path: &str) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path).build()?,
        })
    }

    fn prepare_spec_groups(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<PreparedStatement>>, TpccError> {
        let mut groups = Vec::with_capacity(specs.len());
        for group in specs {
            let mut prepared = Vec::with_capacity(group.len());
            for spec in group {
                let statement = self.database.prepare(spec.sql)?;
                prepared.push(PreparedStatement::Kite {
                    statement,
                    spec: spec.clone(),
                });
            }
            groups.push(prepared);
        }
        Ok(groups)
    }

    fn start_transaction(&self) -> Result<KiteTransaction<'_>, TpccError> {
        Ok(KiteTransaction {
            inner: self.database.new_transaction()?,
        })
    }
}

impl BackendControl for KiteBackend {
    type Transaction<'a>
        = KiteTransaction<'a>
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

impl SimpleExecutor for KiteBackend {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError> {
        self.database.run(sql)?.done()?;
        Ok(())
    }
}

pub struct KiteTransaction<'a> {
    inner: DBTransaction<'a, RocksStorage>,
}

impl<'a> KiteTransaction<'a> {
    pub(crate) fn execute_raw<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<KiteTxnResult<'b>, TpccError> {
        let PreparedStatement::Kite { statement, .. } = statement else {
            return Err(TpccError::InvalidBackend);
        };
        Ok(KiteTxnResult(self.inner.execute(statement, params)?))
    }
}

impl<'a> BackendTransaction for KiteTransaction<'a> {
    fn execute<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<QueryResult<'b>, TpccError> {
        let iter = self.execute_raw(statement, params)?;
        Ok(QueryResult::from_kite(iter))
    }

    fn commit(self) -> Result<(), TpccError> {
        self.inner.commit()?;
        Ok(())
    }
}

pub struct KiteTxnResult<'a>(TransactionIter<'a>);

impl Iterator for KiteTxnResult<'_> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|item| item.map_err(TpccError::from))
    }
}
