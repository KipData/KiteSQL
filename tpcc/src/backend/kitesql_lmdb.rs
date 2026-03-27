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

use super::kitesql_rocksdb::KiteSqlTxnResult;
use super::{
    BackendControl, BackendTransaction, DbParam, PreparedStatement, SimpleExecutor, StatementSpec,
};
use crate::TpccError;
use kite_sql::db::{prepare, DBTransaction, DataBaseBuilder, Database};
use kite_sql::storage::lmdb::{LmdbStorage, LmdbTransaction as KiteSqlLmdbTransaction};
use kite_sql::types::tuple::Tuple;

pub struct KiteSqlLmdbBackend {
    database: Database<LmdbStorage>,
}

impl KiteSqlLmdbBackend {
    pub fn new(path: &str) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path)
                .lmdb_no_sync(true)
                .build_lmdb()?,
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
                let statement = prepare(spec.sql)?;
                prepared.push(PreparedStatement::KiteSql {
                    statement,
                    spec: spec.clone(),
                });
            }
            groups.push(prepared);
        }
        Ok(groups)
    }

    fn start_transaction(&self) -> Result<KiteSqlLmdbTransactionWrapper<'_>, TpccError> {
        Ok(KiteSqlLmdbTransactionWrapper {
            inner: self.database.new_transaction()?,
        })
    }
}

impl BackendControl for KiteSqlLmdbBackend {
    type Transaction<'a>
        = KiteSqlLmdbTransactionWrapper<'a>
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

impl SimpleExecutor for KiteSqlLmdbBackend {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError> {
        self.database.run(sql)?.done()?;
        Ok(())
    }
}

pub struct KiteSqlLmdbTransactionWrapper<'a> {
    inner: DBTransaction<'a, LmdbStorage>,
}

impl<'a> KiteSqlLmdbTransactionWrapper<'a> {
    pub(crate) fn execute_raw<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<KiteSqlTxnResult<'b, KiteSqlLmdbTransaction<'a>>, TpccError> {
        let PreparedStatement::KiteSql { statement, .. } = statement else {
            return Err(TpccError::InvalidBackend);
        };
        Ok(KiteSqlTxnResult::new(
            self.inner.execute(statement, params)?,
        ))
    }
}

impl<'a> BackendTransaction for KiteSqlLmdbTransactionWrapper<'a> {
    fn query_one(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<Tuple, TpccError> {
        self.execute_raw(statement, params)?
            .next_borrowed_tuple()?
            .cloned()
            .ok_or(TpccError::EmptyTuples)
    }

    fn query_nth(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        n: usize,
    ) -> Result<Tuple, TpccError> {
        let mut iter = self.execute_raw(statement, params)?;
        for _ in 0..n {
            if iter.next_borrowed_tuple()?.is_none() {
                return Err(TpccError::EmptyTuples);
            }
        }
        iter.next_borrowed_tuple()?
            .cloned()
            .ok_or(TpccError::EmptyTuples)
    }

    fn execute_drain(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params)?;
        while iter.next_borrowed_tuple()?.is_some() {}
        Ok(())
    }

    fn with_query_one(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params)?;
        let tuple = iter.next_borrowed_tuple()?.ok_or(TpccError::EmptyTuples)?;
        visitor(tuple)
    }

    fn with_query_nth(
        &mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
        n: usize,
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params)?;
        for _ in 0..n {
            if iter.next_borrowed_tuple()?.is_none() {
                return Err(TpccError::EmptyTuples);
            }
        }
        let tuple = iter.next_borrowed_tuple()?.ok_or(TpccError::EmptyTuples)?;
        visitor(tuple)
    }

    fn commit(self) -> Result<(), TpccError> {
        self.inner.commit()?;
        Ok(())
    }
}
