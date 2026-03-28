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
    BackendControl, BackendTransaction, DbParam, PreparedStatement, SimpleExecutor, StatementSpec,
};
use crate::TpccError;
use kite_sql::db::{prepare, DBTransaction, DataBaseBuilder, Database, TransactionIter};
use kite_sql::storage::rocksdb::{RocksStorage, RocksTransaction};
use kite_sql::storage::Transaction;
use kite_sql::types::tuple::Tuple;

pub struct KiteSqlRocksDbBackend {
    database: Database<RocksStorage>,
}

impl KiteSqlRocksDbBackend {
    pub fn new(path: &str, rocksdb_stats: bool) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path)
                .storage_statistics(rocksdb_stats)
                .build_rocksdb()?,
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

    fn start_transaction(&self) -> Result<KiteSqlRocksDbTransaction<'_>, TpccError> {
        Ok(KiteSqlRocksDbTransaction {
            inner: self.database.new_transaction()?,
        })
    }
}

impl BackendControl for KiteSqlRocksDbBackend {
    type Transaction<'a>
        = KiteSqlRocksDbTransaction<'a>
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

    fn storage_metrics(&self) -> Option<String> {
        self.database
            .storage_metrics()
            .map(|metrics| metrics.to_string())
    }
}

impl SimpleExecutor for KiteSqlRocksDbBackend {
    fn execute_batch(&self, sql: &str) -> Result<(), TpccError> {
        self.database.run(sql)?.done()?;
        Ok(())
    }
}

pub struct KiteSqlRocksDbTransaction<'a> {
    inner: DBTransaction<'a, RocksStorage>,
}

impl<'a> KiteSqlRocksDbTransaction<'a> {
    pub(crate) fn execute_raw<'b>(
        &'b mut self,
        statement: &PreparedStatement,
        params: &[DbParam],
    ) -> Result<RocksTxnResult<'b, 'a>, TpccError> {
        let PreparedStatement::KiteSql { statement, .. } = statement else {
            return Err(TpccError::InvalidBackend);
        };
        Ok(KiteSqlTxnResult::new(
            self.inner.execute(statement, params)?,
        ))
    }
}

impl<'a> BackendTransaction for KiteSqlRocksDbTransaction<'a> {
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

pub struct KiteSqlTxnResult<'a, T: Transaction + 'a>(TransactionIter<'a, T>);

impl<'a, T: Transaction + 'a> KiteSqlTxnResult<'a, T> {
    pub(crate) fn new(iter: TransactionIter<'a, T>) -> Self {
        Self(iter)
    }

    pub(crate) fn next_borrowed_tuple(&mut self) -> Result<Option<&Tuple>, TpccError> {
        self.0.next_borrowed_tuple().map_err(TpccError::from)
    }
}

impl<T: Transaction> Iterator for KiteSqlTxnResult<'_, T> {
    type Item = Result<Tuple, TpccError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|item| item.map_err(TpccError::from))
    }
}

pub(crate) type RocksTxnResult<'a, 'txn> = KiteSqlTxnResult<'a, RocksTransaction<'txn>>;
