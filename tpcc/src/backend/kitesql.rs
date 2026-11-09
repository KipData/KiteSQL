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

//! KiteSQL on any of its storages: LMDB and RocksDB in the default build, SlateDB on S3 in the
//! `slatedb-s3` build, where KiteSQL's API is async.

use super::{
    BackendControl, BackendTransaction, DbParam, KiteSqlPreparedStatement, RowVisitor,
    SimpleExecutor, StatementSpec,
};
use crate::TpccError;
use kite_sql::binder::{command_type, CommandType};
use kite_sql::db::{DataBaseBuilder, Database, ProgramIter, Statement, Transaction};
use kite_sql::storage::{Storage, StorageTransaction};
use kite_sql::types::tuple::TupleImpl;

/// A call into KiteSQL, awaited in the `slatedb-s3` build, where KiteSQL's API is async.
#[cfg(not(feature = "slatedb-s3"))]
macro_rules! kite {
    ($call:expr) => {
        $call
    };
}

#[cfg(feature = "slatedb-s3")]
macro_rules! kite {
    ($call:expr) => {
        $call.await
    };
}

/// `Send` in the `slatedb-s3` build, where KiteSQL runs what reads a row inside a `Send` future.
#[cfg(feature = "slatedb-s3")]
pub trait MaybeSend: Send {}
#[cfg(feature = "slatedb-s3")]
impl<T: Send> MaybeSend for T {}
#[cfg(not(feature = "slatedb-s3"))]
pub trait MaybeSend {}
#[cfg(not(feature = "slatedb-s3"))]
impl<T> MaybeSend for T {}

pub struct KiteSqlBackend<S: Storage> {
    database: Database<S>,
}

#[cfg(feature = "kitesql-sync")]
impl KiteSqlBackend<kite_sql::storage::lmdb::LmdbStorage> {
    pub fn lmdb(path: &str, sync: bool) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path).sync(sync).build_lmdb()?,
        })
    }
}

#[cfg(feature = "kitesql-sync")]
impl KiteSqlBackend<kite_sql::storage::rocksdb::RocksStorage> {
    pub fn rocksdb(path: &str, rocksdb_stats: bool, sync: bool) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path)
                .storage_statistics(rocksdb_stats)
                .sync(sync)
                .build_rocksdb()?,
        })
    }
}

#[cfg(feature = "kitesql-sync")]
impl KiteSqlBackend<kite_sql::storage::rocksdb::OptimisticRocksStorage> {
    pub fn optimistic_rocksdb(
        path: &str,
        rocksdb_stats: bool,
        sync: bool,
    ) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path)
                .storage_statistics(rocksdb_stats)
                .sync(sync)
                .build_optimistic()?,
        })
    }
}

#[cfg(feature = "slatedb-s3")]
impl<S: Storage> KiteSqlBackend<S> {
    /// KiteSQL on a storage opened by the caller, such as SlateDB, which the caller closes.
    pub fn with_storage(storage: S) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(".").build_with_storage(storage)?,
        })
    }
}

impl<S: Storage + Sync> BackendControl for KiteSqlBackend<S> {
    type PreparedStatement<'a>
        = KiteSqlPreparedStatement<'a>
    where
        Self: 'a;

    type Transaction<'a>
        = KiteSqlTransaction<'a, S>
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<Self::PreparedStatement<'_>>>, TpccError> {
        let mut groups = Vec::with_capacity(specs.len());
        for group in specs {
            let mut prepared = Vec::with_capacity(group.len());
            for spec in group {
                prepared.push(KiteSqlPreparedStatement {
                    program: self.database.program_sql(spec.sql, &spec.parameters)?,
                    spec: spec.clone(),
                });
            }
            groups.push(prepared);
        }
        Ok(groups)
    }

    type Worker<'a>
        = &'a Self
    where
        Self: 'a;

    fn new_transaction(&self, read_only: bool) -> Result<Self::Transaction<'_>, TpccError> {
        Ok(KiteSqlTransaction {
            inner: self.database.new_transaction(read_only)?,
        })
    }

    fn worker(&self) -> Result<Self::Worker<'_>, TpccError> {
        Ok(self)
    }

    fn storage_metrics(&self) -> Option<String> {
        self.database
            .storage_metrics()
            .map(|metrics| metrics.to_string())
    }

    fn is_async(&self) -> bool {
        cfg!(feature = "slatedb-s3")
    }
}

impl<S: Storage> SimpleExecutor for KiteSqlBackend<S> {
    async fn execute_batch(&mut self, sql: &str) -> Result<(), TpccError> {
        let database = &mut self.database;
        match KiteSqlBatch::classify(&database.prepare_all(sql)?)? {
            KiteSqlBatch::Ddl => kite!(database.ddl(sql))?,
            KiteSqlBatch::Analyze(tables) => {
                for table_name in tables {
                    let start = std::time::Instant::now();
                    println!("[KiteSQL Analyze: {table_name}] started");
                    kite!(database.analyze(&table_name))?;
                    println!(
                        "[KiteSQL Analyze: {table_name}] completed in {:.2}s",
                        start.elapsed().as_secs_f64()
                    );
                }
            }
            KiteSqlBatch::Statements => {
                let mut transaction = database.new_transaction(false)?;
                let iter = kite!(transaction.run(sql))?;
                kite!(iter.done())?;
                kite!(transaction.commit())?;
            }
        }
        Ok(())
    }
}

enum KiteSqlBatch {
    Ddl,
    Analyze(Vec<String>),
    Statements,
}

impl KiteSqlBatch {
    fn classify(statements: &[Statement]) -> Result<Self, TpccError> {
        let all_ddl = statements.iter().try_fold(true, |all_ddl, statement| {
            Ok::<_, kite_sql::errors::DatabaseError>(
                all_ddl && matches!(command_type(statement)?, CommandType::DDL),
            )
        })?;
        if all_ddl {
            return Ok(Self::Ddl);
        }
        if !statements
            .iter()
            .all(|statement| matches!(statement, Statement::Analyze(_)))
        {
            return Ok(Self::Statements);
        }
        let mut tables = Vec::with_capacity(statements.len());
        for statement in statements {
            let Statement::Analyze(analyze) = statement else {
                unreachable!("checked above")
            };
            let table_name = analyze.table_name.as_ref().ok_or_else(|| {
                kite_sql::errors::DatabaseError::UnsupportedStmt(
                    "ANALYZE requires table name".to_string(),
                )
            })?;
            tables.push(table_name.to_string());
        }
        Ok(Self::Analyze(tables))
    }
}

pub struct KiteSqlTransaction<'a, S: Storage> {
    inner: Transaction<'a, S>,
}

impl<'a, S: Storage> KiteSqlTransaction<'a, S> {
    pub(crate) async fn execute_raw<'b>(
        &'b mut self,
        statement: &'b mut KiteSqlPreparedStatement<'a>,
        params: &[DbParam<'_>],
    ) -> Result<KiteSqlTxnResult<'b, 'a, S::TransactionType<'a>>, TpccError> {
        Ok(KiteSqlTxnResult(kite!(statement
            .program
            .execute(&mut self.inner, params))?))
    }
}

impl<'a, S: Storage> BackendTransaction for KiteSqlTransaction<'a, S> {
    type PreparedStatement = KiteSqlPreparedStatement<'a>;

    async fn execute_drain(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        while iter.skip_next_tuple().await? {}
        Ok(())
    }

    async fn with_query_one(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        iter.with_next_tuple(|tuple| visitor(tuple))
            .await?
            .ok_or(TpccError::EmptyTuples)
    }

    async fn with_query_all(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        while iter
            .with_next_tuple(|tuple| visitor(tuple))
            .await?
            .is_some()
        {}
        Ok(())
    }

    async fn with_query_nth(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        n: usize,
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        for _ in 0..n {
            if !iter.skip_next_tuple().await? {
                return Err(TpccError::EmptyTuples);
            }
        }
        iter.with_next_tuple(|tuple| visitor(tuple))
            .await?
            .ok_or(TpccError::EmptyTuples)
    }

    async fn commit(self) -> Result<(), TpccError> {
        kite!(self.inner.commit())?;
        Ok(())
    }
}

pub struct KiteSqlTxnResult<'b, 'a, T: StorageTransaction + 'a>(ProgramIter<'b, 'a, T>);

impl<'a, T: StorageTransaction + 'a> KiteSqlTxnResult<'_, 'a, T> {
    pub(crate) async fn skip_next_tuple(&mut self) -> Result<bool, TpccError> {
        Ok(kite!(self.0.next_tuple(|_, _| ()))?.is_some())
    }

    pub(crate) async fn with_next_tuple<R: MaybeSend>(
        &mut self,
        f: impl FnOnce(&TupleImpl) -> Result<R, TpccError> + MaybeSend,
    ) -> Result<Option<R>, TpccError> {
        match kite!(self.0.next_tuple(|_, tuple| f(tuple)))? {
            Some(result) => result.map(Some),
            None => Ok(None),
        }
    }
}
