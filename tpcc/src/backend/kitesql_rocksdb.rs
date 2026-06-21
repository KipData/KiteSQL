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
    BackendControl, BackendTransaction, DbParam, KiteSqlPreparedStatement, SimpleExecutor,
    StatementSpec,
};
use crate::TpccError;
use kite_sql::binder::{command_type, CommandType};
use kite_sql::db::{
    prepare, prepare_all, DBTransaction, DataBaseBuilder, Database, Statement, TransactionIter,
};
use kite_sql::storage::rocksdb::{OptimisticRocksStorage, RocksStorage};
use kite_sql::storage::{Storage, Transaction};
use kite_sql::types::tuple::Tuple;

pub type KiteSqlRocksDbBackend = KiteSqlRocksBackend<RocksStorage>;
pub type KiteSqlOptimisticRocksDbBackend = KiteSqlRocksBackend<OptimisticRocksStorage>;
pub type KiteSqlRocksDbTransaction<'a> = KiteSqlRocksTransaction<'a, RocksStorage>;

pub struct KiteSqlRocksBackend<S: Storage> {
    database: Database<S>,
}

impl KiteSqlRocksDbBackend {
    pub fn new(path: &str, rocksdb_stats: bool) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path)
                .storage_statistics(rocksdb_stats)
                .build_rocksdb()?,
        })
    }
}

impl KiteSqlOptimisticRocksDbBackend {
    pub fn new(path: &str, rocksdb_stats: bool) -> Result<Self, TpccError> {
        Ok(Self {
            database: DataBaseBuilder::path(path)
                .storage_statistics(rocksdb_stats)
                .build_optimistic()?,
        })
    }
}

impl<S: Storage> KiteSqlRocksBackend<S> {
    fn prepare_spec_groups(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<KiteSqlPreparedStatement>>, TpccError> {
        let mut groups = Vec::with_capacity(specs.len());
        for group in specs {
            let mut prepared = Vec::with_capacity(group.len());
            for spec in group {
                let statement = prepare(spec.sql)?;
                prepared.push(KiteSqlPreparedStatement {
                    statement,
                    spec: spec.clone(),
                });
            }
            groups.push(prepared);
        }
        Ok(groups)
    }

    fn start_transaction(&self) -> Result<KiteSqlRocksTransaction<'_, S>, TpccError> {
        Ok(KiteSqlRocksTransaction {
            inner: self.database.new_transaction()?,
        })
    }
}

impl<S: Storage> BackendControl for KiteSqlRocksBackend<S> {
    type PreparedStatement<'a>
        = KiteSqlPreparedStatement
    where
        Self: 'a;

    type Transaction<'a>
        = KiteSqlRocksTransaction<'a, S>
    where
        Self: 'a;

    fn prepare_statements(
        &self,
        specs: &[Vec<StatementSpec>],
    ) -> Result<Vec<Vec<Self::PreparedStatement<'_>>>, TpccError> {
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

impl<S: Storage> SimpleExecutor for KiteSqlRocksBackend<S> {
    fn execute_batch(&mut self, sql: &str) -> Result<(), TpccError> {
        execute_kitesql_batch(&mut self.database, sql)
    }
}

pub(crate) fn execute_kitesql_batch<S: Storage>(
    database: &mut Database<S>,
    sql: &str,
) -> Result<(), TpccError> {
    let statements = prepare_all(sql)?;
    let all_ddl = statements.iter().try_fold(true, |all_ddl, statement| {
        Ok::<_, kite_sql::errors::DatabaseError>(
            all_ddl && matches!(command_type(statement)?, CommandType::DDL),
        )
    })?;
    if all_ddl {
        database.ddl(sql)?;
        return Ok(());
    }

    if statements
        .iter()
        .all(|statement| matches!(statement, Statement::Analyze(_)))
    {
        for statement in statements {
            let Statement::Analyze(analyze) = statement else {
                unreachable!("checked above")
            };
            let table_name = analyze.table_name.ok_or_else(|| {
                kite_sql::errors::DatabaseError::UnsupportedStmt(
                    "ANALYZE requires table name".to_string(),
                )
            })?;
            let table_name = table_name.to_string();
            let start = std::time::Instant::now();
            println!("[KiteSQL Analyze: {table_name}] started");
            database.analyze(&table_name)?;
            println!(
                "[KiteSQL Analyze: {table_name}] completed in {:.2}s",
                start.elapsed().as_secs_f64()
            );
        }
        return Ok(());
    }

    let mut transaction = database.new_transaction()?;
    for statement in statements {
        transaction.execute(statement, &[])?.done()?;
    }
    transaction.commit()?;
    Ok(())
}

pub struct KiteSqlRocksTransaction<'a, S: Storage> {
    inner: DBTransaction<'a, S>,
}

impl<'a, S: Storage> KiteSqlRocksTransaction<'a, S> {
    pub(crate) fn execute_raw<'b>(
        &'b mut self,
        statement: &mut KiteSqlPreparedStatement,
        params: &[DbParam],
    ) -> Result<KiteSqlTxnResult<'b, S::TransactionType<'a>>, TpccError> {
        Ok(KiteSqlTxnResult::new(
            self.inner.execute(&statement.statement, params)?,
        ))
    }
}

impl<'a, S: Storage> BackendTransaction for KiteSqlRocksTransaction<'a, S> {
    type PreparedStatement = KiteSqlPreparedStatement;

    fn query_one(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam],
    ) -> Result<Tuple, TpccError> {
        self.execute_raw(statement, params)?
            .next_borrowed_tuple()?
            .cloned()
            .ok_or(TpccError::EmptyTuples)
    }

    fn query_nth(
        &mut self,
        statement: &mut Self::PreparedStatement,
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
        statement: &mut Self::PreparedStatement,
        params: &[DbParam],
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params)?;
        while iter.next_borrowed_tuple()?.is_some() {}
        Ok(())
    }

    fn with_query_one(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam],
        visitor: &mut dyn FnMut(&Tuple) -> Result<(), TpccError>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params)?;
        let tuple = iter.next_borrowed_tuple()?.ok_or(TpccError::EmptyTuples)?;
        visitor(tuple)
    }

    fn with_query_nth(
        &mut self,
        statement: &mut Self::PreparedStatement,
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
