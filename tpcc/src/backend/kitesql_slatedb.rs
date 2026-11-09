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

//! KiteSQL's async path on SlateDB over S3.

use super::{
    BackendControl, BackendTransaction, DbParam, KiteSqlBatch, KiteSqlPreparedStatement,
    RowVisitor, SimpleExecutor, StatementSpec,
};
use crate::TpccError;
use kite_sql::db::{DataBaseBuilder, Database, ProgramIter, Transaction};
use kite_sql::storage::slatedb::{
    S3Config, SlateDbCacheOptions, SlateDbStorage, SlateDbTransaction,
};

pub struct KiteSqlSlateDbBackend {
    database: Database<SlateDbStorage>,
    storage: SlateDbStorage,
}

impl KiteSqlSlateDbBackend {
    pub async fn new(
        path: &str,
        config: &S3Config,
        await_durable: bool,
        cache: SlateDbCacheOptions,
    ) -> Result<Self, TpccError> {
        let storage = SlateDbStorage::open_s3(path, config, cache)
            .await?
            .await_durable(await_durable);
        Ok(Self {
            database: DataBaseBuilder::path(".").build_with_storage(storage.clone())?,
            storage,
        })
    }

    pub async fn close(&self) -> Result<(), TpccError> {
        Ok(self.storage.close().await?)
    }
}

impl BackendControl for KiteSqlSlateDbBackend {
    type PreparedStatement<'a>
        = KiteSqlPreparedStatement<'a>
    where
        Self: 'a;

    type Transaction<'a>
        = KiteSqlSlateDbTransaction<'a>
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
        Ok(KiteSqlSlateDbTransaction {
            inner: self.database.new_transaction(read_only)?,
        })
    }

    fn worker(&self) -> Result<Self::Worker<'_>, TpccError> {
        Ok(self)
    }

    fn is_async(&self) -> bool {
        true
    }
}

impl SimpleExecutor for KiteSqlSlateDbBackend {
    async fn execute_batch(&mut self, sql: &str) -> Result<(), TpccError> {
        let database = &mut self.database;
        match KiteSqlBatch::classify(&database.prepare_all(sql)?)? {
            KiteSqlBatch::Ddl => database.ddl(sql).await?,
            KiteSqlBatch::Analyze(tables) => {
                for table_name in tables {
                    let start = std::time::Instant::now();
                    println!("[KiteSQL Analyze: {table_name}] started");
                    database.analyze(&table_name).await?;
                    println!(
                        "[KiteSQL Analyze: {table_name}] completed in {:.2}s",
                        start.elapsed().as_secs_f64()
                    );
                }
            }
            KiteSqlBatch::Statements => {
                let mut transaction = database.new_transaction(false)?;
                transaction.run(sql).await?.done().await?;
                transaction.commit().await?;
            }
        }
        Ok(())
    }
}

pub struct KiteSqlSlateDbTransaction<'a> {
    inner: Transaction<'a, SlateDbStorage>,
}

type SlateDbIter<'b, 'a> = ProgramIter<'b, 'a, SlateDbTransaction>;

impl<'a> KiteSqlSlateDbTransaction<'a> {
    async fn execute_raw<'b>(
        &'b mut self,
        statement: &'b mut KiteSqlPreparedStatement<'a>,
        params: &[DbParam<'_>],
    ) -> Result<SlateDbIter<'b, 'a>, TpccError> {
        Ok(statement.program.execute(&mut self.inner, params).await?)
    }
}

async fn skip_next_tuple(iter: &mut SlateDbIter<'_, '_>) -> Result<bool, TpccError> {
    Ok(iter.next_tuple(|_, _| ()).await?.is_some())
}

async fn visit_next_tuple(
    iter: &mut SlateDbIter<'_, '_>,
    visitor: RowVisitor<'_>,
) -> Result<bool, TpccError> {
    match iter.next_tuple(|_, tuple| visitor(tuple)).await? {
        Some(result) => result.map(|()| true),
        None => Ok(false),
    }
}

impl<'a> BackendTransaction for KiteSqlSlateDbTransaction<'a> {
    type PreparedStatement = KiteSqlPreparedStatement<'a>;

    async fn execute_drain(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        while skip_next_tuple(&mut iter).await? {}
        Ok(())
    }

    async fn with_query_one(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        if visit_next_tuple(&mut iter, visitor).await? {
            Ok(())
        } else {
            Err(TpccError::EmptyTuples)
        }
    }

    async fn with_query_all(
        &mut self,
        statement: &mut Self::PreparedStatement,
        params: &[DbParam<'_>],
        visitor: RowVisitor<'_>,
    ) -> Result<(), TpccError> {
        let mut iter = self.execute_raw(statement, params).await?;
        while visit_next_tuple(&mut iter, visitor).await? {}
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
            if !skip_next_tuple(&mut iter).await? {
                return Err(TpccError::EmptyTuples);
            }
        }
        if visit_next_tuple(&mut iter, visitor).await? {
            Ok(())
        } else {
            Err(TpccError::EmptyTuples)
        }
    }

    async fn commit(self) -> Result<(), TpccError> {
        self.inner.commit().await?;
        Ok(())
    }
}
