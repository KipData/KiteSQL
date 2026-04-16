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

use crate::binder::{command_type, Binder, BinderContext, CommandType};
use crate::errors::DatabaseError;
use crate::execution::{build_write, ExecArena, Executor};
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::table::TableFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::function::char_length::CharLength;
use crate::function::current_date::CurrentDate;
use crate::function::current_timestamp::CurrentTimeStamp;
use crate::function::lower::Lower;
use crate::function::numbers::Numbers;
use crate::function::octet_length::OctetLength;
use crate::function::upper::Upper;
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
use crate::parser::parse_sql;
use crate::planner::operator::Operator;
use crate::planner::LogicalPlan;
#[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
use crate::storage::lmdb::{LmdbConfig, LmdbStorage};
use crate::storage::memory::MemoryStorage;
#[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
use crate::storage::rocksdb::{OptimisticRocksStorage, RocksStorage, StorageConfig};
use crate::storage::{
    CheckpointableStorage, StatisticsMetaCache, Storage, TableCache, Transaction,
    TransactionIsolationLevel, ViewCache,
};
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use crate::utils::lru::SharedLruCache;
use ahash::HashMap;
use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::hash::RandomState;
use std::marker::PhantomData;
use std::mem;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub(crate) type ScalaFunctions = HashMap<FunctionSummary, Arc<dyn ScalarFunctionImpl>>;
pub(crate) type TableFunctions = HashMap<FunctionSummary, Arc<dyn TableFunctionImpl>>;

/// Parsed SQL statement type used by KiteSQL execution APIs.
///
/// This is a type alias for `sqlparser::ast::Statement`. In most cases you do
/// not need to construct it manually; use [`prepare`] or [`prepare_all`] to
/// parse SQL text into statements.
pub type Statement = sqlparser::ast::Statement;

/// Parses a single SQL statement into a reusable [`Statement`].
///
/// This is useful when you want to parse once and execute the same statement
/// multiple times with different parameters. If the input contains multiple
/// statements, only the last one is returned.
///
/// # Examples
///
/// ```rust
/// use kite_sql::db::prepare;
///
/// let statement = prepare("select * from users where id = $1").unwrap();
/// println!("{statement:?}");
/// ```
pub fn prepare<T: AsRef<str>>(sql: T) -> Result<Statement, DatabaseError> {
    let mut stmts = prepare_all(sql)?;
    stmts.pop().ok_or(DatabaseError::EmptyStatement)
}

/// Parses one or more SQL statements into a vector of [`Statement`] values.
///
/// Returns [`DatabaseError::EmptyStatement`] when the input is empty or only
/// contains whitespace.
///
/// # Examples
///
/// ```rust
/// use kite_sql::db::prepare_all;
///
/// let statements = prepare_all("select 1; select 2;").unwrap();
/// assert_eq!(statements.len(), 2);
/// ```
pub fn prepare_all<T: AsRef<str>>(sql: T) -> Result<Vec<Statement>, DatabaseError> {
    let stmts = parse_sql(sql)?;
    if stmts.is_empty() {
        return Err(DatabaseError::EmptyStatement);
    }
    Ok(stmts)
}

#[allow(dead_code)]
pub(crate) enum MetaDataLock {
    Read(ArcRwLockReadGuard<RawRwLock, ()>),
    Write(ArcRwLockWriteGuard<RawRwLock, ()>),
}

/// Builder for creating a [`Database`] instance.
///
/// The builder wires together storage, built-in functions and optional runtime
/// features before the database is opened.
pub struct DataBaseBuilder {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    path: PathBuf,
    scala_functions: ScalaFunctions,
    table_functions: TableFunctions,
    histogram_buckets: Option<usize>,
    transaction_isolation: Option<TransactionIsolationLevel>,
    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    storage_config: StorageConfig,
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    lmdb_config: LmdbConfig,
}

impl DataBaseBuilder {
    /// Creates a builder rooted at the given database path.
    ///
    /// Built-in scalar functions and table functions are registered
    /// automatically.
    pub fn path(path: impl Into<PathBuf> + Send) -> Self {
        let mut builder = DataBaseBuilder {
            path: path.into(),
            scala_functions: Default::default(),
            table_functions: Default::default(),
            histogram_buckets: None,
            transaction_isolation: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
            storage_config: Default::default(),
            #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
            lmdb_config: Default::default(),
        };
        builder = builder.register_scala_function(CharLength::new("char_length".to_lowercase()));
        builder =
            builder.register_scala_function(CharLength::new("character_length".to_lowercase()));
        builder = builder.register_scala_function(CurrentDate::new());
        builder = builder.register_scala_function(CurrentTimeStamp::new());
        builder = builder.register_scala_function(Lower::new());
        builder = builder.register_scala_function(OctetLength::new());
        builder = builder.register_scala_function(Upper::new());
        builder = builder.register_table_function(Numbers::new());
        builder
    }

    /// Sets the default histogram bucket count used by `ANALYZE`.
    pub fn histogram_buckets(mut self, buckets: usize) -> Self {
        self.histogram_buckets = Some(buckets);
        self
    }

    /// Sets the transaction isolation level used by database-created transactions.
    pub fn transaction_isolation(mut self, isolation: TransactionIsolationLevel) -> Self {
        self.transaction_isolation = Some(isolation);
        self
    }

    /// Registers a user-defined scalar function on the database builder.
    pub fn register_scala_function(mut self, function: Arc<dyn ScalarFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.scala_functions.insert(summary, function);
        self
    }

    /// Registers a user-defined table function on the database builder.
    pub fn register_table_function(mut self, function: Arc<dyn TableFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.table_functions.insert(summary, function);
        self
    }

    /// Enables or disables RocksDB statistics collection.
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "rocksdb", feature = "lmdb")
    ))]
    pub fn storage_statistics(mut self, enable: bool) -> Self {
        #[cfg(feature = "rocksdb")]
        {
            self.storage_config.enable_statistics = enable;
        }
        #[cfg(feature = "lmdb")]
        {
            self.lmdb_config.enable_statistics = enable;
        }
        self
    }

    /// Sets the LMDB map size in bytes.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn lmdb_map_size(mut self, map_size: usize) -> Self {
        self.lmdb_config.map_size = map_size;
        self
    }

    /// Sets the LMDB environment flags.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn lmdb_flags(mut self, flags: lmdb::EnvironmentFlags) -> Self {
        self.lmdb_config.flags = flags;
        self
    }

    /// Enables or disables LMDB `NO_SYNC`.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn lmdb_no_sync(mut self, enable: bool) -> Self {
        self.lmdb_config
            .flags
            .set(lmdb::EnvironmentFlags::NO_SYNC, enable);
        self
    }

    /// Sets the maximum number of LMDB readers.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn lmdb_max_readers(mut self, max_readers: u32) -> Self {
        self.lmdb_config.max_readers = Some(max_readers);
        self
    }

    /// Sets the maximum number of LMDB named databases.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn lmdb_max_dbs(mut self, max_dbs: u32) -> Self {
        self.lmdb_config.max_dbs = Some(max_dbs);
        self
    }

    /// Builds a database using a custom storage implementation.
    pub fn build_with_storage<T: Storage>(self, storage: T) -> Result<Database<T>, DatabaseError> {
        Self::_build::<T>(
            storage,
            self.scala_functions,
            self.table_functions,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    /// Builds a database for the current target platform.
    #[cfg(target_arch = "wasm32")]
    pub fn build(self) -> Result<Database<MemoryStorage>, DatabaseError> {
        let storage = MemoryStorage::new();

        Self::_build::<MemoryStorage>(
            storage,
            self.scala_functions,
            self.table_functions,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    /// Builds a RocksDB-backed database.
    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    pub fn build_rocksdb(self) -> Result<Database<RocksStorage>, DatabaseError> {
        let storage = RocksStorage::with_config(self.path, self.storage_config)?;

        Self::_build::<RocksStorage>(
            storage,
            self.scala_functions,
            self.table_functions,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    /// Builds an in-memory database.
    ///
    /// This is convenient for tests, examples and temporary workloads.
    pub fn build_in_memory(self) -> Result<Database<MemoryStorage>, DatabaseError> {
        let storage = MemoryStorage::new();

        Self::_build::<MemoryStorage>(
            storage,
            self.scala_functions,
            self.table_functions,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    /// Builds a LMDB-backed database.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn build_lmdb(self) -> Result<Database<LmdbStorage>, DatabaseError> {
        let storage = LmdbStorage::with_config(self.path, self.lmdb_config)?;

        Self::_build::<LmdbStorage>(
            storage,
            self.scala_functions,
            self.table_functions,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    /// Builds a RocksDB-backed database that uses optimistic transactions.
    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    pub fn build_optimistic(self) -> Result<Database<OptimisticRocksStorage>, DatabaseError> {
        let storage = OptimisticRocksStorage::with_config(self.path, self.storage_config)?;

        Self::_build::<OptimisticRocksStorage>(
            storage,
            self.scala_functions,
            self.table_functions,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    fn _build<T: Storage>(
        storage: T,
        scala_functions: ScalaFunctions,
        table_functions: TableFunctions,
        histogram_buckets: Option<usize>,
        transaction_isolation: Option<TransactionIsolationLevel>,
    ) -> Result<Database<T>, DatabaseError> {
        if matches!(histogram_buckets, Some(0)) {
            return Err(DatabaseError::InvalidValue(
                "histogram buckets must be >= 1".to_string(),
            ));
        }
        let transaction_isolation =
            transaction_isolation.unwrap_or_else(|| storage.default_transaction_isolation());
        storage.validate_transaction_isolation(transaction_isolation)?;
        let meta_cache = SharedLruCache::new(256, 8, RandomState::new())?;
        let table_cache = SharedLruCache::new(48, 4, RandomState::new())?;
        let view_cache = SharedLruCache::new(12, 4, RandomState::new())?;

        Ok(Database {
            storage,
            transaction_isolation,
            mdl: Default::default(),
            state: Arc::new(State {
                scala_functions,
                table_functions,
                meta_cache,
                table_cache,
                view_cache,
                optimizer_pipeline: default_optimizer_pipeline(),
                histogram_buckets,
                _p: Default::default(),
            }),
        })
    }
}

fn default_optimizer_pipeline() -> HepOptimizerPipeline {
    HepOptimizerPipeline::builder()
        .before_batch(
            "Column Pruning".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::ColumnPruning],
        )
        .before_batch(
            "Simplify Filter".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![NormalizationRuleImpl::SimplifyFilter],
        )
        .before_batch(
            "Constant Calculation".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::ConstantCalculation],
        )
        .before_batch(
            "Predicate Pushdown".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![
                NormalizationRuleImpl::PushPredicateThroughJoin,
                NormalizationRuleImpl::PushJoinPredicateIntoScan,
                NormalizationRuleImpl::PushPredicateIntoScan,
            ],
        )
        .before_batch(
            "Limit Pushdown".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![
                NormalizationRuleImpl::LimitProjectTranspose,
                NormalizationRuleImpl::PushLimitThroughJoin,
                NormalizationRuleImpl::PushLimitIntoTableScan,
            ],
        )
        .before_batch(
            "TopK".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![
                NormalizationRuleImpl::MinMaxToTopK,
                NormalizationRuleImpl::TopK,
            ],
        )
        .before_batch(
            "Combine Operators".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![
                NormalizationRuleImpl::CollapseProject,
                NormalizationRuleImpl::CollapseGroupByAgg,
                NormalizationRuleImpl::CombineFilter,
            ],
        )
        .after_batch(
            "Parameterize Mark Apply".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::ParameterizeMarkApply],
        )
        .after_batch(
            "Expression Remapper".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::EvaluatorBind],
        )
        .implementations(vec![
            // DQL
            ImplementationRuleImpl::SimpleAggregate,
            ImplementationRuleImpl::GroupByAggregate,
            ImplementationRuleImpl::Dummy,
            ImplementationRuleImpl::Filter,
            ImplementationRuleImpl::HashJoin,
            ImplementationRuleImpl::Limit,
            ImplementationRuleImpl::Projection,
            ImplementationRuleImpl::ScalarSubquery,
            ImplementationRuleImpl::SeqScan,
            ImplementationRuleImpl::IndexScan,
            ImplementationRuleImpl::FunctionScan,
            ImplementationRuleImpl::Sort,
            ImplementationRuleImpl::TopK,
            ImplementationRuleImpl::Values,
            // DML
            ImplementationRuleImpl::Analyze,
            ImplementationRuleImpl::CopyFromFile,
            ImplementationRuleImpl::CopyToFile,
            ImplementationRuleImpl::Delete,
            ImplementationRuleImpl::Insert,
            ImplementationRuleImpl::Update,
            // DLL
            ImplementationRuleImpl::AddColumn,
            ImplementationRuleImpl::ChangeColumn,
            ImplementationRuleImpl::CreateTable,
            ImplementationRuleImpl::DropColumn,
            ImplementationRuleImpl::DropTable,
            ImplementationRuleImpl::Truncate,
        ])
        .build()
}

pub(crate) struct State<S> {
    scala_functions: ScalaFunctions,
    table_functions: TableFunctions,
    meta_cache: StatisticsMetaCache,
    table_cache: TableCache,
    view_cache: ViewCache,
    optimizer_pipeline: HepOptimizerPipeline,
    histogram_buckets: Option<usize>,
    _p: PhantomData<S>,
}

impl<S: Storage> State<S> {
    fn scala_functions(&self) -> &ScalaFunctions {
        &self.scala_functions
    }
    fn table_functions(&self) -> &TableFunctions {
        &self.table_functions
    }
    pub(crate) fn meta_cache(&self) -> &StatisticsMetaCache {
        &self.meta_cache
    }
    pub(crate) fn table_cache(&self) -> &TableCache {
        &self.table_cache
    }
    pub(crate) fn view_cache(&self) -> &ViewCache {
        &self.view_cache
    }

    fn build_plan<A: AsRef<[(&'static str, DataValue)]>>(
        &self,
        stmt: &Statement,
        params: A,
        transaction: &<S as Storage>::TransactionType<'_>,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut binder = Binder::new(
            BinderContext::new(
                self.table_cache(),
                self.view_cache(),
                transaction,
                self.scala_functions(),
                self.table_functions(),
                Arc::new(AtomicUsize::new(0)),
            ),
            &params,
            None,
        );
        /// Build a logical plan.
        ///
        /// SELECT a,b FROM t1 ORDER BY a LIMIT 1;
        /// Scan(t1)
        ///   Sort(a)
        ///     Limit(1)
        ///       Project(a,b)
        let source_plan = binder.bind(stmt)?;
        let mut best_plan = self
            .optimizer_pipeline
            .instantiate(source_plan)
            .find_best(Some(&transaction.meta_loader(self.meta_cache())))?;

        if let Operator::Analyze(op) = &mut best_plan.operator {
            if op.histogram_buckets.is_none() {
                op.histogram_buckets = self.histogram_buckets;
            }
        }

        Ok(best_plan)
    }

    fn execute<'a, 'txn, A: AsRef<[(&'static str, DataValue)]>>(
        &'a self,
        transaction: &'a mut S::TransactionType<'txn>,
        stmt: &Statement,
        params: A,
    ) -> Result<(SchemaRef, Executor<'a, S::TransactionType<'txn>>), DatabaseError>
    where
        S: 'txn,
    {
        transaction.begin_statement_scope()?;
        match (|| {
            let mut plan = self.build_plan(stmt, params, transaction)?;
            let schema = plan.output_schema().clone();
            let mut arena = ExecArena::default();
            let root = build_write(
                &mut arena,
                plan,
                (&self.table_cache, &self.view_cache, &self.meta_cache),
                transaction,
            );
            let executor = Executor::new(arena, root);

            Ok((schema, executor))
        })() {
            Ok(result) => Ok(result),
            Err(err) => {
                transaction.end_statement_scope()?;
                Err(err)
            }
        }
    }
}

/// Main database handle for executing SQL and creating transactions.
pub struct Database<S: Storage> {
    pub(crate) storage: S,
    transaction_isolation: TransactionIsolationLevel,
    mdl: Arc<RwLock<()>>,
    pub(crate) state: Arc<State<S>>,
}

impl<S: Storage> Database<S> {
    /// Runs one or more SQL statements and returns an iterator for the final result set.
    ///
    /// Earlier statements in the same SQL string are executed eagerly. The last
    /// statement is exposed as a streaming iterator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use kite_sql::db::{DataBaseBuilder, ResultIter};
    ///
    /// let database = DataBaseBuilder::path(".").build_in_memory().unwrap();
    /// database.run("create table t (id int primary key)").unwrap().done().unwrap();
    /// let mut iter = database.run("select * from t").unwrap();
    /// let _schema = iter.schema().clone();
    /// iter.done().unwrap();
    /// ```
    pub fn run<T: AsRef<str>>(&self, sql: T) -> Result<DatabaseIter<'_, S>, DatabaseError> {
        let sql = sql.as_ref();
        let statements = prepare_all(sql).map_err(|err| err.with_sql_context(sql))?;
        let has_ddl = statements
            .iter()
            .try_fold(false, |has_ddl, stmt| {
                Ok::<_, DatabaseError>(has_ddl || matches!(command_type(stmt)?, CommandType::DDL))
            })
            .map_err(|err| err.with_sql_context(sql))?;

        if statements.len() > 1 && has_ddl {
            return Err(DatabaseError::UnsupportedStmt(
                "DDL is not allowed in multi-statement execution".to_string(),
            )
            .with_sql_context(sql));
        }

        let guard = if has_ddl {
            MetaDataLock::Write(self.mdl.write_arc())
        } else {
            MetaDataLock::Read(self.mdl.read_arc())
        };

        let transaction = Box::into_raw(Box::new(
            self.storage
                .transaction_with_isolation(self.transaction_isolation)?,
        ));
        let mut statements = statements.into_iter().peekable();

        while let Some(statement) = statements.next() {
            let (schema, executor) =
                match self
                    .state
                    .execute(unsafe { &mut *transaction }, &statement, &[])
                {
                    Ok(result) => result,
                    Err(err) => {
                        unsafe { drop(Box::from_raw(transaction)) };
                        return Err(err.with_sql_context(sql));
                    }
                };

            if statements.peek().is_some() {
                if let Err(err) = TransactionIter::new(schema, executor, transaction).done() {
                    unsafe { drop(Box::from_raw(transaction)) };
                    return Err(err.with_sql_context(sql));
                }
            } else {
                let inner = Box::into_raw(Box::new(TransactionIter::new(
                    schema,
                    executor,
                    transaction,
                )));
                return Ok(DatabaseIter {
                    transaction,
                    inner,
                    _guard: Some(guard),
                });
            }
        }

        unsafe { drop(Box::from_raw(transaction)) };
        Err(DatabaseError::EmptyStatement.with_sql_context(sql))
    }

    /// Executes a prepared [`Statement`] inside the current transaction.
    pub fn execute<A: AsRef<[(&'static str, DataValue)]>>(
        &self,
        statement: &Statement,
        params: A,
    ) -> Result<DatabaseIter<'_, S>, DatabaseError> {
        let guard = if matches!(command_type(statement)?, CommandType::DDL) {
            MetaDataLock::Write(self.mdl.write_arc())
        } else {
            MetaDataLock::Read(self.mdl.read_arc())
        };
        let transaction = Box::into_raw(Box::new(
            self.storage
                .transaction_with_isolation(self.transaction_isolation)?,
        ));
        let (schema, executor) =
            match self
                .state
                .execute(unsafe { &mut *transaction }, statement, params)
            {
                Ok(result) => result,
                Err(err) => {
                    unsafe { drop(Box::from_raw(transaction)) };
                    return Err(err);
                }
            };
        let inner = Box::into_raw(Box::new(TransactionIter::new(
            schema,
            executor,
            transaction,
        )));
        Ok(DatabaseIter {
            transaction,
            inner,
            _guard: Some(guard),
        })
    }

    /// Opens a new explicit transaction.
    ///
    /// Statements executed through the returned transaction share the same
    /// transactional context until [`DBTransaction::commit`] is called.
    pub fn new_transaction(&self) -> Result<DBTransaction<'_, S>, DatabaseError> {
        let guard = self.mdl.read_arc();
        let transaction = self
            .storage
            .transaction_with_isolation(self.transaction_isolation)?;
        let state = self.state.clone();

        Ok(DBTransaction {
            inner: transaction,
            _guard: guard,
            state,
        })
    }

    /// Returns storage-engine-specific metrics when supported.
    ///
    /// To enable metrics collection when the underlying storage supports it,
    /// configure the database with [`DataBaseBuilder::storage_statistics(true)`](crate::db::DataBaseBuilder::storage_statistics)
    /// during construction. If metrics collection is not enabled, this returns
    /// `None`.
    #[inline]
    pub fn storage_metrics(&self) -> Option<S::Metrics> {
        self.storage.metrics()
    }

    #[inline]
    pub fn transaction_isolation(&self) -> TransactionIsolationLevel {
        self.transaction_isolation
    }
}

impl<S> Database<S>
where
    S: CheckpointableStorage,
{
    /// Creates an online consistent checkpoint in `path`.
    ///
    /// The target path must not exist or must be an empty directory.
    #[inline]
    pub fn checkpoint<P: AsRef<Path>>(&self, path: P) -> Result<(), DatabaseError> {
        self.storage.create_checkpoint(path)
    }
}

/// Borrowing interface for result iterators returned by database execution APIs.
pub trait BorrowResultIter {
    /// Returns the output schema for the current result set.
    fn schema(&self) -> &SchemaRef;

    /// Returns the next row as a borrowed tuple.
    fn next_borrowed_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError>;

    /// Creates a mapped iterator that transforms borrowed tuples into owned output values.
    fn map_result<F, O>(self, mapper: F) -> MappedResultIter<Self, F, O>
    where
        Self: Sized,
        F: for<'a> FnMut(&'a SchemaRef, &'a Tuple) -> Result<O, DatabaseError>,
    {
        let schema = self.schema().clone();
        MappedResultIter {
            inner: self,
            mapper,
            schema,
            _marker: PhantomData,
        }
    }

    /// Finishes consuming the iterator and flushes any remaining work.
    fn done(self) -> Result<(), DatabaseError>;
}

/// Common interface for owned-tuple result iterators.
///
/// This remains for compatibility with existing callers that expect
/// `Iterator<Item = Result<Tuple, DatabaseError>>`.
pub trait ResultIter: BorrowResultIter + Iterator<Item = Result<Tuple, DatabaseError>> {
    #[cfg(feature = "orm")]
    /// Converts this iterator into a typed ORM iterator.
    ///
    /// This is available when the `orm` feature is enabled and the target type
    /// implements `From<(&SchemaRef, Tuple)>`, which is typically generated by
    /// `#[derive(Model)]`.
    fn orm<T>(self) -> OrmIter<Self, T>
    where
        Self: Sized,
        T: for<'a> From<(&'a SchemaRef, Tuple)>,
    {
        OrmIter::new(self)
    }
}

impl<I> ResultIter for I where I: BorrowResultIter + Iterator<Item = Result<Tuple, DatabaseError>> {}

/// Typed adapter over a borrowing result iterator.
pub struct MappedResultIter<I, F, O> {
    inner: I,
    mapper: F,
    schema: SchemaRef,
    _marker: PhantomData<O>,
}

impl<I, F, O> MappedResultIter<I, F, O>
where
    I: BorrowResultIter,
    F: for<'a> FnMut(&'a SchemaRef, &'a Tuple) -> Result<O, DatabaseError>,
{
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn done(self) -> Result<(), DatabaseError> {
        self.inner.done()
    }
}

impl<I, F, O> Iterator for MappedResultIter<I, F, O>
where
    I: BorrowResultIter,
    F: for<'a> FnMut(&'a SchemaRef, &'a Tuple) -> Result<O, DatabaseError>,
{
    type Item = Result<O, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next_borrowed_tuple() {
            Ok(Some(tuple)) => Some((self.mapper)(&self.schema, tuple)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

#[cfg(feature = "orm")]
/// Typed adapter over a [`ResultIter`] that yields ORM models instead of raw tuples.
pub struct OrmIter<I, T> {
    inner: I,
    schema: SchemaRef,
    _marker: PhantomData<T>,
}

#[cfg(feature = "orm")]
impl<I, T> OrmIter<I, T>
where
    I: ResultIter,
    T: for<'a> From<(&'a SchemaRef, Tuple)>,
{
    fn new(inner: I) -> Self {
        let schema = inner.schema().clone();

        Self {
            inner,
            schema,
            _marker: PhantomData,
        }
    }

    /// Returns the schema of the underlying result set.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Finishes the underlying raw iterator.
    pub fn done(self) -> Result<(), DatabaseError> {
        self.inner.done()
    }
}

#[cfg(feature = "orm")]
impl<I, T> Iterator for OrmIter<I, T>
where
    I: ResultIter,
    T: for<'a> From<(&'a SchemaRef, Tuple)>,
{
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.map(|tuple| T::from((&self.schema, tuple))))
    }
}

/// Raw result iterator returned by [`Database::run`] and [`Database::execute`].
pub struct DatabaseIter<'a, S: Storage + 'a> {
    transaction: *mut S::TransactionType<'a>,
    inner: *mut TransactionIter<'a, S::TransactionType<'a>>,
    _guard: Option<MetaDataLock>,
}

impl<S: Storage> Drop for DatabaseIter<'_, S> {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe { drop(Box::from_raw(self.inner)) }
        }
        if !self.transaction.is_null() {
            unsafe { drop(Box::from_raw(self.transaction)) }
        }
    }
}

impl<S: Storage> DatabaseIter<'_, S> {
    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        unsafe { (*self.inner).schema() }
    }

    #[inline]
    pub fn next_borrowed_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
        let result = unsafe { (*self.inner).next_borrowed_tuple() };
        if result.as_ref().is_ok_and(Option::is_none) {
            self._guard = None;
        }
        result
    }

    #[inline]
    pub fn done(mut self) -> Result<(), DatabaseError> {
        unsafe {
            Box::from_raw(mem::replace(&mut self.inner, std::ptr::null_mut())).done()?;
        }
        unsafe {
            Box::from_raw(mem::replace(&mut self.transaction, std::ptr::null_mut())).commit()?;
        }
        Ok(())
    }
}

impl<S: Storage> Iterator for DatabaseIter<'_, S> {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = unsafe { (*self.inner).next() };
        if result.is_none() {
            self._guard = None;
        }
        result
    }
}

impl<S: Storage> BorrowResultIter for DatabaseIter<'_, S> {
    fn schema(&self) -> &SchemaRef {
        DatabaseIter::schema(self)
    }

    fn next_borrowed_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
        DatabaseIter::next_borrowed_tuple(self)
    }

    fn done(self) -> Result<(), DatabaseError> {
        DatabaseIter::done(self)
    }
}

/// Explicit transaction handle created by [`Database::new_transaction`].
pub struct DBTransaction<'a, S: Storage + 'a> {
    inner: S::TransactionType<'a>,
    _guard: ArcRwLockReadGuard<RawRwLock, ()>,
    state: Arc<State<S>>,
}

impl<'txn, S: Storage> DBTransaction<'txn, S> {
    /// Runs SQL inside the current transaction and returns the final result iterator.
    pub fn run<'a, T: AsRef<str>>(
        &'a mut self,
        sql: T,
    ) -> Result<TransactionIter<'a, S::TransactionType<'txn>>, DatabaseError> {
        let sql = sql.as_ref();
        let mut statements = prepare_all(sql).map_err(|err| err.with_sql_context(sql))?;
        let last_statement = statements
            .pop()
            .ok_or_else(|| DatabaseError::EmptyStatement.with_sql_context(sql))?;

        for statement in statements {
            self.execute(&statement, &[])
                .map_err(|err| err.with_sql_context(sql))?
                .done()
                .map_err(|err| err.with_sql_context(sql))?;
        }

        self.execute(&last_statement, &[])
            .map_err(|err| err.with_sql_context(sql))
    }

    /// Executes a prepared [`Statement`] inside the current transaction.
    pub fn execute<'a, A: AsRef<[(&'static str, DataValue)]>>(
        &'a mut self,
        statement: &Statement,
        params: A,
    ) -> Result<TransactionIter<'a, S::TransactionType<'txn>>, DatabaseError> {
        if matches!(command_type(statement)?, CommandType::DDL) {
            return Err(DatabaseError::UnsupportedStmt(
                "`DDL` is not allowed to execute within a transaction".to_string(),
            ));
        }
        let transaction = std::ptr::from_mut(&mut self.inner);
        let (schema, executor) =
            self.state
                .execute(unsafe { &mut *transaction }, statement, params)?;
        Ok(TransactionIter::new(schema, executor, transaction))
    }

    /// Commits the current transaction.
    pub fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit()?;

        Ok(())
    }
}

/// Raw result iterator returned by [`DBTransaction::run`] and [`DBTransaction::execute`].
pub struct TransactionIter<'a, T: Transaction + 'a> {
    executor: Option<Executor<'a, T>>,
    schema: SchemaRef,
    transaction: *mut T,
    statement_scope_active: bool,
}

impl<'a, T: Transaction + 'a> TransactionIter<'a, T> {
    fn new(schema: SchemaRef, executor: Executor<'a, T>, transaction: *mut T) -> Self {
        Self {
            executor: Some(executor),
            schema,
            transaction,
            statement_scope_active: true,
        }
    }

    #[inline]
    fn finish_statement_scope(&mut self) -> Result<(), DatabaseError> {
        if !self.statement_scope_active {
            return Ok(());
        }

        self.executor.take();
        self.statement_scope_active = false;
        unsafe { (*self.transaction).end_statement_scope() }
    }

    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[inline]
    pub fn next_borrowed_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
        let Some(executor) = self.executor.as_mut() else {
            return Ok(None);
        };
        let executor_ptr = std::ptr::from_mut(executor);
        match unsafe { (*executor_ptr).next_tuple() } {
            Ok(Some(tuple)) => Ok(Some(tuple)),
            Ok(None) => {
                self.finish_statement_scope()?;
                Ok(None)
            }
            Err(err) => {
                self.finish_statement_scope()?;
                Err(err)
            }
        }
    }

    #[inline]
    pub fn done(mut self) -> Result<(), DatabaseError> {
        while self.next_borrowed_tuple()?.is_some() {}
        Ok(())
    }
}

impl<T: Transaction> Drop for TransactionIter<'_, T> {
    fn drop(&mut self) {
        let _ = self.finish_statement_scope();
    }
}

impl<T: Transaction> Iterator for TransactionIter<'_, T> {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = {
            let executor = self.executor.as_mut()?;
            executor.next_tuple()
        };
        match result {
            Ok(Some(tuple)) => Some(Ok(tuple.clone())),
            Ok(None) => match self.finish_statement_scope() {
                Ok(()) => None,
                Err(err) => Some(Err(err)),
            },
            Err(err) => match self.finish_statement_scope() {
                Ok(()) => Some(Err(err)),
                Err(scope_err) => Some(Err(scope_err)),
            },
        }
    }
}

impl<T: Transaction> BorrowResultIter for TransactionIter<'_, T> {
    fn schema(&self) -> &SchemaRef {
        TransactionIter::schema(self)
    }

    fn next_borrowed_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
        TransactionIter::next_borrowed_tuple(self)
    }

    fn done(self) -> Result<(), DatabaseError> {
        TransactionIter::done(self)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::binder::{Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::db::{BorrowResultIter, DataBaseBuilder, DatabaseError};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::join::JoinCondition;
    use crate::planner::operator::Operator;
    use crate::storage::{Storage, TableCache, Transaction, TransactionIsolationLevel};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use chrono::{Datelike, Local};
    use std::io::ErrorKind;
    use std::sync::atomic::AtomicUsize;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::thread;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::time::Duration;
    use tempfile::TempDir;

    pub(crate) fn build_table<T: Transaction>(
        table_cache: &TableCache,
        transaction: &mut T,
    ) -> Result<(), DatabaseError> {
        let columns = vec![
            ColumnCatalog::new(
                "c1".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, Some(0), false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c2".to_string(),
                false,
                ColumnDesc::new(LogicalType::Boolean, None, false, None).unwrap(),
            ),
            ColumnCatalog::new(
                "c3".to_string(),
                false,
                ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
            ),
        ];
        let _ = transaction.create_table(table_cache, "t1".to_string().into(), columns, false)?;

        Ok(())
    }

    fn read_single_i32<I>(mut iter: I) -> Result<i32, DatabaseError>
    where
        I: BorrowResultIter + Iterator<Item = Result<Tuple, DatabaseError>>,
    {
        let value = match iter.next().transpose()?.map(|tuple| tuple.values) {
            Some(values) => match values.as_slice() {
                [DataValue::Int32(value)] => *value,
                other => panic!("expected a single Int32 column, got {other:?}"),
            },
            None => panic!("expected one result row"),
        };
        iter.done()?;
        Ok(value)
    }

    #[cfg(feature = "unsafe_txdb_checkpoint")]
    fn query_i32<S: Storage>(
        database: &crate::db::Database<S>,
        sql: &str,
    ) -> Result<i32, DatabaseError> {
        let mut iter = database.run(sql)?;
        let value = match iter.next().transpose()?.map(|tuple| tuple.values) {
            Some(values) => match values.as_slice() {
                [DataValue::Int32(value)] => *value,
                other => panic!("expected a single Int32 column, got {other:?}"),
            },
            None => panic!("expected one result row for query: {sql}"),
        };
        iter.done()?;
        Ok(value)
    }

    #[test]
    fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        let mut transaction = database.storage.transaction()?;

        build_table(database.state.table_cache(), &mut transaction)?;
        transaction.commit()?;

        for result in database.run("select * from t1")? {
            println!("{:#?}", result?);
        }
        Ok(())
    }

    /// use [CurrentDate](crate::function::current_date::CurrentDate) on this case
    #[test]
    fn test_udf() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        let mut iter = kite_sql.run("select current_date()")?;

        assert_eq!(
            iter.schema(),
            &Arc::new(vec![ColumnRef::from(ColumnCatalog::new(
                "current_date()".to_string(),
                true,
                ColumnDesc::new(LogicalType::Date, None, false, None).unwrap()
            ))])
        );
        assert_eq!(
            iter.next().unwrap()?,
            Tuple::new(
                None,
                vec![DataValue::Date32(Local::now().num_days_from_ce())]
            )
        );
        assert!(iter.next().is_none());

        Ok(())
    }

    /// use [Numbers](crate::function::numbers::Numbers) on this case
    #[test]
    fn test_udtf() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        let mut iter = kite_sql.run(
            "SELECT * FROM (select * from table(numbers(10)) a ORDER BY number LIMIT 5) OFFSET 3",
        )?;

        let mut column = ColumnCatalog::new(
            "number".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        );
        let number_column_id = iter.schema()[0].id().unwrap();
        column.set_ref_table("a".to_string().into(), number_column_id, false);

        assert_eq!(iter.schema(), &Arc::new(vec![ColumnRef::from(column)]));
        assert_eq!(
            iter.next().unwrap()?,
            Tuple::new(None, vec![DataValue::Int32(3)])
        );
        assert_eq!(
            iter.next().unwrap()?,
            Tuple::new(None, vec![DataValue::Int32(4)])
        );
        Ok(())
    }

    #[test]
    fn test_join_on_alias_right_key_is_localized() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("CREATE TABLE onecolumn (id INT PRIMARY KEY, x INT NULL)")?
            .done()?;
        kite_sql
            .run("CREATE TABLE empty (e_id INT PRIMARY KEY, x INT)")?
            .done()?;

        let stmt = crate::db::prepare(
            "SELECT * FROM onecolumn AS a(aid, x) JOIN empty AS b(bid, y) ON a.x = b.y",
        )?;
        let transaction = kite_sql.storage.transaction()?;
        let mut binder = Binder::new(
            BinderContext::new(
                kite_sql.state.table_cache(),
                kite_sql.state.view_cache(),
                &transaction,
                kite_sql.state.scala_functions(),
                kite_sql.state.table_functions(),
                Arc::new(AtomicUsize::new(0)),
            ),
            &[],
            None,
        );
        let source_plan = binder.bind(&stmt)?;
        let best_plan = kite_sql.state.build_plan(&stmt, [], &transaction)?;

        let join_plan = match source_plan.operator {
            Operator::Project(_) => source_plan.childrens.pop_only(),
            Operator::Join(_) => source_plan,
            _ => unreachable!("expected a join plan"),
        };
        let Operator::Join(join_op) = join_plan.operator else {
            unreachable!("expected join operator");
        };
        let JoinCondition::On { on, filter } = join_op.on else {
            unreachable!("expected join condition");
        };
        assert!(filter.is_none());
        assert_eq!(on.len(), 1);
        let ScalarExpression::ColumnRef {
            position: left_position,
            ..
        } = on[0].0.unpack_alias_ref()
        else {
            unreachable!("expected left join key column ref");
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = on[0].1.unpack_alias_ref()
        else {
            unreachable!("expected right join key column ref");
        };
        assert_eq!(*left_position, 1);
        assert_eq!(*right_position, 1);

        let join_plan = match best_plan.operator {
            Operator::Project(_) => best_plan.childrens.pop_only(),
            Operator::Join(_) => best_plan,
            _ => unreachable!("expected a join plan"),
        };
        let Operator::Join(join_op) = join_plan.operator else {
            unreachable!("expected join operator");
        };
        let JoinCondition::On { on, .. } = join_op.on else {
            unreachable!("expected join condition");
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = on[0].1.unpack_alias_ref()
        else {
            unreachable!("expected right join key column ref");
        };
        assert_eq!(*right_position, 1);

        Ok(())
    }

    #[test]
    fn test_join_on_with_right_filter_keeps_localized_key() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("CREATE TABLE onecolumn (id INT PRIMARY KEY, x INT NULL)")?
            .done()?;
        kite_sql
            .run("CREATE TABLE twocolumn (t_id INT PRIMARY KEY, x INT NULL, y INT NULL)")?
            .done()?;

        let stmt = crate::db::prepare(
            "SELECT o.x, t.y FROM onecolumn o INNER JOIN twocolumn t ON (o.x=t.x AND t.y=53)",
        )?;
        let transaction = kite_sql.storage.transaction()?;
        let mut binder = Binder::new(
            BinderContext::new(
                kite_sql.state.table_cache(),
                kite_sql.state.view_cache(),
                &transaction,
                kite_sql.state.scala_functions(),
                kite_sql.state.table_functions(),
                Arc::new(AtomicUsize::new(0)),
            ),
            &[],
            None,
        );
        let source_plan = binder.bind(&stmt)?;
        let best_plan = kite_sql.state.build_plan(&stmt, [], &transaction)?;

        let join_plan = match source_plan.operator {
            Operator::Project(_) => source_plan.childrens.pop_only(),
            Operator::Join(_) => source_plan,
            _ => unreachable!("expected a join plan"),
        };
        let Operator::Join(join_op) = join_plan.operator else {
            unreachable!("expected join operator");
        };
        let JoinCondition::On { on, filter } = join_op.on else {
            unreachable!("expected join condition");
        };
        assert_eq!(on.len(), 1);
        let ScalarExpression::ColumnRef {
            position: left_position,
            ..
        } = on[0].0.unpack_alias_ref()
        else {
            unreachable!("expected left join key column ref");
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = on[0].1.unpack_alias_ref()
        else {
            unreachable!("expected right join key column ref");
        };
        assert_eq!(*left_position, 1);
        assert_eq!(*right_position, 1);
        let Some(filter) = filter else {
            unreachable!("expected join filter");
        };
        let mut referenced_columns = Vec::new();
        filter.visit_referenced_columns(true, &mut |column| {
            referenced_columns.push(column.clone());
            true
        });
        assert_eq!(referenced_columns.len(), 1);
        assert_eq!(referenced_columns[0].name(), "y");

        let join_plan = match best_plan.operator {
            Operator::Project(_) => best_plan.childrens.pop_only(),
            Operator::Join(_) => best_plan,
            _ => unreachable!("expected a join plan"),
        };
        let Operator::Join(join_op) = join_plan.operator else {
            unreachable!("expected join operator");
        };
        let JoinCondition::On { on, filter } = join_op.on else {
            unreachable!("expected join condition");
        };
        assert_eq!(on.len(), 1);
        assert!(filter.is_none());
        let ScalarExpression::ColumnRef {
            position: left_position,
            ..
        } = on[0].0.unpack_alias_ref()
        else {
            unreachable!("expected left join key column ref");
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = on[0].1.unpack_alias_ref()
        else {
            unreachable!("expected right join key column ref");
        };
        assert_eq!(*left_position, 0);
        assert_eq!(*right_position, 0);

        Ok(())
    }

    #[test]
    fn test_join_on_with_right_filter_keeps_localized_key_with_data() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("CREATE TABLE onecolumn (id INT PRIMARY KEY, x INT NULL)")?
            .done()?;
        kite_sql
            .run("CREATE TABLE twocolumn (t_id INT PRIMARY KEY, x INT NULL, y INT NULL)")?
            .done()?;
        kite_sql
            .run("INSERT INTO onecolumn(id, x) VALUES (0, 44), (1, NULL), (2, 42)")?
            .done()?;
        kite_sql
            .run(
                "INSERT INTO twocolumn(t_id, x, y) VALUES (0,44,51), (1,NULL,52), (2,42,53), (3,45,45)",
            )?
            .done()?;

        let stmt = crate::db::prepare(
            "SELECT o.x, t.y FROM onecolumn o INNER JOIN twocolumn t ON (o.x=t.x AND t.y=53)",
        )?;
        let transaction = kite_sql.storage.transaction()?;
        let best_plan = kite_sql.state.build_plan(&stmt, [], &transaction)?;
        let join_plan = match best_plan.operator {
            Operator::Project(_) => best_plan.childrens.pop_only(),
            Operator::Join(_) => best_plan,
            _ => unreachable!("expected a join plan"),
        };
        let Operator::Join(join_op) = join_plan.operator else {
            unreachable!("expected join operator");
        };
        let JoinCondition::On { on, filter } = join_op.on else {
            unreachable!("expected join condition");
        };
        assert_eq!(on.len(), 1);
        assert!(filter.is_none());
        let ScalarExpression::ColumnRef {
            position: left_position,
            ..
        } = on[0].0.unpack_alias_ref()
        else {
            unreachable!("expected left join key column ref");
        };
        let ScalarExpression::ColumnRef {
            position: right_position,
            ..
        } = on[0].1.unpack_alias_ref()
        else {
            unreachable!("expected right join key column ref");
        };
        assert_eq!(*left_position, 0);
        assert_eq!(*right_position, 0);
        let (_, right_child) = join_plan.childrens.pop_twins();
        let Operator::Filter(filter_op) = right_child.operator else {
            unreachable!("expected pushed-down filter on right child");
        };
        let ScalarExpression::Binary {
            left_expr,
            right_expr,
            ..
        } = filter_op.predicate
        else {
            unreachable!("expected binary filter predicate");
        };
        let ScalarExpression::ColumnRef {
            position: filter_position,
            ..
        } = left_expr.unpack_alias_ref()
        else {
            unreachable!("expected filter column ref");
        };
        assert_eq!(*filter_position, 1);
        assert!(matches!(
            *right_expr,
            ScalarExpression::Constant(DataValue::Int32(53))
        ));

        Ok(())
    }

    #[test]
    fn test_prepare_statment() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
        kite_sql.run("insert into t1 values(0, 0)")?.done()?;
        kite_sql.run("insert into t1 values(1, 1)")?.done()?;
        kite_sql.run("insert into t1 values(2, 2)")?.done()?;

        // Filter
        {
            let statement = crate::db::prepare("explain select * from t1 where b > $1")?;

            let mut iter = kite_sql.execute(&statement, &[("$1", DataValue::Int32(0))])?;

            assert_eq!(
                iter.next().unwrap()?.values[0].utf8().unwrap(),
                "Projection [t1.a, t1.b] [Project => (Sort Option: Follow)]
  Filter (t1.b > 0), Is Having: false [Filter => (Sort Option: Follow)]
    TableScan t1 -> [a, b] [SeqScan => (Sort Option: None)]"
            )
        }
        // Aggregate
        {
            let statement = crate::db::prepare(
                "explain select a + $1, max(b + $2) from t1 where b > $3 group by a + $4",
            )?;

            let mut iter = kite_sql.execute(
                &statement,
                &[
                    ("$1", DataValue::Int32(0)),
                    ("$2", DataValue::Int32(0)),
                    ("$3", DataValue::Int32(1)),
                    ("$4", DataValue::Int32(0)),
                ],
            )?;
            assert_eq!(
                iter.next().unwrap()?.values[0].utf8().unwrap(),
                "Projection [(t1.a + 0), Max((t1.b + 0))] [Project => (Sort Option: Follow)]
  Aggregate [Max((t1.b + 0))] -> Group By [(t1.a + 0)] [HashAggregate => (Sort Option: None)]
    Filter (t1.b > 1), Is Having: false [Filter => (Sort Option: Follow)]
      TableScan t1 -> [a, b] [SeqScan => (Sort Option: None)]"
            )
        }
        {
            let statement = crate::db::prepare("explain select *, $1 from (select * from t1 where b > $2) left join (select * from t1 where a > $3) on a > $4")?;

            let mut iter = kite_sql.execute(
                &statement,
                &[
                    ("$1", DataValue::Int32(9)),
                    ("$2", DataValue::Int32(0)),
                    ("$3", DataValue::Int32(1)),
                    ("$4", DataValue::Int32(0)),
                ],
            )?;
            assert_eq!(
                iter.next().unwrap()?.values[0].utf8().unwrap(),
                "Projection [t1.a, t1.b, 9] [Project => (Sort Option: Follow)]
  LeftOuter Join Where (t1.a > 0) [NestLoopJoin => (Sort Option: None)]
    Projection [t1.a, t1.b] [Project => (Sort Option: Follow)]
      Filter (t1.b > 0), Is Having: false [Filter => (Sort Option: Follow)]
        TableScan t1 -> [a, b] [SeqScan => (Sort Option: None)]
    Projection [t1.a, t1.b] [Project => (Sort Option: Follow)]
      Filter (t1.a > 1), Is Having: false [Filter => (Sort Option: Follow)]
        TableScan t1 -> [a, b] [SeqScan => (Sort Option: None)]"
            )
        }

        Ok(())
    }

    // FIXME: keep this as a unit test instead of SLT for now. The current
    // sqllogictest runner does not reliably match the pretty-printed multi-line
    // EXPLAIN output produced by correlated IN, even though the plan itself is stable.
    #[test]
    fn test_subquery_explain_uses_parameterized_index_for_in() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table in_outer(id int primary key, a int)")?
            .done()?;
        kite_sql
            .run("create table in_inner(id int primary key, v int)")?
            .done()?;
        kite_sql
            .run("create table in_inner_nn(id int primary key, v int)")?
            .done()?;
        kite_sql
            .run("create index in_inner_v_index on in_inner(v)")?
            .done()?;
        kite_sql
            .run("create index in_inner_nn_v_index on in_inner_nn(v)")?
            .done()?;

        kite_sql
            .run("insert into in_outer values (0, null), (1, 1), (2, 2), (3, 3)")?
            .done()?;
        kite_sql
            .run("insert into in_inner values (0, 2), (1, null)")?
            .done()?;
        kite_sql
            .run("insert into in_inner_nn values (0, 2)")?
            .done()?;

        let collect_plan = |sql: &str| -> Result<String, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let rows = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
            iter.done()?;
            Ok(rows
                .iter()
                .filter_map(|row| match row.values.first() {
                    Some(DataValue::Utf8 { value, .. }) => Some(value.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n"))
        };
        let collect_ids = |sql: &str| -> Result<Vec<i32>, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let mut ids = Vec::new();
            while let Some(row) = iter.next() {
                let row = row?;
                ids.push(row.values[0].i32().unwrap());
            }
            iter.done()?;
            Ok(ids)
        };

        let assert_mark_in_uses_parameterized_index =
            |sql: &str, index_name: &str| -> Result<(), DatabaseError> {
                let explain_plan = collect_plan(sql)?;
                assert!(
                    explain_plan.contains("MarkInApply"),
                    "unexpected explain plan: {explain_plan}"
                );
                assert!(
                    explain_plan.contains(&format!("IndexScan By {index_name} => Probe")),
                    "unexpected explain plan: {explain_plan}"
                );
                Ok(())
            };

        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a in (select v from in_inner where in_inner.v = in_outer.a)",
            "in_inner_v_index",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a not in (select v from in_inner where in_inner.v = in_outer.a)",
            "in_inner_v_index",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a in (select v from in_inner_nn where in_inner_nn.v = in_outer.a)",
            "in_inner_nn_v_index",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a not in (select v from in_inner_nn where in_inner_nn.v = in_outer.a)",
            "in_inner_nn_v_index",
        )?;

        assert_eq!(
            collect_ids(
                "select id from in_outer where a in (select v from in_inner where in_inner.v = in_outer.a) order by id",
            )?,
            vec![2]
        );
        assert_eq!(
            collect_ids(
                "select id from in_outer where a not in (select v from in_inner where in_inner.v = in_outer.a) order by id",
            )?,
            vec![0, 1, 3]
        );
        assert_eq!(
            collect_ids(
                "select id from in_outer where a in (select v from in_inner_nn where in_inner_nn.v = in_outer.a) order by id",
            )?,
            vec![2]
        );
        assert_eq!(
            collect_ids(
                "select id from in_outer where a not in (select v from in_inner_nn where in_inner_nn.v = in_outer.a) order by id",
            )?,
            vec![0, 1, 3]
        );

        kite_sql
            .run("create table in_outer_flag(id int primary key, a int, b int)")?
            .done()?;
        kite_sql
            .run("create table in_inner_flag(id int primary key, v int, flag int)")?
            .done()?;
        kite_sql
            .run("create table in_inner_flag_nn(id int primary key, v int, flag int)")?
            .done()?;
        kite_sql
            .run("create index in_inner_flag_v_index on in_inner_flag(v)")?
            .done()?;
        kite_sql
            .run("create index in_inner_flag_nn_v_index on in_inner_flag_nn(v)")?
            .done()?;

        kite_sql
            .run("insert into in_outer_flag values (0, null, 1), (1, 1, 1), (2, 2, 1), (3, 3, 1)")?
            .done()?;
        kite_sql
            .run("insert into in_inner_flag values (0, 2, 1), (1, null, 1)")?
            .done()?;
        kite_sql
            .run("insert into in_inner_flag_nn values (0, 2, 1)")?
            .done()?;

        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a in (select v from in_inner_flag where in_inner_flag.flag = in_outer_flag.b)",
            "in_inner_flag_v_index",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a not in (select v from in_inner_flag where in_inner_flag.flag = in_outer_flag.b)",
            "in_inner_flag_v_index",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a in (select v from in_inner_flag_nn where in_inner_flag_nn.flag = in_outer_flag.b)",
            "in_inner_flag_nn_v_index",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a not in (select v from in_inner_flag_nn where in_inner_flag_nn.flag = in_outer_flag.b)",
            "in_inner_flag_nn_v_index",
        )?;

        assert_eq!(
            collect_ids(
                "select id from in_outer_flag where a in (select v from in_inner_flag where in_inner_flag.flag = in_outer_flag.b) order by id",
            )?,
            vec![2]
        );
        assert_eq!(
            collect_ids(
                "select id from in_outer_flag where a not in (select v from in_inner_flag where in_inner_flag.flag = in_outer_flag.b) order by id",
            )?,
            Vec::<i32>::new()
        );
        assert_eq!(
            collect_ids(
                "select id from in_outer_flag where a in (select v from in_inner_flag_nn where in_inner_flag_nn.flag = in_outer_flag.b) order by id",
            )?,
            vec![2]
        );
        assert_eq!(
            collect_ids(
                "select id from in_outer_flag where a not in (select v from in_inner_flag_nn where in_inner_flag_nn.flag = in_outer_flag.b) order by id",
            )?,
            vec![1, 3]
        );

        Ok(())
    }

    #[test]
    fn test_subquery_explain_uses_parameterized_index_for_exists() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table exists_outer(id int primary key, a int, b int)")?
            .done()?;
        kite_sql
            .run("create table exists_inner(id int primary key, v int, flag int)")?
            .done()?;
        kite_sql
            .run("create index exists_inner_v_index on exists_inner(v)")?
            .done()?;

        kite_sql
            .run("insert into exists_outer values (0, 1, 1), (1, 1, 2), (2, 2, null), (3, 3, 1)")?
            .done()?;
        kite_sql
            .run("insert into exists_inner values (0, 1, 1), (1, 1, null), (2, 2, 1)")?
            .done()?;

        let collect_plan = |sql: &str| -> Result<String, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let rows = iter.by_ref().collect::<Result<Vec<_>, _>>()?;
            iter.done()?;
            Ok(rows
                .iter()
                .filter_map(|row| match row.values.first() {
                    Some(DataValue::Utf8 { value, .. }) => Some(value.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n"))
        };
        let collect_ids = |sql: &str| -> Result<Vec<i32>, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let mut ids = Vec::new();
            while let Some(row) = iter.next() {
                let row = row?;
                ids.push(row.values[0].i32().unwrap());
            }
            iter.done()?;
            Ok(ids)
        };
        let assert_mark_exists_uses_parameterized_index = |sql: &str| -> Result<(), DatabaseError> {
            let explain_plan = collect_plan(sql)?;
            assert!(
                explain_plan.contains("MarkExistsApply"),
                "unexpected explain plan: {explain_plan}"
            );
            assert!(
                explain_plan.contains("IndexScan By exists_inner_v_index => Probe"),
                "unexpected explain plan: {explain_plan}"
            );
            Ok(())
        };

        assert_mark_exists_uses_parameterized_index(
            "explain select id from exists_outer where exists (select 1 from exists_inner where exists_inner.v = exists_outer.a and exists_inner.flag = exists_outer.b)",
        )?;
        assert_mark_exists_uses_parameterized_index(
            "explain select id from exists_outer where not exists (select 1 from exists_inner where exists_inner.v = exists_outer.a and exists_inner.flag = exists_outer.b)",
        )?;

        assert_eq!(
            collect_ids(
                "select id from exists_outer where exists (select 1 from exists_inner where exists_inner.v = exists_outer.a and exists_inner.flag = exists_outer.b) order by id",
            )?,
            vec![0]
        );
        assert_eq!(
            collect_ids(
                "select id from exists_outer where not exists (select 1 from exists_inner where exists_inner.v = exists_outer.a and exists_inner.flag = exists_outer.b) order by id",
            )?,
            vec![1, 2, 3]
        );

        Ok(())
    }

    #[test]
    fn test_run_multi_statement() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t_multi (a int primary key, b int)")?
            .done()?;

        let mut iter = kite_sql.run(
            "insert into t_multi values(0, 0); insert into t_multi values(1, 1); select * from t_multi order by a",
        )?;
        assert_eq!(
            iter.next().unwrap()?.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            iter.next().unwrap()?.values,
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );
        assert!(iter.next().is_none());
        iter.done()?;

        Ok(())
    }

    #[test]
    fn test_run_multi_statement_disallow_ddl() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        let err = match kite_sql.run("create table t_multi_ddl (a int primary key); select 1") {
            Ok(_) => panic!("multi-statement execution with DDL should be rejected"),
            Err(err) => err,
        };
        match err {
            DatabaseError::UnsupportedStmt(msg) => {
                assert!(msg.contains("multi-statement execution"));
            }
            other => panic!("unexpected error type: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_bind_error_with_span() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t_bind_span(id int primary key)")?
            .done()?;

        let err = match kite_sql.run("select id, missing_col from t_bind_span") {
            Ok(_) => panic!("expected bind error"),
            Err(err) => err,
        };
        println!("{err}");

        match err {
            DatabaseError::ColumnNotFound { span, .. }
            | DatabaseError::InvalidColumn { span, .. } => {
                let span = span.expect("bind error should include span");
                assert_eq!(span.line, 1);
                assert!(span.start >= 12);
                assert!(span.end > span.start);
                assert!(span
                    .highlight
                    .as_deref()
                    .is_some_and(|h| h.contains("missing_col")));
            }
            other => panic!("unexpected error type: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_bind_function_error_with_span() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t_bind_fn_span(id int primary key)")?
            .done()?;

        let err = match kite_sql.run("select missing_fn(id) from t_bind_fn_span") {
            Ok(_) => panic!("expected function bind error"),
            Err(err) => err,
        };
        println!("{err}");

        match err {
            DatabaseError::FunctionNotFound { span, .. } => {
                let span = span.expect("function bind error should include span");
                assert_eq!(span.line, 1);
                assert!(span.start >= 8);
                assert!(span.end > span.start);
                assert!(span
                    .highlight
                    .as_deref()
                    .is_some_and(|h| h.contains("missing_fn(id)")));
            }
            other => panic!("unexpected error type: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;

        let mut tx_1 = kite_sql.new_transaction()?;
        let mut tx_2 = kite_sql.new_transaction()?;

        tx_1.run("insert into t1 values(0, 0)")?.done()?;
        tx_1.run("insert into t1 values(1, 1)")?.done()?;

        assert!(tx_2.run("insert into t1 values(0, 0)")?.done().is_err());
        tx_2.run("insert into t1 values(3, 3)")?.done()?;

        let mut iter_1 = tx_1.run("select * from t1")?;
        let mut iter_2 = tx_2.run("select * from t1")?;

        assert_eq!(
            iter_1.next().unwrap()?.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            iter_1.next().unwrap()?.values,
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );

        assert_eq!(
            iter_2.next().unwrap()?.values,
            vec![DataValue::Int32(3), DataValue::Int32(3)]
        );
        drop(iter_1);
        drop(iter_2);

        tx_1.commit()?;
        tx_2.commit()?;

        let mut tx_3 = kite_sql.new_transaction()?;
        let res = tx_3.run("create table t2 (a int primary key, b int)");
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn test_transaction_run_multi_statement() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql
            .run("create table t_multi_tx (a int primary key, b int)")?
            .done()?;

        let mut tx = kite_sql.new_transaction()?;
        let mut iter = tx.run(
            "insert into t_multi_tx values(0, 0); insert into t_multi_tx values(1, 1); select * from t_multi_tx order by a",
        )?;
        assert_eq!(
            iter.next().unwrap()?.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            iter.next().unwrap()?.values,
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );
        assert!(iter.next().is_none());
        iter.done()?;
        tx.commit()?;

        let mut check_iter = kite_sql.run("select count(*) from t_multi_tx")?;
        assert_eq!(
            check_iter.next().unwrap()?.values,
            vec![DataValue::Int32(2)]
        );
        check_iter.done()?;

        Ok(())
    }

    #[test]
    fn test_autocommit_read_drops_iterator_before_transaction() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_optimistic()?;

        kite_sql
            .run("create table t_iter_drop (a int primary key, b int)")?
            .done()?;

        let mut tx = kite_sql.new_transaction()?;
        tx.run("insert into t_iter_drop values (0, 0), (1, 1)")?
            .done()?;

        assert!(kite_sql.run("select * from t_iter_drop")?.next().is_none());

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_exhausted_database_iter_releases_mdl_guard() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_optimistic()?;

        kite_sql
            .run("create table t_iter_guard (a int primary key, b int)")?
            .done()?;
        kite_sql
            .run("insert into t_iter_guard values (0, 0), (1, 1)")?
            .done()?;

        let mut iter = kite_sql.run("select * from t_iter_guard order by a")?;
        assert_eq!(
            iter.next().unwrap()?.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            iter.next().unwrap()?.values,
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );
        assert!(iter.next().is_none());

        kite_sql.run("drop table t_iter_guard")?.done()?;

        Ok(())
    }

    #[test]
    fn test_read_committed_refreshes_snapshot_each_statement() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .transaction_isolation(TransactionIsolationLevel::ReadCommitted)
            .build_rocksdb()?;

        kite_sql
            .run("create table t_rc (a int primary key, b int)")?
            .done()?;
        kite_sql.run("insert into t_rc values (1, 10)")?.done()?;

        let mut reader = kite_sql.new_transaction()?;
        let mut writer = kite_sql.new_transaction()?;

        assert_eq!(
            read_single_i32(reader.run("select b from t_rc where a = 1")?)?,
            10
        );

        writer.run("update t_rc set b = 20 where a = 1")?.done()?;
        writer.commit()?;

        assert_eq!(
            read_single_i32(reader.run("select b from t_rc where a = 1")?)?,
            20
        );
        reader.commit()?;

        Ok(())
    }

    #[test]
    fn test_repeatable_read_keeps_transaction_snapshot() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path())
            .transaction_isolation(TransactionIsolationLevel::RepeatableRead)
            .build_rocksdb()?;

        kite_sql
            .run("create table t_rr (a int primary key, b int)")?
            .done()?;
        kite_sql.run("insert into t_rr values (1, 10)")?.done()?;

        let mut reader = kite_sql.new_transaction()?;
        let mut writer = kite_sql.new_transaction()?;

        assert_eq!(
            read_single_i32(reader.run("select b from t_rr where a = 1")?)?,
            10
        );

        writer.run("update t_rr set b = 20 where a = 1")?.done()?;
        writer.commit()?;

        assert_eq!(
            read_single_i32(reader.run("select b from t_rr where a = 1")?)?,
            10
        );
        reader.commit()?;

        Ok(())
    }

    #[test]
    fn test_optimistic_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_optimistic()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;

        let mut tx_1 = kite_sql.new_transaction()?;
        let mut tx_2 = kite_sql.new_transaction()?;

        tx_1.run("insert into t1 values(0, 0)")?.done()?;
        tx_1.run("insert into t1 values(1, 1)")?.done()?;

        tx_2.run("insert into t1 values(0, 0)")?.done()?;
        tx_2.run("insert into t1 values(3, 3)")?.done()?;

        let mut iter_1 = tx_1.run("select * from t1")?;
        let mut iter_2 = tx_2.run("select * from t1")?;

        assert_eq!(
            iter_1.next().unwrap()?.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            iter_1.next().unwrap()?.values,
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );

        assert_eq!(
            iter_2.next().unwrap()?.values,
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            iter_2.next().unwrap()?.values,
            vec![DataValue::Int32(3), DataValue::Int32(3)]
        );
        drop(iter_1);
        drop(iter_2);

        tx_1.commit()?;

        assert!(tx_2.commit().is_err());

        let mut tx_3 = kite_sql.new_transaction()?;
        let res = tx_3.run("create table t2 (a int primary key, b int)");
        assert!(res.is_err());

        Ok(())
    }

    #[test]
    fn test_invalid_histogram_buckets() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let result = DataBaseBuilder::path(temp_dir.path())
            .histogram_buckets(0)
            .build_rocksdb();

        assert!(matches!(
            result,
            Err(DatabaseError::InvalidValue(message)) if message == "histogram buckets must be >= 1"
        ));
    }

    #[cfg(feature = "lmdb")]
    #[test]
    fn test_lmdb_rejects_read_committed_isolation() {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let db_path = temp_dir.path().join("kite_sql.lmdb");
        let result = DataBaseBuilder::path(db_path)
            .transaction_isolation(TransactionIsolationLevel::ReadCommitted)
            .build_lmdb();

        match result {
            Err(DatabaseError::UnsupportedStmt(message)) => {
                assert!(message.contains("read committed"));
            }
            Ok(_) => panic!("lmdb should reject read committed isolation"),
            Err(err) => panic!("unexpected error: {err}"),
        }
    }

    #[cfg(feature = "unsafe_txdb_checkpoint")]
    #[test]
    fn test_checkpoint_restores_snapshot() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let live_path = temp_dir.path().join("live");
        let checkpoint_path = temp_dir.path().join("checkpoint");
        let kite_sql = DataBaseBuilder::path(&live_path).build_rocksdb()?;

        kite_sql
            .run("create table t_checkpoint (id int primary key, v int)")?
            .done()?;
        kite_sql
            .run("insert into t_checkpoint values (1, 10), (2, 20)")?
            .done()?;

        kite_sql.checkpoint(&checkpoint_path)?;

        kite_sql
            .run("insert into t_checkpoint values (3, 30)")?
            .done()?;

        let snapshot = DataBaseBuilder::path(&checkpoint_path).build_rocksdb()?;
        assert_eq!(
            query_i32(&snapshot, "select count(*) from t_checkpoint")?,
            2
        );
        assert_eq!(
            query_i32(&kite_sql, "select count(*) from t_checkpoint")?,
            3
        );

        Ok(())
    }

    #[test]
    fn test_checkpoint_rejects_non_empty_target_dir() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let live_path = temp_dir.path().join("live");
        let checkpoint_path = temp_dir.path().join("checkpoint");
        let kite_sql = DataBaseBuilder::path(&live_path).build_rocksdb()?;

        std::fs::create_dir(&checkpoint_path)?;
        std::fs::write(checkpoint_path.join("stale.txt"), b"stale")?;

        let err = kite_sql
            .checkpoint(&checkpoint_path)
            .expect_err("checkpoint should reject non-empty directories");
        assert!(matches!(
            err,
            DatabaseError::IO(ref io_err) if io_err.kind() == ErrorKind::AlreadyExists
        ));

        Ok(())
    }

    #[cfg(not(feature = "unsafe_txdb_checkpoint"))]
    #[test]
    fn test_checkpoint_requires_unsafe_feature() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let live_path = temp_dir.path().join("live");
        let checkpoint_path = temp_dir.path().join("checkpoint");
        let kite_sql = DataBaseBuilder::path(&live_path).build_rocksdb()?;

        kite_sql
            .run("create table t_checkpoint_disabled (id int primary key, v int)")?
            .done()?;

        let err = kite_sql
            .checkpoint(&checkpoint_path)
            .expect_err("checkpoint should require the unsafe feature");
        assert!(matches!(err, DatabaseError::UnsupportedStmt(_)));

        Ok(())
    }

    #[cfg(feature = "unsafe_txdb_checkpoint")]
    #[test]
    fn test_checkpoint_during_concurrent_writes() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let live_path = temp_dir.path().join("live");
        let checkpoint_path = temp_dir.path().join("checkpoint");
        let kite_sql = Arc::new(DataBaseBuilder::path(&live_path).build_rocksdb()?);

        kite_sql
            .run("create table t_checkpoint_concurrent (id int primary key, v int)")?
            .done()?;

        let inserted = Arc::new(AtomicUsize::new(0));
        let writer_db = Arc::clone(&kite_sql);
        let writer_inserted = Arc::clone(&inserted);
        let writer = thread::spawn(move || -> Result<usize, DatabaseError> {
            for i in 0..64 {
                writer_db
                    .run(format!(
                        "insert into t_checkpoint_concurrent values ({i}, {i})"
                    ))?
                    .done()?;
                writer_inserted.store(i + 1, Ordering::SeqCst);

                if i >= 8 {
                    thread::sleep(Duration::from_millis(2));
                }
            }

            Ok(64)
        });

        while inserted.load(Ordering::SeqCst) < 8 {
            thread::yield_now();
        }

        kite_sql.checkpoint(&checkpoint_path)?;

        let total = writer.join().expect("writer thread should not panic")?;
        let snapshot = DataBaseBuilder::path(&checkpoint_path).build_rocksdb()?;
        let snapshot_count = query_i32(&snapshot, "select count(*) from t_checkpoint_concurrent")?;
        let consistent_count = query_i32(
            &snapshot,
            "select count(*) from t_checkpoint_concurrent where id = v",
        )?;

        assert!(snapshot_count >= 8);
        assert!(snapshot_count <= total as i32);
        assert_eq!(snapshot_count, consistent_count);
        assert_eq!(
            query_i32(&kite_sql, "select count(*) from t_checkpoint_concurrent")?,
            total as i32
        );

        Ok(())
    }
}
