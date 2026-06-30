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

#[cfg(feature = "parser")]
pub use crate::binder::{prepare, prepare_all, Statement};
use crate::binder::{Binder, BinderContext};
use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::execution::{build_write, DDLApply, ExecArena, ExecutionContext, Executor};
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::table::{
    ArcTableFunctionImpl, TableFunctionCatalog, TableFunctionImpl,
};
use crate::expression::function::FunctionSummary;
use crate::function::char_length::CharLength;
#[cfg(feature = "time")]
use crate::function::current_date::CurrentDate;
#[cfg(feature = "time")]
use crate::function::current_timestamp::CurrentTimeStamp;
use crate::function::lower::Lower;
use crate::function::numbers::Numbers;
use crate::function::octet_length::OctetLength;
use crate::function::upper::Upper;
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::heuristic::batch::HepBatchStrategy;
use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
use crate::optimizer::rule::implementation::ImplementationRuleImpl;
use crate::optimizer::rule::normalization::NormalizationRuleImpl;
#[cfg(feature = "orm")]
use crate::orm::FromQueryRow;
use crate::planner::operator::Operator;
use crate::planner::{LogicalPlan, PlanArena, TableArenaCell};
#[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
use crate::storage::lmdb::{LmdbConfig, LmdbStorage};
use crate::storage::memory::MemoryStorage;
#[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
use crate::storage::rocksdb::{OptimisticRocksStorage, RocksStorage, StorageConfig};
use crate::storage::table_codec::TableCodec;
use crate::storage::{
    CheckpointableStorage, StatisticsMetaCache, Storage, TableCache, Transaction,
    TransactionIsolationLevel, ViewCache,
};
use crate::types::tuple::{Schema, SchemaView, Tuple};
use crate::types::value::DataValue;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::mem;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) type ScalaFunctions = HashMap<FunctionSummary, Arc<dyn ScalarFunctionImpl>>;
pub(crate) type TableFunctions = HashMap<FunctionSummary, TableFunctionCatalog>;

pub enum CatalogKind {
    Table(crate::catalog::TableName),
    View(crate::catalog::TableName),
    ScalarFunction(Arc<dyn ScalarFunctionImpl>),
    TableFunction(Arc<dyn TableFunctionImpl>),
}

pub(crate) trait BindSource {
    type Iter: ResultIter;
    type Transaction: Transaction;

    fn execute<A, F>(self, params: A, build: F) -> Result<Self::Iter, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, Self::Transaction, A>,
            &mut PlanArena<'_>,
        ) -> Result<LogicalPlan, DatabaseError>;

    #[cfg(feature = "orm")]
    fn explain<A, F>(self, params: A, build: F) -> Result<String, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, Self::Transaction, A>,
            &mut PlanArena<'_>,
        ) -> Result<LogicalPlan, DatabaseError>;
}

/// Builder for creating a [`Database`] instance.
///
/// The builder wires together storage, built-in functions and optional runtime
/// features before the database is opened.
pub struct DataBaseBuilder {
    #[cfg(all(
        not(target_arch = "wasm32"),
        any(feature = "rocksdb", feature = "lmdb")
    ))]
    path: PathBuf,
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
        #[cfg(all(
            not(target_arch = "wasm32"),
            any(feature = "rocksdb", feature = "lmdb")
        ))]
        let path = path.into();
        #[cfg(not(all(
            not(target_arch = "wasm32"),
            any(feature = "rocksdb", feature = "lmdb")
        )))]
        let _ = path;

        DataBaseBuilder {
            #[cfg(all(
                not(target_arch = "wasm32"),
                any(feature = "rocksdb", feature = "lmdb")
            ))]
            path,
            histogram_buckets: None,
            transaction_isolation: None,
            #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
            storage_config: Default::default(),
            #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
            lmdb_config: Default::default(),
        }
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
        Self::_build::<T>(storage, self.histogram_buckets, self.transaction_isolation)
    }

    /// Builds a database for the current target platform.
    #[cfg(all(target_arch = "wasm32", feature = "wasm"))]
    pub fn build(self) -> Result<Database<MemoryStorage>, DatabaseError> {
        let storage = MemoryStorage::new();

        Self::_build::<MemoryStorage>(storage, self.histogram_buckets, self.transaction_isolation)
    }

    /// Builds a RocksDB-backed database.
    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    pub fn build_rocksdb(self) -> Result<Database<RocksStorage>, DatabaseError> {
        let storage = RocksStorage::with_config(self.path, self.storage_config)?;

        Self::_build::<RocksStorage>(storage, self.histogram_buckets, self.transaction_isolation)
    }

    /// Builds an in-memory database.
    ///
    /// This is convenient for tests, examples and temporary workloads.
    pub fn build_in_memory(self) -> Result<Database<MemoryStorage>, DatabaseError> {
        let storage = MemoryStorage::new();

        Self::_build::<MemoryStorage>(storage, self.histogram_buckets, self.transaction_isolation)
    }

    /// Builds a LMDB-backed database.
    #[cfg(all(not(target_arch = "wasm32"), feature = "lmdb"))]
    pub fn build_lmdb(self) -> Result<Database<LmdbStorage>, DatabaseError> {
        let storage = LmdbStorage::with_config(self.path, self.lmdb_config)?;

        Self::_build::<LmdbStorage>(storage, self.histogram_buckets, self.transaction_isolation)
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    /// Builds a RocksDB-backed database that uses optimistic transactions.
    #[cfg(all(not(target_arch = "wasm32"), feature = "rocksdb"))]
    pub fn build_optimistic(self) -> Result<Database<OptimisticRocksStorage>, DatabaseError> {
        let storage = OptimisticRocksStorage::with_config(self.path, self.storage_config)?;

        Self::_build::<OptimisticRocksStorage>(
            storage,
            self.histogram_buckets,
            self.transaction_isolation,
        )
    }

    fn _build<T: Storage>(
        storage: T,
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
        let meta_cache = HashMap::default();
        let table_cache = HashMap::default();
        let view_cache = HashMap::default();
        let table_arena = TableArenaCell::default();

        let mut state = State {
            scala_functions: Default::default(),
            table_functions: Default::default(),
            meta_cache,
            table_cache,
            view_cache,
            table_arena,
            optimizer_pipeline: default_optimizer_pipeline(),
            histogram_buckets,
            _p: Default::default(),
        };

        state.load_scalar_function(CharLength::new("char_length".to_lowercase()));
        state.load_scalar_function(CharLength::new("character_length".to_lowercase()));
        #[cfg(feature = "time")]
        state.load_scalar_function(CurrentDate::new());
        #[cfg(feature = "time")]
        state.load_scalar_function(CurrentTimeStamp::new());
        state.load_scalar_function(Lower::new());
        state.load_scalar_function(OctetLength::new());
        state.load_scalar_function(Upper::new());
        state.load_table_function(Numbers::new())?;

        Ok(Database {
            storage,
            transaction_isolation,
            state,
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
        .after_batch(
            "Limit Into Scan".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![NormalizationRuleImpl::PushLimitIntoTableScan],
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
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyFromFile,
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyToFile,
            ImplementationRuleImpl::Delete,
            ImplementationRuleImpl::Insert,
            ImplementationRuleImpl::Update,
            // DDL
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
    table_arena: TableArenaCell,
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
    pub(crate) fn table_arena(&self) -> &TableArenaCell {
        &self.table_arena
    }

    fn load_scalar_function(&mut self, function: Arc<dyn ScalarFunctionImpl>) {
        self.scala_functions
            .insert(function.summary().clone(), function);
    }

    fn load_table_function(
        &mut self,
        function: Arc<dyn TableFunctionImpl>,
    ) -> Result<(), DatabaseError> {
        let summary = function.summary().clone();
        let mut schema = Schema::new();
        function.output_schema_into(self.table_arena.borrow_mut(), &mut schema);
        self.table_functions.insert(
            summary,
            TableFunctionCatalog {
                schema,
                inner: ArcTableFunctionImpl(function),
            },
        );
        Ok(())
    }

    fn recycle_table_arena(&mut self) {
        let mut live_columns = HashSet::new();
        {
            let mut live = |column: &crate::catalog::ColumnRef| {
                live_columns.insert(column.pos());
            };

            for table in self.table_cache.values() {
                for column in table.columns() {
                    live(column);
                }
            }

            let table_arena = self.table_arena.borrow_mut();
            for view in self.view_cache.values() {
                view.visit_column_refs(table_arena, &mut live);
            }

            for function in self.table_functions.values() {
                for column in &function.schema {
                    live(column);
                }
            }
        }
        self.table_arena
            .borrow_mut()
            .recycle_unreferenced_positions(live_columns);
    }

    pub(crate) fn build_plan<'a, 'txn, A: AsRef<[(&'static str, DataValue)]>, F>(
        &'a self,
        params: A,
        transaction: &<S as Storage>::TransactionType<'txn>,
        build: F,
    ) -> Result<(LogicalPlan, PlanArena<'a>), DatabaseError>
    where
        S: 'txn,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, <S as Storage>::TransactionType<'txn>, A>,
            &mut PlanArena<'a>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let mut plan_arena = PlanArena::new(self.table_arena());
        let mut binder: Binder<'_, '_, <S as Storage>::TransactionType<'txn>, A> = Binder::new(
            BinderContext::new(
                self.table_cache(),
                self.view_cache(),
                transaction,
                self.scala_functions(),
                self.table_functions(),
            ),
            &params,
            None,
        );
        let source_plan = build(&mut binder, &mut plan_arena)?;
        drop(binder);
        let mut best_plan = self.optimizer_pipeline.instantiate(source_plan).find_best(
            Some(&StatisticMetaLoader::new(self.meta_cache())),
            &mut plan_arena,
        )?;

        if let Operator::Analyze(op) = &mut best_plan.operator {
            if op.histogram_buckets.is_none() {
                op.histogram_buckets = self.histogram_buckets;
            }
        }

        Ok((best_plan, plan_arena))
    }

    pub(crate) fn execute<'a, 'txn, A, F>(
        &'a self,
        transaction: &'a mut S::TransactionType<'txn>,
        params: A,
        build: F,
    ) -> Result<
        (
            Schema,
            PlanArena<'a>,
            Executor<'a, S::TransactionType<'txn>>,
        ),
        DatabaseError,
    >
    where
        S: 'txn,
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, S::TransactionType<'txn>, A>,
            &mut PlanArena<'a>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        transaction.begin_statement_scope()?;
        match (|| {
            let (mut plan, mut plan_arena) = self.build_plan(params, transaction, build)?;
            let schema = plan.take_schema(&mut plan_arena);
            let mut arena = ExecArena::new();
            let read_context = ExecutionContext::new(
                &self.table_cache,
                &self.view_cache,
                &self.meta_cache,
                &self.scala_functions,
                &self.table_functions,
            );
            let root = build_write(&mut arena, &mut plan_arena, plan, read_context, transaction);
            let executor = Executor::new(arena, root);

            Ok((schema, plan_arena, executor))
        })() {
            Ok(result) => Ok(result),
            Err(err) => Err(err),
        }
    }

    pub(crate) fn execute_mut<'a, 'txn, A, F>(
        &'a mut self,
        transaction: &'a mut S::TransactionType<'txn>,
        params: A,
        build: F,
    ) -> Result<
        (
            Schema,
            PlanArena<'a>,
            Executor<'a, S::TransactionType<'txn>>,
        ),
        DatabaseError,
    >
    where
        S: 'txn,
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, S::TransactionType<'txn>, A>,
            &mut PlanArena<'a>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        transaction.begin_statement_scope()?;
        let State {
            scala_functions,
            table_functions,
            meta_cache,
            table_cache,
            view_cache,
            table_arena,
            optimizer_pipeline,
            histogram_buckets,
            ..
        } = self;
        let mut plan_arena = PlanArena::new(table_arena);
        let mut binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                transaction,
                scala_functions,
                table_functions,
            ),
            &params,
            None,
        );
        let source_plan = build(&mut binder, &mut plan_arena)?;
        drop(binder);
        let mut plan = optimizer_pipeline
            .instantiate(source_plan)
            .find_best(Some(&StatisticMetaLoader::new(meta_cache)), &mut plan_arena)?;

        if let Operator::Analyze(op) = &mut plan.operator {
            if op.histogram_buckets.is_none() {
                op.histogram_buckets = *histogram_buckets;
            }
        }

        let schema = plan.take_schema(&mut plan_arena);
        let mut arena = ExecArena::new();
        let cache = ExecutionContext::new(
            table_cache,
            view_cache,
            meta_cache,
            scala_functions,
            table_functions,
        );
        let root = build_write(&mut arena, &mut plan_arena, plan, cache, transaction);
        let executor = Executor::new(arena, root);

        Ok((schema, plan_arena, executor))
    }
}

/// Main database handle for executing SQL and creating transactions.
pub struct Database<S: Storage> {
    pub(crate) storage: S,
    pub(crate) transaction_isolation: TransactionIsolationLevel,
    pub(crate) state: State<S>,
}

impl DDLApply {
    fn apply_to<S: Storage>(
        self,
        state: &mut State<S>,
        plan_arena: &PlanArena,
    ) -> Result<bool, DatabaseError> {
        let mut catalog_changed = false;
        match self {
            DDLApply::UpsertTable {
                table,
                clear_statistics,
            } => {
                let name = table.name().clone();
                let table = table.transplant_to_table_arena(plan_arena)?;
                state.table_cache.insert(name.clone(), table);
                if clear_statistics {
                    state
                        .meta_cache
                        .retain(|(cached_table_name, _), _| cached_table_name != &name);
                }
                catalog_changed = true;
            }
            DDLApply::DropTable { name } => {
                state.table_cache.remove(&name);
                state
                    .meta_cache
                    .retain(|(cached_table_name, _), _| cached_table_name != &name);
                catalog_changed = true;
            }
            DDLApply::UpsertView { view } => {
                let name = view.name.clone();
                plan_arena.materialize_into_table_arena();
                state.view_cache.insert(name, view);
                catalog_changed = true;
            }
            DDLApply::DropView { name } => {
                state.view_cache.remove(&name);
                catalog_changed = true;
            }
            DDLApply::UpsertStatisticsMeta {
                table_name,
                index_id,
                meta,
            } => {
                state.meta_cache.insert((table_name, index_id), meta);
            }
            DDLApply::RemoveStatisticsMeta {
                table_name,
                index_id,
            } => {
                state.meta_cache.remove(&(table_name, index_id));
            }
        }
        Ok(catalog_changed)
    }
}

impl<S: Storage> Database<S> {
    pub(crate) fn execute_mut<A, F>(
        &mut self,
        context: &str,
        params: A,
        build: F,
    ) -> Result<(), DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'a, 'txn, 'bind> FnOnce(
            &mut Binder<'bind, '_, S::TransactionType<'txn>, A>,
            &mut PlanArena<'a>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let transaction = Box::into_raw(Box::new(
            self.storage
                .transaction_with_isolation(self.transaction_isolation)?,
        ));
        let state = std::ptr::from_mut(&mut self.state);
        let (schema, plan_arena, executor) =
            match unsafe { (&mut *state).execute_mut(&mut *transaction, params, build) } {
                Ok(result) => result,
                Err(err) => {
                    unsafe { drop(Box::from_raw(transaction)) };
                    return Err(err.with_sql_context(context));
                }
            };
        let (plan_arena, apply) =
            match TransactionIter::new(schema, plan_arena, executor, transaction)
                .done_with_ddl_apply()
            {
                Ok(apply) => apply,
                Err(err) => {
                    unsafe { drop(Box::from_raw(transaction)) };
                    return Err(err.with_sql_context(context));
                }
            };

        if let Err(err) = unsafe { Box::from_raw(transaction).commit() } {
            return Err(err.with_sql_context(context));
        }

        let mut catalog_changed = false;
        for apply in apply {
            catalog_changed |= unsafe { apply.apply_to(&mut *state, &plan_arena) }
                .map_err(|err| err.with_sql_context(context))?;
        }
        if catalog_changed {
            unsafe { (&mut *state).recycle_table_arena() };
        }
        Ok(())
    }

    pub fn analyze(&mut self, table_name: impl AsRef<str>) -> Result<(), DatabaseError> {
        let context = "ANALYZE";
        let table_name: TableName = table_name.as_ref().into();
        self.execute_mut(context, &[], move |binder, arena| {
            binder.bind_analyze(table_name, arena)
        })
    }

    pub fn load(&mut self, kind: CatalogKind) -> Result<(), DatabaseError> {
        match kind {
            CatalogKind::ScalarFunction(function) => {
                self.state.load_scalar_function(function);
                Ok(())
            }
            CatalogKind::TableFunction(function) => {
                self.state.load_table_function(function)?;
                self.state.recycle_table_arena();
                Ok(())
            }
            CatalogKind::Table(name) => {
                let transaction = self.storage.transaction()?;
                let mut table_codec = TableCodec::default();
                let table = transaction
                    .load_table(
                        &mut table_codec,
                        self.state.table_arena.borrow_mut(),
                        name.clone(),
                    )?
                    .ok_or(DatabaseError::TableNotFound)?;
                for index in table.indexes() {
                    let index_id = self.state.table_arena.borrow().index(*index).id;
                    if let Some(meta) = transaction.statistics_meta(
                        &mut table_codec,
                        name.as_ref(),
                        index_id,
                        self.state.table_arena.borrow_mut(),
                    )? {
                        self.state.meta_cache.insert((name.clone(), index_id), meta);
                    }
                }
                self.state.table_cache.insert(name, table);
                self.state.recycle_table_arena();
                Ok(())
            }
            CatalogKind::View(name) => {
                let transaction = self.storage.transaction()?;
                let mut table_codec = TableCodec::default();
                let view = transaction
                    .load_view(
                        &mut table_codec,
                        &self.state.table_cache,
                        &self.state.table_arena,
                        &self.state.scala_functions,
                        &self.state.table_functions,
                        name.clone(),
                    )?
                    .ok_or(DatabaseError::ViewNotFound)?;
                self.state.view_cache.insert(name, view);
                self.state.recycle_table_arena();
                Ok(())
            }
        }
    }

    /// Opens a new explicit transaction.
    ///
    /// Statements executed through the returned transaction share the same
    /// transactional context until [`DBTransaction::commit`] is called.
    pub fn new_transaction(&self) -> Result<DBTransaction<'_, S>, DatabaseError> {
        let transaction = self
            .storage
            .transaction_with_isolation(self.transaction_isolation)?;

        Ok(DBTransaction {
            inner: transaction,
            state: &self.state,
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

impl<'a, S: Storage> BindSource for &'a Database<S> {
    type Iter = DatabaseIter<'a, S>;
    type Transaction = S::TransactionType<'a>;

    fn execute<A, F>(self, params: A, build: F) -> Result<Self::Iter, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, Self::Transaction, A>,
            &mut PlanArena<'_>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let transaction = Box::into_raw(Box::new(
            self.storage
                .transaction_with_isolation(self.transaction_isolation)?,
        ));
        let (schema, plan_arena, executor) =
            match self
                .state
                .execute(unsafe { &mut *transaction }, params, build)
            {
                Ok(result) => result,
                Err(err) => {
                    unsafe { drop(Box::from_raw(transaction)) };
                    return Err(err);
                }
            };
        let inner = Box::into_raw(Box::new(TransactionIter::new(
            schema,
            plan_arena,
            executor,
            transaction,
        )));
        Ok(DatabaseIter { transaction, inner })
    }

    #[cfg(feature = "orm")]
    fn explain<A, F>(self, params: A, build: F) -> Result<String, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, Self::Transaction, A>,
            &mut PlanArena<'_>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let mut transaction = self
            .storage
            .transaction_with_isolation(self.transaction_isolation)?;
        transaction.begin_statement_scope()?;
        let (plan, mut arena) = self.state.build_plan(params, &transaction, build)?;
        Ok(plan.explain(&mut arena, 0))
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

/// Common interface for result iterators returned by database execution APIs.
pub trait ResultIter {
    /// Borrows the output schema for the current result set.
    fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R;

    /// Advances to the next row and runs `f` while the executor row slot is borrowed.
    fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut Tuple) -> R,
    ) -> Result<Option<R>, DatabaseError>;

    /// Finishes consuming the iterator and flushes any remaining work.
    fn done(self) -> Result<(), DatabaseError>;

    #[cfg(feature = "orm")]
    /// Converts this iterator into a typed ORM iterator.
    ///
    /// This is available when the `orm` feature is enabled and the target type
    /// implements `FromQueryRow`, which is typically generated by
    /// `#[derive(Model)]` or `#[derive(Projection)]`.
    fn orm<T>(self) -> OrmIter<Self, T>
    where
        Self: Sized,
        T: FromQueryRow,
    {
        OrmIter::new(self)
    }
}

#[cfg(feature = "orm")]
/// Typed adapter over a [`ResultIter`] that yields ORM models instead of raw tuples.
pub struct OrmIter<I, T> {
    inner: I,
    _marker: PhantomData<T>,
}

#[cfg(feature = "orm")]
impl<I, T> OrmIter<I, T>
where
    I: ResultIter,
    T: FromQueryRow,
{
    fn new(inner: I) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }

    /// Borrows the schema of the underlying result set.
    pub fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        self.inner.schema(f)
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
    T: FromQueryRow,
{
    type Item = Result<T, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next_tuple(|schema, tuple| T::from_query_row(schema, tuple))
            .transpose()
            .map(|row| row.and_then(std::convert::identity))
    }
}

/// Raw result iterator returned by database execution APIs.
pub struct DatabaseIter<'a, S: Storage + 'a> {
    pub(crate) transaction: *mut S::TransactionType<'a>,
    pub(crate) inner: *mut TransactionIter<'a, S::TransactionType<'a>>,
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
    pub fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        unsafe { (*self.inner).schema(f) }
    }

    #[inline]
    pub fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut Tuple) -> R,
    ) -> Result<Option<R>, DatabaseError> {
        unsafe { (*self.inner).next_tuple(f) }
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

impl<S: Storage> ResultIter for DatabaseIter<'_, S> {
    fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        DatabaseIter::schema(self, f)
    }

    fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut Tuple) -> R,
    ) -> Result<Option<R>, DatabaseError> {
        DatabaseIter::next_tuple(self, f)
    }

    fn done(self) -> Result<(), DatabaseError> {
        DatabaseIter::done(self)
    }
}

/// Explicit transaction handle created by [`Database::new_transaction`].
pub struct DBTransaction<'a, S: Storage + 'a> {
    pub(crate) inner: S::TransactionType<'a>,
    pub(crate) state: &'a State<S>,
}

impl<'txn, S: Storage> DBTransaction<'txn, S> {
    /// Commits the current transaction.
    pub fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit()?;

        Ok(())
    }
}

impl<'a, 'txn, S: Storage> BindSource for &'a mut DBTransaction<'txn, S> {
    type Iter = TransactionIter<'a, S::TransactionType<'txn>>;
    type Transaction = S::TransactionType<'txn>;

    fn execute<A, F>(self, params: A, build: F) -> Result<Self::Iter, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, Self::Transaction, A>,
            &mut PlanArena<'_>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        let transaction = std::ptr::from_mut(&mut self.inner);
        let (schema, plan_arena, executor) =
            self.state
                .execute(unsafe { &mut *transaction }, params, build)?;
        Ok(TransactionIter::new(
            schema,
            plan_arena,
            executor,
            transaction,
        ))
    }

    #[cfg(feature = "orm")]
    fn explain<A, F>(self, params: A, build: F) -> Result<String, DatabaseError>
    where
        A: AsRef<[(&'static str, DataValue)]>,
        F: for<'bind> FnOnce(
            &mut Binder<'bind, '_, Self::Transaction, A>,
            &mut PlanArena<'_>,
        ) -> Result<LogicalPlan, DatabaseError>,
    {
        self.inner.begin_statement_scope()?;
        let (plan, mut arena) = self.state.build_plan(params, &self.inner, build)?;
        Ok(plan.explain(&mut arena, 0))
    }
}

/// Raw result iterator returned by transaction execution APIs.
pub struct TransactionIter<'a, T: Transaction + 'a> {
    executor: Option<Executor<'a, T>>,
    plan_arena: Option<PlanArena<'a>>,
    schema: Schema,
    transaction: *mut T,
    statement_scope_active: bool,
    ddl_apply: Vec<DDLApply>,
}

impl<'a, T: Transaction + 'a> TransactionIter<'a, T> {
    pub(crate) fn new(
        schema: Schema,
        plan_arena: PlanArena<'a>,
        executor: Executor<'a, T>,
        transaction: *mut T,
    ) -> Self {
        Self {
            executor: Some(executor),
            plan_arena: Some(plan_arena),
            schema,
            transaction,
            statement_scope_active: true,
            ddl_apply: Vec::new(),
        }
    }

    #[inline]
    fn finish_statement_scope(&mut self) -> Result<(), DatabaseError> {
        if !self.statement_scope_active {
            return Ok(());
        }

        if let Some(mut executor) = self.executor.take() {
            self.ddl_apply.extend(executor.take_ddl_apply());
        }
        self.statement_scope_active = false;
        unsafe { (*self.transaction).end_statement_scope() }
    }

    #[inline]
    pub fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        let plan_arena = self
            .plan_arena
            .as_ref()
            .expect("result iterator schema is unavailable after statement completion");
        let schema = SchemaView::new(&self.schema, plan_arena);
        f(&schema)
    }

    #[inline]
    pub fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut Tuple) -> R,
    ) -> Result<Option<R>, DatabaseError> {
        let Some(executor) = self.executor.as_mut() else {
            return Ok(None);
        };
        let executor_ptr = std::ptr::from_mut(executor);
        let plan_arena = self
            .plan_arena
            .as_mut()
            .expect("result iterator plan arena is unavailable after statement completion");
        match unsafe { (*executor_ptr).next_tuple(plan_arena) } {
            Ok(Some(tuple)) => {
                let schema = SchemaView::new(&self.schema, plan_arena);
                Ok(Some(f(&schema, tuple)))
            }
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
        while self.next_tuple(|_, _| ())?.is_some() {}
        Ok(())
    }

    fn done_with_ddl_apply(mut self) -> Result<(PlanArena<'a>, Vec<DDLApply>), DatabaseError> {
        while self.next_tuple(|_, _| ())?.is_some() {}
        Ok((
            self.plan_arena
                .take()
                .expect("DDL apply plan arena is unavailable after statement completion"),
            std::mem::take(&mut self.ddl_apply),
        ))
    }
}

impl<T: Transaction> Drop for TransactionIter<'_, T> {
    fn drop(&mut self) {
        let _ = self.finish_statement_scope();
    }
}

impl<T: Transaction> ResultIter for TransactionIter<'_, T> {
    fn schema<R>(&self, f: impl FnOnce(&SchemaView<'_, '_>) -> R) -> R {
        TransactionIter::schema(self, f)
    }

    fn next_tuple<R>(
        &mut self,
        f: impl FnOnce(&SchemaView<'_, '_>, &mut Tuple) -> R,
    ) -> Result<Option<R>, DatabaseError> {
        TransactionIter::next_tuple(self, f)
    }

    fn done(self) -> Result<(), DatabaseError> {
        TransactionIter::done(self)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::binder::{Binder, BinderContext};
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use crate::db::CatalogKind;
    use crate::db::{DataBaseBuilder, DatabaseError, ResultIter};
    use crate::expression::ScalarExpression;
    use crate::planner::operator::join::JoinCondition;
    use crate::planner::operator::Operator;
    use crate::planner::PlanArena;
    use crate::storage::{
        table_codec::TableCodec, Storage, TableCache, Transaction, TransactionIsolationLevel,
    };
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use chrono::{Datelike, Local};
    use std::io::ErrorKind;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::sync::atomic::AtomicUsize;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::sync::atomic::Ordering;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::sync::Arc;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::thread;
    #[cfg(feature = "unsafe_txdb_checkpoint")]
    use std::time::Duration;
    use tempfile::TempDir;

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn database_handles_are_send_sync() {
        #[cfg(feature = "rocksdb")]
        assert_send_sync::<super::Database<crate::storage::rocksdb::RocksStorage>>();
        #[cfg(feature = "lmdb")]
        assert_send_sync::<super::Database<crate::storage::lmdb::LmdbStorage>>();
    }

    pub(crate) fn build_table<T: Transaction>(
        table_cache: &mut TableCache,
        transaction: &mut T,
        plan_arena: &mut PlanArena,
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
        let mut table_codec = TableCodec::default();
        if let Some(table) = transaction.create_table(
            &mut table_codec,
            plan_arena,
            "t1".to_string().into(),
            columns,
            false,
        )? {
            let table = table.transplant_to_table_arena(plan_arena)?;
            table_cache.insert(table.name().clone(), table);
        }

        Ok(())
    }

    fn tuple_owned(tuple: &Tuple) -> Tuple {
        Tuple {
            pk: tuple.pk.clone(),
            values: tuple.values.clone(),
        }
    }

    fn next_tuple_owned<I: ResultIter>(iter: &mut I) -> Result<Option<Tuple>, DatabaseError> {
        iter.next_tuple(|_, tuple| tuple_owned(tuple))
    }

    fn next_values<I: ResultIter>(iter: &mut I) -> Result<Option<Vec<DataValue>>, DatabaseError> {
        iter.next_tuple(|_, tuple| tuple.values.clone())
    }

    fn read_single_i32<I>(mut iter: I) -> Result<i32, DatabaseError>
    where
        I: ResultIter,
    {
        let value = match iter.next_tuple(|_, tuple| match tuple.values.as_slice() {
            [DataValue::Int32(value)] => *value,
            other => panic!("expected a single Int32 column, got {other:?}"),
        })? {
            Some(value) => value,
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
        let value = match iter.next_tuple(|_, tuple| match tuple.values.as_slice() {
            [DataValue::Int32(value)] => *value,
            other => panic!("expected a single Int32 column, got {other:?}"),
        })? {
            Some(value) => value,
            None => panic!("expected one result row for query: {sql}"),
        };
        iter.done()?;
        Ok(value)
    }

    #[test]
    fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut database = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        database.ddl("create table t1(c1 int primary key, c2 boolean, c3 int)")?;

        let mut iter = database.run("select * from t1")?;
        while iter
            .next_tuple(|_, tuple| {
                println!("{tuple:#?}");
                ()
            })?
            .is_some()
        {}
        iter.done()?;
        Ok(())
    }

    /// use [CurrentDate](crate::function::current_date::CurrentDate) on this case
    #[test]
    fn test_udf() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;
        let mut iter = kite_sql.run("select current_date()")?;

        iter.schema(|schema| {
            assert_eq!(schema.len(), 1);
            let column = schema.get(0).unwrap();
            assert_eq!(column.name(), "current_date()");
            assert_eq!(column.datatype(), &LogicalType::Date);
        });
        assert_eq!(
            next_tuple_owned(&mut iter)?.unwrap(),
            Tuple::new(
                None,
                vec![DataValue::Date32(Local::now().num_days_from_ce())]
            )
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());

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

        iter.schema(|schema| {
            assert_eq!(schema.len(), 1);
            let column = schema.get(0).unwrap();
            assert_eq!(column.name(), "number");
            assert_eq!(column.datatype(), &LogicalType::Integer);
        });
        assert_eq!(
            next_tuple_owned(&mut iter)?.unwrap(),
            Tuple::new(None, vec![DataValue::Int32(3)])
        );
        assert_eq!(
            next_tuple_owned(&mut iter)?.unwrap(),
            Tuple::new(None, vec![DataValue::Int32(4)])
        );
        Ok(())
    }

    #[test]
    fn test_join_on_alias_right_key_is_localized() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("CREATE TABLE onecolumn (id INT PRIMARY KEY, x INT NULL)")?;
        kite_sql.ddl("CREATE TABLE empty (e_id INT PRIMARY KEY, x INT)")?;

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
            ),
            &[],
            None,
        );
        let mut source_plan_arena = PlanArena::new(kite_sql.state.table_arena());
        let source_plan = binder.bind(&stmt, &mut source_plan_arena)?;
        let (best_plan, _best_plan_arena) =
            kite_sql
                .state
                .build_plan([], &transaction, |binder, arena| binder.bind(&stmt, arena))?;

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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("CREATE TABLE onecolumn (id INT PRIMARY KEY, x INT NULL)")?;
        kite_sql.ddl("CREATE TABLE twocolumn (t_id INT PRIMARY KEY, x INT NULL, y INT NULL)")?;

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
            ),
            &[],
            None,
        );
        let mut source_plan_arena = PlanArena::new(kite_sql.state.table_arena());
        let source_plan = binder.bind(&stmt, &mut source_plan_arena)?;
        let (best_plan, _best_plan_arena) =
            kite_sql
                .state
                .build_plan([], &transaction, |binder, arena| binder.bind(&stmt, arena))?;

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
        filter.visit_referenced_columns(&mut source_plan_arena, &mut |_, column| {
            referenced_columns.push(*column);
            true
        });
        assert_eq!(referenced_columns.len(), 1);
        assert_eq!(source_plan_arena.column(referenced_columns[0]).name(), "y");

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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("CREATE TABLE onecolumn (id INT PRIMARY KEY, x INT NULL)")?;
        kite_sql.ddl("CREATE TABLE twocolumn (t_id INT PRIMARY KEY, x INT NULL, y INT NULL)")?;
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
        let (best_plan, _best_plan_arena) =
            kite_sql
                .state
                .build_plan([], &transaction, |binder, arena| binder.bind(&stmt, arena))?;
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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table t1 (a int primary key, b int)")?;
        kite_sql.run("insert into t1 values(0, 0)")?.done()?;
        kite_sql.run("insert into t1 values(1, 1)")?.done()?;
        kite_sql.run("insert into t1 values(2, 2)")?.done()?;

        // Filter
        {
            let statement = crate::db::prepare("explain select * from t1 where b > $1")?;

            let mut iter = kite_sql.execute(statement, &[("$1", DataValue::Int32(0))])?;

            let row = next_tuple_owned(&mut iter)?.unwrap();
            let plan = row.values[0].utf8().unwrap();
            assert!(plan.contains("Projection"));
            assert!(plan.contains("Filter ("));
            assert!(plan.contains(" > 0"));
            assert!(plan.contains("TableScan t1 -> [#"));
        }
        // Aggregate
        {
            let statement = crate::db::prepare(
                "explain select a + $1, max(b + $2) from t1 where b > $3 group by a + $4",
            )?;

            let mut iter = kite_sql.execute(
                statement,
                &[
                    ("$1", DataValue::Int32(0)),
                    ("$2", DataValue::Int32(0)),
                    ("$3", DataValue::Int32(1)),
                    ("$4", DataValue::Int32(0)),
                ],
            )?;
            let row = next_tuple_owned(&mut iter)?.unwrap();
            let plan = row.values[0].utf8().unwrap();
            assert!(plan.contains("Projection"));
            assert!(plan.contains("Aggregate"));
            assert!(plan.contains("Filter ("));
            assert!(plan.contains(" > 1"));
            assert!(plan.contains("TableScan t1 -> [#"));
        }
        {
            let statement = crate::db::prepare("explain select *, $1 from (select * from t1 where b > $2) left join (select * from t1 where a > $3) on a > $4")?;

            let mut iter = kite_sql.execute(
                statement,
                &[
                    ("$1", DataValue::Int32(9)),
                    ("$2", DataValue::Int32(0)),
                    ("$3", DataValue::Int32(1)),
                    ("$4", DataValue::Int32(0)),
                ],
            )?;
            let row = next_tuple_owned(&mut iter)?.unwrap();
            let plan = row.values[0].utf8().unwrap();
            assert!(plan.contains("Projection"));
            assert!(plan.contains("LeftOuter Join"));
            assert!(plan.contains("9"));
            assert!(plan.contains("0"));
            assert!(plan.contains("1"));
            assert!(plan.contains("TableScan t1 -> [#"));
        }

        Ok(())
    }

    // FIXME: keep this as a unit test instead of SLT for now. The current
    // sqllogictest runner does not reliably match the pretty-printed multi-line
    // EXPLAIN output produced by correlated IN, even though the plan itself is stable.
    #[test]
    fn test_subquery_explain_uses_parameterized_index_for_in() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table in_outer(id int primary key, a int)")?;
        kite_sql.ddl("create table in_inner(id int primary key, v int)")?;
        kite_sql.ddl("create table in_inner_nn(id int primary key, v int)")?;
        kite_sql.ddl("create index in_inner_v_index on in_inner(v)")?;
        kite_sql.ddl("create index in_inner_nn_v_index on in_inner_nn(v)")?;

        kite_sql
            .run("insert into in_outer values (0, null), (1, 1), (2, 2), (3, 3)")?
            .done()?;
        kite_sql
            .run("insert into in_inner values (0, 2), (1, null)")?
            .done()?;
        kite_sql
            .run("insert into in_inner_nn values (0, 2)")?
            .done()?;

        kite_sql.ddl("create table in_outer_flag(id int primary key, a int, b int)")?;
        kite_sql.ddl("create table in_inner_flag(id int primary key, v int, flag int)")?;
        kite_sql.ddl("create table in_inner_flag_nn(id int primary key, v int, flag int)")?;
        kite_sql.ddl("create index in_inner_flag_v_index on in_inner_flag(v)")?;
        kite_sql.ddl("create index in_inner_flag_nn_v_index on in_inner_flag_nn(v)")?;

        kite_sql
            .run("insert into in_outer_flag values (0, null, 1), (1, 1, 1), (2, 2, 1), (3, 3, 1)")?
            .done()?;
        kite_sql
            .run("insert into in_inner_flag values (0, 2, 1), (1, null, 1)")?
            .done()?;
        kite_sql
            .run("insert into in_inner_flag_nn values (0, 2, 1)")?
            .done()?;

        let collect_plan = |sql: &str| -> Result<String, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let mut lines = Vec::new();
            while let Some(row) = next_tuple_owned(&mut iter)? {
                if let Some(DataValue::Utf8 { value, .. }) = row.values.first() {
                    lines.push(value.clone());
                }
            }
            iter.done()?;
            Ok(lines.join("\n"))
        };
        let collect_ids = |sql: &str| -> Result<Vec<i32>, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let mut ids = Vec::new();
            while let Some(row) = next_tuple_owned(&mut iter)? {
                ids.push(row.values[0].i32().unwrap());
            }
            iter.done()?;
            Ok(ids)
        };

        let assert_mark_in_uses_parameterized_index = |sql: &str| -> Result<(), DatabaseError> {
            let explain_plan = collect_plan(sql)?;
            assert!(
                explain_plan.contains("MarkAnyApply"),
                "unexpected explain plan: {explain_plan}"
            );
            assert!(
                explain_plan.contains("IndexScan By #") && explain_plan.contains("=> Probe"),
                "unexpected explain plan: {explain_plan}"
            );
            Ok(())
        };

        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a in (select v from in_inner where in_inner.v = in_outer.a)",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a not in (select v from in_inner where in_inner.v = in_outer.a)",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a in (select v from in_inner_nn where in_inner_nn.v = in_outer.a)",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer where a not in (select v from in_inner_nn where in_inner_nn.v = in_outer.a)",
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

        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a in (select v from in_inner_flag where in_inner_flag.flag = in_outer_flag.b)",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a not in (select v from in_inner_flag where in_inner_flag.flag = in_outer_flag.b)",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a in (select v from in_inner_flag_nn where in_inner_flag_nn.flag = in_outer_flag.b)",
        )?;
        assert_mark_in_uses_parameterized_index(
            "explain select id from in_outer_flag where a not in (select v from in_inner_flag_nn where in_inner_flag_nn.flag = in_outer_flag.b)",
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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table exists_outer(id int primary key, a int, b int)")?;
        kite_sql.ddl("create table exists_inner(id int primary key, v int, flag int)")?;
        kite_sql.ddl("create index exists_inner_v_index on exists_inner(v)")?;

        kite_sql
            .run("insert into exists_outer values (0, 1, 1), (1, 1, 2), (2, 2, null), (3, 3, 1)")?
            .done()?;
        kite_sql
            .run("insert into exists_inner values (0, 1, 1), (1, 1, null), (2, 2, 1)")?
            .done()?;

        let collect_plan = |sql: &str| -> Result<String, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let mut lines = Vec::new();
            while let Some(row) = next_tuple_owned(&mut iter)? {
                if let Some(DataValue::Utf8 { value, .. }) = row.values.first() {
                    lines.push(value.clone());
                }
            }
            iter.done()?;
            Ok(lines.join("\n"))
        };
        let collect_ids = |sql: &str| -> Result<Vec<i32>, DatabaseError> {
            let mut iter = kite_sql.run(sql)?;
            let mut ids = Vec::new();
            while let Some(row) = next_tuple_owned(&mut iter)? {
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
                explain_plan.contains("IndexScan By #") && explain_plan.contains("=> Probe"),
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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table t_multi (a int primary key, b int)")?;

        let mut iter = kite_sql.run(
            "insert into t_multi values(0, 0); insert into t_multi values(1, 1); select * from t_multi order by a",
        )?;
        assert_eq!(
            next_values(&mut iter)?.unwrap(),
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            next_values(&mut iter)?.unwrap(),
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());
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
                assert!(msg.contains("DDL and ANALYZE"));
            }
            other => panic!("unexpected error type: {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn test_bind_error_with_span() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table t_bind_span(id int primary key)")?;

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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table t_bind_fn_span(id int primary key)")?;

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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table t1 (a int primary key, b int)")?;

        let mut tx_1 = kite_sql.new_transaction()?;
        let mut tx_2 = kite_sql.new_transaction()?;

        tx_1.run("insert into t1 values(0, 0)")?.done()?;
        tx_1.run("insert into t1 values(1, 1)")?.done()?;

        assert!(tx_2.run("insert into t1 values(0, 0)")?.done().is_err());
        tx_2.run("insert into t1 values(3, 3)")?.done()?;

        let mut iter_1 = tx_1.run("select * from t1")?;
        let mut iter_2 = tx_2.run("select * from t1")?;

        assert_eq!(
            next_values(&mut iter_1)?.unwrap(),
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            next_values(&mut iter_1)?.unwrap(),
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );

        assert_eq!(
            next_values(&mut iter_2)?.unwrap(),
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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        kite_sql.ddl("create table t_multi_tx (a int primary key, b int)")?;

        let mut tx = kite_sql.new_transaction()?;
        let mut iter = tx.run(
            "insert into t_multi_tx values(0, 0); insert into t_multi_tx values(1, 1); select * from t_multi_tx order by a",
        )?;
        assert_eq!(
            next_values(&mut iter)?.unwrap(),
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            next_values(&mut iter)?.unwrap(),
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;
        tx.commit()?;

        let mut check_iter = kite_sql.run("select count(*) from t_multi_tx")?;
        assert_eq!(
            next_values(&mut check_iter)?.unwrap(),
            vec![DataValue::Int32(2)]
        );
        check_iter.done()?;

        Ok(())
    }

    #[test]
    fn test_autocommit_read_drops_iterator_before_transaction() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_optimistic()?;

        kite_sql.ddl("create table t_iter_drop (a int primary key, b int)")?;

        let mut tx = kite_sql.new_transaction()?;
        tx.run("insert into t_iter_drop values (0, 0), (1, 1)")?
            .done()?;

        let mut iter = kite_sql.run("select * from t_iter_drop")?;
        assert!(iter.next_tuple(|_, _| ())?.is_none());

        tx.commit()?;

        Ok(())
    }

    #[test]
    fn test_exhausted_database_iter_releases_mdl_guard() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_optimistic()?;

        kite_sql.ddl("create table t_iter_guard (a int primary key, b int)")?;
        kite_sql
            .run("insert into t_iter_guard values (0, 0), (1, 1)")?
            .done()?;

        let mut iter = kite_sql.run("select * from t_iter_guard order by a")?;
        assert_eq!(
            next_values(&mut iter)?.unwrap(),
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            next_values(&mut iter)?.unwrap(),
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );
        assert!(iter.next_tuple(|_, _| ())?.is_none());
        iter.done()?;

        kite_sql.ddl("drop table t_iter_guard")?;

        Ok(())
    }

    #[test]
    fn test_read_committed_refreshes_snapshot_each_statement() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path())
            .transaction_isolation(TransactionIsolationLevel::ReadCommitted)
            .build_rocksdb()?;

        kite_sql.ddl("create table t_rc (a int primary key, b int)")?;
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
    fn test_rocksdb_defaults_to_repeatable_read() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build_rocksdb()?;

        assert_eq!(
            kite_sql.transaction_isolation(),
            TransactionIsolationLevel::RepeatableRead
        );

        Ok(())
    }

    #[test]
    fn test_repeatable_read_keeps_transaction_snapshot() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path())
            .transaction_isolation(TransactionIsolationLevel::RepeatableRead)
            .build_rocksdb()?;

        kite_sql.ddl("create table t_rr (a int primary key, b int)")?;
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
        let mut kite_sql = DataBaseBuilder::path(temp_dir.path()).build_optimistic()?;

        kite_sql.ddl("create table t1 (a int primary key, b int)")?;

        let mut tx_1 = kite_sql.new_transaction()?;
        let mut tx_2 = kite_sql.new_transaction()?;

        tx_1.run("insert into t1 values(0, 0)")?.done()?;
        tx_1.run("insert into t1 values(1, 1)")?.done()?;

        tx_2.run("insert into t1 values(0, 0)")?.done()?;
        tx_2.run("insert into t1 values(3, 3)")?.done()?;

        let mut iter_1 = tx_1.run("select * from t1")?;
        let mut iter_2 = tx_2.run("select * from t1")?;

        assert_eq!(
            next_values(&mut iter_1)?.unwrap(),
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            next_values(&mut iter_1)?.unwrap(),
            vec![DataValue::Int32(1), DataValue::Int32(1)]
        );

        assert_eq!(
            next_values(&mut iter_2)?.unwrap(),
            vec![DataValue::Int32(0), DataValue::Int32(0)]
        );
        assert_eq!(
            next_values(&mut iter_2)?.unwrap(),
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
        let mut kite_sql = DataBaseBuilder::path(&live_path).build_rocksdb()?;

        kite_sql.ddl("create table t_checkpoint (id int primary key, v int)")?;
        kite_sql
            .run("insert into t_checkpoint values (1, 10), (2, 20)")?
            .done()?;

        kite_sql.checkpoint(&checkpoint_path)?;

        kite_sql
            .run("insert into t_checkpoint values (3, 30)")?
            .done()?;

        let mut snapshot = DataBaseBuilder::path(&checkpoint_path).build_rocksdb()?;
        snapshot.load(CatalogKind::Table("t_checkpoint".to_string().into()))?;
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
        let mut kite_sql = DataBaseBuilder::path(&live_path).build_rocksdb()?;

        kite_sql.ddl("create table t_checkpoint_disabled (id int primary key, v int)")?;

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
        let mut kite_sql = DataBaseBuilder::path(&live_path).build_rocksdb()?;

        kite_sql.ddl("create table t_checkpoint_concurrent (id int primary key, v int)")?;
        let kite_sql = Arc::new(std::sync::Mutex::new(kite_sql));

        let inserted = Arc::new(AtomicUsize::new(0));
        let writer_db = Arc::clone(&kite_sql);
        let writer_inserted = Arc::clone(&inserted);
        let writer = thread::spawn(move || -> Result<usize, DatabaseError> {
            for i in 0..64 {
                writer_db
                    .lock()
                    .unwrap()
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

        kite_sql.lock().unwrap().checkpoint(&checkpoint_path)?;

        let total = writer.join().expect("writer thread should not panic")?;
        let mut snapshot = DataBaseBuilder::path(&checkpoint_path).build_rocksdb()?;
        snapshot.load(CatalogKind::Table(
            "t_checkpoint_concurrent".to_string().into(),
        ))?;
        let snapshot_count = query_i32(&snapshot, "select count(*) from t_checkpoint_concurrent")?;
        let consistent_count = query_i32(
            &snapshot,
            "select count(*) from t_checkpoint_concurrent where id = v",
        )?;

        assert!(snapshot_count >= 8);
        assert!(snapshot_count <= total as i32);
        assert_eq!(snapshot_count, consistent_count);
        assert_eq!(
            query_i32(
                &kite_sql.lock().unwrap(),
                "select count(*) from t_checkpoint_concurrent",
            )?,
            total as i32
        );

        Ok(())
    }
}
