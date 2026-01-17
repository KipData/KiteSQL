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
use crate::execution::{build_write, Executor};
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
use crate::planner::LogicalPlan;
use crate::storage::memory::MemoryStorage;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::rocksdb::{OptimisticRocksStorage, RocksStorage};
use crate::storage::{StatisticsMetaCache, Storage, TableCache, Transaction, ViewCache};
use crate::types::tuple::{SchemaRef, Tuple};
use crate::types::value::DataValue;
use crate::utils::lru::SharedLruCache;
use ahash::HashMap;
use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use parking_lot::{RawRwLock, RwLock};
use std::hash::RandomState;
use std::marker::PhantomData;
use std::mem;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub(crate) type ScalaFunctions = HashMap<FunctionSummary, Arc<dyn ScalarFunctionImpl>>;
pub(crate) type TableFunctions = HashMap<FunctionSummary, Arc<dyn TableFunctionImpl>>;

pub type Statement = sqlparser::ast::Statement;

#[allow(dead_code)]
pub(crate) enum MetaDataLock {
    Read(ArcRwLockReadGuard<RawRwLock, ()>),
    Write(ArcRwLockWriteGuard<RawRwLock, ()>),
}

pub struct DataBaseBuilder {
    #[cfg_attr(target_arch = "wasm32", allow(dead_code))]
    path: PathBuf,
    scala_functions: ScalaFunctions,
    table_functions: TableFunctions,
}

impl DataBaseBuilder {
    pub fn path(path: impl Into<PathBuf> + Send) -> Self {
        let mut builder = DataBaseBuilder {
            path: path.into(),
            scala_functions: Default::default(),
            table_functions: Default::default(),
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

    pub fn register_scala_function(mut self, function: Arc<dyn ScalarFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.scala_functions.insert(summary, function);
        self
    }

    pub fn register_table_function(mut self, function: Arc<dyn TableFunctionImpl>) -> Self {
        let summary = function.summary().clone();

        self.table_functions.insert(summary, function);
        self
    }

    pub fn build_with_storage<T: Storage>(self, storage: T) -> Result<Database<T>, DatabaseError> {
        Self::_build::<T>(storage, self.scala_functions, self.table_functions)
    }

    #[cfg(target_arch = "wasm32")]
    pub fn build(self) -> Result<Database<MemoryStorage>, DatabaseError> {
        let storage = MemoryStorage::new();

        Self::_build::<MemoryStorage>(storage, self.scala_functions, self.table_functions)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn build(self) -> Result<Database<RocksStorage>, DatabaseError> {
        let storage = RocksStorage::new(self.path)?;

        Self::_build::<RocksStorage>(storage, self.scala_functions, self.table_functions)
    }

    pub fn build_in_memory(self) -> Result<Database<MemoryStorage>, DatabaseError> {
        let storage = MemoryStorage::new();

        Self::_build::<MemoryStorage>(storage, self.scala_functions, self.table_functions)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn build_optimistic(self) -> Result<Database<OptimisticRocksStorage>, DatabaseError> {
        let storage = OptimisticRocksStorage::new(self.path)?;

        Self::_build::<OptimisticRocksStorage>(storage, self.scala_functions, self.table_functions)
    }

    fn _build<T: Storage>(
        storage: T,
        scala_functions: ScalaFunctions,
        table_functions: TableFunctions,
    ) -> Result<Database<T>, DatabaseError> {
        let meta_cache = SharedLruCache::new(256, 8, RandomState::new())?;
        let table_cache = SharedLruCache::new(48, 4, RandomState::new())?;
        let view_cache = SharedLruCache::new(12, 4, RandomState::new())?;

        Ok(Database {
            storage,
            mdl: Default::default(),
            state: Arc::new(State {
                scala_functions,
                table_functions,
                meta_cache,
                table_cache,
                view_cache,
                optimizer_pipeline: default_optimizer_pipeline(),
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
            vec![
                NormalizationRuleImpl::SimplifyFilter,
                NormalizationRuleImpl::ConstantCalculation,
            ],
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
            "Combine Operators".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![
                NormalizationRuleImpl::CollapseProject,
                NormalizationRuleImpl::CollapseGroupByAgg,
                NormalizationRuleImpl::CombineFilter,
            ],
        )
        .before_batch(
            "TopK".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![NormalizationRuleImpl::TopK],
        )
        .after_batch(
            "Eliminate Aggregate".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![
                NormalizationRuleImpl::EliminateRedundantSort,
                NormalizationRuleImpl::UseStreamDistinct,
            ],
        )
        .after_batch(
            "Expression Remapper".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![
                NormalizationRuleImpl::BindExpressionPosition,
                NormalizationRuleImpl::EvaluatorBind,
            ],
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

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build_plan<A: AsRef<[(&'static str, DataValue)]>>(
        &self,
        stmt: &Statement,
        params: A,
        table_cache: &TableCache,
        view_cache: &ViewCache,
        meta_cache: &StatisticsMetaCache,
        transaction: &<S as Storage>::TransactionType<'_>,
        scala_functions: &ScalaFunctions,
        table_functions: &TableFunctions,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut binder = Binder::new(
            BinderContext::new(
                table_cache,
                view_cache,
                transaction,
                scala_functions,
                table_functions,
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
        let best_plan = self
            .optimizer_pipeline
            .instantiate(source_plan)
            .find_best(Some(&transaction.meta_loader(meta_cache)))?;

        Ok(best_plan)
    }

    fn prepare<T: AsRef<str>>(&self, sql: T) -> Result<Statement, DatabaseError> {
        let mut stmts = parse_sql(sql)?;
        stmts.pop().ok_or(DatabaseError::EmptyStatement)
    }

    fn execute<'a, A: AsRef<[(&'static str, DataValue)]>>(
        &'a self,
        transaction: &'a mut S::TransactionType<'_>,
        stmt: &Statement,
        params: A,
    ) -> Result<(SchemaRef, Executor<'a>), DatabaseError> {
        let mut plan = self.build_plan(
            stmt,
            params,
            self.table_cache(),
            self.view_cache(),
            self.meta_cache(),
            transaction,
            self.scala_functions(),
            self.table_functions(),
        )?;
        let schema = plan.output_schema().clone();
        let executor = build_write(
            plan,
            (&self.table_cache, &self.view_cache, &self.meta_cache),
            transaction,
        );

        Ok((schema, executor))
    }
}

pub struct Database<S: Storage> {
    pub(crate) storage: S,
    mdl: Arc<RwLock<()>>,
    pub(crate) state: Arc<State<S>>,
}

impl<S: Storage> Database<S> {
    /// Run SQL queries.
    pub fn run<T: AsRef<str>>(&self, sql: T) -> Result<DatabaseIter<'_, S>, DatabaseError> {
        let statement = self.prepare(sql)?;

        self.execute(&statement, &[])
    }

    pub fn prepare<T: AsRef<str>>(&self, sql: T) -> Result<Statement, DatabaseError> {
        self.state.prepare(sql)
    }

    pub fn execute<A: AsRef<[(&'static str, DataValue)]>>(
        &self,
        statement: &Statement,
        params: A,
    ) -> Result<DatabaseIter<'_, S>, DatabaseError> {
        let _guard = if matches!(command_type(statement)?, CommandType::DDL) {
            MetaDataLock::Write(self.mdl.write_arc())
        } else {
            MetaDataLock::Read(self.mdl.read_arc())
        };
        let transaction = Box::into_raw(Box::new(self.storage.transaction()?));
        let (schema, executor) =
            self.state
                .execute(unsafe { &mut (*transaction) }, statement, params)?;
        let inner = Box::into_raw(Box::new(TransactionIter::new(schema, executor)));
        Ok(DatabaseIter { transaction, inner })
    }

    pub fn new_transaction(&self) -> Result<DBTransaction<'_, S>, DatabaseError> {
        let guard = self.mdl.read_arc();
        let transaction = self.storage.transaction()?;
        let state = self.state.clone();

        Ok(DBTransaction {
            inner: transaction,
            _guard: guard,
            state,
        })
    }
}

pub trait ResultIter: Iterator<Item = Result<Tuple, DatabaseError>> {
    fn schema(&self) -> &SchemaRef;

    fn done(self) -> Result<(), DatabaseError>;
}

pub struct DatabaseIter<'a, S: Storage + 'a> {
    transaction: *mut S::TransactionType<'a>,
    inner: *mut TransactionIter<'a>,
}

impl<S: Storage> Drop for DatabaseIter<'_, S> {
    fn drop(&mut self) {
        if !self.transaction.is_null() {
            unsafe { drop(Box::from_raw(self.transaction)) }
        }
        if !self.inner.is_null() {
            unsafe { drop(Box::from_raw(self.inner)) }
        }
    }
}

impl<S: Storage> Iterator for DatabaseIter<'_, S> {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe { (*self.inner).next() }
    }
}

impl<S: Storage> ResultIter for DatabaseIter<'_, S> {
    fn schema(&self) -> &SchemaRef {
        unsafe { (*self.inner).schema() }
    }

    fn done(mut self) -> Result<(), DatabaseError> {
        unsafe {
            Box::from_raw(mem::replace(&mut self.inner, std::ptr::null_mut())).done()?;
        }
        unsafe {
            Box::from_raw(mem::replace(&mut self.transaction, std::ptr::null_mut())).commit()?;
        }
        Ok(())
    }
}

pub struct DBTransaction<'a, S: Storage + 'a> {
    inner: S::TransactionType<'a>,
    _guard: ArcRwLockReadGuard<RawRwLock, ()>,
    state: Arc<State<S>>,
}

impl<S: Storage> DBTransaction<'_, S> {
    pub fn run<T: AsRef<str>>(&mut self, sql: T) -> Result<TransactionIter<'_>, DatabaseError> {
        let statement = self.state.prepare(sql)?;

        self.execute(&statement, &[])
    }

    pub fn prepare<T: AsRef<str>>(&self, sql: T) -> Result<Statement, DatabaseError> {
        self.state.prepare(sql)
    }

    pub fn execute<A: AsRef<[(&'static str, DataValue)]>>(
        &mut self,
        statement: &Statement,
        params: A,
    ) -> Result<TransactionIter<'_>, DatabaseError> {
        if matches!(command_type(statement)?, CommandType::DDL) {
            return Err(DatabaseError::UnsupportedStmt(
                "`DDL` is not allowed to execute within a transaction".to_string(),
            ));
        }
        let (schema, executor) = self.state.execute(&mut self.inner, statement, params)?;
        Ok(TransactionIter::new(schema, executor))
    }

    pub fn commit(self) -> Result<(), DatabaseError> {
        self.inner.commit()?;

        Ok(())
    }
}

pub struct TransactionIter<'a> {
    executor: Executor<'a>,
    schema: SchemaRef,
}

impl<'a> TransactionIter<'a> {
    fn new(schema: SchemaRef, executor: Executor<'a>) -> Self {
        Self { executor, schema }
    }
}

impl Iterator for TransactionIter<'_> {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.executor.next()
    }
}

impl ResultIter for TransactionIter<'_> {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn done(mut self) -> Result<(), DatabaseError> {
        for result in self.by_ref() {
            let _ = result?;
        }
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod test {
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::db::{DataBaseBuilder, DatabaseError, ResultIter};
    use crate::storage::{Storage, TableCache, Transaction};
    use crate::types::tuple::Tuple;
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use chrono::{Datelike, Local};
    use std::sync::Arc;
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

    #[test]
    fn test_run_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let database = DataBaseBuilder::path(temp_dir.path()).build()?;
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
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;
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
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;
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
    fn test_prepare_statment() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

        kite_sql
            .run("create table t1 (a int primary key, b int)")?
            .done()?;
        kite_sql.run("insert into t1 values(0, 0)")?.done()?;
        kite_sql.run("insert into t1 values(1, 1)")?.done()?;
        kite_sql.run("insert into t1 values(2, 2)")?.done()?;

        // Filter
        {
            let statement = kite_sql.prepare("explain select * from t1 where b > ?1")?;

            let mut iter = kite_sql.execute(&statement, &[("?1", DataValue::Int32(0))])?;

            assert_eq!(
                iter.next().unwrap()?.values[0].utf8().unwrap(),
                "Projection [t1.a, t1.b] [Project => (Sort Option: Follow)]
  Filter (t1.b > 0), Is Having: false [Filter => (Sort Option: Follow)]
    TableScan t1 -> [a, b] [SeqScan => (Sort Option: None)]"
            )
        }
        // Aggregate
        {
            let statement = kite_sql.prepare(
                "explain select a + ?1, max(b + ?2) from t1 where b > ?3 group by a + ?4",
            )?;

            let mut iter = kite_sql.execute(
                &statement,
                &[
                    ("?1", DataValue::Int32(0)),
                    ("?2", DataValue::Int32(0)),
                    ("?3", DataValue::Int32(1)),
                    ("?4", DataValue::Int32(0)),
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
            let statement = kite_sql.prepare("explain select *, ?1 from (select * from t1 where b > ?2) left join (select * from t1 where a > ?3) on a > ?4")?;

            let mut iter = kite_sql.execute(
                &statement,
                &[
                    ("?1", DataValue::Int32(9)),
                    ("?2", DataValue::Int32(0)),
                    ("?3", DataValue::Int32(1)),
                    ("?4", DataValue::Int32(0)),
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

    #[test]
    fn test_transaction_sql() -> Result<(), DatabaseError> {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let kite_sql = DataBaseBuilder::path(temp_dir.path()).build()?;

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
}
