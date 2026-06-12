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

pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;

use self::ddl::add_column::AddColumn;
use self::ddl::change_column::ChangeColumn;
use self::dql::join::nested_loop_join::NestedLoopJoin;
use self::dql::mark_apply::MarkApply;
use self::dql::scalar_apply::ScalarApply;
use crate::db::{ScalaFunctions, TableFunctions};
use crate::errors::DatabaseError;
use crate::execution::ddl::create_index::CreateIndex;
use crate::execution::ddl::create_table::CreateTable;
use crate::execution::ddl::create_view::CreateView;
use crate::execution::ddl::drop_column::DropColumn;
use crate::execution::ddl::drop_index::DropIndex;
use crate::execution::ddl::drop_table::DropTable;
use crate::execution::ddl::drop_view::DropView;
use crate::execution::ddl::truncate::Truncate;
use crate::execution::dml::analyze::Analyze;
use crate::execution::dml::copy_from_file::CopyFromFile;
use crate::execution::dml::copy_to_file::CopyToFile;
use crate::execution::dml::delete::Delete;
use crate::execution::dml::insert::Insert;
use crate::execution::dml::update::Update;
use crate::execution::dql::aggregate::hash_agg::HashAggExecutor;
use crate::execution::dql::aggregate::simple_agg::SimpleAggExecutor;
use crate::execution::dql::aggregate::stream_distinct::StreamDistinctExecutor;
use crate::execution::dql::describe::Describe;
use crate::execution::dql::dummy::Dummy;
use crate::execution::dql::explain::Explain;
use crate::execution::dql::filter::Filter;
use crate::execution::dql::function_scan::FunctionScan;
use crate::execution::dql::index_scan::IndexScan;
use crate::execution::dql::join::hash_join::HashJoin;
use crate::execution::dql::limit::Limit;
use crate::execution::dql::projection::Projection;
use crate::execution::dql::scalar_subquery::ScalarSubquery;
use crate::execution::dql::seq_scan::SeqScan;
use crate::execution::dql::set_membership::SetMembership;
use crate::execution::dql::show_table::ShowTables;
use crate::execution::dql::show_view::ShowViews;
use crate::execution::dql::sort::Sort;
use crate::execution::dql::top_k::TopK;
use crate::execution::dql::union::Union;
use crate::execution::dql::values::Values;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::types::index::RuntimeIndexProbe;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;

#[derive(Clone, Copy)]
pub(crate) struct ReadExecutionContext<'a> {
    table_cache: &'a TableCache,
    view_cache: &'a ViewCache,
    meta_cache: &'a StatisticsMetaCache,
    scala_functions: &'a ScalaFunctions,
    table_functions: &'a TableFunctions,
}

pub(crate) struct WriteExecutionContext<'a> {
    table_cache: &'a mut TableCache,
    view_cache: &'a mut ViewCache,
    meta_cache: &'a mut StatisticsMetaCache,
    scala_functions: &'a ScalaFunctions,
    table_functions: &'a TableFunctions,
}

impl<'a> ReadExecutionContext<'a> {
    pub(crate) fn new(
        table_cache: &'a TableCache,
        view_cache: &'a ViewCache,
        meta_cache: &'a StatisticsMetaCache,
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
    ) -> Self {
        Self {
            table_cache,
            view_cache,
            meta_cache,
            scala_functions,
            table_functions,
        }
    }

    pub(crate) fn table_cache(self) -> &'a TableCache {
        self.table_cache
    }

    pub(crate) fn scala_functions(self) -> &'a ScalaFunctions {
        self.scala_functions
    }

    pub(crate) fn table_functions(self) -> &'a TableFunctions {
        self.table_functions
    }
}

impl<'a> WriteExecutionContext<'a> {
    pub(crate) fn new(
        table_cache: &'a mut TableCache,
        view_cache: &'a mut ViewCache,
        meta_cache: &'a mut StatisticsMetaCache,
        scala_functions: &'a ScalaFunctions,
        table_functions: &'a TableFunctions,
    ) -> Self {
        Self {
            table_cache,
            view_cache,
            meta_cache,
            scala_functions,
            table_functions,
        }
    }

    fn read(&self) -> ReadExecutionContext<'_> {
        ReadExecutionContext::new(
            self.table_cache,
            self.view_cache,
            self.meta_cache,
            self.scala_functions,
            self.table_functions,
        )
    }

    pub(crate) fn table_cache_mut(&mut self) -> &mut TableCache {
        self.table_cache
    }

    pub(crate) fn view_cache_mut(&mut self) -> &mut ViewCache {
        self.view_cache
    }

    pub(crate) fn meta_cache_mut(&mut self) -> &mut StatisticsMetaCache {
        self.meta_cache
    }

    pub(crate) fn table_meta_cache_mut(&mut self) -> (&mut TableCache, &mut StatisticsMetaCache) {
        (self.table_cache, self.meta_cache)
    }
}

enum ExecutionContext<'a> {
    Read(ReadExecutionContext<'a>),
    Write(WriteExecutionContext<'a>),
}

impl<'a> ExecutionContext<'a> {
    fn read(&self) -> ReadExecutionContext<'_> {
        match self {
            ExecutionContext::Read(context) => *context,
            ExecutionContext::Write(context) => context.read(),
        }
    }

    fn is_same_read_context(&self, other: ReadExecutionContext<'_>) -> bool {
        let current = self.read();
        std::ptr::eq(current.table_cache, other.table_cache)
            && std::ptr::eq(current.view_cache, other.view_cache)
            && std::ptr::eq(current.meta_cache, other.meta_cache)
            && std::ptr::eq(current.scala_functions, other.scala_functions)
            && std::ptr::eq(current.table_functions, other.table_functions)
    }
}

pub(crate) trait IntoReadExecutionContext<'a> {
    fn into_read_execution_context(self) -> ReadExecutionContext<'a>;
}

impl<'a> IntoReadExecutionContext<'a> for ReadExecutionContext<'a> {
    fn into_read_execution_context(self) -> ReadExecutionContext<'a> {
        self
    }
}

impl<'a> IntoReadExecutionContext<'a>
    for (
        &'a TableCache,
        &'a ViewCache,
        &'a StatisticsMetaCache,
        &'a ScalaFunctions,
        &'a TableFunctions,
    )
{
    fn into_read_execution_context(self) -> ReadExecutionContext<'a> {
        ReadExecutionContext::new(self.0, self.1, self.2, self.3, self.4)
    }
}

pub(crate) type ExecId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecStatus {
    Continue,
    End,
}

#[derive(Debug, Default)]
pub(crate) struct ExecResult {
    pub(crate) tuple: Tuple,
    pub(crate) status: Option<ExecStatus>,
}

pub struct Executor<'a, T: Transaction + 'a> {
    arena: ExecArena<'a, T>,
    root: ExecId,
}

impl<'a, T: Transaction + 'a> Executor<'a, T> {
    pub(crate) fn new(arena: ExecArena<'a, T>, root: ExecId) -> Self {
        Self { arena, root }
    }

    pub(crate) fn next_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
        if !self.arena.next_tuple(self.root)? {
            return Ok(None);
        }
        Ok(Some(self.arena.result_tuple()))
    }
}

impl<T: Transaction> Iterator for Executor<'_, T> {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_tuple() {
            Ok(Some(tuple)) => Some(Ok(tuple.clone())),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ExecNode<'a, T: Transaction + 'a> {
    AddColumn(AddColumn),
    Analyze(Analyze),
    ChangeColumn(ChangeColumn),
    CopyFromFile(CopyFromFile),
    CopyToFile(CopyToFile),
    CreateIndex(CreateIndex),
    CreateTable(CreateTable),
    CreateView(CreateView),
    Delete(Delete),
    Describe(Describe),
    DropColumn(DropColumn),
    DropIndex(DropIndex),
    DropTable(DropTable),
    DropView(DropView),
    Dummy(Dummy),
    Explain(Explain),
    Filter(Filter),
    FunctionScan(FunctionScan),
    HashAgg(HashAggExecutor),
    HashJoin(HashJoin),
    IndexScan(IndexScan<'a, T>),
    Insert(Insert),
    Limit(Limit),
    MarkApply(MarkApply),
    NestedLoopJoin(NestedLoopJoin),
    Projection(Projection),
    ScalarApply(ScalarApply),
    ScalarSubquery(ScalarSubquery),
    SetMembership(SetMembership),
    SeqScan(SeqScan<'a, T>),
    ShowTables(ShowTables<'a, T>),
    ShowViews(ShowViews<'a, T>),
    SimpleAgg(SimpleAggExecutor),
    Sort(Sort),
    StreamDistinct(StreamDistinctExecutor),
    TopK(TopK),
    Truncate(Truncate),
    Union(Union),
    Update(Update),
    Values(Values),
    Empty,
}

pub(crate) trait ExecutorNode<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        context: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError>;
}

impl<'a, T: Transaction + 'a> ExecNode<'a, T> {
    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        match self {
            ExecNode::AddColumn(exec) => {
                <AddColumn as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::Analyze(exec) => <Analyze as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::ChangeColumn(exec) => {
                <ChangeColumn as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::CopyFromFile(exec) => {
                <CopyFromFile as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::CopyToFile(exec) => {
                <CopyToFile as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::CreateIndex(exec) => {
                <CreateIndex as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::CreateTable(exec) => {
                <CreateTable as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::CreateView(exec) => {
                <CreateView as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::Delete(exec) => <Delete as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Describe(exec) => <Describe as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::DropColumn(exec) => {
                <DropColumn as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::DropIndex(exec) => {
                <DropIndex as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::DropTable(exec) => {
                <DropTable as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::DropView(exec) => <DropView as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Dummy(exec) => <Dummy as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Explain(exec) => <Explain as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Filter(exec) => <Filter as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::FunctionScan(exec) => {
                <FunctionScan as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::HashAgg(exec) => {
                <HashAggExecutor as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::HashJoin(exec) => <HashJoin as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::IndexScan(exec) => {
                <IndexScan<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::Insert(exec) => <Insert as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Limit(exec) => <Limit as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::MarkApply(exec) => {
                <MarkApply as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::NestedLoopJoin(exec) => {
                <NestedLoopJoin as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::Projection(exec) => {
                <Projection as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::ScalarApply(exec) => {
                <ScalarApply as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::ScalarSubquery(exec) => {
                <ScalarSubquery as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::SetMembership(exec) => {
                <SetMembership as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::SeqScan(exec) => {
                <SeqScan<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::ShowTables(exec) => {
                <ShowTables<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::ShowViews(exec) => {
                <ShowViews<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::SimpleAgg(exec) => {
                <SimpleAggExecutor as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::Sort(exec) => <Sort as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::StreamDistinct(exec) => {
                <StreamDistinctExecutor as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::TopK(exec) => <TopK as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Truncate(exec) => <Truncate as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Union(exec) => <Union as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Update(exec) => <Update as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Values(exec) => <Values as ExecutorNode<'a, T>>::next_tuple(exec, arena),
            ExecNode::Empty => unreachable!("executor node re-entered while active"),
        }
    }
}

pub(crate) struct ExecArena<'a, T: Transaction + 'a> {
    nodes: Vec<ExecNode<'a, T>>,
    result: ExecResult,
    projection_tmp: Vec<DataValue>,
    context: Option<ExecutionContext<'a>>,
    transaction: *mut T,
    runtime_probe_stack: Vec<RuntimeIndexProbe>,
}

impl<'a, T: Transaction + 'a> Default for ExecArena<'a, T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            result: ExecResult::default(),
            projection_tmp: Vec::new(),
            context: None,
            transaction: std::ptr::null_mut(),
            runtime_probe_stack: Vec::new(),
        }
    }
}

impl<'a, T: Transaction + 'a> ExecArena<'a, T> {
    pub(crate) fn init_context<C>(&mut self, context: C, transaction: &'a T)
    where
        C: IntoReadExecutionContext<'a>,
    {
        let context = context.into_read_execution_context();
        if let Some(current) = &self.context {
            debug_assert!(current.is_same_read_context(context));
            debug_assert_eq!(self.transaction, transaction as *const T as *mut T);
        } else {
            self.context = Some(ExecutionContext::Read(context));
            self.transaction = transaction as *const T as *mut T;
        }
    }

    pub(crate) fn init_context_mut(
        &mut self,
        context: WriteExecutionContext<'a>,
        transaction: &'a mut T,
    ) {
        if let Some(current) = &self.context {
            debug_assert!(current.is_same_read_context(context.read()));
            debug_assert_eq!(self.transaction, transaction as *mut T);
        } else {
            self.context = Some(ExecutionContext::Write(context));
            self.transaction = transaction;
        }
    }

    pub(crate) fn push(&mut self, node: ExecNode<'a, T>) -> ExecId {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    pub(crate) fn read_context(&self) -> ReadExecutionContext<'a> {
        match self
            .context
            .as_ref()
            .expect("execution arena context initialized")
        {
            ExecutionContext::Read(context) => *context,
            ExecutionContext::Write(_) => {
                panic!("stored read execution context required")
            }
        }
    }

    pub(crate) fn table_cache(&self) -> &TableCache {
        self.context
            .as_ref()
            .expect("execution arena context initialized")
            .read()
            .table_cache
    }

    pub(crate) fn transaction(&self) -> &'a T {
        unsafe { &*self.transaction }
    }

    pub(crate) fn transaction_mut(&mut self) -> &mut T {
        unsafe { &mut *self.transaction }
    }

    pub(crate) fn write_context_mut(&mut self) -> (&mut T, &mut WriteExecutionContext<'a>) {
        let context = self
            .context
            .as_mut()
            .expect("execution arena context initialized");
        let ExecutionContext::Write(context) = context else {
            panic!("write execution context required")
        };
        unsafe { (&mut *self.transaction, context) }
    }

    pub(crate) fn push_runtime_probe(&mut self, value: RuntimeIndexProbe) {
        self.runtime_probe_stack.push(value);
    }

    pub(crate) fn pop_runtime_probe(&mut self) -> RuntimeIndexProbe {
        self.runtime_probe_stack
            .pop()
            .expect("runtime probe scope initialized")
    }

    pub(crate) fn runtime_probe_depth(&self) -> usize {
        self.runtime_probe_stack.len()
    }

    #[inline]
    pub(crate) fn result_tuple(&self) -> &Tuple {
        &self.result.tuple
    }

    #[inline]
    pub(crate) fn result_tuple_mut(&mut self) -> &mut Tuple {
        &mut self.result.tuple
    }

    #[inline]
    pub(crate) fn with_projection_tmp<R, E>(
        &mut self,
        f: impl FnOnce(&Tuple, &mut Vec<DataValue>) -> Result<R, E>,
    ) -> Result<R, E> {
        let ExecArena {
            result,
            projection_tmp,
            ..
        } = self;
        let ret = f(&result.tuple, projection_tmp)?;
        std::mem::swap(&mut result.tuple.values, projection_tmp);
        Ok(ret)
    }

    #[inline]
    pub(crate) fn resume(&mut self) {
        self.result.status = Some(ExecStatus::Continue);
    }

    #[inline]
    pub(crate) fn finish(&mut self) {
        self.result.status = Some(ExecStatus::End);
    }

    #[inline]
    pub(crate) fn produce_tuple(&mut self, tuple: Tuple) {
        self.result.tuple = tuple;
        self.resume();
    }

    pub(crate) fn next_tuple(&mut self, id: ExecId) -> Result<bool, DatabaseError> {
        self.result.status = None;
        let mut node = std::mem::replace(&mut self.nodes[id], ExecNode::Empty);
        let result = node.next_tuple(self);
        self.nodes[id] = node;
        result?;

        match self.result.status.unwrap_or(ExecStatus::End) {
            ExecStatus::Continue => Ok(true),
            ExecStatus::End => Ok(false),
        }
    }
}

pub(crate) trait ReadExecutor<'a, T: Transaction + 'a>: Sized {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) trait WriteExecutor<'a, T: Transaction + 'a>: Sized {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

macro_rules! impl_read_executor_node_via_from {
    ($ty:ty, $input:ty) => {
        impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for $ty
        where
            Self: ReadExecutor<'a, T> + From<$input>,
        {
            type Input = $input;

            fn into_executor(
                input: Self::Input,
                arena: &mut ExecArena<'a, T>,
                cache: ReadExecutionContext<'_>,
                transaction: &T,
            ) -> ExecId {
                <Self as ReadExecutor<'a, T>>::into_executor(
                    Self::from(input),
                    arena,
                    cache,
                    transaction,
                )
            }

            fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
                <$ty>::next_tuple(self, arena)
            }
        }
    };
}

macro_rules! impl_write_executor_node_via_from {
    ($ty:ty, $input:ty) => {
        impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for $ty
        where
            Self: WriteExecutor<'a, T> + From<$input>,
        {
            type Input = $input;

            fn into_executor(
                input: Self::Input,
                arena: &mut ExecArena<'a, T>,
                cache: ReadExecutionContext<'_>,
                transaction: &T,
            ) -> ExecId {
                <Self as WriteExecutor<'a, T>>::into_executor(
                    Self::from(input),
                    arena,
                    cache,
                    transaction,
                )
            }

            fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
                <$ty>::next_tuple(self, arena)
            }
        }
    };
}

impl_read_executor_node_via_from!(
    CopyToFile,
    (
        crate::planner::operator::copy_to_file::CopyToFileOperator,
        LogicalPlan
    )
);

impl_write_executor_node_via_from!(
    AddColumn,
    crate::planner::operator::alter_table::add_column::AddColumnOperator
);
impl_write_executor_node_via_from!(
    Analyze,
    (
        crate::planner::operator::analyze::AnalyzeOperator,
        LogicalPlan
    )
);
impl_write_executor_node_via_from!(
    ChangeColumn,
    crate::planner::operator::alter_table::change_column::ChangeColumnOperator
);
impl_write_executor_node_via_from!(
    CopyFromFile,
    crate::planner::operator::copy_from_file::CopyFromFileOperator
);
impl_write_executor_node_via_from!(
    CreateIndex,
    (
        crate::planner::operator::create_index::CreateIndexOperator,
        LogicalPlan
    )
);
impl_write_executor_node_via_from!(
    CreateTable,
    crate::planner::operator::create_table::CreateTableOperator
);
impl_write_executor_node_via_from!(
    CreateView,
    crate::planner::operator::create_view::CreateViewOperator
);
impl_write_executor_node_via_from!(
    Delete,
    (
        crate::planner::operator::delete::DeleteOperator,
        LogicalPlan
    )
);
impl_write_executor_node_via_from!(
    DropColumn,
    crate::planner::operator::alter_table::drop_column::DropColumnOperator
);
impl_write_executor_node_via_from!(
    DropIndex,
    crate::planner::operator::drop_index::DropIndexOperator
);
impl_write_executor_node_via_from!(
    DropTable,
    crate::planner::operator::drop_table::DropTableOperator
);
impl_write_executor_node_via_from!(
    DropView,
    crate::planner::operator::drop_view::DropViewOperator
);
impl_write_executor_node_via_from!(
    Insert,
    (
        crate::planner::operator::insert::InsertOperator,
        LogicalPlan
    )
);
impl_write_executor_node_via_from!(
    Truncate,
    crate::planner::operator::truncate::TruncateOperator
);
impl_write_executor_node_via_from!(
    Update,
    (
        crate::planner::operator::update::UpdateOperator,
        LogicalPlan
    )
);

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ShowTables<'a, T> {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::ShowTables(input))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        ShowTables::next_tuple(self, arena)
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ShowViews<'a, T> {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ReadExecutionContext<'_>,
        _: &T,
    ) -> ExecId {
        arena.push(ExecNode::ShowViews(input))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        ShowViews::next_tuple(self, arena)
    }
}

pub(crate) fn build_read<'a, T>(
    arena: &mut ExecArena<'a, T>,
    plan: LogicalPlan,
    cache: ReadExecutionContext<'_>,
    transaction: &T,
) -> ExecId
where
    T: Transaction + 'a,
{
    let LogicalPlan {
        operator,
        childrens,
        physical_option,
        _output_schema_ref,
    } = plan;

    match operator {
        Operator::Dummy => <Dummy as ExecutorNode<'a, T>>::into_executor(
            Dummy::default(),
            arena,
            cache,
            transaction,
        ),
        Operator::Aggregate(op) => {
            let input = childrens.pop_only();

            if op.groupby_exprs.is_empty() {
                <SimpleAggExecutor as ExecutorNode<'a, T>>::into_executor(
                    (op, input),
                    arena,
                    cache,
                    transaction,
                )
            } else if op.is_distinct
                && op.agg_calls.is_empty()
                && matches!(
                    physical_option,
                    Some(PhysicalOption {
                        plan: PlanImpl::StreamDistinct,
                        ..
                    })
                )
            {
                <StreamDistinctExecutor as ExecutorNode<'a, T>>::into_executor(
                    (op, input),
                    arena,
                    cache,
                    transaction,
                )
            } else {
                <HashAggExecutor as ExecutorNode<'a, T>>::into_executor(
                    (op, input),
                    arena,
                    cache,
                    transaction,
                )
            }
        }
        Operator::Filter(op) => <Filter as ExecutorNode<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            cache,
            transaction,
        ),
        Operator::ScalarApply(op) => {
            let (left, right) = childrens.pop_twins();
            <ScalarApply as ExecutorNode<'a, T>>::into_executor(
                (op, left, right),
                arena,
                cache,
                transaction,
            )
        }
        Operator::MarkApply(op) => {
            let (left, right) = childrens.pop_twins();
            <MarkApply as ExecutorNode<'a, T>>::into_executor(
                (op, left, right),
                arena,
                cache,
                transaction,
            )
        }
        Operator::Join(op) => {
            let use_hash_join = matches!(
                &op.on,
                JoinCondition::On { on, .. } if !on.is_empty()
            ) && matches!(
                physical_option,
                Some(PhysicalOption {
                    plan: PlanImpl::HashJoin,
                    ..
                })
            );
            let (left, right) = childrens.pop_twins();

            if use_hash_join {
                <HashJoin as ExecutorNode<'a, T>>::into_executor(
                    (op, left, right),
                    arena,
                    cache,
                    transaction,
                )
            } else {
                <NestedLoopJoin as ExecutorNode<'a, T>>::into_executor(
                    (op, left, right),
                    arena,
                    cache,
                    transaction,
                )
            }
        }
        Operator::Project(op) => <Projection as ExecutorNode<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            cache,
            transaction,
        ),
        Operator::ScalarSubquery(op) => <ScalarSubquery as ExecutorNode<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            cache,
            transaction,
        ),
        Operator::TableScan(op) => {
            if let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(index_info),
                ..
            }) = physical_option
            {
                if let Some(lookup) = index_info.lookup.clone() {
                    return <IndexScan<'a, T> as ExecutorNode<'a, T>>::into_executor(
                        (
                            op,
                            index_info.meta.clone(),
                            lookup,
                            index_info.covered_deserializers.clone(),
                            index_info.cover_mapping.clone(),
                        ),
                        arena,
                        cache,
                        transaction,
                    );
                }
            }

            <SeqScan<'a, T> as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::FunctionScan(op) => {
            <FunctionScan as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::Sort(op) => <Sort as ExecutorNode<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            cache,
            transaction,
        ),
        Operator::Limit(op) => <Limit as ExecutorNode<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            cache,
            transaction,
        ),
        Operator::TopK(op) => <TopK as ExecutorNode<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            cache,
            transaction,
        ),
        Operator::Values(op) => {
            <Values as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::ShowTable => <ShowTables<'a, T> as ExecutorNode<'a, T>>::into_executor(
            ShowTables { metas: None },
            arena,
            cache,
            transaction,
        ),
        Operator::ShowView => <ShowViews<'a, T> as ExecutorNode<'a, T>>::into_executor(
            ShowViews { metas: None },
            arena,
            cache,
            transaction,
        ),
        Operator::Explain => <Explain as ExecutorNode<'a, T>>::into_executor(
            childrens.pop_only(),
            arena,
            cache,
            transaction,
        ),
        Operator::Describe(op) => {
            <Describe as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::Union(_) => <Union as ExecutorNode<'a, T>>::into_executor(
            childrens.pop_twins(),
            arena,
            cache,
            transaction,
        ),
        Operator::SetMembership(op) => {
            let (left, right) = childrens.pop_twins();
            <SetMembership as ExecutorNode<'a, T>>::into_executor(
                (op.kind, left, right),
                arena,
                cache,
                transaction,
            )
        }
        _ => unreachable!(),
    }
}

pub(crate) fn build_write<'a, T>(
    arena: &mut ExecArena<'a, T>,
    plan: LogicalPlan,
    context: WriteExecutionContext<'a>,
    transaction: &'a mut T,
) -> ExecId
where
    T: Transaction + 'a,
{
    let cache = context.read();
    let root = build_write_inner(arena, plan, cache, transaction);
    arena.init_context_mut(context, transaction);
    root
}

pub(crate) fn build_write_read_context<'a, T, C>(
    arena: &mut ExecArena<'a, T>,
    plan: LogicalPlan,
    context: C,
    transaction: &'a mut T,
) -> ExecId
where
    T: Transaction + 'a,
    C: IntoReadExecutionContext<'a>,
{
    let cache = context.into_read_execution_context();
    arena.init_context(cache, transaction);
    build_write_inner(arena, plan, cache, transaction)
}

fn build_write_inner<'a, T: Transaction + 'a>(
    arena: &mut ExecArena<'a, T>,
    plan: LogicalPlan,
    cache: ReadExecutionContext<'_>,
    transaction_ref: &T,
) -> ExecId {
    let LogicalPlan {
        operator,
        childrens,
        physical_option,
        _output_schema_ref,
    } = plan;

    match operator {
        Operator::Insert(op) => {
            let input = childrens.pop_only();

            <Insert as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Update(op) => {
            let input = childrens.pop_only();

            <Update as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Delete(op) => {
            let input = childrens.pop_only();

            <Delete as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction_ref,
            )
        }
        Operator::AddColumn(op) => {
            <AddColumn as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::ChangeColumn(op) => {
            <ChangeColumn as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::DropColumn(op) => {
            <DropColumn as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::CreateTable(op) => {
            <CreateTable as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::CreateIndex(op) => {
            let input = childrens.pop_only();

            <CreateIndex as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction_ref,
            )
        }
        Operator::CreateView(op) => {
            <CreateView as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::DropTable(op) => {
            <DropTable as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::DropView(op) => {
            <DropView as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::DropIndex(op) => {
            <DropIndex as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::Truncate(op) => {
            <Truncate as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::CopyFromFile(op) => {
            <CopyFromFile as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction_ref)
        }
        Operator::CopyToFile(op) => {
            let input = childrens.pop_only();

            <CopyToFile as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Analyze(op) => {
            let input = childrens.pop_only();

            <Analyze as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction_ref,
            )
        }
        operator => {
            let plan = LogicalPlan {
                operator,
                childrens,
                physical_option,
                _output_schema_ref,
            };
            build_read(arena, plan, cache, transaction_ref)
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_utils {
    use super::*;

    static EMPTY_SCALA_FUNCTIONS: std::sync::LazyLock<ScalaFunctions> =
        std::sync::LazyLock::new(ScalaFunctions::default);
    static EMPTY_TABLE_FUNCTIONS: std::sync::LazyLock<TableFunctions> =
        std::sync::LazyLock::new(TableFunctions::default);

    impl<'a> IntoReadExecutionContext<'a> for (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache) {
        fn into_read_execution_context(self) -> ReadExecutionContext<'a> {
            ReadExecutionContext::new(
                self.0,
                self.1,
                self.2,
                &EMPTY_SCALA_FUNCTIONS,
                &EMPTY_TABLE_FUNCTIONS,
            )
        }
    }

    pub(crate) fn execute<'a, T, E>(
        executor: E,
        cache: impl IntoReadExecutionContext<'a>,
        transaction: &'a T,
    ) -> Executor<'a, T>
    where
        T: Transaction + 'a,
        E: ReadExecutor<'a, T>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::default();
        arena.init_context(cache, transaction);
        let root = executor.into_executor(&mut arena, cache, transaction);
        Executor::new(arena, root)
    }

    pub(crate) fn execute_mut<'a, T, E>(
        executor: E,
        cache: impl IntoReadExecutionContext<'a>,
        transaction: &'a T,
    ) -> Executor<'a, T>
    where
        T: Transaction + 'a,
        E: WriteExecutor<'a, T>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::default();
        arena.init_context(cache, transaction);
        let root = executor.into_executor(&mut arena, cache, transaction);
        Executor::new(arena, root)
    }

    pub(crate) fn execute_input<'a, T, E>(
        input: E::Input,
        cache: impl IntoReadExecutionContext<'a>,
        transaction: &'a T,
    ) -> Executor<'a, T>
    where
        T: Transaction + 'a,
        E: ExecutorNode<'a, T>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::default();
        arena.init_context(cache, transaction);
        let root = E::into_executor(input, &mut arena, cache, transaction);
        Executor::new(arena, root)
    }

    #[allow(dead_code)]
    pub(crate) fn execute_input_mut<'a, T, E>(
        input: E::Input,
        cache: impl IntoReadExecutionContext<'a>,
        transaction: &'a T,
    ) -> Executor<'a, T>
    where
        T: Transaction + 'a,
        E: ExecutorNode<'a, T>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::default();
        arena.init_context(cache, transaction);
        let root = E::into_executor(input, &mut arena, cache, transaction);
        Executor::new(arena, root)
    }

    pub fn try_collect<T: Transaction>(
        executor: Executor<'_, T>,
    ) -> Result<Vec<Tuple>, DatabaseError> {
        let mut executor = executor;
        let mut tuples = Vec::new();

        while let Some(tuple) = executor.next_tuple()? {
            tuples.push(tuple.clone());
        }
        Ok(tuples)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(unused_imports)]
pub(crate) use test_utils::{execute, execute_input, execute_input_mut, execute_mut, try_collect};
