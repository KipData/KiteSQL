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
mod ddl_apply;
pub(crate) mod dml;
pub(crate) mod dql;

pub(crate) use ddl_apply::DDLApply;

use self::ddl::add_column::AddColumn;
use self::ddl::change_column::ChangeColumn;
use self::dql::join::nested_loop_join::NestedLoopJoin;
use self::dql::mark_apply::MarkApply;
use self::dql::scalar_apply::ScalarApply;
use crate::catalog::{view::View, ColumnCatalog, TableCatalog, TableName};
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
#[cfg(feature = "copy")]
use crate::execution::dml::copy_from_file::CopyFromFile;
#[cfg(feature = "copy")]
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
use crate::expression::ScalarExpression;
use crate::optimizer::core::statistics_meta::StatisticsMeta;
use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl};
use crate::planner::{LogicalPlan, PlanArena};
use crate::storage::table_codec::TableCodec;
use crate::storage::{
    IndexIter, IndexRanges, Iter, StatisticsMetaCache, TableCache, TableIter, Transaction,
    TupleIter, ViewCache, ViewIter,
};
use crate::types::index::{Index, IndexId, IndexMetaRef, IndexType, RuntimeIndexProbe};
use crate::types::serialize::TupleValueSerializableImpl;
use crate::types::tuple::{Tuple, TupleId};
use crate::types::value::DataValue;
use crate::types::{ColumnId, LogicalType};

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
    runtime: ExecuteRuntime<'a, T>,
    root: ExecId,
}

impl<'a, T: Transaction + 'a> Executor<'a, T> {
    pub(crate) fn new(runtime: ExecuteRuntime<'a, T>, root: ExecId) -> Self {
        Self { runtime, root }
    }

    pub(crate) fn next_tuple(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<Option<&Tuple>, DatabaseError> {
        if !self.runtime.next_tuple(self.root, plan_arena)? {
            return Ok(None);
        }
        Ok(Some(self.runtime.result_tuple()))
    }

    pub(crate) fn take_ddl_apply(&mut self) -> Vec<DDLApply> {
        self.runtime.take_ddl_apply()
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ExecNode {
    AddColumn(AddColumn),
    Analyze(Analyze),
    ChangeColumn(ChangeColumn),
    #[cfg(feature = "copy")]
    CopyFromFile(CopyFromFile),
    #[cfg(feature = "copy")]
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
    IndexScan(IndexScan),
    Insert(Insert),
    Limit(Limit),
    MarkApply(MarkApply),
    NestedLoopJoin(NestedLoopJoin),
    Projection(Projection),
    ScalarApply(ScalarApply),
    ScalarSubquery(ScalarSubquery),
    SetMembership(SetMembership),
    SeqScan(SeqScan),
    ShowTables(ShowTables),
    ShowViews(ShowViews),
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

pub(crate) trait ExecutorNode<'a>: Sized {
    fn next_tuple(
        &mut self,
        runtime: &mut dyn ExecRuntime<'a>,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<(), DatabaseError>;
}

pub(crate) type RuntimeCursorId = usize;
pub(crate) type TupleValueSerializerIter<'b> =
    Box<dyn Iterator<Item = TupleValueSerializableImpl> + 'b>;
pub(crate) type TupleValueSerializerFactory<'b> = dyn FnMut() -> TupleValueSerializerIter<'b> + 'b;

pub(crate) trait ExecRuntime<'a> {
    fn next_tuple(
        &mut self,
        id: ExecId,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<bool, DatabaseError>;

    fn result_tuple(&self) -> &Tuple;

    fn result_tuple_mut(&mut self) -> &mut Tuple;

    fn resume(&mut self);

    fn finish(&mut self);

    fn produce_tuple(&mut self, tuple: Tuple);

    fn push_ddl_apply(&mut self, apply: DDLApply);

    fn project_tuple(&mut self, exprs: &[ScalarExpression]) -> Result<(), DatabaseError>;

    fn push_runtime_probe(&mut self, value: RuntimeIndexProbe);

    fn pop_runtime_probe(&mut self) -> RuntimeIndexProbe;

    fn runtime_probe_depth(&self) -> usize;

    fn read_context(&self) -> ReadExecutionContext<'a>;

    fn build_read_plan(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        plan: LogicalPlan,
        cache: ReadExecutionContext<'_>,
    ) -> ExecId;

    fn open_seq_scan(
        &mut self,
        plan_arena: &PlanArena<'a>,
        op: TableScanOperator,
    ) -> Result<RuntimeCursorId, DatabaseError>;

    #[allow(clippy::too_many_arguments)]
    fn open_index_scan(
        &mut self,
        plan_arena: &PlanArena<'a>,
        op: TableScanOperator,
        index_by: IndexMetaRef,
        ranges: IndexRanges,
        covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
        cover_mapping: Option<Vec<usize>>,
    ) -> Result<RuntimeCursorId, DatabaseError>;

    fn next_scan_tuple(&mut self, id: RuntimeCursorId) -> Result<bool, DatabaseError>;

    fn open_table_iter(&mut self) -> Result<RuntimeCursorId, DatabaseError>;

    fn next_table_name(&mut self, id: RuntimeCursorId) -> Result<Option<String>, DatabaseError>;

    fn open_view_iter(
        &mut self,
        plan_arena: &PlanArena<'a>,
    ) -> Result<RuntimeCursorId, DatabaseError>;

    fn next_view_name(&mut self, id: RuntimeCursorId) -> Result<Option<String>, DatabaseError>;

    fn transaction_table(
        &self,
        table_name: crate::catalog::TableName,
    ) -> Result<Option<&'a crate::catalog::TableCatalog>, DatabaseError>;

    fn transaction_add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError>;

    fn transaction_del_index(
        &mut self,
        table_name: &str,
        index: &Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError>;

    fn transaction_append_tuple(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        serializers: &[TupleValueSerializableImpl],
        is_overwrite: bool,
    ) -> Result<(), DatabaseError>;

    fn transaction_remove_tuple(
        &mut self,
        table_name: &str,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError>;

    fn transaction_create_table(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<Option<TableCatalog>, DatabaseError>;

    fn transaction_drop_table(
        &mut self,
        table_name: TableName,
        if_exists: bool,
    ) -> Result<bool, DatabaseError>;

    fn transaction_create_view(
        &mut self,
        plan_arena: &PlanArena<'a>,
        view: View,
        or_replace: bool,
    ) -> Result<View, DatabaseError>;

    fn transaction_drop_view(
        &mut self,
        view_name: TableName,
        if_exists: bool,
    ) -> Result<bool, DatabaseError>;

    fn transaction_add_index_meta(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        index_name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<(TableCatalog, IndexId), DatabaseError>;

    fn transaction_drop_index(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: TableName,
        index_name: &str,
        if_exists: bool,
    ) -> Result<Option<(TableCatalog, IndexId)>, DatabaseError>;

    fn transaction_drop_data(&mut self, table_name: &str) -> Result<(), DatabaseError>;

    fn transaction_add_column(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<(TableCatalog, ColumnId), DatabaseError>;

    #[allow(clippy::too_many_arguments)]
    fn transaction_change_column(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        old_column_name: &str,
        new_column_name: &str,
        new_data_type: &LogicalType,
        default_change: &DefaultChange,
        not_null_change: &NotNullChange,
    ) -> Result<TableCatalog, DatabaseError>;

    fn transaction_drop_column(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        column_name: &str,
    ) -> Result<TableCatalog, DatabaseError>;

    #[allow(clippy::too_many_arguments)]
    fn transaction_rewrite_table_in_batches(
        &mut self,
        table_name: &TableName,
        pk_ty: &LogicalType,
        old_values_len: usize,
        old_deserializers: &mut TupleValueSerializerFactory<'_>,
        new_serializers: &mut TupleValueSerializerFactory<'_>,
        rewrite: &mut dyn FnMut(&mut Tuple) -> Result<(), DatabaseError>,
        after_write: &mut dyn FnMut(&mut dyn ExecRuntime<'a>, &Tuple) -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError>;

    fn transaction_visit_table_in_batches(
        &mut self,
        table_name: &TableName,
        pk_ty: &LogicalType,
        old_values_len: usize,
        old_deserializers: &mut TupleValueSerializerFactory<'_>,
        visit: &mut dyn FnMut(&Tuple) -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError>;

    fn transaction_save_statistics_meta(
        &mut self,
        table_name: &TableName,
        meta: StatisticsMeta,
    ) -> Result<(), DatabaseError>;
}

pub(crate) struct ExecArena {
    nodes: Vec<ExecNode>,
    result: ExecResult,
    projection_tmp: Vec<DataValue>,
}

enum RuntimeCursor<'a, T: Transaction + 'a> {
    SeqScan(TupleIter<'a, T>),
    IndexScan(IndexIter<'a, T>),
    Tables(TableIter<'a, T>),
    Views(ViewIter<'a, T>),
}

pub(crate) struct ExecuteRuntime<'a, T: Transaction + 'a> {
    arena: ExecArena,
    table_codec: TableCodec,
    context: Option<ExecutionContext<'a>>,
    transaction: *mut T,
    runtime_probe_stack: Vec<RuntimeIndexProbe>,
    ddl_apply: Vec<DDLApply>,
    cursors: Vec<RuntimeCursor<'a, T>>,
}

impl ExecArena {
    pub(crate) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            result: ExecResult::default(),
            projection_tmp: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, node: ExecNode) -> ExecId {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }
}

impl<'a, T: Transaction + 'a> ExecuteRuntime<'a, T> {
    pub(crate) fn new(arena: ExecArena) -> Self {
        Self {
            arena,
            table_codec: TableCodec::default(),
            context: None,
            transaction: std::ptr::null_mut(),
            runtime_probe_stack: Vec::new(),
            ddl_apply: Vec::new(),
            cursors: Vec::new(),
        }
    }

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

    pub(crate) fn take_ddl_apply(&mut self) -> Vec<DDLApply> {
        std::mem::take(&mut self.ddl_apply)
    }
}

impl<'a, T: Transaction + 'a> ExecRuntime<'a> for ExecuteRuntime<'a, T> {
    fn next_tuple(
        &mut self,
        id: ExecId,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<bool, DatabaseError> {
        self.arena.result.status = None;
        let mut node = std::mem::replace(&mut self.arena.nodes[id], ExecNode::Empty);
        let result = match &mut node {
            ExecNode::AddColumn(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Analyze(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::ChangeColumn(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            #[cfg(feature = "copy")]
            ExecNode::CopyFromFile(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            #[cfg(feature = "copy")]
            ExecNode::CopyToFile(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::CreateIndex(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::CreateTable(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::CreateView(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Delete(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Describe(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::DropColumn(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::DropIndex(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::DropTable(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::DropView(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Dummy(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Explain(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Filter(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::FunctionScan(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::HashAgg(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::HashJoin(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::IndexScan(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Insert(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Limit(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::MarkApply(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::NestedLoopJoin(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Projection(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::ScalarApply(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::ScalarSubquery(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::SetMembership(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::SeqScan(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::ShowTables(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::ShowViews(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::SimpleAgg(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Sort(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::StreamDistinct(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::TopK(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Truncate(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Union(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Update(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Values(exec) => ExecutorNode::next_tuple(exec, self, plan_arena),
            ExecNode::Empty => unreachable!("executor node re-entered while active"),
        };
        self.arena.nodes[id] = node;
        result?;

        match self.arena.result.status.unwrap_or(ExecStatus::End) {
            ExecStatus::Continue => Ok(true),
            ExecStatus::End => Ok(false),
        }
    }

    #[inline]
    fn result_tuple(&self) -> &Tuple {
        &self.arena.result.tuple
    }

    #[inline]
    fn result_tuple_mut(&mut self) -> &mut Tuple {
        &mut self.arena.result.tuple
    }

    #[inline]
    fn resume(&mut self) {
        self.arena.result.status = Some(ExecStatus::Continue);
    }

    #[inline]
    fn finish(&mut self) {
        self.arena.result.status = Some(ExecStatus::End);
    }

    #[inline]
    fn produce_tuple(&mut self, tuple: Tuple) {
        self.arena.result.tuple = tuple;
        self.resume();
    }

    #[inline]
    fn push_ddl_apply(&mut self, apply: DDLApply) {
        self.ddl_apply.push(apply);
    }

    #[inline]
    fn project_tuple(&mut self, exprs: &[ScalarExpression]) -> Result<(), DatabaseError> {
        let ExecArena {
            result,
            projection_tmp,
            ..
        } = &mut self.arena;
        let ret: Result<(), DatabaseError> = (|| {
            projection_tmp.clear();
            projection_tmp.reserve(exprs.len());
            for expr in exprs {
                projection_tmp.push(expr.eval(Some(&result.tuple))?);
            }
            Ok(())
        })();
        ret?;
        std::mem::swap(&mut result.tuple.values, projection_tmp);
        Ok(())
    }

    #[inline]
    fn push_runtime_probe(&mut self, value: RuntimeIndexProbe) {
        self.runtime_probe_stack.push(value);
    }

    #[inline]
    fn pop_runtime_probe(&mut self) -> RuntimeIndexProbe {
        self.runtime_probe_stack
            .pop()
            .expect("runtime probe scope initialized")
    }

    #[inline]
    fn runtime_probe_depth(&self) -> usize {
        self.runtime_probe_stack.len()
    }

    #[inline]
    fn read_context(&self) -> ReadExecutionContext<'a> {
        match self
            .context
            .as_ref()
            .expect("execution arena context initialized")
        {
            ExecutionContext::Read(context) => *context,
            ExecutionContext::Write(context) => unsafe {
                ReadExecutionContext::new(
                    &*(&*context.table_cache as *const TableCache),
                    &*(&*context.view_cache as *const ViewCache),
                    &*(&*context.meta_cache as *const StatisticsMetaCache),
                    context.scala_functions,
                    context.table_functions,
                )
            },
        }
    }

    fn build_read_plan(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        plan: LogicalPlan,
        cache: ReadExecutionContext<'_>,
    ) -> ExecId {
        let transaction = unsafe { &*self.transaction };
        build_read::<T>(&mut self.arena, plan_arena, plan, cache, transaction)
    }

    fn open_seq_scan(
        &mut self,
        plan_arena: &PlanArena<'a>,
        TableScanOperator {
            table_name,
            columns,
            limit,
            with_pk,
            ..
        }: TableScanOperator,
    ) -> Result<RuntimeCursorId, DatabaseError> {
        let transaction = unsafe { &*self.transaction };
        let context = self.read_context();
        let cursor = transaction.read(
            &mut self.table_codec,
            plan_arena,
            context.table_cache,
            table_name,
            limit,
            columns,
            with_pk,
        )?;
        let id = self.cursors.len();
        self.cursors.push(RuntimeCursor::SeqScan(cursor));
        Ok(id)
    }

    fn open_index_scan(
        &mut self,
        plan_arena: &PlanArena<'a>,
        TableScanOperator {
            table_name,
            columns,
            limit,
            with_pk,
            ..
        }: TableScanOperator,
        index_by: IndexMetaRef,
        ranges: IndexRanges,
        covered_deserializers: Option<Vec<TupleValueSerializableImpl>>,
        cover_mapping: Option<Vec<usize>>,
    ) -> Result<RuntimeCursorId, DatabaseError> {
        let transaction = unsafe { &*self.transaction };
        let context = self.read_context();
        let cursor = transaction.read_by_index(
            context.table_cache,
            plan_arena,
            table_name,
            limit,
            columns,
            index_by,
            ranges,
            with_pk,
            covered_deserializers,
            cover_mapping,
        )?;
        let id = self.cursors.len();
        self.cursors.push(RuntimeCursor::IndexScan(cursor));
        Ok(id)
    }

    fn next_scan_tuple(&mut self, id: RuntimeCursorId) -> Result<bool, DatabaseError> {
        match self
            .cursors
            .get_mut(id)
            .expect("runtime cursor initialized")
        {
            RuntimeCursor::SeqScan(cursor) => {
                cursor.next_tuple_into(&mut self.table_codec, &mut self.arena.result.tuple)
            }
            RuntimeCursor::IndexScan(cursor) => {
                cursor.next_tuple_into(&mut self.table_codec, &mut self.arena.result.tuple)
            }
            RuntimeCursor::Tables(_) | RuntimeCursor::Views(_) => {
                unreachable!("runtime cursor is not a tuple scan")
            }
        }
    }

    fn open_table_iter(&mut self) -> Result<RuntimeCursorId, DatabaseError> {
        let transaction = unsafe { &*self.transaction };
        let cursor = transaction.tables(&mut self.table_codec)?;
        let id = self.cursors.len();
        self.cursors.push(RuntimeCursor::Tables(cursor));
        Ok(id)
    }

    fn next_table_name(&mut self, id: RuntimeCursorId) -> Result<Option<String>, DatabaseError> {
        match self
            .cursors
            .get_mut(id)
            .expect("runtime cursor initialized")
        {
            RuntimeCursor::Tables(cursor) => {
                Ok(cursor.try_next()?.map(|table| table.table_name.to_string()))
            }
            RuntimeCursor::SeqScan(_) | RuntimeCursor::IndexScan(_) | RuntimeCursor::Views(_) => {
                unreachable!("runtime cursor is not a table iterator")
            }
        }
    }

    fn open_view_iter(
        &mut self,
        plan_arena: &PlanArena<'a>,
    ) -> Result<RuntimeCursorId, DatabaseError> {
        let transaction = unsafe { &*self.transaction };
        let context = self.read_context();
        let cursor = transaction.views(
            &mut self.table_codec,
            context.table_cache(),
            plan_arena.table_arena_cell(),
            context.scala_functions(),
            context.table_functions(),
        )?;
        let id = self.cursors.len();
        self.cursors.push(RuntimeCursor::Views(cursor));
        Ok(id)
    }

    fn next_view_name(&mut self, id: RuntimeCursorId) -> Result<Option<String>, DatabaseError> {
        match self
            .cursors
            .get_mut(id)
            .expect("runtime cursor initialized")
        {
            RuntimeCursor::Views(cursor) => {
                Ok(cursor.try_next()?.map(|view| view.name.to_string()))
            }
            RuntimeCursor::SeqScan(_) | RuntimeCursor::IndexScan(_) | RuntimeCursor::Tables(_) => {
                unreachable!("runtime cursor is not a view iterator")
            }
        }
    }

    fn transaction_table(
        &self,
        table_name: crate::catalog::TableName,
    ) -> Result<Option<&'a crate::catalog::TableCatalog>, DatabaseError> {
        let transaction = unsafe { &*self.transaction };
        let table_cache = self.read_context().table_cache;
        transaction.table(table_cache, table_name)
    }

    fn transaction_add_index(
        &mut self,
        table_name: &str,
        index: Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.add_index(table_codec, table_name, index, tuple_id)
    }

    fn transaction_del_index(
        &mut self,
        table_name: &str,
        index: &Index,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.del_index(table_codec, table_name, index, tuple_id)
    }

    fn transaction_append_tuple(
        &mut self,
        table_name: &str,
        tuple: Tuple,
        serializers: &[TupleValueSerializableImpl],
        is_overwrite: bool,
    ) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.append_tuple(table_codec, table_name, tuple, serializers, is_overwrite)
    }

    fn transaction_remove_tuple(
        &mut self,
        table_name: &str,
        tuple_id: &TupleId,
    ) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.remove_tuple(table_codec, table_name, tuple_id)
    }

    fn transaction_create_table(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: TableName,
        columns: Vec<ColumnCatalog>,
        if_not_exists: bool,
    ) -> Result<Option<TableCatalog>, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.create_table(table_codec, plan_arena, table_name, columns, if_not_exists)
    }

    fn transaction_drop_table(
        &mut self,
        table_name: TableName,
        if_exists: bool,
    ) -> Result<bool, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.drop_table(table_codec, table_name, if_exists)
    }

    fn transaction_create_view(
        &mut self,
        plan_arena: &PlanArena<'a>,
        view: View,
        or_replace: bool,
    ) -> Result<View, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.create_view(table_codec, plan_arena, view, or_replace)
    }

    fn transaction_drop_view(
        &mut self,
        view_name: TableName,
        if_exists: bool,
    ) -> Result<bool, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.drop_view(table_codec, view_name, if_exists)
    }

    fn transaction_add_index_meta(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        index_name: String,
        column_ids: Vec<ColumnId>,
        ty: IndexType,
    ) -> Result<(TableCatalog, IndexId), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.add_index_meta(
            table_codec,
            plan_arena,
            table_name,
            index_name,
            column_ids,
            ty,
        )
    }

    fn transaction_drop_index(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: TableName,
        index_name: &str,
        if_exists: bool,
    ) -> Result<Option<(TableCatalog, IndexId)>, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.drop_index(table_codec, plan_arena, table_name, index_name, if_exists)
    }

    fn transaction_drop_data(&mut self, table_name: &str) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.drop_data(table_codec, table_name)
    }

    fn transaction_add_column(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        column: &ColumnCatalog,
        if_not_exists: bool,
    ) -> Result<(TableCatalog, ColumnId), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.add_column(table_codec, plan_arena, table_name, column, if_not_exists)
    }

    fn transaction_change_column(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        old_column_name: &str,
        new_column_name: &str,
        new_data_type: &LogicalType,
        default_change: &DefaultChange,
        not_null_change: &NotNullChange,
    ) -> Result<TableCatalog, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.change_column(
            table_codec,
            plan_arena,
            table_name,
            old_column_name,
            new_column_name,
            new_data_type,
            default_change,
            not_null_change,
        )
    }

    fn transaction_drop_column(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
        table_name: &TableName,
        column_name: &str,
    ) -> Result<TableCatalog, DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.drop_column(table_codec, plan_arena, table_name, column_name)
    }

    fn transaction_rewrite_table_in_batches(
        &mut self,
        table_name: &TableName,
        pk_ty: &LogicalType,
        old_values_len: usize,
        old_deserializers: &mut TupleValueSerializerFactory<'_>,
        new_serializers: &mut TupleValueSerializerFactory<'_>,
        rewrite: &mut dyn FnMut(&mut Tuple) -> Result<(), DatabaseError>,
        after_write: &mut dyn FnMut(&mut dyn ExecRuntime<'a>, &Tuple) -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError> {
        let runtime = self as *mut Self;
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        crate::execution::ddl::rewrite_table_in_batches(
            transaction,
            table_codec,
            table_name,
            pk_ty,
            old_values_len,
            old_deserializers,
            new_serializers,
            rewrite,
            |_, _, tuple| unsafe { after_write(&mut *runtime, tuple) },
        )
    }

    fn transaction_visit_table_in_batches(
        &mut self,
        table_name: &TableName,
        pk_ty: &LogicalType,
        old_values_len: usize,
        old_deserializers: &mut TupleValueSerializerFactory<'_>,
        visit: &mut dyn FnMut(&Tuple) -> Result<(), DatabaseError>,
    ) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        crate::execution::ddl::visit_table_in_batches(
            transaction,
            table_codec,
            table_name,
            pk_ty,
            old_values_len,
            old_deserializers,
            visit,
        )
    }

    fn transaction_save_statistics_meta(
        &mut self,
        table_name: &TableName,
        meta: StatisticsMeta,
    ) -> Result<(), DatabaseError> {
        let transaction = unsafe { &mut *self.transaction };
        let table_codec = &mut self.table_codec;
        transaction.save_statistics_meta(table_codec, table_name, meta)
    }
}

pub(crate) trait ReadExecutor<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) trait WriteExecutor<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena,
        plan_arena: &mut PlanArena<'a>,
        cache: ReadExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) fn build_read<'a, T>(
    arena: &mut ExecArena,
    plan_arena: &mut PlanArena<'a>,
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
        ..
    } = plan;

    match operator {
        Operator::Dummy => <Dummy as ReadExecutor<'a, T>>::into_executor(
            Dummy::default(),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Aggregate(op) => {
            let input = childrens.pop_only();

            if op.groupby_exprs.is_empty() {
                <SimpleAggExecutor as ReadExecutor<'a, T>>::into_executor(
                    (op, input),
                    arena,
                    plan_arena,
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
                <StreamDistinctExecutor as ReadExecutor<'a, T>>::into_executor(
                    (op, input),
                    arena,
                    plan_arena,
                    cache,
                    transaction,
                )
            } else {
                <HashAggExecutor as ReadExecutor<'a, T>>::into_executor(
                    (op, input),
                    arena,
                    plan_arena,
                    cache,
                    transaction,
                )
            }
        }
        Operator::Filter(op) => <Filter as ReadExecutor<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::ScalarApply(op) => {
            let (left, right) = childrens.pop_twins();
            <ScalarApply as ReadExecutor<'a, T>>::into_executor(
                (op, left, right),
                arena,
                plan_arena,
                cache,
                transaction,
            )
        }
        Operator::MarkApply(op) => {
            let (left, right) = childrens.pop_twins();
            <MarkApply as ReadExecutor<'a, T>>::into_executor(
                (op, left, right),
                arena,
                plan_arena,
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
                <HashJoin as ReadExecutor<'a, T>>::into_executor(
                    (op, left, right),
                    arena,
                    plan_arena,
                    cache,
                    transaction,
                )
            } else {
                <NestedLoopJoin as ReadExecutor<'a, T>>::into_executor(
                    (op, left, right),
                    arena,
                    plan_arena,
                    cache,
                    transaction,
                )
            }
        }
        Operator::Project(op) => <Projection as ReadExecutor<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::ScalarSubquery(op) => <ScalarSubquery as ReadExecutor<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            plan_arena,
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
                    return <IndexScan as ReadExecutor<'a, T>>::into_executor(
                        (
                            op,
                            index_info.meta.clone(),
                            lookup,
                            index_info.covered_deserializers.clone(),
                            index_info.cover_mapping.clone(),
                        ),
                        arena,
                        plan_arena,
                        cache,
                        transaction,
                    );
                }
            }

            <SeqScan as ReadExecutor<'a, T>>::into_executor(
                op,
                arena,
                plan_arena,
                cache,
                transaction,
            )
        }
        Operator::FunctionScan(op) => <FunctionScan as ReadExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Sort(op) => <Sort as ReadExecutor<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Limit(op) => <Limit as ReadExecutor<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::TopK(op) => <TopK as ReadExecutor<'a, T>>::into_executor(
            (op, childrens.pop_only()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Values(op) => <Values as ReadExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::ShowTable => <ShowTables as ReadExecutor<'a, T>>::into_executor(
            ShowTables { cursor: None },
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::ShowView => <ShowViews as ReadExecutor<'a, T>>::into_executor(
            ShowViews { cursor: None },
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Explain => <Explain as ReadExecutor<'a, T>>::into_executor(
            childrens.pop_only(),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Describe(op) => <Describe as ReadExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Union(_) => <Union as ReadExecutor<'a, T>>::into_executor(
            childrens.pop_twins(),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::SetMembership(op) => {
            let (left, right) = childrens.pop_twins();
            <SetMembership as ReadExecutor<'a, T>>::into_executor(
                (op.kind, left, right),
                arena,
                plan_arena,
                cache,
                transaction,
            )
        }
        _ => unreachable!(),
    }
}

pub(crate) fn build_write<'a, T>(
    arena: &mut ExecArena,
    plan_arena: &mut PlanArena<'a>,
    plan: LogicalPlan,
    context: &WriteExecutionContext<'a>,
    transaction: &T,
) -> ExecId
where
    T: Transaction + 'a,
{
    let cache = context.read();
    build_write_inner(arena, plan_arena, plan, cache, transaction)
}

pub(crate) fn build_write_read_context<'a, T, C>(
    arena: &mut ExecArena,
    plan_arena: &mut PlanArena<'a>,
    plan: LogicalPlan,
    context: C,
    transaction: &T,
) -> ExecId
where
    T: Transaction + 'a,
    C: IntoReadExecutionContext<'a>,
{
    let cache = context.into_read_execution_context();
    build_write_inner(arena, plan_arena, plan, cache, transaction)
}

fn build_write_inner<'a, T: Transaction + 'a>(
    arena: &mut ExecArena,
    plan_arena: &mut PlanArena<'a>,
    plan: LogicalPlan,
    cache: ReadExecutionContext<'_>,
    transaction_ref: &T,
) -> ExecId {
    let LogicalPlan {
        operator,
        childrens,
        physical_option,
        ..
    } = plan;

    match operator {
        Operator::Insert(op) => {
            let input = childrens.pop_only();

            <Insert as WriteExecutor<'a, T>>::into_executor(
                (op, input),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Update(op) => {
            let input = childrens.pop_only();

            <Update as WriteExecutor<'a, T>>::into_executor(
                (op, input),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Delete(op) => {
            let input = childrens.pop_only();

            <Delete as WriteExecutor<'a, T>>::into_executor(
                (op, input),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::AddColumn(op) => <AddColumn as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::ChangeColumn(op) => <ChangeColumn as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropColumn(op) => <DropColumn as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::CreateTable(op) => <CreateTable as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::CreateIndex(op) => {
            let input = childrens.pop_only();

            <CreateIndex as WriteExecutor<'a, T>>::into_executor(
                (op, input),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::CreateView(op) => <CreateView as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropTable(op) => <DropTable as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropView(op) => <DropView as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropIndex(op) => <DropIndex as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::Truncate(op) => <Truncate as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        #[cfg(feature = "copy")]
        Operator::CopyFromFile(op) => <CopyFromFile as WriteExecutor<'a, T>>::into_executor(
            op,
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        #[cfg(feature = "copy")]
        Operator::CopyToFile(op) => {
            let input = childrens.pop_only();

            <CopyToFile as WriteExecutor<'a, T>>::into_executor(
                (op, input),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Analyze(op) => {
            let input = childrens.pop_only();

            <Analyze as WriteExecutor<'a, T>>::into_executor(
                (op, input),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        operator => {
            let mut plan = LogicalPlan::new(operator, *childrens);
            plan.physical_option = physical_option;
            build_read(arena, plan_arena, plan, cache, transaction_ref)
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

    pub(crate) struct TestExecutor<'a, T: Transaction + 'a> {
        executor: Executor<'a, T>,
        plan_arena: PlanArena<'a>,
    }

    impl<'a, T: Transaction + 'a> TestExecutor<'a, T> {
        pub(crate) fn next_tuple(&mut self) -> Result<Option<&Tuple>, DatabaseError> {
            self.executor.next_tuple(&mut self.plan_arena)
        }
    }

    impl<T: Transaction> Iterator for TestExecutor<'_, T> {
        type Item = Result<Tuple, DatabaseError>;

        fn next(&mut self) -> Option<Self::Item> {
            match self.next_tuple() {
                Ok(Some(tuple)) => Some(Ok(tuple.clone())),
                Ok(None) => None,
                Err(err) => Some(Err(err)),
            }
        }
    }

    pub(crate) fn execute<'a, T, E>(
        executor: E,
        cache: impl IntoReadExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: ReadExecutor<'a, T, Input = E>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::new();
        let root = <E as ReadExecutor<'a, T>>::into_executor(
            executor,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        let mut runtime = ExecuteRuntime::new(arena);
        runtime.init_context(cache, transaction);
        TestExecutor {
            executor: Executor::new(runtime, root),
            plan_arena,
        }
    }

    pub(crate) fn execute_mut<'a, T, E>(
        executor: E,
        cache: impl IntoReadExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: WriteExecutor<'a, T, Input = E>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::new();
        let root = <E as WriteExecutor<'a, T>>::into_executor(
            executor,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        let mut runtime = ExecuteRuntime::new(arena);
        runtime.init_context(cache, transaction);
        TestExecutor {
            executor: Executor::new(runtime, root),
            plan_arena,
        }
    }

    pub(crate) fn execute_input<'a, T, E>(
        input: E::Input,
        cache: impl IntoReadExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: ReadExecutor<'a, T>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::new();
        let root = <E as ReadExecutor<'a, T>>::into_executor(
            input,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        let mut runtime = ExecuteRuntime::new(arena);
        runtime.init_context(cache, transaction);
        TestExecutor {
            executor: Executor::new(runtime, root),
            plan_arena,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn execute_input_mut<'a, T, E>(
        input: E::Input,
        cache: impl IntoReadExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: WriteExecutor<'a, T>,
    {
        let cache = cache.into_read_execution_context();
        let mut arena = ExecArena::new();
        let root = <E as WriteExecutor<'a, T>>::into_executor(
            input,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        let mut runtime = ExecuteRuntime::new(arena);
        runtime.init_context(cache, transaction);
        TestExecutor {
            executor: Executor::new(runtime, root),
            plan_arena,
        }
    }

    pub fn try_collect<T: Transaction>(
        executor: TestExecutor<'_, T>,
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
