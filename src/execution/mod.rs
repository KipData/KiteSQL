#[cfg(test)]
use crate::planner::PlanArena;
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
#[cfg(feature = "spill")]
pub(crate) mod spill;

pub(crate) use ddl_apply::DDLApply;

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
#[cfg(feature = "copy")]
use crate::execution::dml::copy_from_file::CopyFromFile;
#[cfg(feature = "copy")]
use crate::execution::dml::copy_to_file::CopyToFile;
use crate::execution::dml::delete::Delete;
use crate::execution::dml::insert::Insert;
use crate::execution::dml::update::Update;
use crate::execution::dql::aggregate::hash_agg::HashAggExecutor;
use crate::execution::dql::aggregate::simple_agg::SimpleAggExecutor;
use crate::execution::dql::aggregate::stream_agg::StreamAggExecutor;
use crate::execution::dql::aggregate::stream_distinct::StreamDistinctExecutor;
use crate::execution::dql::describe::Describe;
use crate::execution::dql::dummy::Dummy;
use crate::execution::dql::explain::Explain;
#[cfg(feature = "spill")]
use crate::execution::dql::external_sort::ExternalSort;
use crate::execution::dql::filter::Filter;
use crate::execution::dql::function_scan::FunctionScan;
use crate::execution::dql::index_scan::IndexScan;
use crate::execution::dql::join::hash_join::HashJoin;
use crate::execution::dql::limit::Limit;
use crate::execution::dql::projection::Projection;
use crate::execution::dql::recursive_cte::{RecursiveCte, RecursiveInput, RecursiveScan};
use crate::execution::dql::scalar_subquery::ScalarSubquery;
use crate::execution::dql::seq_scan::SeqScan;
use crate::execution::dql::set_membership::SetMembership;
use crate::execution::dql::show_table::ShowTables;
use crate::execution::dql::show_view::ShowViews;
use crate::execution::dql::sort::Sort;
use crate::execution::dql::top_k::TopK;
use crate::execution::dql::union::Union;
use crate::execution::dql::values::Values;
use crate::execution::dql::window::Window;
use crate::expression::ScalarExpression;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl};
use crate::planner::MetaArena;
use crate::planner::{ExprRef, LogicalPlan, PlanKeeper};
use crate::storage::table_codec::TableCodec;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::types::index::RuntimeIndexProbe;
use crate::types::tuple::{Tuple, TupleLike};
use crate::types::value::DataValue;

#[derive(Clone, Copy)]
pub(crate) struct ExecutionContext<'a> {
    table_cache: &'a TableCache,
    view_cache: &'a ViewCache,
    meta_cache: &'a StatisticsMetaCache,
    scala_functions: &'a ScalaFunctions,
    table_functions: &'a TableFunctions,
}

impl<'a> ExecutionContext<'a> {
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

    fn is_same_context(&self, other: ExecutionContext<'_>) -> bool {
        std::ptr::eq(self.table_cache, other.table_cache)
            && std::ptr::eq(self.view_cache, other.view_cache)
            && std::ptr::eq(self.meta_cache, other.meta_cache)
            && std::ptr::eq(self.scala_functions, other.scala_functions)
            && std::ptr::eq(self.table_functions, other.table_functions)
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

/// Resolves either an arena-backed expression or a direct scalar expression.
pub(crate) trait RewriteExpression {
    fn expression<'a>(&'a self, arena: &'a dyn MetaArena) -> &'a ScalarExpression;
}

impl RewriteExpression for ExprRef {
    fn expression<'a>(&'a self, arena: &'a dyn MetaArena) -> &'a ScalarExpression {
        arena.expression(*self)
    }
}

impl RewriteExpression for ScalarExpression {
    fn expression<'a>(&'a self, _arena: &'a dyn MetaArena) -> &'a ScalarExpression {
        self
    }
}

pub struct Executor<'a, T: Transaction + 'a> {
    arena: ExecArena<'a, T>,
    root: ExecId,
    // Never read: it only keeps the plan that `arena` borrows from alive, and must stay
    // declared after `arena` so the executors are dropped before the plan is freed.
    #[allow(dead_code)]
    keeper: PlanKeeper<'a>,
}

impl<'a, T: Transaction + 'a> Executor<'a, T> {
    pub(crate) fn new(arena: ExecArena<'a, T>, root: ExecId, keeper: PlanKeeper<'a>) -> Self {
        Self {
            arena,
            root,
            keeper,
        }
    }

    pub(crate) fn next_tuple(
        &mut self,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<Option<&mut Tuple>, DatabaseError> {
        if !self.arena.next_tuple(self.root, plan_arena)? {
            return Ok(None);
        }
        Ok(Some(self.arena.result_tuple_mut()))
    }

    pub(crate) fn take_ddl_apply(&mut self) -> Vec<DDLApply> {
        self.arena.take_ddl_apply()
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ExecNode<'a, T: Transaction + 'a> {
    AddColumn(AddColumn<'a>),
    Analyze(Analyze<'a>),
    ChangeColumn(ChangeColumn<'a>),
    #[cfg(feature = "copy")]
    CopyFromFile(CopyFromFile<'a>),
    #[cfg(feature = "copy")]
    CopyToFile(CopyToFile<'a>),
    CreateIndex(CreateIndex<'a>),
    CreateTable(CreateTable<'a>),
    CreateView(CreateView<'a>),
    Delete(Delete<'a>),
    Describe(Describe),
    DropColumn(DropColumn<'a>),
    DropIndex(DropIndex<'a>),
    DropTable(DropTable<'a>),
    DropView(DropView<'a>),
    Dummy(Dummy),
    Explain(Explain<'a>),
    #[cfg(feature = "spill")]
    ExternalSort(ExternalSort<'a>),
    Filter(Filter),
    FunctionScan(FunctionScan<'a>),
    HashAgg(HashAggExecutor<'a>),
    HashJoin(HashJoin),
    IndexScan(IndexScan<'a, T>),
    Insert(Insert<'a>),
    Limit(Limit),
    MarkApply(MarkApply<'a>),
    NestedLoopJoin(NestedLoopJoin<'a>),
    Projection(Projection<'a>),
    RecursiveCte(RecursiveCte<'a, T>),
    RecursiveScan(RecursiveScan),
    ScalarApply(ScalarApply),
    ScalarSubquery(ScalarSubquery),
    SetMembership(SetMembership),
    SeqScan(SeqScan<'a, T>),
    ShowTables(ShowTables<'a, T>),
    ShowViews(ShowViews<'a, T>),
    SimpleAgg(SimpleAggExecutor<'a>),
    Sort(Sort<'a>),
    StreamAgg(StreamAggExecutor<'a>),
    StreamDistinct(StreamDistinctExecutor<'a>),
    TopK(TopK<'a>),
    Truncate(Truncate<'a>),
    Union(Union),
    Update(Update<'a>),
    Values(Values<'a>),
    Window(Window<'a>),
}

pub(crate) trait ExecutorNode<'a, T: Transaction + 'a>: Sized {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError>;
}

impl<'a, T: Transaction + 'a> ExecNode<'a, T> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<(), DatabaseError> {
        match self {
            ExecNode::AddColumn(exec) => {
                <AddColumn as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Analyze(exec) => {
                <Analyze as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ChangeColumn(exec) => {
                <ChangeColumn as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            #[cfg(feature = "copy")]
            ExecNode::CopyFromFile(exec) => {
                <CopyFromFile as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            #[cfg(feature = "copy")]
            ExecNode::CopyToFile(exec) => {
                <CopyToFile as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::CreateIndex(exec) => {
                <CreateIndex as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::CreateTable(exec) => {
                <CreateTable as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::CreateView(exec) => {
                <CreateView as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Delete(exec) => {
                <Delete as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Describe(exec) => {
                <Describe as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::DropColumn(exec) => {
                <DropColumn as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::DropIndex(exec) => {
                <DropIndex as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::DropTable(exec) => {
                <DropTable as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::DropView(exec) => {
                <DropView as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Dummy(exec) => {
                <Dummy as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Explain(exec) => {
                <Explain<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            #[cfg(feature = "spill")]
            ExecNode::ExternalSort(exec) => {
                <ExternalSort<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Filter(exec) => {
                <Filter as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::FunctionScan(exec) => {
                <FunctionScan<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::HashAgg(exec) => {
                <HashAggExecutor<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::HashJoin(exec) => {
                <HashJoin as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::IndexScan(exec) => {
                <IndexScan<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Insert(exec) => {
                <Insert as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Limit(exec) => {
                <Limit as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::MarkApply(exec) => {
                <MarkApply<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::NestedLoopJoin(exec) => {
                <NestedLoopJoin<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Projection(exec) => {
                <Projection<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::RecursiveCte(exec) => {
                <RecursiveCte<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::RecursiveScan(exec) => {
                <RecursiveScan as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ScalarApply(exec) => {
                <ScalarApply as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ScalarSubquery(exec) => {
                <ScalarSubquery as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::SetMembership(exec) => {
                <SetMembership as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::SeqScan(exec) => {
                <SeqScan<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ShowTables(exec) => {
                <ShowTables<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ShowViews(exec) => {
                <ShowViews<'a, T> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::SimpleAgg(exec) => {
                <SimpleAggExecutor<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Sort(exec) => {
                <Sort<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::StreamAgg(exec) => {
                <StreamAggExecutor<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::StreamDistinct(exec) => {
                <StreamDistinctExecutor<'a> as ExecutorNode<'a, T>>::next_tuple(
                    exec, arena, plan_arena,
                )
            }
            ExecNode::TopK(exec) => {
                <TopK<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Truncate(exec) => {
                <Truncate as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Union(exec) => {
                <Union as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Update(exec) => {
                <Update<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Values(exec) => {
                <Values<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Window(exec) => {
                <Window<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
        }
    }
}

struct ExecNodes<'a, T: Transaction + 'a> {
    items: Vec<std::cell::RefCell<ExecNode<'a, T>>>,
    pos: ExecId,
    executing: usize,
}

impl<'a, T: Transaction + 'a> ExecNodes<'a, T> {
    fn position(&self) -> ExecId {
        self.pos
    }

    fn seek(&mut self, pos: ExecId) {
        assert!(pos <= self.items.len(), "node position out of bounds");
        self.pos = pos;
    }

    fn push(&mut self, node: ExecNode<'a, T>) -> ExecId {
        let id = self.pos;
        if id == self.items.len() {
            assert_eq!(self.executing, 0, "cannot grow nodes during execution");
            self.items.push(std::cell::RefCell::new(node));
        } else {
            *self.items[id].borrow_mut() = node;
        }
        self.pos += 1;
        id
    }

    fn clear(&mut self) {
        assert_eq!(self.executing, 0, "cannot clear nodes during execution");
        self.items.clear();
        self.pos = 0;
    }
}

pub(crate) struct ExecArena<'a, T: Transaction + 'a> {
    nodes: ExecNodes<'a, T>,
    result: ExecResult,
    table_codec: TableCodec,
    context: Option<ExecutionContext<'a>>,
    transaction: *mut T,
    runtime_probe_stack: Vec<RuntimeIndexProbe>,
    ddl_apply: Vec<DDLApply>,
    recursive_input: Option<RecursiveInput>,
}

pub(crate) struct ExecArenaLocalState<'b, 'a, T: Transaction + 'a> {
    transaction: *mut T,
    pub(crate) table_codec: &'b mut TableCodec,
    pub(crate) context: ExecutionContext<'a>,
    pub(crate) result: &'b mut ExecResult,
    pub(crate) plan_arena: &'b (dyn MetaArena + 'a),
    ddl_apply: &'b mut Vec<DDLApply>,
}

impl<'b, 'a, T: Transaction + 'a> ExecArenaLocalState<'b, 'a, T> {
    pub(crate) fn transaction(&self) -> &'a T {
        unsafe { &*self.transaction }
    }

    pub(crate) fn transaction_codec_mut(&mut self) -> (&mut T, &mut TableCodec) {
        unsafe { (&mut *self.transaction, &mut *self.table_codec) }
    }

    pub(crate) fn index_values_transaction_codec_mut(
        &mut self,
    ) -> (&[DataValue], &mut T, &mut TableCodec) {
        unsafe {
            (
                &self.result.tuple.values,
                &mut *self.transaction,
                &mut *self.table_codec,
            )
        }
    }

    pub(crate) fn transaction_codec(&mut self) -> (&'a T, &mut TableCodec) {
        unsafe { (&*self.transaction, &mut *self.table_codec) }
    }

    pub(crate) fn write_transaction_codec_ddl_apply_mut(
        &mut self,
    ) -> (&mut T, &mut TableCodec, &mut Vec<DDLApply>) {
        unsafe {
            (
                &mut *self.transaction,
                &mut *self.table_codec,
                self.ddl_apply,
            )
        }
    }
}

impl<'a, T: Transaction + 'a> ExecArena<'a, T> {
    pub(crate) fn set_statement_stamp(&mut self, stamp: u64) {
        self.table_codec.set_stamp(stamp);
    }

    pub(crate) fn new() -> Self {
        Self {
            nodes: ExecNodes {
                items: Vec::new(),
                pos: 0,
                executing: 0,
            },
            result: ExecResult::default(),
            table_codec: TableCodec::default(),
            context: None,
            transaction: std::ptr::null_mut(),
            runtime_probe_stack: Vec::new(),
            ddl_apply: Vec::new(),
            recursive_input: None,
        }
    }
}

impl<'a, T: Transaction + 'a> ExecArena<'a, T> {
    pub(crate) fn init_context(&mut self, context: ExecutionContext<'a>, transaction: &'a T) {
        if let Some(current) = &self.context {
            debug_assert!(current.is_same_context(context));
            debug_assert_eq!(self.transaction, transaction as *const T as *mut T);
        } else {
            self.context = Some(context);
            self.transaction = transaction as *const T as *mut T;
        }
    }

    pub(crate) fn push(&mut self, node: ExecNode<'a, T>) -> ExecId {
        self.nodes.push(node)
    }

    pub(crate) fn push_ddl_apply(&mut self, apply: DDLApply) {
        self.ddl_apply.push(apply);
    }

    pub(crate) fn take_ddl_apply(&mut self) -> Vec<DDLApply> {
        std::mem::take(&mut self.ddl_apply)
    }

    pub(crate) fn context(&self) -> ExecutionContext<'a> {
        *self
            .context
            .as_ref()
            .expect("execution arena context initialized")
    }

    pub(crate) fn table_cache(&self) -> &TableCache {
        self.context
            .as_ref()
            .expect("execution arena context initialized")
            .table_cache
    }

    pub(crate) fn transaction(&self) -> &'a T {
        unsafe { &*self.transaction }
    }

    pub(crate) fn transaction_codec_mut(&mut self) -> (&mut T, &mut TableCodec) {
        (unsafe { &mut *self.transaction }, &mut self.table_codec)
    }

    pub(crate) fn local_state<'b>(
        &'b mut self,
        plan_arena: &'b (dyn MetaArena + 'a),
    ) -> ExecArenaLocalState<'b, 'a, T> {
        let context = *self
            .context
            .as_ref()
            .expect("execution arena context initialized");
        ExecArenaLocalState {
            transaction: self.transaction,
            table_codec: &mut self.table_codec,
            context,
            result: &mut self.result,
            plan_arena,
            ddl_apply: &mut self.ddl_apply,
        }
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

    pub(crate) fn set_recursive_input(&mut self, input: RecursiveInput) {
        debug_assert!(self.recursive_input.is_none());
        self.recursive_input = Some(input);
    }

    pub(crate) fn take_recursive_input(&mut self) -> RecursiveInput {
        self.recursive_input
            .take()
            .expect("recursive input initialized")
    }

    pub(crate) fn reset_for_rebuild(&mut self) {
        debug_assert!(self.runtime_probe_stack.is_empty());
        debug_assert!(self.ddl_apply.is_empty());
        self.nodes.clear();
        self.result.tuple = Tuple::default();
        self.result.status = None;
        self.recursive_input = None;
    }

    #[inline]
    pub(crate) fn result_tuple(&self) -> &Tuple {
        &self.result.tuple
    }

    #[inline]
    pub(crate) fn result_tuple_mut(&mut self) -> &mut Tuple {
        &mut self.result.tuple
    }

    pub(crate) fn materialize_tuple(&mut self) -> Tuple {
        std::mem::take(&mut self.result.tuple)
    }

    pub(crate) fn rewrite<E: RewriteExpression>(
        &mut self,
        exprs: &[E],
        arena: &dyn MetaArena,
        input: Option<&dyn TupleLike>,
    ) -> Result<(), DatabaseError> {
        let values = &mut self.result.tuple.values;
        let base = values.len();
        values.reserve(exprs.len());

        for expr in exprs {
            let value = {
                let input_values = &values[..base];
                let current: &dyn TupleLike = input.unwrap_or(&input_values);
                expr.expression(arena)
                    .eval(arena, Some(current))
                    .map(|value| value.into_owned())
            };
            match value {
                Ok(value) => values.push(value),
                Err(error) => {
                    values.truncate(base);
                    return Err(error);
                }
            }
        }

        values.rotate_left(base);
        values.truncate(exprs.len());
        Ok(())
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

    pub(crate) fn next_tuple(
        &mut self,
        id: ExecId,
        plan_arena: &mut (dyn MetaArena + 'a),
    ) -> Result<bool, DatabaseError> {
        self.result.status = None;
        let slot = &self.nodes.items[id] as *const std::cell::RefCell<ExecNode<'a, T>>;
        self.nodes.executing += 1;
        // SAFETY: push/clear cannot relocate or destroy slots while executing.
        // Access to nodes always goes through RefCell: recursive calls and
        // subtree rebuilds cannot borrow/overwrite an active node. The payload
        // is behind UnsafeCell, separate from the arena's own mutable state.
        // On unwind the borrow is released; executing remains nonzero, preventing
        // relocation even if the caller catches the panic (the arena is poisoned).
        let result = unsafe { (&*slot).borrow_mut().next_tuple(self, plan_arena) };
        self.nodes.executing -= 1;
        result?;

        match self.result.status.unwrap_or(ExecStatus::End) {
            ExecStatus::Continue => Ok(true),
            ExecStatus::End => Ok(false),
        }
    }
}

pub(crate) trait ReadExecutor<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) trait WriteExecutor<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut (dyn MetaArena + 'a),
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) fn build_read<'a, T>(
    arena: &mut ExecArena<'a, T>,
    plan_arena: &mut (dyn MetaArena + 'a),
    plan: &'a LogicalPlan,
    cache: ExecutionContext<'_>,
    transaction: &T,
) -> ExecId
where
    T: Transaction + 'a,
{
    macro_rules! read {
        ($executor:ty, $input:expr) => {
            <$executor as ReadExecutor<'a, T>>::into_executor(
                $input,
                arena,
                plan_arena,
                cache,
                transaction,
            )
        };
    }

    let physical_option = plan.physical_option.as_ref();
    match &plan.operator {
        Operator::Dummy => read!(Dummy, ()),
        Operator::Aggregate(op) => {
            let input = plan.childrens.only();

            if op.groupby_exprs.is_empty() {
                read!(SimpleAggExecutor<'a>, (op, input))
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
                read!(StreamDistinctExecutor<'a>, (op, input))
            } else if matches!(
                physical_option,
                Some(PhysicalOption {
                    plan: PlanImpl::StreamAggregate,
                    ..
                })
            ) {
                read!(StreamAggExecutor<'a>, (op, input))
            } else {
                read!(HashAggExecutor<'a>, (op, input))
            }
        }
        Operator::Filter(op) => read!(Filter, (op, plan.childrens.only())),
        Operator::ScalarApply(op) => {
            let (left, right) = plan.childrens.twins();
            read!(ScalarApply, (op, left, right))
        }
        Operator::MarkApply(op) => {
            let (left, right) = plan.childrens.twins();
            read!(MarkApply<'a>, (op, left, right))
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
            let (left, right) = plan.childrens.twins();

            if use_hash_join {
                read!(HashJoin, (op, left, right))
            } else {
                read!(NestedLoopJoin<'a>, (op, left, right))
            }
        }
        Operator::Project(op) => read!(Projection<'a>, (op, plan.childrens.only())),
        Operator::ScalarSubquery(op) => {
            read!(ScalarSubquery, (op, plan.childrens.only()))
        }
        Operator::TableScan(op) => {
            if let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(info),
                ..
            }) = physical_option
            {
                if let Some(lookup) = &info.lookup {
                    return read!(IndexScan<'a, T>, (op, info.as_ref(), lookup));
                }
            }
            read!(SeqScan<'a, T>, op)
        }
        Operator::FunctionScan(op) => read!(FunctionScan<'a>, op),
        Operator::Sort(op) => {
            #[cfg(feature = "spill")]
            {
                read!(ExternalSort<'a>, (op, plan.childrens.only()))
            }
            #[cfg(not(feature = "spill"))]
            {
                read!(Sort<'a>, (op, plan.childrens.only()))
            }
        }
        Operator::Limit(op) => read!(Limit, (op, plan.childrens.only())),
        Operator::TopK(op) => read!(TopK<'a>, (op, plan.childrens.only())),
        Operator::Values(op) => read!(Values<'a>, op),
        Operator::Window(op) => read!(Window<'a>, (op, plan.childrens.only())),
        Operator::ShowTable => read!(ShowTables<'a, T>, ()),
        Operator::ShowView => read!(ShowViews<'a, T>, ()),
        Operator::Explain => read!(Explain<'a>, plan.childrens.only()),
        Operator::Describe(op) => read!(Describe, op),
        Operator::Union(_) => read!(Union, plan.childrens.twins()),
        Operator::RecursiveCte(_) => {
            read!(RecursiveCte<'a, T>, plan.childrens.twins())
        }
        Operator::RecursiveScan(op) => read!(RecursiveScan, op),
        Operator::SetMembership(op) => {
            let (left, right) = plan.childrens.twins();
            read!(SetMembership, (op.kind, left, right))
        }
        _ => unreachable!(),
    }
}

pub(crate) fn build_write<'a, T>(
    arena: &mut ExecArena<'a, T>,
    plan_arena: &mut (dyn MetaArena + 'a),
    plan: &'a LogicalPlan,
    cache: ExecutionContext<'a>,
    transaction: &'a mut T,
) -> ExecId
where
    T: Transaction + 'a,
{
    arena.init_context(cache, transaction);
    let transaction_ref: &T = transaction;
    macro_rules! write {
        ($executor:ty, $input:expr) => {
            <$executor as WriteExecutor<'a, T>>::into_executor(
                $input,
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        };
    }

    match &plan.operator {
        Operator::Insert(op) => write!(Insert<'a>, (op, plan.childrens.only())),
        Operator::Update(op) => write!(Update<'a>, (op, plan.childrens.only())),
        Operator::Delete(op) => write!(Delete<'a>, (op, plan.childrens.only())),
        Operator::AddColumn(op) => write!(AddColumn<'a>, op),
        Operator::ChangeColumn(op) => write!(ChangeColumn<'a>, op),
        Operator::DropColumn(op) => write!(DropColumn<'a>, op),
        Operator::CreateTable(op) => write!(CreateTable<'a>, op),
        Operator::CreateIndex(op) => write!(CreateIndex<'a>, (op, plan.childrens.only())),
        Operator::CreateView(op) => write!(CreateView<'a>, op),
        Operator::DropTable(op) => write!(DropTable<'a>, op),
        Operator::DropView(op) => write!(DropView<'a>, op),
        Operator::DropIndex(op) => write!(DropIndex<'a>, op),
        Operator::Truncate(op) => write!(Truncate<'a>, op),
        #[cfg(feature = "copy")]
        Operator::CopyFromFile(op) => write!(CopyFromFile<'a>, op),
        #[cfg(feature = "copy")]
        Operator::CopyToFile(op) => <CopyToFile<'a> as ReadExecutor<'a, T>>::into_executor(
            (op, plan.childrens.only()),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::Analyze(op) => write!(Analyze<'a>, (op, plan.childrens.only())),
        _ => build_read(arena, plan_arena, plan, cache, transaction_ref),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_utils {
    use super::*;

    static EMPTY_SCALA_FUNCTIONS: std::sync::LazyLock<ScalaFunctions> =
        std::sync::LazyLock::new(ScalaFunctions::default);
    static EMPTY_TABLE_FUNCTIONS: std::sync::LazyLock<TableFunctions> =
        std::sync::LazyLock::new(TableFunctions::default);

    pub(crate) fn empty_context<'a>(
        table_cache: &'a TableCache,
        view_cache: &'a ViewCache,
        meta_cache: &'a StatisticsMetaCache,
    ) -> ExecutionContext<'a> {
        ExecutionContext::new(
            table_cache,
            view_cache,
            meta_cache,
            &EMPTY_SCALA_FUNCTIONS,
            &EMPTY_TABLE_FUNCTIONS,
        )
    }

    pub(crate) struct TestExecutor<'a, T: Transaction + 'a> {
        executor: Executor<'a, T>,
        plan_arena: PlanArena<'a>,
    }

    impl<T: Transaction> TestExecutor<'_, T> {
        pub(crate) fn next_tuple(&mut self) -> Result<Option<&mut Tuple>, DatabaseError> {
            self.executor.next_tuple(&mut self.plan_arena)
        }
    }

    pub(crate) fn execute_input<'a, T, E>(
        input: E::Input,
        cache: ExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: ReadExecutor<'a, T>,
    {
        let mut arena = ExecArena::new();
        arena.init_context(cache, transaction);
        let root = <E as ReadExecutor<'a, T>>::into_executor(
            input,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        TestExecutor {
            executor: Executor::new(arena, root, PlanKeeper::empty()),
            plan_arena,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn execute_input_mut<'a, T, E>(
        input: E::Input,
        cache: ExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: WriteExecutor<'a, T>,
    {
        let mut arena = ExecArena::new();
        arena.init_context(cache, transaction);
        let root = <E as WriteExecutor<'a, T>>::into_executor(
            input,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        TestExecutor {
            executor: Executor::new(arena, root, PlanKeeper::empty()),
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
pub(crate) use test_utils::{empty_context, execute_input, execute_input_mut, try_collect};

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::memory::MemoryTransaction;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    #[test]
    fn active_nodes_cannot_be_overwritten_or_relocated() {
        let table_arena = crate::planner::TableArenaCell::default();
        let mut plan_arena = crate::planner::PlanArena::new(&table_arena);
        let mut arena = ExecArena::<'_, MemoryTransaction>::new();
        arena.push(ExecNode::Dummy(Dummy::default()));
        let slot =
            &arena.nodes.items[0] as *const std::cell::RefCell<ExecNode<'_, MemoryTransaction>>;
        arena.nodes.executing = 1;
        // Same access pattern as next_tuple; mutations must fail before changing storage.
        let active = unsafe { (&*slot).borrow_mut() };
        assert!(catch_unwind(AssertUnwindSafe(|| {
            arena.push(ExecNode::Dummy(Dummy::default()));
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| arena.nodes.clear())).is_err());
        arena.nodes.seek(0);
        assert!(catch_unwind(AssertUnwindSafe(|| {
            arena.push(ExecNode::Dummy(Dummy::default()));
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            arena.next_tuple(0, &mut plan_arena).unwrap();
        }))
        .is_err());
        drop(active);
        assert_eq!(arena.nodes.items.len(), 1);
        assert_eq!(arena.nodes.items.as_ptr(), slot);
    }
}
