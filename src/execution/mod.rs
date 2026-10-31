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
pub(crate) mod runs;
#[cfg(feature = "spill")]
pub(crate) mod spill;

pub(crate) use ddl_apply::DDLApply;

use self::ddl::add_column::AddColumn;
use self::ddl::change_column::ChangeColumn;
use self::dql::join::nested_loop_join::NestedLoopJoin;
use self::dql::mark_apply::MarkApply;
use self::dql::scalar_query_init::ScalarQueryInit;
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
use crate::execution::dql::join::index_probe_join::IndexProbeJoin;
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
use crate::planner::{ExecMetaArena, ExprRef, LogicalPlan, PlanArena, PlanKeeper};
use crate::storage::table_codec::TableCodec;
use crate::storage::{IndexIter, StorageTransaction, TableCache, TableIter, TupleIter, ViewIter};
use crate::types::index::RuntimeIndexProbe;
use crate::types::tuple::{ConcatPool, Input, Schema, Tuple, TupleId, TupleImpl};
use crate::types::value::DataValue;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::mem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecStatus {
    Continue,
    End,
}

#[derive(Debug, Default)]
pub(crate) struct ExecResult {
    pub(crate) tuple: TupleImpl,
    pub(crate) status: Option<ExecStatus>,
    concat_pool: ConcatPool,
    spare_values: Vec<DataValue>,
}

impl ExecResult {
    #[inline]
    pub(crate) fn output_mut(&mut self) -> &mut Tuple {
        self.concat_pool.retain_leftmost(&mut self.tuple)
    }
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

/// A statement built into an executor tree, like a compiled SQLite program.
///
/// It holds no transaction, so an executor taken back from a finished iterator can run
/// again in another one.
pub struct Executor<'a> {
    root: ExecNode<'a>,
    iters: usize,
    pub(crate) schema: Schema,
    pub(crate) buffer: ExecBuffer,
    // Set once an execution started; the next one resets the tree first.
    ran: bool,
    // Keeps the plan the tree borrows from alive, so it must stay declared after `root`.
    keeper: PlanKeeper<'a>,
}

impl<'a> Executor<'a> {
    pub(crate) fn new(plan: LogicalPlan, plan_arena: &(dyn MetaArena + 'a)) -> Self {
        let schema = plan.read_schema().clone();
        let keeper = PlanKeeper::new(plan);
        let mut iters = IterCounter::default();
        let root = build_write(&mut iters, plan_arena, keeper.plan());
        Self {
            root,
            iters: iters.0,
            schema,
            buffer: ExecBuffer::default(),
            ran: false,
            keeper,
        }
    }

    pub(crate) fn from_plan(
        (plan, plan_arena): (LogicalPlan, PlanArena<'a>),
    ) -> (Self, PlanArena<'a>) {
        (Self::new(plan, &plan_arena), plan_arena)
    }

    pub(crate) fn plan(&self) -> &'a LogicalPlan {
        self.keeper.plan()
    }

    pub(crate) fn iters(&self) -> usize {
        self.iters
    }

    pub(crate) fn next_tuple<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<bool, DatabaseError> {
        let mut context = ExecContext::new(&mut self.buffer, arena);
        self.root.next_tuple(&mut context, plan_arena)
    }

    pub(crate) fn start<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecArena<'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        self.buffer.reset();
        if mem::replace(&mut self.ran, true) {
            let mut context = ExecContext::new(&mut self.buffer, arena);
            self.root.reset(&mut context, plan_arena)?;
        }
        Ok(())
    }
}

pub(crate) enum ExecNode<'a> {
    AddColumn(Box<AddColumn<'a>>),
    Analyze(Box<Analyze<'a>>),
    ChangeColumn(Box<ChangeColumn<'a>>),
    #[cfg(feature = "copy")]
    CopyFromFile(Box<CopyFromFile<'a>>),
    #[cfg(feature = "copy")]
    CopyToFile(Box<CopyToFile<'a>>),
    CreateIndex(Box<CreateIndex<'a>>),
    CreateTable(Box<CreateTable<'a>>),
    CreateView(Box<CreateView<'a>>),
    Delete(Box<Delete<'a>>),
    Describe(Box<Describe>),
    DropColumn(Box<DropColumn<'a>>),
    DropIndex(Box<DropIndex<'a>>),
    DropTable(Box<DropTable<'a>>),
    DropView(Box<DropView<'a>>),
    Dummy(Box<Dummy>),
    Explain(Box<Explain<'a>>),
    #[cfg(feature = "spill")]
    ExternalSort(Box<ExternalSort<'a>>),
    Filter(Box<Filter<'a>>),
    FunctionScan(Box<FunctionScan<'a>>),
    HashAgg(Box<HashAggExecutor<'a>>),
    HashJoin(Box<HashJoin<'a>>),
    IndexScan(Box<IndexScan<'a>>),
    Insert(Box<Insert<'a>>),
    Limit(Box<Limit<'a>>),
    MarkApply(Box<MarkApply<'a>>),
    IndexProbeJoin(Box<IndexProbeJoin<'a>>),
    NestedLoopJoin(Box<NestedLoopJoin<'a>>),
    Projection(Box<Projection<'a>>),
    RecursiveCte(Box<RecursiveCte<'a>>),
    RecursiveScan(Box<RecursiveScan>),
    ScalarQueryInit(Box<ScalarQueryInit<'a>>),
    ScalarSubquery(Box<ScalarSubquery<'a>>),
    SetMembership(Box<SetMembership<'a>>),
    SeqScan(Box<SeqScan<'a>>),
    ShowTables(Box<ShowTables>),
    ShowViews(Box<ShowViews>),
    SimpleAgg(Box<SimpleAggExecutor<'a>>),
    Sort(Box<Sort<'a>>),
    StreamAgg(Box<StreamAggExecutor<'a>>),
    StreamDistinct(Box<StreamDistinctExecutor<'a>>),
    TopK(Box<TopK<'a>>),
    Truncate(Box<Truncate<'a>>),
    Union(Box<Union<'a>>),
    Update(Box<Update<'a>>),
    Values(Box<Values<'a>>),
    Window(Box<Window<'a>>),
}

pub(crate) trait ExecutorNode<'a, T: StorageTransaction + 'a>: Sized {
    // Restores the state the node had right after it was built, resetting its inputs too.
    fn reset<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError>;

    fn next_tuple<A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError>;
}

impl<'a> ExecNode<'a> {
    pub(crate) fn reset<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<(), DatabaseError> {
        match self {
            ExecNode::AddColumn(exec) => {
                <AddColumn as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Analyze(exec) => {
                <Analyze as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::ChangeColumn(exec) => {
                <ChangeColumn as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            #[cfg(feature = "copy")]
            ExecNode::CopyFromFile(exec) => {
                <CopyFromFile as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            #[cfg(feature = "copy")]
            ExecNode::CopyToFile(exec) => {
                <CopyToFile as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::CreateIndex(exec) => {
                <CreateIndex as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::CreateTable(exec) => {
                <CreateTable as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::CreateView(exec) => {
                <CreateView as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Delete(exec) => {
                <Delete as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Describe(exec) => {
                <Describe as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::DropColumn(exec) => {
                <DropColumn as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::DropIndex(exec) => {
                <DropIndex as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::DropTable(exec) => {
                <DropTable as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::DropView(exec) => {
                <DropView as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Dummy(exec) => <Dummy as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena),
            ExecNode::Explain(exec) => {
                <Explain<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            #[cfg(feature = "spill")]
            ExecNode::ExternalSort(exec) => {
                <ExternalSort<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Filter(exec) => {
                <Filter<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::FunctionScan(exec) => {
                <FunctionScan<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::HashAgg(exec) => {
                <HashAggExecutor<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::HashJoin(exec) => {
                <HashJoin<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::IndexScan(exec) => {
                <IndexScan<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Insert(exec) => {
                <Insert as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Limit(exec) => {
                <Limit<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::MarkApply(exec) => {
                <MarkApply<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::IndexProbeJoin(exec) => {
                <IndexProbeJoin<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::NestedLoopJoin(exec) => {
                <NestedLoopJoin<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Projection(exec) => {
                <Projection<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::RecursiveCte(exec) => {
                <RecursiveCte<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::RecursiveScan(exec) => {
                <RecursiveScan as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::ScalarQueryInit(exec) => {
                <ScalarQueryInit<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::ScalarSubquery(exec) => {
                <ScalarSubquery<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::SetMembership(exec) => {
                <SetMembership<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::SeqScan(exec) => {
                <SeqScan<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::ShowTables(exec) => {
                <ShowTables as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::ShowViews(exec) => {
                <ShowViews as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::SimpleAgg(exec) => {
                <SimpleAggExecutor<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Sort(exec) => {
                <Sort<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::StreamAgg(exec) => {
                <StreamAggExecutor<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::StreamDistinct(exec) => {
                <StreamDistinctExecutor<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::TopK(exec) => {
                <TopK<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Truncate(exec) => {
                <Truncate as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Union(exec) => {
                <Union<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Update(exec) => {
                <Update<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Values(exec) => {
                <Values<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
            ExecNode::Window(exec) => {
                <Window<'a> as ExecutorNode<'a, T>>::reset(exec, arena, plan_arena)
            }
        }
    }

    pub(crate) fn next_tuple<T: StorageTransaction + 'a, A: MetaArena + 'a>(
        &mut self,
        arena: &mut ExecContext<'_, 'a, '_, T>,
        plan_arena: &mut ExecMetaArena<A>,
    ) -> Result<bool, DatabaseError> {
        arena.buffer.result.status = None;
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
                <Filter<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::FunctionScan(exec) => {
                <FunctionScan<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::HashAgg(exec) => {
                <HashAggExecutor<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::HashJoin(exec) => {
                <HashJoin<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::IndexScan(exec) => {
                <IndexScan<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Insert(exec) => {
                <Insert as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Limit(exec) => {
                <Limit<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::MarkApply(exec) => {
                <MarkApply<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::IndexProbeJoin(exec) => {
                <IndexProbeJoin<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::NestedLoopJoin(exec) => {
                <NestedLoopJoin<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Projection(exec) => {
                <Projection<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::RecursiveCte(exec) => {
                <RecursiveCte<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::RecursiveScan(exec) => {
                <RecursiveScan as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ScalarQueryInit(exec) => {
                <ScalarQueryInit<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ScalarSubquery(exec) => {
                <ScalarSubquery<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::SetMembership(exec) => {
                <SetMembership<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::SeqScan(exec) => {
                <SeqScan<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ShowTables(exec) => {
                <ShowTables as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::ShowViews(exec) => {
                <ShowViews as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
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
                <Union<'a> as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
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
        }?;
        match arena.buffer.result.status.unwrap_or(ExecStatus::End) {
            ExecStatus::Continue => Ok(true),
            ExecStatus::End => Ok(false),
        }
    }
}

pub(crate) type IterSlot = usize;

#[derive(Default)]
pub(crate) struct ExecBuffer {
    result: ExecResult,
    table_codec: TableCodec,
    runtime_probe_stack: Vec<RuntimeIndexProbe>,
}

pub(crate) struct ExecArena<'a, 't, T: StorageTransaction + 'a> {
    table_cache: &'a TableCache,
    transaction: *mut T,
    ddl_apply: Vec<DDLApply>,
    recursive_input: Option<RecursiveInput>,
    iters: Vec<StorageIter<'t, T>>,
    _lifetimes: PhantomData<&'t &'a ()>,
}

pub(crate) struct ExecContext<'c, 'a, 't, T: StorageTransaction + 'a> {
    buffer: &'c mut ExecBuffer,
    arena: &'c mut ExecArena<'a, 't, T>,
}

pub(crate) enum StorageIter<'t, T: StorageTransaction + 't> {
    Empty,
    Tuple(TupleIter<'t, T>),
    Index(Box<IndexIter<'t, T>>),
    Table(TableIter<'t, T>),
    View(ViewIter<'t, T>),
}

impl<'t, T: StorageTransaction + 't> StorageIter<'t, T> {
    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, StorageIter::Empty)
    }

    pub(crate) fn as_tuple(&mut self) -> &mut TupleIter<'t, T> {
        match self {
            StorageIter::Tuple(iter) => iter,
            _ => unreachable!("storage iterator is not a tuple iterator"),
        }
    }

    pub(crate) fn as_index(&mut self) -> &mut IndexIter<'t, T> {
        match self {
            StorageIter::Index(iter) => iter,
            _ => unreachable!("storage iterator is not an index iterator"),
        }
    }

    pub(crate) fn as_table(&mut self) -> &mut TableIter<'t, T> {
        match self {
            StorageIter::Table(iter) => iter,
            _ => unreachable!("storage iterator is not a table iterator"),
        }
    }

    pub(crate) fn as_view(&mut self) -> &mut ViewIter<'t, T> {
        match self {
            StorageIter::View(iter) => iter,
            _ => unreachable!("storage iterator is not a view iterator"),
        }
    }
}

pub(crate) struct ExecArenaLocalState<'b, 'a, 't, T: StorageTransaction + 'a> {
    transaction: *mut T,
    pub(crate) table_codec: &'b mut TableCodec,
    pub(crate) result: &'b mut ExecResult,
    pub(crate) plan_arena: &'b (dyn MetaArena + 'a),
    pub(crate) iters: &'b mut [StorageIter<'t, T>],
    ddl_apply: &'b mut Vec<DDLApply>,
}

impl<'b, 'a, 't, T: StorageTransaction + 'a> ExecArenaLocalState<'b, 'a, 't, T> {
    pub(crate) fn transaction(&self) -> &'t T {
        unsafe { &*self.transaction }
    }

    pub(crate) fn transaction_codec_mut(&mut self) -> (&mut T, &mut TableCodec) {
        unsafe { (&mut *self.transaction, &mut *self.table_codec) }
    }

    pub(crate) fn tuple_transaction_codec_mut(&mut self) -> (&TupleImpl, &mut T, &mut TableCodec) {
        unsafe {
            (
                &self.result.tuple,
                &mut *self.transaction,
                &mut *self.table_codec,
            )
        }
    }

    pub(crate) fn transaction_codec(&mut self) -> (&'t T, &mut TableCodec) {
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

impl ExecBuffer {
    pub(crate) fn set_statement_stamp(&mut self, stamp: u64) {
        self.table_codec.set_stamp(stamp);
    }

    /// Drops what an unfinished execution may have left behind.
    pub(crate) fn reset(&mut self) {
        self.result.status = None;
        self.runtime_probe_stack.clear();
    }

    #[inline]
    pub(crate) fn result_mut(&mut self) -> &mut TupleImpl {
        &mut self.result.tuple
    }
}

impl<'a, 't, T: StorageTransaction + 'a> ExecArena<'a, 't, T> {
    pub(crate) fn new(table_cache: &'a TableCache, transaction: &'t T) -> Self {
        Self {
            table_cache,
            transaction: transaction as *const T as *mut T,
            ddl_apply: Vec::new(),
            recursive_input: None,
            iters: Vec::new(),
            _lifetimes: PhantomData,
        }
    }

    pub(crate) fn with_iters(mut self, iters: usize) -> Self {
        self.iters.resize_with(iters, || StorageIter::Empty);
        self
    }

    pub(crate) fn release_iters(&mut self) {
        self.iters.fill_with(|| StorageIter::Empty);
    }

    pub(crate) fn take_ddl_apply(&mut self) -> Vec<DDLApply> {
        std::mem::take(&mut self.ddl_apply)
    }
}

impl<'c, 'a, 't, T: StorageTransaction + 'a> ExecContext<'c, 'a, 't, T> {
    pub(crate) fn new(buffer: &'c mut ExecBuffer, arena: &'c mut ExecArena<'a, 't, T>) -> Self {
        Self { buffer, arena }
    }

    pub(crate) fn push_ddl_apply(&mut self, apply: DDLApply) {
        self.arena.ddl_apply.push(apply);
    }

    pub(crate) fn table_cache(&self) -> &'a TableCache {
        self.arena.table_cache
    }

    pub(crate) fn iter_mut(&mut self, slot: IterSlot) -> &mut StorageIter<'t, T> {
        &mut self.arena.iters[slot]
    }

    pub(crate) fn transaction_codec_mut(&mut self) -> (&mut T, &mut TableCodec) {
        (
            unsafe { &mut *self.arena.transaction },
            &mut self.buffer.table_codec,
        )
    }

    pub(crate) fn local_state<'b>(
        &'b mut self,
        plan_arena: &'b (dyn MetaArena + 'a),
    ) -> ExecArenaLocalState<'b, 'a, 't, T> {
        ExecArenaLocalState {
            transaction: self.arena.transaction,
            table_codec: &mut self.buffer.table_codec,
            result: &mut self.buffer.result,
            plan_arena,
            iters: &mut self.arena.iters,
            ddl_apply: &mut self.arena.ddl_apply,
        }
    }

    pub(crate) fn push_runtime_probe(&mut self, value: RuntimeIndexProbe) {
        self.buffer.runtime_probe_stack.push(value);
    }

    pub(crate) fn pop_runtime_probe(&mut self) -> RuntimeIndexProbe {
        self.buffer
            .runtime_probe_stack
            .pop()
            .expect("index probe join pushes a probe before resetting its input")
    }

    pub(crate) fn set_recursive_input(&mut self, input: RecursiveInput) {
        debug_assert!(self.arena.recursive_input.is_none());
        self.arena.recursive_input = Some(input);
    }

    pub(crate) fn iter_len(&self) -> usize {
        self.arena.iters.len()
    }

    // Slots are allocated in build order, so a subtree built last owns the tail.
    pub(crate) fn truncate_iters(&mut self, len: usize) {
        self.arena.iters.truncate(len);
    }

    #[inline]
    pub(crate) fn result_tuple(&self) -> &TupleImpl {
        &self.buffer.result.tuple
    }

    #[inline]
    pub(crate) fn result_mut(&mut self) -> &mut TupleImpl {
        self.buffer.result_mut()
    }

    #[inline]
    pub(crate) fn output_tuple_mut(&mut self) -> &mut Tuple {
        self.buffer.result.output_mut()
    }

    #[inline]
    pub(crate) fn concat(
        &mut self,
        left: TupleImpl,
        right: TupleImpl,
        pk: Option<TupleId>,
    ) -> TupleImpl {
        self.buffer.result.concat_pool.concat(left, right, pk)
    }

    #[inline]
    pub(crate) fn release(&mut self, tuple: TupleImpl) {
        self.buffer.result.concat_pool.release(tuple);
    }

    pub(crate) fn take_tuple(&mut self) -> TupleImpl {
        std::mem::take(&mut self.buffer.result.tuple)
    }

    pub(crate) fn rewrite<E: RewriteExpression, TInput: Input + ?Sized>(
        &mut self,
        exprs: &[E],
        arena: &dyn MetaArena,
        input: Option<&TInput>,
    ) -> Result<(), DatabaseError> {
        let result = &mut self.buffer.result;
        let pool = self.buffer.table_codec.bytes_pool();
        result.spare_values.reserve(exprs.len());
        for expr in exprs {
            let expression = expr.expression(arena);
            let value = match input {
                Some(input) => expression.eval(arena, Some(input)),
                None => expression.eval(arena, Some(&result.tuple)),
            };
            match value {
                Ok(Cow::Owned(value)) => result.spare_values.push(value),
                Ok(Cow::Borrowed(value)) => result.spare_values.push(pool.clone_value(value)),
                Err(error) => {
                    pool.recycle(&mut result.spare_values);
                    return Err(error);
                }
            }
        }

        let pk = result.tuple.take_pk();
        let tuple = result.concat_pool.retain_rightmost(&mut result.tuple);
        std::mem::swap(&mut tuple.values, &mut result.spare_values);
        tuple.pk = pk;
        pool.recycle(&mut result.spare_values);
        Ok(())
    }

    #[inline]
    pub(crate) fn resume(&mut self) {
        self.buffer.result.status = Some(ExecStatus::Continue);
    }

    #[inline]
    pub(crate) fn finish(&mut self) {
        self.buffer.result.status = Some(ExecStatus::End);
    }

    #[inline]
    pub(crate) fn produce(&mut self, tuple: TupleImpl) {
        self.buffer.result.tuple = tuple;
        self.resume();
    }
}

/// What building an executor tree needs: storage iterator slots, plus the rows a
/// recursive step reads, which only exist while a recursive CTE runs.
pub(crate) trait ExecBuilder {
    fn alloc_iter(&mut self) -> IterSlot;

    fn take_recursive_input(&mut self) -> RecursiveInput;
}

/// Counts the iterator slots of a tree built ahead of any transaction.
#[derive(Default)]
struct IterCounter(usize);

impl ExecBuilder for IterCounter {
    fn alloc_iter(&mut self) -> IterSlot {
        self.0 += 1;
        self.0 - 1
    }

    fn take_recursive_input(&mut self) -> RecursiveInput {
        unreachable!("recursive steps are built while their CTE runs")
    }
}

impl<'a, T: StorageTransaction + 'a> ExecBuilder for ExecContext<'_, 'a, '_, T> {
    fn alloc_iter(&mut self) -> IterSlot {
        self.arena.iters.push(StorageIter::Empty);
        self.arena.iters.len() - 1
    }

    fn take_recursive_input(&mut self) -> RecursiveInput {
        self.arena
            .recursive_input
            .take()
            .expect("recursive input initialized")
    }
}

pub(crate) trait ReadExecutor<'a>: Sized {
    type Input;

    fn into_executor<B: ExecBuilder>(
        input: Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a>;
}

pub(crate) trait WriteExecutor<'a>: Sized {
    type Input;

    fn into_executor<B: ExecBuilder>(
        input: Self::Input,
        arena: &mut B,
        plan_arena: &(dyn MetaArena + 'a),
    ) -> ExecNode<'a>;
}

pub(crate) fn build_read<'a, B: ExecBuilder>(
    arena: &mut B,
    plan_arena: &(dyn MetaArena + 'a),
    plan: &'a LogicalPlan,
) -> ExecNode<'a> {
    macro_rules! read {
        ($executor:ty, $input:expr) => {
            <$executor as ReadExecutor<'a>>::into_executor($input, arena, plan_arena)
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
        Operator::Filter(op) => read!(Filter<'a>, (op, plan.childrens.only())),
        Operator::ScalarQueryInit(op) => {
            let (left, right) = plan.childrens.twins();
            read!(ScalarQueryInit<'a>, (op, left, right))
        }
        Operator::MarkApply(op) => {
            let (left, right) = plan.childrens.twins();
            read!(MarkApply<'a>, (op, left, right))
        }
        Operator::IndexProbeJoin(op) => {
            let (left, right) = plan.childrens.twins();
            read!(IndexProbeJoin<'a>, (op, left, right))
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
                read!(HashJoin<'a>, (op, left, right))
            } else {
                read!(NestedLoopJoin<'a>, (op, left, right))
            }
        }
        Operator::Project(op) => read!(Projection<'a>, (op, plan.childrens.only())),
        Operator::ScalarSubquery(op) => {
            read!(ScalarSubquery<'a>, (op, plan.childrens.only()))
        }
        Operator::TableScan(op) => {
            if let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(info),
                ..
            }) = physical_option
            {
                if let Some(lookup) = &info.lookup {
                    return read!(IndexScan<'a>, (op, info.as_ref(), lookup));
                }
            }
            read!(SeqScan<'a>, op)
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
        Operator::Limit(op) => read!(Limit<'a>, (op, plan.childrens.only())),
        Operator::TopK(op) => read!(TopK<'a>, (op, plan.childrens.only())),
        Operator::Values(op) => read!(Values<'a>, op),
        Operator::Window(op) => read!(Window<'a>, (op, plan.childrens.only())),
        Operator::ShowTable => read!(ShowTables, ()),
        Operator::ShowView => read!(ShowViews, ()),
        Operator::Explain => read!(Explain<'a>, plan.childrens.only()),
        Operator::Describe(op) => read!(Describe, op),
        Operator::Union(_) => read!(Union<'a>, plan.childrens.twins()),
        Operator::RecursiveCte(_) => {
            read!(RecursiveCte<'a>, plan.childrens.twins())
        }
        Operator::RecursiveScan(op) => read!(RecursiveScan, op),
        Operator::SetMembership(op) => {
            let (left, right) = plan.childrens.twins();
            read!(SetMembership<'a>, (op.kind, left, right))
        }
        _ => unreachable!(),
    }
}

pub(crate) fn build_write<'a, B: ExecBuilder>(
    arena: &mut B,
    plan_arena: &(dyn MetaArena + 'a),
    plan: &'a LogicalPlan,
) -> ExecNode<'a> {
    macro_rules! write {
        ($executor:ty, $input:expr) => {
            <$executor as WriteExecutor<'a>>::into_executor($input, arena, plan_arena)
        };
    }

    match &plan.operator {
        Operator::ScalarQueryInit(op) => {
            let (input, init) = plan.childrens.twins();
            let input = build_write(arena, plan_arena, input);
            ScalarQueryInit::build(op, input, init, arena, plan_arena)
        }
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
        Operator::CopyToFile(op) => <CopyToFile<'a> as ReadExecutor<'a>>::into_executor(
            (op, plan.childrens.only()),
            arena,
            plan_arena,
        ),
        Operator::Analyze(op) => write!(Analyze<'a>, (op, plan.childrens.only())),
        _ => build_read(arena, plan_arena, plan),
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_utils {
    use super::*;

    pub(crate) struct TestExecutor<'a, T: StorageTransaction + 'a> {
        root: ExecNode<'a>,
        buffer: ExecBuffer,
        arena: ExecArena<'a, 'a, T>,
        plan_arena: ExecMetaArena<PlanArena<'a>>,
    }

    impl<T: StorageTransaction> TestExecutor<'_, T> {
        pub(crate) fn next_tuple(&mut self) -> Result<Option<&mut TupleImpl>, DatabaseError> {
            let mut context = ExecContext::new(&mut self.buffer, &mut self.arena);
            if !self.root.next_tuple(&mut context, &mut self.plan_arena)? {
                return Ok(None);
            }
            Ok(Some(self.buffer.result_mut()))
        }
    }

    pub(crate) fn execute_input<'a, T, E>(
        input: E::Input,
        cache: &'a TableCache,
        plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: StorageTransaction + 'a,
        E: ReadExecutor<'a>,
    {
        let mut buffer = ExecBuffer::default();
        let mut arena = ExecArena::new(cache, transaction);
        let root = <E as ReadExecutor<'a>>::into_executor(
            input,
            &mut ExecContext::new(&mut buffer, &mut arena),
            &plan_arena,
        );
        TestExecutor {
            root,
            buffer,
            arena,
            plan_arena: ExecMetaArena::new(plan_arena),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn execute_input_mut<'a, T, E>(
        input: E::Input,
        cache: &'a TableCache,
        plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: StorageTransaction + 'a,
        E: WriteExecutor<'a>,
    {
        let mut buffer = ExecBuffer::default();
        let mut arena = ExecArena::new(cache, transaction);
        let root = <E as WriteExecutor<'a>>::into_executor(
            input,
            &mut ExecContext::new(&mut buffer, &mut arena),
            &plan_arena,
        );
        TestExecutor {
            root,
            buffer,
            arena,
            plan_arena: ExecMetaArena::new(plan_arena),
        }
    }

    pub fn try_collect<T: StorageTransaction>(
        executor: TestExecutor<'_, T>,
    ) -> Result<Vec<TupleImpl>, DatabaseError> {
        let mut executor = executor;
        let mut tuples = Vec::new();

        while let Some(tuple) = executor.next_tuple()? {
            tuples.push(std::mem::take(tuple));
        }
        Ok(tuples)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(unused_imports)]
pub(crate) use test_utils::{execute_input, execute_input_mut, try_collect};

#[cfg(test)]
mod test {
    use super::*;
    use crate::planner::TableArenaCell;
    use crate::storage::memory::MemoryStorage;
    use crate::storage::Storage;

    #[test]
    fn exec_node_stays_pointer_sized() {
        // Every variant is boxed so a statement's node Vec stays small and allocator-friendly.
        assert_eq!(
            std::mem::size_of::<ExecNode<'_>>(),
            2 * std::mem::size_of::<usize>()
        );
    }

    #[test]
    fn rewrite_projects_joined_row() -> Result<(), DatabaseError> {
        use crate::catalog::{ColumnCatalog, ColumnDesc};
        use crate::types::LogicalType;

        let table_arena = TableArenaCell::default();
        let mut plan_arena = PlanArena::new(&table_arena);
        let column = plan_arena.alloc_column(ColumnCatalog::new(
            "c".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        ));
        let exprs = [3, 0, 1].map(|position| {
            plan_arena.alloc_expression(ScalarExpression::column_expr(column, position))
        });
        let ints = |values: [i32; 2]| Tuple::new(None, values.map(DataValue::Int32).to_vec());

        let tables = TableCache::default();
        let storage = MemoryStorage::new();
        let transaction = storage.transaction()?;
        let mut exec_buffer = ExecBuffer::default();
        let mut exec_arena = ExecArena::new(&tables, &transaction);
        let mut arena = ExecContext::new(&mut exec_buffer, &mut exec_arena);
        for base in [0, 10] {
            arena.buffer.result.tuple = TupleImpl::concat(
                ints([base, base + 1]).into(),
                ints([base + 2, base + 3]).into(),
                Some(DataValue::Int32(base)),
            );
            arena.rewrite(&exprs, &plan_arena, None::<&[DataValue]>)?;
            assert!(matches!(arena.result_tuple(), TupleImpl::Tuple(_)));
            assert_eq!(arena.result_tuple().pk(), Some(&DataValue::Int32(base)));
            assert_eq!(
                *arena.result_tuple(),
                [base + 3, base, base + 1].map(DataValue::Int32)
            );
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
