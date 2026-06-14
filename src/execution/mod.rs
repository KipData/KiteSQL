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
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::{Operator, PhysicalOption, PlanImpl};
use crate::planner::{LogicalPlan, PlanArena};
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

pub struct Executor<'a, T: Transaction + 'a> {
    arena: ExecArena<'a, T>,
    root: ExecId,
}

impl<'a, T: Transaction + 'a> Executor<'a, T> {
    pub(crate) fn new(arena: ExecArena<'a, T>, root: ExecId) -> Self {
        Self { arena, root }
    }

    pub(crate) fn next_tuple(
        &mut self,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<Option<&Tuple>, DatabaseError> {
        if !self.arena.next_tuple(self.root, plan_arena)? {
            return Ok(None);
        }
        Ok(Some(self.arena.result_tuple()))
    }

    pub(crate) fn take_ddl_apply(&mut self) -> Vec<DDLApply> {
        self.arena.take_ddl_apply()
    }
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ExecNode<'a, T: Transaction + 'a> {
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
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<(), DatabaseError>;
}

impl<'a, T: Transaction + 'a> ExecNode<'a, T> {
    fn next_tuple(
        &mut self,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut PlanArena<'a>,
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
                <Explain as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Filter(exec) => {
                <Filter as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::FunctionScan(exec) => {
                <FunctionScan as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::HashAgg(exec) => {
                <HashAggExecutor as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
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
                <MarkApply as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::NestedLoopJoin(exec) => {
                <NestedLoopJoin as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Projection(exec) => {
                <Projection as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
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
                <SimpleAggExecutor as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Sort(exec) => {
                <Sort as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::StreamDistinct(exec) => {
                <StreamDistinctExecutor as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::TopK(exec) => {
                <TopK as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Truncate(exec) => {
                <Truncate as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Union(exec) => {
                <Union as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Update(exec) => {
                <Update as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Values(exec) => {
                <Values as ExecutorNode<'a, T>>::next_tuple(exec, arena, plan_arena)
            }
            ExecNode::Empty => unreachable!("executor node re-entered while active"),
        }
    }
}

pub(crate) struct ExecArena<'a, T: Transaction + 'a> {
    nodes: Vec<ExecNode<'a, T>>,
    result: ExecResult,
    table_codec: TableCodec,
    projection_tmp: Vec<DataValue>,
    context: Option<ExecutionContext<'a>>,
    transaction: *mut T,
    runtime_probe_stack: Vec<RuntimeIndexProbe>,
    ddl_apply: Vec<DDLApply>,
}

pub(crate) struct ExecArenaLocalState<'b, 'a, T: Transaction + 'a> {
    transaction: *mut T,
    pub(crate) table_codec: &'b mut TableCodec,
    pub(crate) context: ExecutionContext<'a>,
    pub(crate) result: &'b mut ExecResult,
    pub(crate) plan_arena: &'b PlanArena<'a>,
    ddl_apply: &'b mut Vec<DDLApply>,
}

impl<'b, 'a, T: Transaction + 'a> ExecArenaLocalState<'b, 'a, T> {
    pub(crate) fn transaction(&self) -> &'a T {
        unsafe { &*self.transaction }
    }

    pub(crate) fn transaction_codec_mut(&mut self) -> (&mut T, &mut TableCodec) {
        unsafe { (&mut *self.transaction, &mut *self.table_codec) }
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
    pub(crate) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            result: ExecResult::default(),
            table_codec: TableCodec::default(),
            projection_tmp: Vec::new(),
            context: None,
            transaction: std::ptr::null_mut(),
            runtime_probe_stack: Vec::new(),
            ddl_apply: Vec::new(),
        }
    }
}

pub(crate) fn with_projection_tmp_value<'a, T: Transaction + 'a>(
    arena: &mut ExecArena<'a, T>,
    tuple: Option<&dyn TupleLike>,
    exprs: &[ScalarExpression],
    f: impl FnOnce(&mut ExecArena<'a, T>, &DataValue) -> Result<(), DatabaseError>,
) -> Result<(), DatabaseError> {
    arena.with_projection_tmp(|arena, projection_tmp| {
        {
            let tuple = tuple.unwrap_or_else(|| arena.result_tuple() as &dyn TupleLike);
            projection_tmp.reserve(exprs.len());
            for expr in exprs.iter() {
                projection_tmp.push(expr.eval(Some(tuple))?);
            }
        }

        if projection_tmp.len() > 1 {
            let value = DataValue::Tuple(std::mem::take(projection_tmp), false);
            let ret = f(arena, &value);
            let DataValue::Tuple(values, _) = value else {
                unreachable!()
            };
            *projection_tmp = values;
            ret?;
        } else if let Some(value) = projection_tmp.first() {
            f(arena, value)?;
        }
        Ok(())
    })
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
        let id = self.nodes.len();
        self.nodes.push(node);
        id
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
        plan_arena: &'b PlanArena<'a>,
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

    #[inline]
    pub(crate) fn result_tuple(&self) -> &Tuple {
        &self.result.tuple
    }

    #[inline]
    pub(crate) fn result_tuple_mut(&mut self) -> &mut Tuple {
        &mut self.result.tuple
    }

    #[inline]
    pub(crate) fn with_projection_tmp<R>(
        &mut self,
        f: impl FnOnce(&mut Self, &mut Vec<DataValue>) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        let mut projection_tmp = std::mem::take(&mut self.projection_tmp);
        projection_tmp.clear();
        let ret = f(self, &mut projection_tmp);
        projection_tmp.clear();
        self.projection_tmp = projection_tmp;
        ret
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
        plan_arena: &mut PlanArena<'a>,
    ) -> Result<bool, DatabaseError> {
        self.result.status = None;
        let mut node = std::mem::replace(&mut self.nodes[id], ExecNode::Empty);
        let result = node.next_tuple(self, plan_arena);
        self.nodes[id] = node;
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
        plan_arena: &mut PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) trait WriteExecutor<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        plan_arena: &mut PlanArena<'a>,
        cache: ExecutionContext<'_>,
        transaction: &T,
    ) -> ExecId;
}

pub(crate) fn build_read<'a, T>(
    arena: &mut ExecArena<'a, T>,
    plan_arena: &mut PlanArena<'a>,
    plan: LogicalPlan,
    cache: ExecutionContext<'_>,
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
                    HashJoin::from((op, left, right)),
                    arena,
                    plan_arena,
                    cache,
                    transaction,
                )
            } else {
                <NestedLoopJoin as ReadExecutor<'a, T>>::into_executor(
                    NestedLoopJoin::from((op, left, right)),
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
                    return <IndexScan<'a, T> as ReadExecutor<'a, T>>::into_executor(
                        IndexScan::from((
                            op,
                            index_info.meta,
                            lookup,
                            index_info.covered_deserializers.clone(),
                            index_info.cover_mapping.clone(),
                        )),
                        arena,
                        plan_arena,
                        cache,
                        transaction,
                    );
                }
            }

            <SeqScan<'a, T> as ReadExecutor<'a, T>>::into_executor(
                SeqScan::from(op),
                arena,
                plan_arena,
                cache,
                transaction,
            )
        }
        Operator::FunctionScan(op) => <FunctionScan as ReadExecutor<'a, T>>::into_executor(
            FunctionScan::from(op),
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
            Values::from(op),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::ShowTable => <ShowTables<'a, T> as ReadExecutor<'a, T>>::into_executor(
            ShowTables { metas: None },
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::ShowView => <ShowViews<'a, T> as ReadExecutor<'a, T>>::into_executor(
            ShowViews { metas: None },
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Explain => <Explain as ReadExecutor<'a, T>>::into_executor(
            Explain::from(childrens.pop_only()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Describe(op) => <Describe as ReadExecutor<'a, T>>::into_executor(
            Describe::from(op),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::Union(_) => <Union as ReadExecutor<'a, T>>::into_executor(
            Union::from(childrens.pop_twins()),
            arena,
            plan_arena,
            cache,
            transaction,
        ),
        Operator::SetMembership(op) => {
            let (left, right) = childrens.pop_twins();
            <SetMembership as ReadExecutor<'a, T>>::into_executor(
                SetMembership::from((op.kind, left, right)),
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
    arena: &mut ExecArena<'a, T>,
    plan_arena: &mut PlanArena<'a>,
    plan: LogicalPlan,
    cache: ExecutionContext<'a>,
    transaction: &'a mut T,
) -> ExecId
where
    T: Transaction + 'a,
{
    arena.init_context(cache, transaction);
    let transaction_ref: &T = transaction;
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
                Insert::from((op, input)),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Update(op) => {
            let input = childrens.pop_only();

            <Update as WriteExecutor<'a, T>>::into_executor(
                Update::from((op, input)),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Delete(op) => {
            let input = childrens.pop_only();

            <Delete as WriteExecutor<'a, T>>::into_executor(
                Delete::from((op, input)),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::AddColumn(op) => <AddColumn as WriteExecutor<'a, T>>::into_executor(
            AddColumn::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::ChangeColumn(op) => <ChangeColumn as WriteExecutor<'a, T>>::into_executor(
            ChangeColumn::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropColumn(op) => <DropColumn as WriteExecutor<'a, T>>::into_executor(
            DropColumn::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::CreateTable(op) => <CreateTable as WriteExecutor<'a, T>>::into_executor(
            CreateTable::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::CreateIndex(op) => {
            let input = childrens.pop_only();

            <CreateIndex as WriteExecutor<'a, T>>::into_executor(
                CreateIndex::from((op, input)),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::CreateView(op) => <CreateView as WriteExecutor<'a, T>>::into_executor(
            CreateView::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropTable(op) => <DropTable as WriteExecutor<'a, T>>::into_executor(
            DropTable::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropView(op) => <DropView as WriteExecutor<'a, T>>::into_executor(
            DropView::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::DropIndex(op) => <DropIndex as WriteExecutor<'a, T>>::into_executor(
            DropIndex::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        Operator::Truncate(op) => <Truncate as WriteExecutor<'a, T>>::into_executor(
            Truncate::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        #[cfg(feature = "copy")]
        Operator::CopyFromFile(op) => <CopyFromFile as WriteExecutor<'a, T>>::into_executor(
            CopyFromFile::from(op),
            arena,
            plan_arena,
            cache,
            transaction_ref,
        ),
        #[cfg(feature = "copy")]
        Operator::CopyToFile(op) => {
            let input = childrens.pop_only();

            <CopyToFile as ReadExecutor<'a, T>>::into_executor(
                CopyToFile::from((op, input)),
                arena,
                plan_arena,
                cache,
                transaction_ref,
            )
        }
        Operator::Analyze(op) => {
            let input = childrens.pop_only();

            <Analyze as WriteExecutor<'a, T>>::into_executor(
                Analyze::from((op, input)),
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
        cache: ExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: ReadExecutor<'a, T, Input = E>,
    {
        let mut arena = ExecArena::new();
        arena.init_context(cache, transaction);
        let root = <E as ReadExecutor<'a, T>>::into_executor(
            executor,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        TestExecutor {
            executor: Executor::new(arena, root),
            plan_arena,
        }
    }

    pub(crate) fn execute_mut<'a, T, E>(
        executor: E,
        cache: ExecutionContext<'a>,
        mut plan_arena: PlanArena<'a>,
        transaction: &'a T,
    ) -> TestExecutor<'a, T>
    where
        T: Transaction + 'a,
        E: WriteExecutor<'a, T, Input = E>,
    {
        let mut arena = ExecArena::new();
        arena.init_context(cache, transaction);
        let root = <E as WriteExecutor<'a, T>>::into_executor(
            executor,
            &mut arena,
            &mut plan_arena,
            cache,
            transaction,
        );
        TestExecutor {
            executor: Executor::new(arena, root),
            plan_arena,
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
            executor: Executor::new(arena, root),
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
            executor: Executor::new(arena, root),
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
pub(crate) use test_utils::{
    empty_context, execute, execute_input, execute_input_mut, execute_mut, try_collect,
};
