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

pub(crate) type ExecutionCaches<'a> = (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache);
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

pub(crate) trait ExecutorNode<'a, T: Transaction + 'a>: Sized {
    type Input;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
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
                <ShowTables as ExecutorNode<'a, T>>::next_tuple(exec, arena)
            }
            ExecNode::ShowViews(exec) => {
                <ShowViews as ExecutorNode<'a, T>>::next_tuple(exec, arena)
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
    cache: Option<ExecutionCaches<'a>>,
    transaction: *mut T,
    runtime_probe_stack: Vec<RuntimeIndexProbe>,
}

impl<'a, T: Transaction + 'a> Default for ExecArena<'a, T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            result: ExecResult::default(),
            projection_tmp: Vec::new(),
            cache: None,
            transaction: std::ptr::null_mut(),
            runtime_probe_stack: Vec::new(),
        }
    }
}

impl<'a, T: Transaction + 'a> ExecArena<'a, T> {
    pub(crate) fn init_context(&mut self, cache: ExecutionCaches<'a>, transaction: *mut T) {
        if let Some(current) = self.cache {
            debug_assert!(std::ptr::eq(current.0, cache.0));
            debug_assert!(std::ptr::eq(current.1, cache.1));
            debug_assert!(std::ptr::eq(current.2, cache.2));
            debug_assert_eq!(self.transaction, transaction);
        } else {
            self.cache = Some(cache);
            self.transaction = transaction;
        }
    }

    pub(crate) fn push(&mut self, node: ExecNode<'a, T>) -> ExecId {
        let id = self.nodes.len();
        self.nodes.push(node);
        id
    }

    pub(crate) fn table_cache(&self) -> &'a TableCache {
        self.cache.expect("execution arena context initialized").0
    }

    pub(crate) fn view_cache(&self) -> &'a ViewCache {
        self.cache.expect("execution arena context initialized").1
    }

    pub(crate) fn meta_cache(&self) -> &'a StatisticsMetaCache {
        self.cache.expect("execution arena context initialized").2
    }

    pub(crate) fn transaction(&self) -> &'a T {
        unsafe { &*self.transaction }
    }

    pub(crate) fn transaction_mut(&mut self) -> &'a mut T {
        unsafe { &mut *self.transaction }
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
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId;
}

pub(crate) trait WriteExecutor<'a, T: Transaction + 'a>: Sized {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
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
                cache: ExecutionCaches<'a>,
                transaction: *mut T,
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
                cache: ExecutionCaches<'a>,
                transaction: *mut T,
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

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ShowTables {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::ShowTables(input))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        ShowTables::next_tuple(self, arena)
    }
}

impl<'a, T: Transaction + 'a> ExecutorNode<'a, T> for ShowViews {
    type Input = Self;

    fn into_executor(
        input: Self::Input,
        arena: &mut ExecArena<'a, T>,
        _: ExecutionCaches<'a>,
        _: *mut T,
    ) -> ExecId {
        arena.push(ExecNode::ShowViews(input))
    }

    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        ShowViews::next_tuple(self, arena)
    }
}

pub(crate) fn build_read<'a, T: Transaction + 'a>(
    arena: &mut ExecArena<'a, T>,
    plan: LogicalPlan,
    cache: ExecutionCaches<'a>,
    transaction: *mut T,
) -> ExecId {
    arena.init_context(cache, transaction);

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
        Operator::ShowTable => <ShowTables as ExecutorNode<'a, T>>::into_executor(
            ShowTables { metas: None },
            arena,
            cache,
            transaction,
        ),
        Operator::ShowView => <ShowViews as ExecutorNode<'a, T>>::into_executor(
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

pub(crate) fn build_write<'a, T: Transaction + 'a>(
    arena: &mut ExecArena<'a, T>,
    plan: LogicalPlan,
    cache: ExecutionCaches<'a>,
    transaction: *mut T,
) -> ExecId {
    arena.init_context(cache, transaction);

    let LogicalPlan {
        operator,
        childrens,
        physical_option,
        _output_schema_ref,
    } = plan;

    match operator {
        Operator::Insert(op) => {
            let input = childrens.pop_only();

            <Insert as ExecutorNode<'a, T>>::into_executor((op, input), arena, cache, transaction)
        }
        Operator::Update(op) => {
            let input = childrens.pop_only();

            <Update as ExecutorNode<'a, T>>::into_executor((op, input), arena, cache, transaction)
        }
        Operator::Delete(op) => {
            let input = childrens.pop_only();

            <Delete as ExecutorNode<'a, T>>::into_executor((op, input), arena, cache, transaction)
        }
        Operator::AddColumn(op) => {
            <AddColumn as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::ChangeColumn(op) => {
            <ChangeColumn as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::DropColumn(op) => {
            <DropColumn as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::CreateTable(op) => {
            <CreateTable as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::CreateIndex(op) => {
            let input = childrens.pop_only();

            <CreateIndex as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction,
            )
        }
        Operator::CreateView(op) => {
            <CreateView as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::DropTable(op) => {
            <DropTable as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::DropView(op) => {
            <DropView as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::DropIndex(op) => {
            <DropIndex as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::Truncate(op) => {
            <Truncate as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::CopyFromFile(op) => {
            <CopyFromFile as ExecutorNode<'a, T>>::into_executor(op, arena, cache, transaction)
        }
        Operator::CopyToFile(op) => {
            let input = childrens.pop_only();

            <CopyToFile as ExecutorNode<'a, T>>::into_executor(
                (op, input),
                arena,
                cache,
                transaction,
            )
        }
        Operator::Analyze(op) => {
            let input = childrens.pop_only();

            <Analyze as ExecutorNode<'a, T>>::into_executor((op, input), arena, cache, transaction)
        }
        operator => {
            let plan = LogicalPlan {
                operator,
                childrens,
                physical_option,
                _output_schema_ref,
            };
            build_read(arena, plan, cache, transaction)
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn execute<'a, T, E>(
    executor: E,
    cache: ExecutionCaches<'a>,
    transaction: *mut T,
) -> Executor<'a, T>
where
    T: Transaction + 'a,
    E: ReadExecutor<'a, T>,
{
    let mut arena = ExecArena::default();
    arena.init_context(cache, transaction);
    let root = executor.into_executor(&mut arena, cache, transaction);
    Executor::new(arena, root)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn execute_mut<'a, T, E>(
    executor: E,
    cache: ExecutionCaches<'a>,
    transaction: *mut T,
) -> Executor<'a, T>
where
    T: Transaction + 'a,
    E: WriteExecutor<'a, T>,
{
    let mut arena = ExecArena::default();
    arena.init_context(cache, transaction);
    let root = executor.into_executor(&mut arena, cache, transaction);
    Executor::new(arena, root)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn execute_input<'a, T, E>(
    input: E::Input,
    cache: ExecutionCaches<'a>,
    transaction: *mut T,
) -> Executor<'a, T>
where
    T: Transaction + 'a,
    E: ExecutorNode<'a, T>,
{
    let mut arena = ExecArena::default();
    arena.init_context(cache, transaction);
    let root = E::into_executor(input, &mut arena, cache, transaction);
    Executor::new(arena, root)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(dead_code)]
pub(crate) fn execute_input_mut<'a, T, E>(
    input: E::Input,
    cache: ExecutionCaches<'a>,
    transaction: *mut T,
) -> Executor<'a, T>
where
    T: Transaction + 'a,
    E: ExecutorNode<'a, T>,
{
    let mut arena = ExecArena::default();
    arena.init_context(cache, transaction);
    let root = E::into_executor(input, &mut arena, cache, transaction);
    Executor::new(arena, root)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub fn try_collect<T: Transaction>(executor: Executor<'_, T>) -> Result<Vec<Tuple>, DatabaseError> {
    let mut executor = executor;
    let mut tuples = Vec::new();

    while let Some(tuple) = executor.next_tuple()? {
        tuples.push(tuple.clone());
    }
    Ok(tuples)
}
