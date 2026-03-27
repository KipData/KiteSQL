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
use crate::execution::dql::except::Except;
use crate::execution::dql::explain::Explain;
use crate::execution::dql::filter::Filter;
use crate::execution::dql::function_scan::FunctionScan;
use crate::execution::dql::index_scan::IndexScan;
use crate::execution::dql::join::hash_join::HashJoin;
use crate::execution::dql::limit::Limit;
use crate::execution::dql::projection::Projection;
use crate::execution::dql::scalar_subquery::ScalarSubquery;
use crate::execution::dql::seq_scan::SeqScan;
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
use crate::types::index::IndexInfo;
use crate::types::tuple::Tuple;

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
    Except(Except),
    Explain(Explain),
    Filter(Filter),
    FunctionScan(FunctionScan),
    HashAgg(HashAggExecutor),
    HashJoin(HashJoin),
    IndexScan(IndexScan<'a, T>),
    Insert(Insert),
    Limit(Limit),
    NestedLoopJoin(NestedLoopJoin),
    Projection(Projection),
    ScalarSubquery(ScalarSubquery),
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

pub(crate) trait ExecNodeRunner<'a, T: Transaction + 'a> {
    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError>;
}

macro_rules! impl_exec_node_runner {
    ($($ty:ty),* $(,)?) => {
        $(
            impl<'a, T: Transaction + 'a> ExecNodeRunner<'a, T> for $ty {
                fn next_tuple(
                    &mut self,
                    arena: &mut ExecArena<'a, T>,
                ) -> Result<(), DatabaseError> {
                    <$ty>::next_tuple(self, arena)
                }
            }
        )*
    };
}

impl_exec_node_runner!(
    AddColumn,
    Analyze,
    ChangeColumn,
    CopyFromFile,
    CopyToFile,
    CreateIndex,
    CreateTable,
    CreateView,
    Delete,
    Describe,
    DropColumn,
    DropIndex,
    DropTable,
    DropView,
    Dummy,
    Except,
    Explain,
    Filter,
    FunctionScan,
    HashAggExecutor,
    HashJoin,
    IndexScan<'a, T>,
    Insert,
    Limit,
    NestedLoopJoin,
    Projection,
    ScalarSubquery,
    SeqScan<'a, T>,
    ShowTables,
    ShowViews,
    SimpleAggExecutor,
    Sort,
    StreamDistinctExecutor,
    TopK,
    Truncate,
    Union,
    Update,
    Values,
);

impl<'a, T: Transaction + 'a> ExecNodeRunner<'a, T> for ExecNode<'a, T> {
    fn next_tuple(&mut self, arena: &mut ExecArena<'a, T>) -> Result<(), DatabaseError> {
        match self {
            ExecNode::AddColumn(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Analyze(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::ChangeColumn(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::CopyFromFile(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::CopyToFile(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::CreateIndex(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::CreateTable(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::CreateView(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Delete(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Describe(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::DropColumn(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::DropIndex(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::DropTable(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::DropView(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Dummy(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Except(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Explain(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Filter(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::FunctionScan(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::HashAgg(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::HashJoin(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::IndexScan(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Insert(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Limit(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::NestedLoopJoin(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Projection(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::ScalarSubquery(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::SeqScan(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::ShowTables(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::ShowViews(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::SimpleAgg(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Sort(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::StreamDistinct(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::TopK(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Truncate(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Union(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Update(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Values(exec) => ExecNodeRunner::next_tuple(exec, arena),
            ExecNode::Empty => unreachable!("executor node re-entered while active"),
        }
    }
}

pub(crate) struct ExecArena<'a, T: Transaction + 'a> {
    nodes: Vec<ExecNode<'a, T>>,
    result: ExecResult,
    cache: Option<ExecutionCaches<'a>>,
    transaction: *mut T,
}

impl<'a, T: Transaction + 'a> Default for ExecArena<'a, T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
            result: ExecResult::default(),
            cache: None,
            transaction: std::ptr::null_mut(),
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

    #[inline]
    pub(crate) fn result_tuple(&self) -> &Tuple {
        &self.result.tuple
    }

    #[inline]
    pub(crate) fn result_tuple_mut(&mut self) -> &mut Tuple {
        &mut self.result.tuple
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
        let result = ExecNodeRunner::next_tuple(&mut node, self);
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
        ..
    } = plan;

    match operator {
        Operator::Dummy => Dummy::default().into_executor(arena, cache, transaction),
        Operator::Aggregate(op) => {
            let input = childrens.pop_only();

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).into_executor(arena, cache, transaction)
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
                StreamDistinctExecutor::from((op, input)).into_executor(arena, cache, transaction)
            } else {
                HashAggExecutor::from((op, input)).into_executor(arena, cache, transaction)
            }
        }
        Operator::Filter(op) => {
            let input = childrens.pop_only();

            Filter::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::Join(op) => {
            let (left_input, right_input) = childrens.pop_twins();

            match &op.on {
                JoinCondition::On { on, .. }
                    if !on.is_empty()
                        && matches!(
                            physical_option,
                            Some(PhysicalOption {
                                plan: PlanImpl::HashJoin,
                                ..
                            })
                        ) =>
                {
                    HashJoin::from((op, left_input, right_input)).into_executor(
                        arena,
                        cache,
                        transaction,
                    )
                }
                _ => NestedLoopJoin::from((op, left_input, right_input)).into_executor(
                    arena,
                    cache,
                    transaction,
                ),
            }
        }
        Operator::Project(op) => {
            let input = childrens.pop_only();

            Projection::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::ScalarSubquery(op) => {
            let input = childrens.pop_only();

            ScalarSubquery::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::TableScan(op) => {
            if let Some(PhysicalOption {
                plan: PlanImpl::IndexScan(index_info),
                ..
            }) = physical_option
            {
                let IndexInfo {
                    meta,
                    range,
                    covered_deserializers,
                    cover_mapping,
                    ..
                } = *index_info;
                if let Some(range) = range {
                    return IndexScan::from((
                        op,
                        meta,
                        range,
                        covered_deserializers,
                        cover_mapping,
                    ))
                    .into_executor(arena, cache, transaction);
                }
            }

            SeqScan::from(op).into_executor(arena, cache, transaction)
        }
        Operator::FunctionScan(op) => {
            FunctionScan::from(op).into_executor(arena, cache, transaction)
        }
        Operator::Sort(op) => {
            let input = childrens.pop_only();

            Sort::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::Limit(op) => {
            let input = childrens.pop_only();

            Limit::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::TopK(op) => {
            let input = childrens.pop_only();

            TopK::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::Values(op) => Values::from(op).into_executor(arena, cache, transaction),
        Operator::ShowTable => arena.push(ExecNode::ShowTables(ShowTables { metas: None })),
        Operator::ShowView => arena.push(ExecNode::ShowViews(ShowViews { metas: None })),
        Operator::Explain => {
            let input = childrens.pop_only();

            Explain::from(input).into_executor(arena, cache, transaction)
        }
        Operator::Describe(op) => Describe::from(op).into_executor(arena, cache, transaction),
        Operator::Union(_) => {
            let (left_input, right_input) = childrens.pop_twins();

            Union::from((left_input, right_input)).into_executor(arena, cache, transaction)
        }
        Operator::Except(_) => {
            let (left_input, right_input) = childrens.pop_twins();

            Except::from((left_input, right_input)).into_executor(arena, cache, transaction)
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

            Insert::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::Update(op) => {
            let input = childrens.pop_only();

            Update::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::Delete(op) => {
            let input = childrens.pop_only();

            Delete::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::AddColumn(op) => AddColumn::from(op).into_executor(arena, cache, transaction),
        Operator::ChangeColumn(op) => {
            ChangeColumn::from(op).into_executor(arena, cache, transaction)
        }
        Operator::DropColumn(op) => DropColumn::from(op).into_executor(arena, cache, transaction),
        Operator::CreateTable(op) => CreateTable::from(op).into_executor(arena, cache, transaction),
        Operator::CreateIndex(op) => {
            let input = childrens.pop_only();

            CreateIndex::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::CreateView(op) => CreateView::from(op).into_executor(arena, cache, transaction),
        Operator::DropTable(op) => DropTable::from(op).into_executor(arena, cache, transaction),
        Operator::DropView(op) => DropView::from(op).into_executor(arena, cache, transaction),
        Operator::DropIndex(op) => DropIndex::from(op).into_executor(arena, cache, transaction),
        Operator::Truncate(op) => Truncate::from(op).into_executor(arena, cache, transaction),
        Operator::CopyFromFile(op) => {
            CopyFromFile::from(op).into_executor(arena, cache, transaction)
        }
        Operator::CopyToFile(op) => {
            let input = childrens.pop_only();

            CopyToFile::from((op, input)).into_executor(arena, cache, transaction)
        }
        Operator::Analyze(op) => {
            let input = childrens.pop_only();

            Analyze::from((op, input)).into_executor(arena, cache, transaction)
        }
        operator => build_read(
            arena,
            LogicalPlan {
                operator,
                childrens,
                physical_option,
                _output_schema_ref,
            },
            cache,
            transaction,
        ),
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
pub fn try_collect<T: Transaction>(executor: Executor<'_, T>) -> Result<Vec<Tuple>, DatabaseError> {
    let mut executor = executor;
    let mut tuples = Vec::new();

    while let Some(tuple) = executor.next_tuple()? {
        tuples.push(tuple.clone());
    }
    Ok(tuples)
}
