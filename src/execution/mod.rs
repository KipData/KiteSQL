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

pub struct Executor<'a, T: Transaction + 'a> {
    arena: ExecArena<'a, T>,
    root: ExecId,
}

impl<'a, T: Transaction + 'a> Executor<'a, T> {
    pub(crate) fn new(arena: ExecArena<'a, T>, root: ExecId) -> Self {
        Self { arena, root }
    }
}

impl<T: Transaction> Iterator for Executor<'_, T> {
    type Item = Result<Tuple, DatabaseError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.arena.next_tuple(self.root).transpose()
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

pub(crate) struct ExecArena<'a, T: Transaction + 'a> {
    nodes: Vec<ExecNode<'a, T>>,
    cache: Option<ExecutionCaches<'a>>,
    transaction: *mut T,
}

impl<'a, T: Transaction + 'a> Default for ExecArena<'a, T> {
    fn default() -> Self {
        Self {
            nodes: Vec::new(),
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

    pub(crate) fn next_tuple(&mut self, id: ExecId) -> Result<Option<Tuple>, DatabaseError> {
        let node = std::mem::replace(&mut self.nodes[id], ExecNode::Empty);
        let (result, node) = match node {
            ExecNode::AddColumn(mut exec) => (exec.next_tuple(self), ExecNode::AddColumn(exec)),
            ExecNode::Analyze(mut exec) => (exec.next_tuple(self), ExecNode::Analyze(exec)),
            ExecNode::ChangeColumn(mut exec) => {
                (exec.next_tuple(self), ExecNode::ChangeColumn(exec))
            }
            ExecNode::CopyFromFile(mut exec) => {
                (exec.next_tuple(self), ExecNode::CopyFromFile(exec))
            }
            ExecNode::CopyToFile(mut exec) => (exec.next_tuple(self), ExecNode::CopyToFile(exec)),
            ExecNode::CreateIndex(mut exec) => (exec.next_tuple(self), ExecNode::CreateIndex(exec)),
            ExecNode::CreateTable(mut exec) => (exec.next_tuple(self), ExecNode::CreateTable(exec)),
            ExecNode::CreateView(mut exec) => (exec.next_tuple(self), ExecNode::CreateView(exec)),
            ExecNode::Delete(mut exec) => (exec.next_tuple(self), ExecNode::Delete(exec)),
            ExecNode::Describe(mut exec) => (exec.next_tuple(self), ExecNode::Describe(exec)),
            ExecNode::DropColumn(mut exec) => (exec.next_tuple(self), ExecNode::DropColumn(exec)),
            ExecNode::DropIndex(mut exec) => (exec.next_tuple(self), ExecNode::DropIndex(exec)),
            ExecNode::DropTable(mut exec) => (exec.next_tuple(self), ExecNode::DropTable(exec)),
            ExecNode::DropView(mut exec) => (exec.next_tuple(self), ExecNode::DropView(exec)),
            ExecNode::Dummy(mut exec) => (exec.next_tuple(self), ExecNode::Dummy(exec)),
            ExecNode::Except(mut exec) => (exec.next_tuple(self), ExecNode::Except(exec)),
            ExecNode::Explain(mut exec) => (exec.next_tuple(self), ExecNode::Explain(exec)),
            ExecNode::Filter(mut exec) => (exec.next_tuple(self), ExecNode::Filter(exec)),
            ExecNode::FunctionScan(mut exec) => {
                (exec.next_tuple(self), ExecNode::FunctionScan(exec))
            }
            ExecNode::HashAgg(mut exec) => (exec.next_tuple(self), ExecNode::HashAgg(exec)),
            ExecNode::HashJoin(mut exec) => (exec.next_tuple(self), ExecNode::HashJoin(exec)),
            ExecNode::IndexScan(mut exec) => (exec.next_tuple(self), ExecNode::IndexScan(exec)),
            ExecNode::Insert(mut exec) => (exec.next_tuple(self), ExecNode::Insert(exec)),
            ExecNode::Limit(mut exec) => (exec.next_tuple(self), ExecNode::Limit(exec)),
            ExecNode::NestedLoopJoin(mut exec) => {
                (exec.next_tuple(self), ExecNode::NestedLoopJoin(exec))
            }
            ExecNode::Projection(mut exec) => (exec.next_tuple(self), ExecNode::Projection(exec)),
            ExecNode::ScalarSubquery(mut exec) => {
                (exec.next_tuple(self), ExecNode::ScalarSubquery(exec))
            }
            ExecNode::SeqScan(mut exec) => (exec.next_tuple(self), ExecNode::SeqScan(exec)),
            ExecNode::ShowTables(mut exec) => (exec.next_tuple(self), ExecNode::ShowTables(exec)),
            ExecNode::ShowViews(mut exec) => (exec.next_tuple(self), ExecNode::ShowViews(exec)),
            ExecNode::SimpleAgg(mut exec) => (exec.next_tuple(self), ExecNode::SimpleAgg(exec)),
            ExecNode::Sort(mut exec) => (exec.next_tuple(self), ExecNode::Sort(exec)),
            ExecNode::StreamDistinct(mut exec) => {
                (exec.next_tuple(self), ExecNode::StreamDistinct(exec))
            }
            ExecNode::TopK(mut exec) => (exec.next_tuple(self), ExecNode::TopK(exec)),
            ExecNode::Truncate(mut exec) => (exec.next_tuple(self), ExecNode::Truncate(exec)),
            ExecNode::Union(mut exec) => (exec.next_tuple(self), ExecNode::Union(exec)),
            ExecNode::Update(mut exec) => (exec.next_tuple(self), ExecNode::Update(exec)),
            ExecNode::Values(mut exec) => (exec.next_tuple(self), ExecNode::Values(exec)),
            ExecNode::Empty => unreachable!("executor node re-entered while active"),
        };
        self.nodes[id] = node;
        result
    }
}

pub(crate) trait ReadExecutor<'a, T: Transaction + 'a>: Sized {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId;

    #[allow(dead_code)]
    fn execute(self, cache: ExecutionCaches<'a>, transaction: *mut T) -> Executor<'a, T> {
        let mut arena = ExecArena::default();
        arena.init_context(cache, transaction);
        let root = self.into_executor(&mut arena, cache, transaction);
        Executor::new(arena, root)
    }
}

pub(crate) trait WriteExecutor<'a, T: Transaction + 'a>: Sized {
    fn into_executor(
        self,
        arena: &mut ExecArena<'a, T>,
        cache: ExecutionCaches<'a>,
        transaction: *mut T,
    ) -> ExecId;

    #[allow(dead_code)]
    fn execute_mut(self, cache: ExecutionCaches<'a>, transaction: *mut T) -> Executor<'a, T> {
        let mut arena = ExecArena::default();
        arena.init_context(cache, transaction);
        let root = self.into_executor(&mut arena, cache, transaction);
        Executor::new(arena, root)
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
pub fn try_collect<T: Transaction>(executor: Executor<'_, T>) -> Result<Vec<Tuple>, DatabaseError> {
    executor.collect()
}
