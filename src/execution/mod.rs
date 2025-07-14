pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;
pub(crate) mod marco;

use self::ddl::add_column::AddColumn;
use self::dql::join::nested_loop_join::NestedLoopJoin;
use crate::errors::DatabaseError;
use crate::execution::ddl::create_index::CreateIndex;
use crate::execution::ddl::create_table::CreateTable;
use crate::execution::ddl::create_view::CreateView;
use crate::execution::ddl::drop_column::DropColumn;
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
use crate::execution::dql::describe::Describe;
use crate::execution::dql::dummy::Dummy;
use crate::execution::dql::explain::Explain;
use crate::execution::dql::filter::Filter;
use crate::execution::dql::function_scan::FunctionScan;
use crate::execution::dql::index_scan::IndexScan;
use crate::execution::dql::join::hash_join::HashJoin;
use crate::execution::dql::limit::Limit;
use crate::execution::dql::projection::Projection;
use crate::execution::dql::seq_scan::SeqScan;
use crate::execution::dql::show_table::ShowTables;
use crate::execution::dql::show_view::ShowViews;
use crate::execution::dql::sort::Sort;
use crate::execution::dql::union::Union;
use crate::execution::dql::values::Values;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::{Operator, PhysicalOption};
use crate::planner::LogicalPlan;
use crate::storage::{StatisticsMetaCache, TableCache, Transaction, ViewCache};
use crate::types::index::IndexInfo;
use crate::types::tuple::Tuple;
use std::ops::Coroutine;

pub type Executor<'a> =
    Box<dyn Coroutine<Yield = Result<Tuple, DatabaseError>, Return = ()> + 'a + Unpin>;

pub trait ReadExecutor<'a, T: Transaction + 'a> {
    fn execute(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a>;
}

pub trait WriteExecutor<'a, T: Transaction + 'a> {
    fn execute_mut(
        self,
        cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
        transaction: *mut T,
    ) -> Executor<'a>;
}

pub fn build_read<'a, T: Transaction + 'a>(
    plan: LogicalPlan,
    cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
    transaction: *mut T,
) -> Executor<'a> {
    let LogicalPlan {
        operator,
        childrens,
        ..
    } = plan;

    match operator {
        Operator::Dummy => Dummy {}.execute(cache, transaction),
        Operator::Aggregate(op) => {
            let input = childrens.pop_only();

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(cache, transaction)
            } else {
                HashAggExecutor::from((op, input)).execute(cache, transaction)
            }
        }
        Operator::Filter(op) => {
            let input = childrens.pop_only();

            Filter::from((op, input)).execute(cache, transaction)
        }
        Operator::Join(op) => {
            let (left_input, right_input) = childrens.pop_twins();

            match &op.on {
                JoinCondition::On { on, .. }
                    if !on.is_empty() && plan.physical_option == Some(PhysicalOption::HashJoin) =>
                {
                    HashJoin::from((op, left_input, right_input)).execute(cache, transaction)
                }
                _ => {
                    NestedLoopJoin::from((op, left_input, right_input)).execute(cache, transaction)
                }
            }
        }
        Operator::Project(op) => {
            let input = childrens.pop_only();

            Projection::from((op, input)).execute(cache, transaction)
        }
        Operator::TableScan(op) => {
            if let Some(PhysicalOption::IndexScan(IndexInfo {
                meta,
                range: Some(range),
            })) = plan.physical_option
            {
                IndexScan::from((op, meta, range)).execute(cache, transaction)
            } else {
                SeqScan::from(op).execute(cache, transaction)
            }
        }
        Operator::FunctionScan(op) => FunctionScan::from(op).execute(cache, transaction),
        Operator::Sort(op) => {
            let input = childrens.pop_only();

            Sort::from((op, input)).execute(cache, transaction)
        }
        Operator::Limit(op) => {
            let input = childrens.pop_only();

            Limit::from((op, input)).execute(cache, transaction)
        }
        Operator::Values(op) => Values::from(op).execute(cache, transaction),
        Operator::ShowTable => ShowTables.execute(cache, transaction),
        Operator::ShowView => ShowViews.execute(cache, transaction),
        Operator::Explain => {
            let input = childrens.pop_only();

            Explain::from(input).execute(cache, transaction)
        }
        Operator::Describe(op) => Describe::from(op).execute(cache, transaction),
        Operator::Union(_) => {
            let (left_input, right_input) = childrens.pop_twins();

            Union::from((left_input, right_input)).execute(cache, transaction)
        }
        _ => unreachable!(),
    }
}

pub fn build_write<'a, T: Transaction + 'a>(
    plan: LogicalPlan,
    cache: (&'a TableCache, &'a ViewCache, &'a StatisticsMetaCache),
    transaction: *mut T,
) -> Executor<'a> {
    let LogicalPlan {
        operator,
        childrens,
        physical_option,
        _output_schema_ref,
    } = plan;

    match operator {
        Operator::Insert(op) => {
            let input = childrens.pop_only();

            Insert::from((op, input)).execute_mut(cache, transaction)
        }
        Operator::Update(op) => {
            let input = childrens.pop_only();

            Update::from((op, input)).execute_mut(cache, transaction)
        }
        Operator::Delete(op) => {
            let input = childrens.pop_only();

            Delete::from((op, input)).execute_mut(cache, transaction)
        }
        Operator::AddColumn(op) => {
            let input = childrens.pop_only();
            AddColumn::from((op, input)).execute_mut(cache, transaction)
        }
        Operator::DropColumn(op) => {
            let input = childrens.pop_only();
            DropColumn::from((op, input)).execute_mut(cache, transaction)
        }
        Operator::CreateTable(op) => CreateTable::from(op).execute_mut(cache, transaction),
        Operator::CreateIndex(op) => {
            let input = childrens.pop_only();

            CreateIndex::from((op, input)).execute_mut(cache, transaction)
        }
        Operator::CreateView(op) => CreateView::from(op).execute_mut(cache, transaction),
        Operator::DropTable(op) => DropTable::from(op).execute_mut(cache, transaction),
        Operator::DropView(op) => DropView::from(op).execute_mut(cache, transaction),
        Operator::Truncate(op) => Truncate::from(op).execute_mut(cache, transaction),
        Operator::CopyFromFile(op) => CopyFromFile::from(op).execute_mut(cache, transaction),
        Operator::CopyToFile(op) => {
            let input = childrens.pop_only();

            CopyToFile::from((op, input)).execute(cache, transaction)
        }

        Operator::Analyze(op) => {
            let input = childrens.pop_only();

            Analyze::from((op, input)).execute_mut(cache, transaction)
        }
        operator => build_read(
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

#[cfg(test)]
pub fn try_collect(mut executor: Executor) -> Result<Vec<Tuple>, DatabaseError> {
    let mut output = Vec::new();

    while let std::ops::CoroutineState::Yielded(tuple) =
        std::pin::Pin::new(&mut executor).resume(())
    {
        output.push(tuple?);
    }
    Ok(output)
}
