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
pub(crate) mod macros;

use crate::errors::DatabaseError;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::{BestPhysicalOption, ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::rule::implementation::ddl::add_column::AddColumnImplementation;
use crate::optimizer::rule::implementation::ddl::change_column::ChangeColumnImplementation;
use crate::optimizer::rule::implementation::ddl::create_table::CreateTableImplementation;
use crate::optimizer::rule::implementation::ddl::drop_column::DropColumnImplementation;
use crate::optimizer::rule::implementation::ddl::drop_table::DropTableImplementation;
use crate::optimizer::rule::implementation::ddl::truncate::TruncateImplementation;
use crate::optimizer::rule::implementation::dml::analyze::AnalyzeImplementation;
use crate::optimizer::rule::implementation::dml::copy_from_file::CopyFromFileImplementation;
use crate::optimizer::rule::implementation::dml::copy_to_file::CopyToFileImplementation;
use crate::optimizer::rule::implementation::dml::delete::DeleteImplementation;
use crate::optimizer::rule::implementation::dml::insert::InsertImplementation;
use crate::optimizer::rule::implementation::dml::update::UpdateImplementation;
use crate::optimizer::rule::implementation::dql::aggregate::{
    GroupByAggregateImplementation, SimpleAggregateImplementation,
};
use crate::optimizer::rule::implementation::dql::dummy::DummyImplementation;
use crate::optimizer::rule::implementation::dql::filter::FilterImplementation;
use crate::optimizer::rule::implementation::dql::function_scan::FunctionScanImplementation;
use crate::optimizer::rule::implementation::dql::join::JoinImplementation;
use crate::optimizer::rule::implementation::dql::limit::LimitImplementation;
use crate::optimizer::rule::implementation::dql::projection::ProjectionImplementation;
use crate::optimizer::rule::implementation::dql::scalar_subquery::ScalarSubqueryImplementation;
use crate::optimizer::rule::implementation::dql::sort::SortImplementation;
use crate::optimizer::rule::implementation::dql::table_scan::{
    IndexScanImplementation, SeqScanImplementation,
};
use crate::optimizer::rule::implementation::dql::top_k::TopKImplementation;
use crate::optimizer::rule::implementation::dql::values::ValuesImplementation;
use crate::planner::operator::Operator;
use crate::storage::Transaction;

#[derive(Debug, Copy, Clone)]
pub enum ImplementationRuleImpl {
    // DQL
    GroupByAggregate,
    SimpleAggregate,
    Dummy,
    Filter,
    HashJoin,
    Limit,
    Projection,
    ScalarSubquery,
    SeqScan,
    FunctionScan,
    IndexScan,
    Sort,
    TopK,
    Values,
    // DML
    Analyze,
    CopyFromFile,
    CopyToFile,
    Delete,
    Insert,
    Update,
    // DDL
    AddColumn,
    ChangeColumn,
    CreateTable,
    DropColumn,
    DropTable,
    Truncate,
}

impl MatchPattern for ImplementationRuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            ImplementationRuleImpl::GroupByAggregate => GroupByAggregateImplementation.pattern(),
            ImplementationRuleImpl::SimpleAggregate => SimpleAggregateImplementation.pattern(),
            ImplementationRuleImpl::Dummy => DummyImplementation.pattern(),
            ImplementationRuleImpl::Filter => FilterImplementation.pattern(),
            ImplementationRuleImpl::HashJoin => JoinImplementation.pattern(),
            ImplementationRuleImpl::Limit => LimitImplementation.pattern(),
            ImplementationRuleImpl::Projection => ProjectionImplementation.pattern(),
            ImplementationRuleImpl::ScalarSubquery => ScalarSubqueryImplementation.pattern(),
            ImplementationRuleImpl::SeqScan => SeqScanImplementation.pattern(),
            ImplementationRuleImpl::IndexScan => IndexScanImplementation.pattern(),
            ImplementationRuleImpl::FunctionScan => FunctionScanImplementation.pattern(),
            ImplementationRuleImpl::Sort => SortImplementation.pattern(),
            ImplementationRuleImpl::TopK => TopKImplementation.pattern(),
            ImplementationRuleImpl::Values => ValuesImplementation.pattern(),
            ImplementationRuleImpl::CopyFromFile => CopyFromFileImplementation.pattern(),
            ImplementationRuleImpl::CopyToFile => CopyToFileImplementation.pattern(),
            ImplementationRuleImpl::Delete => DeleteImplementation.pattern(),
            ImplementationRuleImpl::Insert => InsertImplementation.pattern(),
            ImplementationRuleImpl::Update => UpdateImplementation.pattern(),
            ImplementationRuleImpl::AddColumn => AddColumnImplementation.pattern(),
            ImplementationRuleImpl::ChangeColumn => ChangeColumnImplementation.pattern(),
            ImplementationRuleImpl::CreateTable => CreateTableImplementation.pattern(),
            ImplementationRuleImpl::DropColumn => DropColumnImplementation.pattern(),
            ImplementationRuleImpl::DropTable => DropTableImplementation.pattern(),
            ImplementationRuleImpl::Truncate => TruncateImplementation.pattern(),
            ImplementationRuleImpl::Analyze => AnalyzeImplementation.pattern(),
        }
    }
}

impl<T: Transaction> ImplementationRule<T> for ImplementationRuleImpl {
    fn update_best_option(
        &self,
        operator: &Operator,
        loader: &StatisticMetaLoader<'_, T>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        match self {
            ImplementationRuleImpl::GroupByAggregate => GroupByAggregateImplementation
                .update_best_option(operator, loader, best_physical_option)?,
            ImplementationRuleImpl::SimpleAggregate => SimpleAggregateImplementation
                .update_best_option(operator, loader, best_physical_option)?,
            ImplementationRuleImpl::Dummy => {
                DummyImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Filter => {
                FilterImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::HashJoin => {
                JoinImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Limit => {
                LimitImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Projection => ProjectionImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::ScalarSubquery => ScalarSubqueryImplementation
                .update_best_option(operator, loader, best_physical_option)?,
            ImplementationRuleImpl::SeqScan => {
                SeqScanImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::IndexScan => IndexScanImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::FunctionScan => FunctionScanImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::Sort => {
                SortImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::TopK => {
                TopKImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Values => {
                ValuesImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::CopyFromFile => CopyFromFileImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::CopyToFile => CopyToFileImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::Delete => {
                DeleteImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Insert => {
                InsertImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Update => {
                UpdateImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::AddColumn => AddColumnImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::ChangeColumn => ChangeColumnImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::CreateTable => CreateTableImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::DropColumn => DropColumnImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::DropTable => DropTableImplementation.update_best_option(
                operator,
                loader,
                best_physical_option,
            )?,
            ImplementationRuleImpl::Truncate => {
                TruncateImplementation.update_best_option(operator, loader, best_physical_option)?
            }
            ImplementationRuleImpl::Analyze => {
                AnalyzeImplementation.update_best_option(operator, loader, best_physical_option)?
            }
        }

        Ok(())
    }
}
