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
use crate::optimizer::core::memo::GroupExpression;
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
use crate::optimizer::rule::implementation::ddl::add_column::AddColumnImplementation;
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
            ImplementationRuleImpl::CreateTable => CreateTableImplementation.pattern(),
            ImplementationRuleImpl::DropColumn => DropColumnImplementation.pattern(),
            ImplementationRuleImpl::DropTable => DropTableImplementation.pattern(),
            ImplementationRuleImpl::Truncate => TruncateImplementation.pattern(),
            ImplementationRuleImpl::Analyze => AnalyzeImplementation.pattern(),
        }
    }
}

impl<T: Transaction> ImplementationRule<T> for ImplementationRuleImpl {
    fn to_expression(
        &self,
        operator: &Operator,
        loader: &StatisticMetaLoader<'_, T>,
        group_expr: &mut GroupExpression,
    ) -> Result<(), DatabaseError> {
        match self {
            ImplementationRuleImpl::GroupByAggregate => {
                GroupByAggregateImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::SimpleAggregate => {
                SimpleAggregateImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Dummy => {
                DummyImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Filter => {
                FilterImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::HashJoin => {
                JoinImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Limit => {
                LimitImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Projection => {
                ProjectionImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::SeqScan => {
                SeqScanImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::IndexScan => {
                IndexScanImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::FunctionScan => {
                FunctionScanImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Sort => {
                SortImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::TopK => {
                TopKImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Values => {
                ValuesImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::CopyFromFile => {
                CopyFromFileImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::CopyToFile => {
                CopyToFileImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Delete => {
                DeleteImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Insert => {
                InsertImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Update => {
                UpdateImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::AddColumn => {
                AddColumnImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::CreateTable => {
                CreateTableImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::DropColumn => {
                DropColumnImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::DropTable => {
                DropTableImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Truncate => {
                TruncateImplementation.to_expression(operator, loader, group_expr)?
            }
            ImplementationRuleImpl::Analyze => {
                AnalyzeImplementation.to_expression(operator, loader, group_expr)?
            }
        }

        Ok(())
    }
}
