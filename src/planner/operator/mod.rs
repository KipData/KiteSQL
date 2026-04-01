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

pub mod aggregate;
pub mod alter_table;
pub mod analyze;
pub mod copy_from_file;
pub mod copy_to_file;
pub mod create_index;
pub mod create_table;
pub mod create_view;
pub mod delete;
pub mod describe;
pub mod drop_index;
pub mod drop_table;
pub mod drop_view;
pub mod except;
pub mod filter;
pub mod function_scan;
pub mod insert;
pub mod join;
pub mod limit;
pub mod mark_apply;
pub mod project;
pub mod scalar_apply;
pub mod scalar_subquery;
pub mod sort;
pub mod table_scan;
pub mod top_k;
pub mod truncate;
pub mod union;
pub mod update;
pub mod values;

use self::{
    aggregate::AggregateOperator, alter_table::add_column::AddColumnOperator,
    alter_table::change_column::ChangeColumnOperator, filter::FilterOperator,
    join::JoinOperator, limit::LimitOperator, mark_apply::MarkApplyOperator,
    project::ProjectOperator, scalar_apply::ScalarApplyOperator,
    scalar_subquery::ScalarSubqueryOperator, sort::SortOperator, table_scan::TableScanOperator,
};
use crate::catalog::ColumnRef;
use crate::expression::ScalarExpression;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::analyze::AnalyzeOperator;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::create_view::CreateViewOperator;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::describe::DescribeOperator;
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::drop_view::DropViewOperator;
use crate::planner::operator::except::ExceptOperator;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::types::index::IndexInfo;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Operator {
    // DQL
    Dummy,
    Aggregate(AggregateOperator),
    ScalarApply(ScalarApplyOperator),
    MarkApply(MarkApplyOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    ScalarSubquery(ScalarSubqueryOperator),
    TableScan(TableScanOperator),
    FunctionScan(FunctionScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
    TopK(TopKOperator),
    Values(ValuesOperator),
    ShowTable,
    ShowView,
    Explain,
    Describe(DescribeOperator),
    Except(ExceptOperator),
    Union(UnionOperator),
    // DML
    Insert(InsertOperator),
    Update(UpdateOperator),
    Delete(DeleteOperator),
    Analyze(AnalyzeOperator),
    // DDL
    AddColumn(AddColumnOperator),
    ChangeColumn(ChangeColumnOperator),
    DropColumn(DropColumnOperator),
    CreateTable(CreateTableOperator),
    CreateIndex(CreateIndexOperator),
    CreateView(CreateViewOperator),
    DropTable(DropTableOperator),
    DropView(DropViewOperator),
    DropIndex(DropIndexOperator),
    Truncate(TruncateOperator),
    // Copy
    CopyFromFile(CopyFromFileOperator),
    CopyToFile(CopyToFileOperator),
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum SortOption {
    OrderBy {
        fields: Vec<SortField>,
        // When indexing, the output columns can ignore the order of the first few columns due to equality queries in the range prefix, thus satisfying diverse sort_fields.
        // e.g.: index (c1, c2, c3) range where c1 = 1, c2 = 2, c3 > 3,
        // sort_fields can be c1, c2, c3, or even just c2, c3, in which case ignore_prefix_len is 2.
        ignore_prefix_len: usize,
    },
    Follow,
    None,
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct PhysicalOption {
    pub plan: PlanImpl,
    sort_option: SortOption,
}

impl PhysicalOption {
    pub fn new(plan: PlanImpl, sort_option: SortOption) -> Self {
        Self { plan, sort_option }
    }

    pub fn sort_option(&self) -> &SortOption {
        &self.sort_option
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum PlanImpl {
    Dummy,
    SimpleAggregate,
    HashAggregate,
    StreamDistinct,
    ScalarApply,
    MarkApply,
    Filter,
    HashJoin,
    NestLoopJoin,
    Project,
    ScalarSubquery,
    SeqScan,
    FunctionScan,
    IndexScan(Box<IndexInfo>),
    Sort,
    Limit,
    TopK,
    Values,
    Insert,
    Update,
    Delete,
    AddColumn,
    ChangeColumn,
    DropColumn,
    CreateTable,
    DropTable,
    Truncate,
    Show,
    CopyFromFile,
    CopyToFile,
    Analyze,
}

impl Operator {
    pub fn output_exprs(&self, output_exprs: &mut Vec<ScalarExpression>) -> bool {
        match self {
            Operator::Dummy => false,
            Operator::Aggregate(op) => {
                output_exprs.clear();
                output_exprs.extend(op.agg_calls.iter().chain(op.groupby_exprs.iter()).cloned());
                true
            }
            Operator::ScalarApply(_)
            | Operator::MarkApply(_)
            | Operator::Filter(_)
            | Operator::Join(_)
            | Operator::ScalarSubquery(_) => false,
            Operator::Project(op) => {
                output_exprs.clear();
                output_exprs.extend(op.exprs.iter().cloned());
                true
            }
            Operator::TableScan(op) => {
                output_exprs.clear();
                output_exprs.extend(op.columns.values().enumerate().map(|(position, column)| {
                    ScalarExpression::column_expr(column.clone(), position)
                }));
                true
            }
            Operator::Sort(_) | Operator::Limit(_) | Operator::TopK(_) => false,
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            })
            | Operator::Except(ExceptOperator {
                left_schema_ref: schema_ref,
                ..
            }) => {
                output_exprs.clear();
                output_exprs.extend(
                    schema_ref
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(position, column)| ScalarExpression::column_expr(column, position)),
                );
                true
            }
            Operator::FunctionScan(op) => {
                output_exprs.clear();
                output_exprs.extend(
                    op.table_function
                        .inner
                        .output_schema()
                        .iter()
                        .enumerate()
                        .map(|(position, column)| {
                            ScalarExpression::column_expr(column.clone(), position)
                        }),
                );
                true
            }
            Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_)
            | Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_) => false,
        }
    }

    pub fn visit_referenced_columns(
        &self,
        only_column_ref: bool,
        f: &mut impl FnMut(&ColumnRef) -> bool,
    ) -> bool {
        match self {
            Operator::Aggregate(op) => op
                .agg_calls
                .iter()
                .chain(op.groupby_exprs.iter())
                .all(|expr| expr.visit_referenced_columns(only_column_ref, f)),
            Operator::ScalarApply(_) => true,
            Operator::MarkApply(op) => op
                .predicates()
                .iter()
                .all(|expr| expr.visit_referenced_columns(only_column_ref, f)),
            Operator::Filter(op) => op.predicate.visit_referenced_columns(only_column_ref, f),
            Operator::Join(op) => {
                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        if !left_expr.visit_referenced_columns(only_column_ref, f)
                            || !right_expr.visit_referenced_columns(only_column_ref, f)
                        {
                            return false;
                        }
                    }

                    if let Some(filter_expr) = filter {
                        return filter_expr.visit_referenced_columns(only_column_ref, f);
                    }
                }
                true
            }
            Operator::Project(op) => op
                .exprs
                .iter()
                .all(|expr| expr.visit_referenced_columns(only_column_ref, f)),
            Operator::ScalarSubquery(_) => true,
            Operator::TableScan(op) => op.columns.values().all(f),
            Operator::FunctionScan(op) => op
                .table_function
                .args
                .iter()
                .all(|expr| expr.visit_referenced_columns(only_column_ref, f)),
            Operator::Sort(op) => op
                .sort_fields
                .iter()
                .map(|field| &field.expr)
                .all(|expr| expr.visit_referenced_columns(only_column_ref, f)),
            Operator::TopK(op) => op
                .sort_fields
                .iter()
                .map(|field| &field.expr)
                .all(|expr| expr.visit_referenced_columns(only_column_ref, f)),
            Operator::Values(ValuesOperator { schema_ref, .. }) => schema_ref.iter().all(f),
            Operator::Union(UnionOperator {
                left_schema_ref,
                _right_schema_ref,
            })
            | Operator::Except(ExceptOperator {
                left_schema_ref,
                _right_schema_ref,
            }) => left_schema_ref
                .iter()
                .chain(_right_schema_ref.iter())
                .all(f),
            Operator::Analyze(_) => true,
            Operator::Delete(op) => op.primary_keys.iter().all(f),
            Operator::Dummy
            | Operator::Limit(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Update(_)
            | Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_) => true,
        }
    }

    pub fn any_referenced_column(
        &self,
        only_column_ref: bool,
        mut predicate: impl FnMut(&ColumnRef) -> bool,
    ) -> bool {
        let mut found = false;
        self.visit_referenced_columns(only_column_ref, &mut |column| {
            found = predicate(column);
            !found
        });
        found
    }

    pub fn all_referenced_columns(
        &self,
        only_column_ref: bool,
        mut predicate: impl FnMut(&ColumnRef) -> bool,
    ) -> bool {
        let mut all = true;
        self.visit_referenced_columns(only_column_ref, &mut |column| {
            all = predicate(column);
            all
        });
        all
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Operator::Dummy => write!(f, "Dummy"),
            Operator::Aggregate(op) => write!(f, "{op}"),
            Operator::ScalarApply(op) => write!(f, "{op}"),
            Operator::MarkApply(op) => write!(f, "{op}"),
            Operator::Filter(op) => write!(f, "{op}"),
            Operator::Join(op) => write!(f, "{op}"),
            Operator::Project(op) => write!(f, "{op}"),
            Operator::ScalarSubquery(op) => write!(f, "{op}"),
            Operator::TableScan(op) => write!(f, "{op}"),
            Operator::FunctionScan(op) => write!(f, "{op}"),
            Operator::Sort(op) => write!(f, "{op}"),
            Operator::Limit(op) => write!(f, "{op}"),
            Operator::TopK(op) => write!(f, "{op}"),
            Operator::Values(op) => write!(f, "{op}"),
            Operator::ShowTable => write!(f, "Show Tables"),
            Operator::ShowView => write!(f, "Show Views"),
            Operator::Explain => unreachable!(),
            Operator::Describe(op) => write!(f, "{op}"),
            Operator::Insert(op) => write!(f, "{op}"),
            Operator::Update(op) => write!(f, "{op}"),
            Operator::Delete(op) => write!(f, "{op}"),
            Operator::Analyze(op) => write!(f, "{op}"),
            Operator::AddColumn(op) => write!(f, "{op}"),
            Operator::ChangeColumn(op) => write!(f, "{op}"),
            Operator::DropColumn(op) => write!(f, "{op}"),
            Operator::CreateTable(op) => write!(f, "{op}"),
            Operator::CreateIndex(op) => write!(f, "{op}"),
            Operator::CreateView(op) => write!(f, "{op}"),
            Operator::DropTable(op) => write!(f, "{op}"),
            Operator::DropView(op) => write!(f, "{op}"),
            Operator::DropIndex(op) => write!(f, "{op}"),
            Operator::Truncate(op) => write!(f, "{op}"),
            Operator::CopyFromFile(op) => write!(f, "{op}"),
            Operator::CopyToFile(op) => write!(f, "{op}"),
            Operator::Union(op) => write!(f, "{op}"),
            Operator::Except(op) => write!(f, "{op}"),
        }
    }
}

impl fmt::Display for PhysicalOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} => (Sort Option: {})", self.plan, self.sort_option)?;
        Ok(())
    }
}

impl fmt::Display for SortOption {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SortOption::OrderBy {
                fields,
                ignore_prefix_len,
            } => {
                write!(f, "OrderBy: (")?;
                for (i, sort_field) in fields.iter().enumerate() {
                    write!(f, "{sort_field}")?;
                    if fields.len() - 1 != i {
                        write!(f, ", ")?;
                    }
                }
                write!(f, ") ignore_prefix_len: {ignore_prefix_len}")
            }
            SortOption::Follow => write!(f, "Follow"),
            SortOption::None => write!(f, "None"),
        }
    }
}

impl fmt::Display for PlanImpl {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PlanImpl::Dummy => write!(f, "Dummy"),
            PlanImpl::SimpleAggregate => write!(f, "SimpleAggregate"),
            PlanImpl::HashAggregate => write!(f, "HashAggregate"),
            PlanImpl::StreamDistinct => write!(f, "StreamDistinct"),
            PlanImpl::ScalarApply => write!(f, "ScalarApply"),
            PlanImpl::MarkApply => write!(f, "MarkApply"),
            PlanImpl::Filter => write!(f, "Filter"),
            PlanImpl::HashJoin => write!(f, "HashJoin"),
            PlanImpl::NestLoopJoin => write!(f, "NestLoopJoin"),
            PlanImpl::Project => write!(f, "Project"),
            PlanImpl::ScalarSubquery => write!(f, "ScalarSubquery"),
            PlanImpl::SeqScan => write!(f, "SeqScan"),
            PlanImpl::FunctionScan => write!(f, "FunctionScan"),
            PlanImpl::IndexScan(index) => write!(f, "IndexScan By {index}"),
            PlanImpl::Sort => write!(f, "Sort"),
            PlanImpl::Limit => write!(f, "Limit"),
            PlanImpl::TopK => write!(f, "TopK"),
            PlanImpl::Values => write!(f, "Values"),
            PlanImpl::Insert => write!(f, "Insert"),
            PlanImpl::Update => write!(f, "Update"),
            PlanImpl::Delete => write!(f, "Delete"),
            PlanImpl::AddColumn => write!(f, "AddColumn"),
            PlanImpl::ChangeColumn => write!(f, "ChangeColumn"),
            PlanImpl::DropColumn => write!(f, "DropColumn"),
            PlanImpl::CreateTable => write!(f, "CreateTable"),
            PlanImpl::DropTable => write!(f, "DropTable"),
            PlanImpl::Truncate => write!(f, "Truncate"),
            PlanImpl::Show => write!(f, "Show"),
            PlanImpl::CopyFromFile => write!(f, "CopyFromFile"),
            PlanImpl::CopyToFile => write!(f, "CopyToFile"),
            PlanImpl::Analyze => write!(f, "Analyze"),
        }
    }
}
