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
pub mod drop_table;
pub mod drop_view;
pub mod filter;
pub mod function_scan;
pub mod insert;
pub mod join;
pub mod limit;
pub mod project;
pub mod sort;
pub mod table_scan;
pub mod truncate;
pub mod union;
pub mod update;
pub mod values;

use self::{
    aggregate::AggregateOperator, alter_table::add_column::AddColumnOperator,
    filter::FilterOperator, join::JoinOperator, limit::LimitOperator, project::ProjectOperator,
    sort::SortOperator, table_scan::TableScanOperator,
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
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::drop_view::DropViewOperator;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::types::index::IndexInfo;
use itertools::Itertools;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Operator {
    // DQL
    Dummy,
    Aggregate(AggregateOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    TableScan(TableScanOperator),
    FunctionScan(FunctionScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
    Values(ValuesOperator),
    Show,
    Explain,
    Describe(DescribeOperator),
    Union(UnionOperator),
    // DML
    Insert(InsertOperator),
    Update(UpdateOperator),
    Delete(DeleteOperator),
    Analyze(AnalyzeOperator),
    // DDL
    AddColumn(AddColumnOperator),
    DropColumn(DropColumnOperator),
    CreateTable(CreateTableOperator),
    CreateIndex(CreateIndexOperator),
    CreateView(CreateViewOperator),
    DropTable(DropTableOperator),
    DropView(DropViewOperator),
    Truncate(TruncateOperator),
    // Copy
    CopyFromFile(CopyFromFileOperator),
    CopyToFile(CopyToFileOperator),
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum PhysicalOption {
    Dummy,
    SimpleAggregate,
    HashAggregate,
    Filter,
    HashJoin,
    NestLoopJoin,
    Project,
    SeqScan,
    FunctionScan,
    IndexScan(IndexInfo),
    Sort,
    Limit,
    Values,
    Insert,
    Update,
    Delete,
    AddColumn,
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
    pub fn output_exprs(&self) -> Option<Vec<ScalarExpression>> {
        match self {
            Operator::Dummy => None,
            Operator::Aggregate(op) => Some(
                op.agg_calls
                    .iter()
                    .chain(op.groupby_exprs.iter())
                    .cloned()
                    .collect_vec(),
            ),
            Operator::Filter(_) | Operator::Join(_) => None,
            Operator::Project(op) => Some(op.exprs.clone()),
            Operator::TableScan(op) => Some(
                op.columns
                    .iter()
                    .cloned()
                    .map(|(_, column)| ScalarExpression::ColumnRef(column))
                    .collect_vec(),
            ),
            Operator::Sort(_) | Operator::Limit(_) => None,
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            }) => Some(
                schema_ref
                    .iter()
                    .cloned()
                    .map(ScalarExpression::ColumnRef)
                    .collect_vec(),
            ),
            Operator::FunctionScan(op) => Some(
                op.table_function
                    .inner
                    .output_schema()
                    .iter()
                    .map(|column| ScalarExpression::ColumnRef(column.clone()))
                    .collect_vec(),
            ),
            Operator::Show
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Analyze(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_) => None,
        }
    }

    pub fn referenced_columns(&self, only_column_ref: bool) -> Vec<ColumnRef> {
        match self {
            Operator::Aggregate(op) => op
                .agg_calls
                .iter()
                .chain(op.groupby_exprs.iter())
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Filter(op) => op.predicate.referenced_columns(only_column_ref),
            Operator::Join(op) => {
                let mut exprs = Vec::new();

                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        exprs.append(&mut left_expr.referenced_columns(only_column_ref));
                        exprs.append(&mut right_expr.referenced_columns(only_column_ref));
                    }

                    if let Some(filter_expr) = filter {
                        exprs.append(&mut filter_expr.referenced_columns(only_column_ref));
                    }
                }
                exprs
            }
            Operator::Project(op) => op
                .exprs
                .iter()
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::TableScan(op) => op
                .columns
                .iter()
                .map(|(_, column)| column)
                .cloned()
                .collect_vec(),
            Operator::FunctionScan(op) => op
                .table_function
                .args
                .iter()
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Sort(op) => op
                .sort_fields
                .iter()
                .map(|field| &field.expr)
                .flat_map(|expr| expr.referenced_columns(only_column_ref))
                .collect_vec(),
            Operator::Values(ValuesOperator { schema_ref, .. }) => Vec::clone(schema_ref),
            Operator::Union(UnionOperator {
                left_schema_ref,
                _right_schema_ref,
            }) => left_schema_ref
                .iter()
                .chain(_right_schema_ref.iter())
                .cloned()
                .collect_vec(),
            Operator::Analyze(_) => vec![],
            Operator::Delete(op) => op.primary_keys.clone(),
            Operator::Dummy
            | Operator::Limit(_)
            | Operator::Show
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::Insert(_)
            | Operator::Update(_)
            | Operator::AddColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::Truncate(_)
            | Operator::CopyFromFile(_)
            | Operator::CopyToFile(_) => vec![],
        }
    }
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Operator::Dummy => write!(f, "Dummy"),
            Operator::Aggregate(op) => write!(f, "{}", op),
            Operator::Filter(op) => write!(f, "{}", op),
            Operator::Join(op) => write!(f, "{}", op),
            Operator::Project(op) => write!(f, "{}", op),
            Operator::TableScan(op) => write!(f, "{}", op),
            Operator::FunctionScan(op) => write!(f, "{}", op),
            Operator::Sort(op) => write!(f, "{}", op),
            Operator::Limit(op) => write!(f, "{}", op),
            Operator::Values(op) => write!(f, "{}", op),
            Operator::Show => write!(f, "Show Tables"),
            Operator::Explain => unreachable!(),
            Operator::Describe(op) => write!(f, "{}", op),
            Operator::Insert(op) => write!(f, "{}", op),
            Operator::Update(op) => write!(f, "{}", op),
            Operator::Delete(op) => write!(f, "{}", op),
            Operator::Analyze(op) => write!(f, "{}", op),
            Operator::AddColumn(op) => write!(f, "{}", op),
            Operator::DropColumn(op) => write!(f, "{}", op),
            Operator::CreateTable(op) => write!(f, "{}", op),
            Operator::CreateIndex(op) => write!(f, "{}", op),
            Operator::CreateView(op) => write!(f, "{}", op),
            Operator::DropTable(op) => write!(f, "{}", op),
            Operator::DropView(op) => write!(f, "{}", op),
            Operator::Truncate(op) => write!(f, "{}", op),
            Operator::CopyFromFile(op) => write!(f, "{}", op),
            Operator::CopyToFile(op) => write!(f, "{}", op),
            Operator::Union(op) => write!(f, "{}", op),
        }
    }
}

impl fmt::Display for PhysicalOption {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            PhysicalOption::Dummy => write!(f, "Dummy"),
            PhysicalOption::SimpleAggregate => write!(f, "SimpleAggregate"),
            PhysicalOption::HashAggregate => write!(f, "HashAggregate"),
            PhysicalOption::Filter => write!(f, "Filter"),
            PhysicalOption::HashJoin => write!(f, "HashJoin"),
            PhysicalOption::NestLoopJoin => write!(f, "NestLoopJoin"),
            PhysicalOption::Project => write!(f, "Project"),
            PhysicalOption::SeqScan => write!(f, "SeqScan"),
            PhysicalOption::FunctionScan => write!(f, "FunctionScan"),
            PhysicalOption::IndexScan(index) => write!(f, "IndexScan By {}", index),
            PhysicalOption::Sort => write!(f, "Sort"),
            PhysicalOption::Limit => write!(f, "Limit"),
            PhysicalOption::Values => write!(f, "Values"),
            PhysicalOption::Insert => write!(f, "Insert"),
            PhysicalOption::Update => write!(f, "Update"),
            PhysicalOption::Delete => write!(f, "Delete"),
            PhysicalOption::AddColumn => write!(f, "AddColumn"),
            PhysicalOption::DropColumn => write!(f, "DropColumn"),
            PhysicalOption::CreateTable => write!(f, "CreateTable"),
            PhysicalOption::DropTable => write!(f, "DropTable"),
            PhysicalOption::Truncate => write!(f, "Truncate"),
            PhysicalOption::Show => write!(f, "Show"),
            PhysicalOption::CopyFromFile => write!(f, "CopyFromFile"),
            PhysicalOption::CopyToFile => write!(f, "CopyToFile"),
            PhysicalOption::Analyze => write!(f, "Analyze"),
        }
    }
}
