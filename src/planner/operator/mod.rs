// GRCOV_EXCL_START
#[cfg(test)]
use crate::planner::PlanArena;
// GRCOV_EXCL_STOP
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
#[cfg(feature = "copy")]
pub mod copy_from_file;
#[cfg(feature = "copy")]
pub mod copy_to_file;
pub mod create_index;
pub mod create_table;
pub mod create_view;
pub mod delete;
pub mod describe;
pub mod drop_index;
pub mod drop_table;
pub mod drop_view;
pub mod filter;
pub mod function_scan;
pub mod index_probe_join;
pub mod insert;
pub mod join;
pub mod limit;
pub mod mark_apply;
pub mod project;
pub mod recursive_cte;
pub mod scalar_query_init;
pub mod scalar_subquery;
pub mod set_membership;
pub mod sort;
pub mod table_scan;
pub mod top_k;
pub mod truncate;
pub mod union;
pub mod update;
pub mod values;
pub mod visitor;
pub mod visitor_mut;
pub mod window;

use self::recursive_cte::{RecursiveCteOperator, RecursiveScanOperator};
use self::{
    aggregate::AggregateOperator, alter_table::add_column::AddColumnOperator,
    alter_table::change_column::ChangeColumnOperator, filter::FilterOperator, join::JoinOperator,
    limit::LimitOperator, mark_apply::MarkApplyOperator, project::ProjectOperator,
    scalar_query_init::ScalarQueryInitOperator, scalar_subquery::ScalarSubqueryOperator,
    sort::SortOperator, table_scan::TableScanOperator,
};
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::visitor::{walk_expr, ExprVisitor};
use crate::expression::AliasType;
use crate::planner::operator::alter_table::change_column::DefaultChange as ColumnDefaultChange;
use crate::planner::operator::alter_table::drop_column::DropColumnOperator;
use crate::planner::operator::analyze::AnalyzeOperator;
#[cfg(feature = "copy")]
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
#[cfg(feature = "copy")]
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::create_index::CreateIndexOperator;
use crate::planner::operator::create_table::CreateTableOperator;
use crate::planner::operator::create_view::CreateViewOperator;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::describe::DescribeOperator;
use crate::planner::operator::drop_index::DropIndexOperator;
use crate::planner::operator::drop_table::DropTableOperator;
use crate::planner::operator::drop_view::DropViewOperator;
use crate::planner::operator::function_scan::FunctionScanOperator;
use crate::planner::operator::index_probe_join::IndexProbeJoinOperator;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::set_membership::SetMembershipOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::visitor::{walk_plan, ChildrenVisit, OperatorVisitor};
use crate::planner::{fmt_explain_list, Explain, ExprRef, LogicalPlan, MetaArena};
use crate::types::index::{IndexInfo, IndexMetaRef};
use kite_sql_serde_macros::ReferenceSerialization;
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Operator {
    // DQL
    Dummy,
    Aggregate(AggregateOperator),
    ScalarQueryInit(ScalarQueryInitOperator),
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
    SetMembership(SetMembershipOperator),
    Union(UnionOperator),
    RecursiveCte(RecursiveCteOperator),
    RecursiveScan(RecursiveScanOperator),
    IndexProbeJoin(IndexProbeJoinOperator),
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
    #[cfg(feature = "copy")]
    CopyFromFile(CopyFromFileOperator),
    #[cfg(feature = "copy")]
    CopyToFile(CopyToFileOperator),
    Window(window::WindowOperator),
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

#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum RowCountOption {
    Follow,
    Unknown,
    Estimated { rows: usize, ndv: Option<usize> },
}

// Relative CPU units: a row costs four units, an operator/hash operation one.
// This is the same scale as row = 1 and operator = 0.25; I/O is not modeled.
pub(crate) const CPU_ROW_WORK: usize = 4;
pub(crate) const CPU_OPERATOR_WORK: usize = 1;
// Rows assumed for a table without statistics (RFC 0001 §3.9).
pub(crate) const DEFAULT_TABLE_ROWS: usize = 1000;
// Rebuilding the inner executor for each outer row of an index probe.
pub(crate) const PROBE_RESET_WORK: usize = CPU_ROW_WORK;
// Hashing, inserting and later freeing one build row. Calibrated on LMDB, where a hash join
// costs ~7 scanned rows per build/probe pair (RFC 0001 §4.2).
pub(crate) const HASH_BUILD_WORK: usize = 6 * CPU_ROW_WORK;

pub(crate) fn rows_work(rows: usize, width: usize) -> usize {
    rows.saturating_mul(CPU_ROW_WORK.saturating_add(width.saturating_mul(CPU_OPERATOR_WORK)))
}

fn hash_build_work(build: &InputCost) -> usize {
    rows_work(build.rows, build.width).saturating_add(build.rows.saturating_mul(HASH_BUILD_WORK))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InputCost {
    pub(crate) rows: usize,
    pub(crate) width: usize,
    pub(crate) work: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinImpl {
    Hash,
    NestLoop { full: bool },
    IndexProbe { lookup_work: usize },
}

impl JoinImpl {
    pub(crate) fn work(
        self,
        left: &InputCost,
        right: &InputCost,
        out_rows: usize,
        out_width: usize,
    ) -> usize {
        let join = match self {
            // Build rows are stored whole; each probe row costs one hash lookup.
            JoinImpl::Hash => right
                .work
                .saturating_add(hash_build_work(left))
                .saturating_add(right.rows.saturating_mul(CPU_OPERATOR_WORK)),
            // The inner input is re-run and compared for every outer row.
            JoinImpl::NestLoop { full } => left
                .rows
                .saturating_mul(
                    right
                        .work
                        .saturating_add(right.rows.saturating_mul(CPU_OPERATOR_WORK)),
                )
                .saturating_add(if full { right.work } else { 0 }),
            JoinImpl::IndexProbe { lookup_work } => left.rows.saturating_mul(lookup_work),
        };
        left.work
            .saturating_add(join)
            .saturating_add(rows_work(out_rows, out_width))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlanCost {
    pub(crate) rows: Option<usize>,
    pub(crate) width: usize,
    pub(crate) startup: Option<usize>,
    pub(crate) total: Option<usize>,
}

impl PlanCost {
    pub(crate) fn input(&self) -> Option<InputCost> {
        Some(InputCost {
            rows: self.rows?,
            width: self.width,
            work: self.total?,
        })
    }
}

/// The inputs' costs, shaped like `Childrens`; `None` marks an input that cannot be priced.
pub(crate) enum ChildCosts {
    None,
    Only(Option<PlanCost>),
    Twins(Option<PlanCost>, Option<PlanCost>),
}

fn input(cost: &Option<PlanCost>) -> Option<InputCost> {
    cost.as_ref()?.input()
}

fn startup(cost: &Option<PlanCost>) -> Option<usize> {
    cost.as_ref()?.startup
}

#[derive(Debug, Clone)]
pub struct PhysicalOption {
    pub plan: PlanImpl,
    pub(crate) sort_option: SortOption,
    pub(crate) row_count: RowCountOption,
    pub(crate) lookup_work: Option<usize>,
    pub(crate) join_decided: bool,
}

impl PhysicalOption {
    pub fn new(plan: PlanImpl, sort_option: SortOption, row_count: RowCountOption) -> Self {
        Self {
            plan,
            sort_option,
            row_count,
            lookup_work: None,
            join_decided: false,
        }
    }

    pub fn sort_option(&self) -> &SortOption {
        &self.sort_option
    }

    fn index_probe_work(
        &self,
        left: &Option<PlanCost>,
        right: &Option<PlanCost>,
        rows: usize,
        cost: &PlanCost,
    ) -> Option<usize> {
        let right_cost = (*right)?;
        let probed = InputCost {
            rows: 1,
            width: right_cost.width,
            work: right_cost.startup?,
        };
        let lookup_work = self.lookup_work.unwrap_or_else(|| {
            probed
                .work
                .saturating_add(PROBE_RESET_WORK)
                .saturating_add(rows_work(probed.rows, probed.width))
        });
        Some(JoinImpl::IndexProbe { lookup_work }.work(&input(left)?, &probed, rows, cost.width))
    }

    /// Join formulas: RFC 0001 §4.2 (`rfc/0001-greedy-join-reorder-owns-join-decisions.md`);
    /// probe `lookup_work`: §4.3.
    pub(crate) fn calculate_work(
        &self,
        operator: &Operator,
        children: &ChildCosts,
        rows: Option<usize>,
        cost: &PlanCost,
    ) -> Option<usize> {
        match (&self.plan, operator, children) {
            (PlanImpl::Filter, Operator::Filter(_), ChildCosts::Only(child))
            | (PlanImpl::Project, Operator::Project(_), ChildCosts::Only(child)) => {
                Some(input(child)?.work)
            }
            (PlanImpl::MarkApply, Operator::MarkApply(op), ChildCosts::Twins(left, right))
                if op.parameterized_probe().is_some() && !op.materialize =>
            {
                self.index_probe_work(left, right, rows?, cost)
            }
            (
                PlanImpl::IndexProbeJoin,
                Operator::IndexProbeJoin(_),
                ChildCosts::Twins(left, right),
            ) => self.index_probe_work(left, right, rows?, cost),
            (PlanImpl::HashJoin, Operator::Join(_), ChildCosts::Twins(left, right)) => {
                Some(JoinImpl::Hash.work(&input(left)?, &input(right)?, rows?, cost.width))
            }
            (PlanImpl::NestLoopJoin, Operator::Join(join), ChildCosts::Twins(left, right)) => {
                let (outer, inner) = if join.join_type == join::JoinType::RightOuter {
                    (right, left)
                } else {
                    (left, right)
                };
                Some(
                    JoinImpl::NestLoop {
                        full: join.join_type == join::JoinType::Full,
                    }
                    .work(&input(outer)?, &input(inner)?, rows?, cost.width),
                )
            }
            _ => Some(cost.startup?.saturating_add(rows_work(rows?, cost.width))),
        }
    }

    pub(crate) fn calculate_startup_work(
        &self,
        operator: &Operator,
        children: &ChildCosts,
    ) -> Option<usize> {
        let work = |child: &Option<PlanCost>| Some(input(child)?.work);
        match (&self.plan, operator, children) {
            (
                PlanImpl::SeqScan | PlanImpl::IndexScan(_) | PlanImpl::Values | PlanImpl::Dummy,
                _,
                ChildCosts::None,
            ) => Some(0),
            (
                PlanImpl::Project
                | PlanImpl::Filter
                | PlanImpl::Limit
                | PlanImpl::StreamAggregate
                | PlanImpl::StreamDistinct,
                _,
                ChildCosts::Only(child),
            ) => startup(child),
            (PlanImpl::HashJoin, _, ChildCosts::Twins(left, right)) => {
                input(left).and_then(|build| {
                    Some(
                        build
                            .work
                            .saturating_add(hash_build_work(&build))
                            .saturating_add(startup(right)?),
                    )
                })
            }
            (PlanImpl::NestLoopJoin, _, ChildCosts::Twins(left, right)) => startup(left)
                .and_then(|left| startup(right).map(|right| left.saturating_add(right))),
            (PlanImpl::Sort | PlanImpl::TopK | PlanImpl::Window, _, ChildCosts::Only(child)) => {
                let keys = match operator {
                    Operator::Sort(op) => op.sort_fields.len(),
                    Operator::TopK(op) => op.sort_fields.len(),
                    Operator::Window(op) => op.sort_fields.len(),
                    _ => unreachable!(),
                };
                work(child).and_then(|read| {
                    let rows = child.as_ref()?.rows?;
                    let comparisons = if rows <= 1 {
                        0
                    } else {
                        ((rows as f64) * (rows as f64).log2()).ceil() as usize
                    };
                    Some(
                        read.saturating_add(
                            comparisons.saturating_mul(
                                2usize
                                    .saturating_mul(keys)
                                    .saturating_mul(CPU_OPERATOR_WORK),
                            ),
                        ),
                    )
                })
            }
            (
                PlanImpl::SimpleAggregate | PlanImpl::HashAggregate,
                Operator::Aggregate(op),
                ChildCosts::Only(child),
            ) => {
                let per_row = op
                    .agg_calls
                    .len()
                    .saturating_mul(CPU_OPERATOR_WORK)
                    .saturating_add(if matches!(self.plan, PlanImpl::HashAggregate) {
                        CPU_ROW_WORK.saturating_add(
                            op.groupby_exprs.len().saturating_mul(CPU_OPERATOR_WORK),
                        )
                    } else {
                        0
                    });
                work(child).and_then(|read| {
                    Some(read.saturating_add(child.as_ref()?.rows?.saturating_mul(per_row)))
                })
            }
            (PlanImpl::MarkApply, Operator::MarkApply(op), ChildCosts::Twins(left, right)) => {
                startup(left).and_then(|left| {
                    let right = if op.materialize {
                        work(right)
                    } else {
                        startup(right)
                    }?;
                    Some(left.saturating_add(right))
                })
            }
            (
                PlanImpl::IndexProbeJoin,
                Operator::IndexProbeJoin(_),
                ChildCosts::Twins(left, right),
            ) => startup(left)
                .and_then(|left| startup(right).map(|right| left.saturating_add(right))),
            (PlanImpl::ScalarQueryInit, _, ChildCosts::Twins(left, right)) => {
                startup(left).and_then(|left| work(right).map(|right| left.saturating_add(right)))
            }
            _ => None,
        }
    }
}

// Probe lookup work and Greedy's join decision are pass-local, not plan semantics or wire data.
impl PartialEq for PhysicalOption {
    fn eq(&self, other: &Self) -> bool {
        self.plan == other.plan
            && self.sort_option == other.sort_option
            && self.row_count == other.row_count
    }
}

impl Eq for PhysicalOption {}

impl Hash for PhysicalOption {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.plan.hash(state);
        self.sort_option.hash(state);
        self.row_count.hash(state);
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum PlanImpl {
    Dummy,
    SimpleAggregate,
    HashAggregate,
    StreamAggregate,
    StreamDistinct,
    ScalarQueryInit,
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
    #[cfg(feature = "copy")]
    CopyFromFile,
    #[cfg(feature = "copy")]
    CopyToFile,
    Analyze,
    Window,
    // Appended to keep the serialized indices of earlier variants stable.
    IndexProbeJoin,
}

impl Explain for ColumnRef {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        let column = arena.column(*self);
        if let Some(table_name) = column.table_name() {
            write!(f, "{}.{}", table_name, column.name())
        } else {
            f.write_str(column.name())
        }
    }
}

impl Explain for IndexMetaRef {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str(&arena.index(*self).name)
    }
}

macro_rules! impl_display_explain {
    ($( $(#[$meta:meta])* $ty:ty),* $(,)?) => {
        $(
            $(#[$meta])*
            impl Explain for $ty {
                fn fmt<A: MetaArena + ?Sized>(&self, _arena: &A, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    std::fmt::Display::fmt(self, f)
                }
            }
        )*
    };
}

impl_display_explain!(
    MarkApplyOperator,
    IndexProbeJoinOperator,
    ScalarSubqueryOperator,
    FunctionScanOperator,
    LimitOperator,
    DescribeOperator,
    InsertOperator,
    DeleteOperator,
    AddColumnOperator,
    DropColumnOperator,
    CreateTableOperator,
    CreateViewOperator,
    DropTableOperator,
    DropViewOperator,
    DropIndexOperator,
    TruncateOperator,
    #[cfg(feature = "copy")]
    CopyToFileOperator,
);

impl Explain for Operator {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            Operator::Dummy => f.write_str("Dummy"),
            Operator::Aggregate(op) => Explain::fmt(op, arena, f),
            Operator::ScalarQueryInit(op) => Explain::fmt(op, arena, f),
            Operator::MarkApply(op) => Explain::fmt(op, arena, f),
            Operator::IndexProbeJoin(op) => Explain::fmt(op, arena, f),
            Operator::Filter(op) => Explain::fmt(op, arena, f),
            Operator::Join(op) => Explain::fmt(op, arena, f),
            Operator::Project(op) => Explain::fmt(op, arena, f),
            Operator::ScalarSubquery(op) => Explain::fmt(op, arena, f),
            Operator::TableScan(op) => Explain::fmt(op, arena, f),
            Operator::FunctionScan(op) => Explain::fmt(op, arena, f),
            Operator::Sort(op) => Explain::fmt(op, arena, f),
            Operator::Limit(op) => Explain::fmt(op, arena, f),
            Operator::TopK(op) => Explain::fmt(op, arena, f),
            Operator::Values(op) => Explain::fmt(op, arena, f),
            Operator::ShowTable => f.write_str("Show Tables"),
            Operator::ShowView => f.write_str("Show Views"),
            Operator::Explain => unreachable!(),
            Operator::Describe(op) => Explain::fmt(op, arena, f),
            Operator::SetMembership(op) => Explain::fmt(op, arena, f),
            Operator::Union(op) => Explain::fmt(op, arena, f),
            Operator::RecursiveCte(op) => Explain::fmt(op, arena, f),
            Operator::RecursiveScan(op) => Explain::fmt(op, arena, f),
            Operator::Insert(op) => Explain::fmt(op, arena, f),
            Operator::Update(op) => Explain::fmt(op, arena, f),
            Operator::Delete(op) => Explain::fmt(op, arena, f),
            Operator::Analyze(op) => Explain::fmt(op, arena, f),
            Operator::AddColumn(op) => Explain::fmt(op, arena, f),
            Operator::ChangeColumn(op) => Explain::fmt(op, arena, f),
            Operator::DropColumn(op) => Explain::fmt(op, arena, f),
            Operator::CreateTable(op) => Explain::fmt(op, arena, f),
            Operator::CreateIndex(op) => Explain::fmt(op, arena, f),
            Operator::CreateView(op) => Explain::fmt(op, arena, f),
            Operator::DropTable(op) => Explain::fmt(op, arena, f),
            Operator::DropView(op) => Explain::fmt(op, arena, f),
            Operator::DropIndex(op) => Explain::fmt(op, arena, f),
            Operator::Truncate(op) => Explain::fmt(op, arena, f),
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(op) => Explain::fmt(op, arena, f),
            #[cfg(feature = "copy")]
            Operator::CopyToFile(op) => Explain::fmt(op, arena, f),
            Operator::Window(op) => Explain::fmt(op, arena, f),
        }
    }
}

/// Where a referenced-column visit starts: one operator, or a plan and all its inputs.
pub(crate) enum ColumnVisitRoot<'a> {
    Operator(&'a Operator),
    Plan(&'a LogicalPlan),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CommandType {
    DQL,
    DML,
    DDL,
    Analyze,
}

impl CommandType {
    /// Only queries leave storage untouched.
    pub fn is_read_only(self) -> bool {
        matches!(self, CommandType::DQL)
    }
}

impl Operator {
    /// Classifies a plan by its root, matching `command_type` of SQL statements.
    pub fn command_type(&self) -> CommandType {
        match self {
            Operator::Dummy
            | Operator::Aggregate(_)
            | Operator::ScalarQueryInit(_)
            | Operator::MarkApply(_)
            | Operator::Filter(_)
            | Operator::Join(_)
            | Operator::Project(_)
            | Operator::ScalarSubquery(_)
            | Operator::TableScan(_)
            | Operator::FunctionScan(_)
            | Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::TopK(_)
            | Operator::Values(_)
            | Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::SetMembership(_)
            | Operator::Union(_)
            | Operator::RecursiveCte(_)
            | Operator::RecursiveScan(_)
            | Operator::IndexProbeJoin(_)
            | Operator::Window(_) => CommandType::DQL,
            #[cfg(feature = "copy")]
            Operator::CopyToFile(_) => CommandType::DQL,
            Operator::Insert(_)
            | Operator::Update(_)
            | Operator::Delete(_)
            | Operator::Truncate(_) => CommandType::DML,
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(_) => CommandType::DML,
            Operator::Analyze(_) => CommandType::Analyze,
            Operator::AddColumn(_)
            | Operator::ChangeColumn(_)
            | Operator::DropColumn(_)
            | Operator::CreateTable(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropTable(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_) => CommandType::DDL,
        }
    }

    pub fn visit_referenced_columns<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut impl FnMut(&A, &ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        Self::referenced_columns(ColumnVisitRoot::Operator(self), arena, f)
    }

    /// Calls `f` for each referenced column until it returns false; returns whether it ran to the end.
    pub(crate) fn referenced_columns<A: MetaArena + ?Sized>(
        root: ColumnVisitRoot<'_>,
        arena: &A,
        f: &mut impl FnMut(&A, &ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        struct ReferencedColumnVisitor<'a, A: ?Sized, F> {
            arena: &'a A,
            f: &'a mut F,
            keep_going: bool,
        }

        impl<A, F> ExprVisitor<A> for ReferencedColumnVisitor<'_, A, F>
        where
            A: MetaArena + ?Sized,
            F: FnMut(&A, &ColumnRef) -> bool,
        {
            fn visit(&mut self, expr: ExprRef, arena: &A) -> Result<(), DatabaseError> {
                if self.keep_going {
                    walk_expr(self, expr, arena)?;
                }
                Ok(())
            }

            fn visit_column_ref(
                &mut self,
                column: &ColumnRef,
                _position: usize,
            ) -> Result<(), DatabaseError> {
                if self.keep_going {
                    self.keep_going = (self.f)(self.arena, column);
                }
                Ok(())
            }

            // The alias target column is still owned by the plan.
            fn visit_alias(
                &mut self,
                expr: ExprRef,
                alias: &AliasType,
                arena: &A,
            ) -> Result<(), DatabaseError> {
                if let AliasType::Expr(alias_expr) = alias {
                    self.visit(*alias_expr, arena)?;
                }
                self.visit(expr, arena)
            }
        }

        impl<'operator, A, F> OperatorVisitor<'operator> for ReferencedColumnVisitor<'_, A, F>
        where
            A: MetaArena + ?Sized,
            F: FnMut(&A, &ColumnRef) -> bool,
        {
            fn visit_plan(&mut self, plan: &'operator LogicalPlan) -> Result<(), DatabaseError> {
                if self.keep_going {
                    walk_plan(self, plan)?;
                }
                Ok(())
            }

            fn visit_aggregate(
                &mut self,
                op: &'operator AggregateOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for expr in op.agg_calls.iter().chain(&op.groupby_exprs) {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_scalar_query_init(
                &mut self,
                op: &'operator ScalarQueryInitOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for (_, expr) in &op.param_bindings {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_mark_apply(
                &mut self,
                op: &'operator MarkApplyOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for expr in &op.predicates {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                if let Some(expr) = &op.parameterized_probe {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                if let Some(expr) = op.guard {
                    ExprVisitor::visit(self, expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_filter(
                &mut self,
                op: &'operator FilterOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                ExprVisitor::visit(self, op.predicate, self.arena)
            }

            fn visit_join(
                &mut self,
                op: &'operator JoinOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        ExprVisitor::visit(self, *left_expr, self.arena)?;
                        ExprVisitor::visit(self, *right_expr, self.arena)?;
                    }
                    if let Some(expr) = filter {
                        ExprVisitor::visit(self, *expr, self.arena)?;
                    }
                }
                Ok(())
            }

            fn visit_project(
                &mut self,
                op: &'operator ProjectOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for expr in &op.exprs {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_table_scan(
                &mut self,
                op: &'operator TableScanOperator,
            ) -> Result<(), DatabaseError> {
                for (position, column) in op.columns().enumerate() {
                    ExprVisitor::visit_column_ref(self, &column, position)?;
                }
                Ok(())
            }

            fn visit_function_scan(
                &mut self,
                op: &'operator FunctionScanOperator,
            ) -> Result<(), DatabaseError> {
                for expr in &op.table_function.args {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_sort(
                &mut self,
                op: &'operator SortOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for field in &op.sort_fields {
                    ExprVisitor::visit(self, field.expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_window(
                &mut self,
                op: &'operator window::WindowOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for expr in op
                    .sort_fields
                    .iter()
                    .map(|field| &field.expr)
                    .chain(op.functions.iter().flat_map(|function| &function.args))
                {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_top_k(
                &mut self,
                op: &'operator TopKOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for field in &op.sort_fields {
                    ExprVisitor::visit(self, field.expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_values(&mut self, op: &'operator ValuesOperator) -> Result<(), DatabaseError> {
                for (position, column) in op.schema_ref.iter().enumerate() {
                    ExprVisitor::visit_column_ref(self, column, position)?;
                }
                Ok(())
            }

            fn visit_union(
                &mut self,
                op: &'operator UnionOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for (position, column) in op
                    .left_schema_ref
                    .iter()
                    .chain(&op._right_schema_ref)
                    .enumerate()
                {
                    ExprVisitor::visit_column_ref(self, column, position)?;
                }
                Ok(())
            }

            fn visit_recursive_cte(
                &mut self,
                op: &'operator RecursiveCteOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for (position, column) in op.schema_ref.iter().enumerate() {
                    ExprVisitor::visit_column_ref(self, column, position)?;
                }
                Ok(())
            }

            fn visit_recursive_scan(
                &mut self,
                op: &'operator RecursiveScanOperator,
            ) -> Result<(), DatabaseError> {
                for (position, column) in op.schema_ref.iter().enumerate() {
                    ExprVisitor::visit_column_ref(self, column, position)?;
                }
                Ok(())
            }

            fn visit_set_membership(
                &mut self,
                op: &'operator SetMembershipOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for (position, column) in op
                    .left_schema_ref
                    .iter()
                    .chain(&op._right_schema_ref)
                    .enumerate()
                {
                    ExprVisitor::visit_column_ref(self, column, position)?;
                }
                Ok(())
            }

            fn visit_delete(
                &mut self,
                op: &'operator DeleteOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for (position, column) in op.primary_keys.iter().enumerate() {
                    ExprVisitor::visit_column_ref(self, column, position)?;
                }
                Ok(())
            }

            fn visit_update(
                &mut self,
                op: &'operator UpdateOperator,
                _children: &mut ChildrenVisit<'operator>,
            ) -> Result<(), DatabaseError> {
                for (_, expr) in &op.value_exprs {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_add_column(
                &mut self,
                op: &'operator AddColumnOperator,
            ) -> Result<(), DatabaseError> {
                if let Some(expr) = &op.column.desc().default {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_change_column(
                &mut self,
                op: &'operator ChangeColumnOperator,
            ) -> Result<(), DatabaseError> {
                if let ColumnDefaultChange::Set(expr) = &op.default_change {
                    ExprVisitor::visit(self, *expr, self.arena)?;
                }
                Ok(())
            }

            fn visit_create_table(
                &mut self,
                op: &'operator CreateTableOperator,
            ) -> Result<(), DatabaseError> {
                for column in &op.columns {
                    if let Some(expr) = &column.desc().default {
                        ExprVisitor::visit(self, *expr, self.arena)?;
                    }
                }
                Ok(())
            }
        }

        let mut visitor = ReferencedColumnVisitor {
            arena,
            f,
            keep_going: true,
        };
        match root {
            ColumnVisitRoot::Operator(operator) => {
                visitor.visit_operator(operator, None, &mut ChildrenVisit::empty())?
            }
            ColumnVisitRoot::Plan(plan) => visitor.visit_plan(plan)?,
        }
        Ok(visitor.keep_going)
    }

    pub fn any_referenced_column(
        &self,
        arena: &(dyn MetaArena + '_),
        mut predicate: impl FnMut(&ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        let mut found = false;
        self.visit_referenced_columns(arena, &mut |_, column| {
            found = predicate(column);
            !found
        })?;
        Ok(found)
    }

    pub fn all_referenced_columns(
        &self,
        arena: &(dyn MetaArena + '_),
        mut predicate: impl FnMut(&ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        let mut all = true;
        self.visit_referenced_columns(arena, &mut |_, column| {
            all = predicate(column);
            all
        })?;
        Ok(all)
    }
}

impl Explain for PlanImpl {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            PlanImpl::Dummy => f.write_str("Dummy"),
            PlanImpl::SimpleAggregate => f.write_str("SimpleAggregate"),
            PlanImpl::HashAggregate => f.write_str("HashAggregate"),
            PlanImpl::StreamAggregate => f.write_str("StreamAggregate"),
            PlanImpl::StreamDistinct => f.write_str("StreamDistinct"),
            PlanImpl::ScalarQueryInit => f.write_str("ScalarQueryInit"),
            PlanImpl::MarkApply => f.write_str("MarkApply"),
            PlanImpl::IndexProbeJoin => f.write_str("IndexProbeJoin"),
            PlanImpl::Filter => f.write_str("Filter"),
            PlanImpl::HashJoin => f.write_str("HashJoin"),
            PlanImpl::NestLoopJoin => f.write_str("NestLoopJoin"),
            PlanImpl::Project => f.write_str("Project"),
            PlanImpl::ScalarSubquery => f.write_str("ScalarSubquery"),
            PlanImpl::SeqScan => f.write_str("SeqScan"),
            PlanImpl::FunctionScan => f.write_str("FunctionScan"),
            PlanImpl::IndexScan(index) => write!(f, "IndexScan By {}", index.explain(arena)),
            PlanImpl::Sort => f.write_str("Sort"),
            PlanImpl::Limit => f.write_str("Limit"),
            PlanImpl::TopK => f.write_str("TopK"),
            PlanImpl::Values => f.write_str("Values"),
            PlanImpl::Insert => f.write_str("Insert"),
            PlanImpl::Update => f.write_str("Update"),
            PlanImpl::Delete => f.write_str("Delete"),
            PlanImpl::AddColumn => f.write_str("AddColumn"),
            PlanImpl::ChangeColumn => f.write_str("ChangeColumn"),
            PlanImpl::DropColumn => f.write_str("DropColumn"),
            PlanImpl::CreateTable => f.write_str("CreateTable"),
            PlanImpl::DropTable => f.write_str("DropTable"),
            PlanImpl::Truncate => f.write_str("Truncate"),
            PlanImpl::Show => f.write_str("Show"),
            #[cfg(feature = "copy")]
            PlanImpl::CopyFromFile => f.write_str("CopyFromFile"),
            #[cfg(feature = "copy")]
            PlanImpl::CopyToFile => f.write_str("CopyToFile"),
            PlanImpl::Analyze => f.write_str("Analyze"),
            PlanImpl::Window => f.write_str("Window"),
        }
    }
}

impl Explain for SortOption {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match self {
            SortOption::OrderBy {
                fields,
                ignore_prefix_len,
            } => {
                f.write_str("OrderBy: (")?;
                fmt_explain_list(fields, ", ", arena, f)?;
                write!(f, ") ignore_prefix_len: {ignore_prefix_len}")
            }
            SortOption::Follow => f.write_str("Follow"),
            SortOption::None => f.write_str("None"),
        }
    }
}

impl Explain for PhysicalOption {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(
            f,
            "{} => (Sort Option: {})",
            self.plan.explain(arena),
            self.sort_option.explain(arena)
        )
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::view::View;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::expression::function::table::{
        ArcTableFunctionImpl, TableFunction, TableFunctionCatalog, TableFunctionImpl,
    };
    use crate::expression::ScalarExpression;
    use crate::function::numbers::Numbers;
    use crate::planner::operator::alter_table::change_column::{DefaultChange, NotNullChange};
    use crate::planner::operator::delete::DeleteOperator;
    use crate::planner::operator::mark_apply::MarkApplyQuantifier;
    use crate::planner::operator::set_membership::SetMembershipKind;
    use crate::planner::operator::sort::SortField;
    use crate::planner::operator::values::ValuesOperator;
    use crate::planner::test::PlanArenaTestExt;
    use crate::planner::ExprRef;
    use crate::planner::{Childrens, LogicalPlan, TableArenaCell};
    use crate::types::index::{IndexInfo, IndexMeta, IndexMetaRef, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    #[test]
    fn rows_work_charges_rows_without_columns() {
        assert_eq!(rows_work(3, 0), 3 * CPU_ROW_WORK);
        assert_eq!(rows_work(3, 2), 3 * (CPU_ROW_WORK + 2));
        assert_eq!(rows_work(usize::MAX, 1), usize::MAX);
    }

    #[test]
    fn join_work_includes_inputs_and_prefers_cheap_sides() {
        let small = InputCost {
            rows: 10,
            width: 1,
            work: 50,
        };
        let large = InputCost {
            rows: 1000,
            width: 1,
            work: 5000,
        };
        let output = rows_work(100, 2);
        // Both inputs read once, the build side stored and hashed, every probe row looked up once.
        assert_eq!(
            JoinImpl::Hash.work(&small, &large, 100, 2),
            50 + 5000 + rows_work(10, 1) + 10 * HASH_BUILD_WORK + 1000 + output
        );
        assert!(
            JoinImpl::Hash.work(&small, &large, 100, 2)
                < JoinImpl::Hash.work(&large, &small, 100, 2)
        );
        // The inner input is re-run and compared for every outer row.
        let nest_loop = JoinImpl::NestLoop { full: false };
        assert_eq!(
            nest_loop.work(&small, &large, 100, 2),
            50 + 10 * (5000 + 1000) + output
        );
        assert!(nest_loop.work(&small, &large, 100, 2) < nest_loop.work(&large, &small, 100, 2));
        // A full join reads the inner input once more for its unmatched rows.
        assert_eq!(
            JoinImpl::NestLoop { full: true }.work(&small, &large, 100, 2),
            nest_loop.work(&small, &large, 100, 2) + 5000
        );
        // The probed input is never read whole: a few outer rows beat hashing a large input.
        let probe = JoinImpl::IndexProbe { lookup_work: 30 };
        assert_eq!(probe.work(&small, &large, 100, 2), 50 + 10 * 30 + output);
        assert!(probe.work(&small, &large, 100, 2) < JoinImpl::Hash.work(&small, &large, 100, 2));
        let huge = InputCost {
            rows: usize::MAX,
            width: 1,
            work: usize::MAX,
        };
        assert_eq!(nest_loop.work(&huge, &small, 0, 2), usize::MAX);
    }

    fn column_catalog(name: &str) -> ColumnCatalog {
        ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        )
    }

    fn column(name: &str, arena: &mut PlanArena) -> ColumnRef {
        arena.alloc_column(column_catalog(name))
    }

    fn index_info(meta: IndexMetaRef) -> IndexInfo {
        IndexInfo {
            meta,
            sort_option: SortOption::None,
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_aggregate_hint: None,
        }
    }

    fn column_expr(column: ColumnRef, position: usize, arena: &mut PlanArena) -> ExprRef {
        arena.alloc_expression(ScalarExpression::column_expr(column, position))
    }

    fn referenced_columns(
        operator: &Operator,
        arena: &mut PlanArena,
    ) -> Result<Vec<ColumnRef>, DatabaseError> {
        let mut columns = Vec::new();
        operator.visit_referenced_columns(arena, &mut |_, column| {
            columns.push(*column);
            true
        })?;
        Ok(columns)
    }

    #[test]
    fn physical_option_and_sort_option_explain() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let sort_option = SortOption::OrderBy {
            fields: vec![SortField::new(
                arena.alloc_expression(ScalarExpression::from(1i32)),
                false,
                true,
            )],
            ignore_prefix_len: 2,
        };
        assert_eq!(
            sort_option.explain(&arena).to_string(),
            "OrderBy: (1 Desc Nulls First) ignore_prefix_len: 2"
        );
        assert_eq!(SortOption::Follow.explain(&arena).to_string(), "Follow");
        assert_eq!(SortOption::None.explain(&arena).to_string(), "None");

        let physical =
            PhysicalOption::new(PlanImpl::TopK, sort_option.clone(), RowCountOption::Unknown);
        assert_eq!(
            physical.explain(&arena).to_string(),
            "TopK => (Sort Option: OrderBy: (1 Desc Nulls First) ignore_prefix_len: 2)"
        );
        assert_eq!(physical.sort_option(), &sort_option);
    }

    #[test]
    fn plan_impl_explain_covers_physical_variants() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let cases = [
            (PlanImpl::Dummy, "Dummy"),
            (PlanImpl::SimpleAggregate, "SimpleAggregate"),
            (PlanImpl::HashAggregate, "HashAggregate"),
            (PlanImpl::StreamAggregate, "StreamAggregate"),
            (PlanImpl::StreamDistinct, "StreamDistinct"),
            (PlanImpl::ScalarQueryInit, "ScalarQueryInit"),
            (PlanImpl::MarkApply, "MarkApply"),
            (PlanImpl::IndexProbeJoin, "IndexProbeJoin"),
            (PlanImpl::Filter, "Filter"),
            (PlanImpl::HashJoin, "HashJoin"),
            (PlanImpl::NestLoopJoin, "NestLoopJoin"),
            (PlanImpl::Project, "Project"),
            (PlanImpl::ScalarSubquery, "ScalarSubquery"),
            (PlanImpl::SeqScan, "SeqScan"),
            (PlanImpl::FunctionScan, "FunctionScan"),
            (PlanImpl::Sort, "Sort"),
            (PlanImpl::Limit, "Limit"),
            (PlanImpl::TopK, "TopK"),
            (PlanImpl::Values, "Values"),
            (PlanImpl::Insert, "Insert"),
            (PlanImpl::Update, "Update"),
            (PlanImpl::Delete, "Delete"),
            (PlanImpl::AddColumn, "AddColumn"),
            (PlanImpl::ChangeColumn, "ChangeColumn"),
            (PlanImpl::DropColumn, "DropColumn"),
            (PlanImpl::CreateTable, "CreateTable"),
            (PlanImpl::DropTable, "DropTable"),
            (PlanImpl::Truncate, "Truncate"),
            (PlanImpl::Show, "Show"),
            (PlanImpl::Analyze, "Analyze"),
        ];

        for (plan, expected) in cases {
            assert_eq!(plan.explain(&arena).to_string(), expected);
        }

        let meta = arena.alloc_index(IndexMeta {
            id: 1,
            column_ids: vec![1],
            table_name: "users".into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "idx_users_id".to_string(),
            ty: IndexType::Normal,
        });
        assert_eq!(
            PlanImpl::IndexScan(Box::new(index_info(meta)))
                .explain(&arena)
                .to_string(),
            "IndexScan By idx_users_id => EMPTY"
        );
        #[cfg(feature = "copy")]
        {
            assert_eq!(
                PlanImpl::CopyFromFile.explain(&arena).to_string(),
                "CopyFromFile"
            );
            assert_eq!(
                PlanImpl::CopyToFile.explain(&arena).to_string(),
                "CopyToFile"
            );
        }
    }

    #[test]
    fn referenced_column_helpers_stop_on_predicate_result() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = column("left", &mut arena);
        let right = column("right", &mut arena);
        let values = Operator::Values(ValuesOperator::new(
            arena.alloc_expression_rows(&[vec![DataValue::Int32(1), DataValue::Int32(2)]]),
            1,
            vec![left, right],
        ));

        assert!(values.any_referenced_column(&arena, |column| *column == right)?);
        assert!(
            !values.any_referenced_column(&arena, |column| *column != left && *column != right)?
        );
        assert!(values
            .all_referenced_columns(&arena, |column| { *column == left || *column == right })?);
        assert!(!values.all_referenced_columns(&arena, |column| *column == left)?);

        let delete = Operator::Delete(DeleteOperator {
            table_name: "users".into(),
            primary_keys: vec![left],
        });
        assert!(delete.any_referenced_column(&arena, |column| *column == left)?);
        assert!(Operator::Dummy.all_referenced_columns(&arena, |_| false)?);
        assert!(!Operator::Dummy.any_referenced_column(&arena, |_| true)?);
        Ok(())
    }

    #[test]
    fn referenced_column_visitor_covers_expression_driven_variants() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let a = column("a", &mut arena);
        let b = column("b", &mut arena);
        let c = column("c", &mut arena);
        let d = column("d", &mut arena);

        let aggregate = Operator::Aggregate(AggregateOperator {
            agg_calls: vec![column_expr(a, 0, &mut arena)],
            groupby_exprs: vec![column_expr(b, 1, &mut arena)],
            is_distinct: false,
            force_spill: false,
        });
        assert_eq!(referenced_columns(&aggregate, &mut arena)?, vec![a, b]);

        let mut mark_apply = MarkApplyOperator::new_exists(d, vec![column_expr(c, 2, &mut arena)]);
        mark_apply.set_parameterized_probe(Some(column_expr(d, 3, &mut arena)));
        assert_eq!(
            referenced_columns(&Operator::MarkApply(mark_apply), &mut arena)?,
            vec![c, d]
        );

        let filter = Operator::Filter(FilterOperator {
            predicate: column_expr(a, 0, &mut arena),
            is_optimized: false,
            having: false,
        });
        assert_eq!(referenced_columns(&filter, &mut arena)?, vec![a]);

        let join = Operator::Join(JoinOperator {
            join_type: join::JoinType::Inner,
            force_nested_loop: false,
            limit_pushed: false,
            on: JoinCondition::On {
                on: vec![(column_expr(a, 0, &mut arena), column_expr(b, 1, &mut arena))],
                filter: Some(column_expr(c, 2, &mut arena)),
            },
        });
        assert_eq!(referenced_columns(&join, &mut arena)?, vec![a, b, c]);
        assert!(!join.all_referenced_columns(&arena, |column| *column == a)?);

        let project = Operator::Project(ProjectOperator::new(vec![
            column_expr(b, 1, &mut arena),
            column_expr(c, 2, &mut arena),
        ]));
        assert_eq!(referenced_columns(&project, &mut arena)?, vec![b, c]);

        let update = Operator::Update(UpdateOperator {
            table_name: "users".into(),
            value_exprs: vec![(b, column_expr(a, 0, &mut arena))],
        });
        assert_eq!(referenced_columns(&update, &mut arena)?, vec![a]);

        let add_column = Operator::AddColumn(AddColumnOperator {
            table_name: "users".into(),
            if_not_exists: false,
            column: ColumnCatalog::new(
                "added".to_string(),
                true,
                ColumnDesc::new(
                    LogicalType::Integer,
                    None,
                    false,
                    Some(arena.alloc_expression(ScalarExpression::from(1_i32))),
                )?,
            ),
        });
        assert!(referenced_columns(&add_column, &mut arena)?.is_empty());

        let change_column = Operator::ChangeColumn(ChangeColumnOperator {
            table_name: "users".into(),
            old_column_name: "old".to_string(),
            new_column_name: "new".to_string(),
            data_type: LogicalType::Integer,
            default_change: DefaultChange::Set(column_expr(b, 1, &mut arena)),
            not_null_change: NotNullChange::NoChange,
        });
        assert_eq!(referenced_columns(&change_column, &mut arena)?, vec![b]);

        let create_table = Operator::CreateTable(CreateTableOperator {
            table_name: "created".into(),
            columns: vec![ColumnCatalog::new(
                "value".to_string(),
                true,
                ColumnDesc::new(
                    LogicalType::Integer,
                    None,
                    false,
                    Some(arena.alloc_expression(ScalarExpression::from(2_i32))),
                )?,
            )],
            if_not_exists: false,
        });
        assert!(referenced_columns(&create_table, &mut arena)?.is_empty());

        let table_scan = Operator::TableScan(TableScanOperator {
            table_name: "users".into(),
            deserializers: crate::storage::deserializers_for_test(vec![a, d]),
            limit: (None, None),
            index_infos: Vec::new(),
            with_pk: false,
        });
        assert_eq!(referenced_columns(&table_scan, &mut arena)?, vec![a, d]);

        let function_scan = Operator::FunctionScan(FunctionScanOperator {
            table_function: TableFunction {
                args: vec![column_expr(c, 2, &mut arena)],
                catalog: TableFunctionCatalog {
                    schema: Vec::new(),
                    inner: ArcTableFunctionImpl(Numbers::new()),
                },
            },
        });
        assert_eq!(referenced_columns(&function_scan, &mut arena)?, vec![c]);

        let sort = Operator::Sort(SortOperator {
            sort_fields: vec![SortField::from(column_expr(a, 0, &mut arena))],
        });
        assert_eq!(referenced_columns(&sort, &mut arena)?, vec![a]);

        let top_k = Operator::TopK(TopKOperator {
            sort_fields: vec![SortField::from(column_expr(b, 1, &mut arena))],
            limit: 3,
            offset: None,
        });
        assert_eq!(referenced_columns(&top_k, &mut arena)?, vec![b]);

        let union = Operator::Union(UnionOperator {
            left_schema_ref: vec![a],
            _right_schema_ref: vec![b],
        });
        assert_eq!(referenced_columns(&union, &mut arena)?, vec![a, b]);

        let set_membership = Operator::SetMembership(SetMembershipOperator {
            kind: SetMembershipKind::Intersect,
            left_schema_ref: vec![c],
            _right_schema_ref: vec![d],
        });
        assert_eq!(referenced_columns(&set_membership, &mut arena)?, vec![c, d]);

        let delete = Operator::Delete(DeleteOperator {
            table_name: "users".into(),
            primary_keys: vec![a],
        });
        assert_eq!(referenced_columns(&delete, &mut arena)?, vec![a]);

        let scalar = Operator::ScalarQueryInit(ScalarQueryInitOperator {
            value: arena.alloc_expression(ScalarExpression::Constant(DataValue::Null)),
            param_bindings: Vec::new(),
        });
        assert!(referenced_columns(&scalar, &mut arena)?.is_empty());
        let no_reference_operators = [
            Operator::ScalarSubquery(ScalarSubqueryOperator),
            Operator::Analyze(AnalyzeOperator {
                table_name: "users".into(),
                index_metas: vec![IndexMetaRef::new(1)],
                histogram_buckets: Some(8),
                hll_precision: None,
                statistics_relative_error: None,
            }),
        ];
        for operator in no_reference_operators {
            assert!(referenced_columns(&operator, &mut arena)?.is_empty());
        }
        Ok(())
    }

    #[test]
    fn recursive_operators_visit_explain_and_build() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let value = column("value", &mut arena);
        let depth = column("depth", &mut arena);
        let schema = vec![value, depth];

        let recursive_cte = Operator::RecursiveCte(RecursiveCteOperator {
            schema_ref: schema.clone(),
        });
        assert_eq!(referenced_columns(&recursive_cte, &mut arena)?, schema);
        assert_eq!(
            recursive_cte.explain(&arena).to_string(),
            "Recursive CTE: [value, depth]"
        );

        let recursive_scan = Operator::RecursiveScan(RecursiveScanOperator {
            schema_ref: schema.clone(),
        });
        assert_eq!(referenced_columns(&recursive_scan, &mut arena)?, schema);
        assert_eq!(
            recursive_scan.explain(&arena).to_string(),
            "Recursive Scan: [value, depth]"
        );

        let anchor = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let recursive = LogicalPlan::new(Operator::ShowView, Childrens::None);
        let plan = RecursiveCteOperator::build(schema, anchor, recursive);
        assert!(matches!(plan.operator, Operator::RecursiveCte(_)));
        assert!(matches!(*plan.childrens, Childrens::Twins { .. }));
        Ok(())
    }

    #[test]
    fn mark_apply_constructors_and_accessors_cover_quantified_paths() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);
        let output = ColumnRef::new(10);
        let probe = arena.alloc_expression(ScalarExpression::from(true));
        let one = arena.alloc_expression(ScalarExpression::from(1_i32));

        let mut any = MarkApplyOperator::new_in(output, vec![one]);
        assert_eq!(any.to_string(), "MarkAnyApply");
        assert_eq!(any.predicates().len(), 1);
        any.predicates_mut()
            .push(arena.alloc_expression(ScalarExpression::from(2_i32)));
        assert_eq!(any.predicates().len(), 2);
        assert_eq!(*any.output_column(), output);
        assert!(any.parameterized_probe().is_none());
        any.set_parameterized_probe(Some(probe));
        assert_eq!(any.parameterized_probe(), Some(&probe));
        any.set_parameterized_probe(None);
        assert!(any.parameterized_probe().is_none());

        let all = MarkApplyOperator::new_quantified(
            MarkApplyQuantifier::All,
            output,
            vec![arena.alloc_expression(ScalarExpression::from(false))],
        );
        assert_eq!(all.to_string(), "MarkAllApply");

        let in_plan = MarkApplyOperator::build_in(left.clone(), right.clone(), output, vec![one]);
        assert_eq!(in_plan.operator.explain(&arena).to_string(), "MarkAnyApply");
        assert!(matches!(*in_plan.childrens, Childrens::Twins { .. }));

        let all_plan = MarkApplyOperator::build_quantified(
            left,
            right,
            MarkApplyQuantifier::All,
            output,
            vec![one],
        );
        assert_eq!(
            all_plan.operator.explain(&arena).to_string(),
            "MarkAllApply"
        );
        assert!(matches!(*all_plan.childrens, Childrens::Twins { .. }));
    }

    #[test]
    fn ddl_operator_explain_formats_table_index_and_column_actions() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", &mut arena);
        let name = column("name", &mut arena);

        let view = View {
            name: "active_users".into(),
            plan: Box::new(LogicalPlan::new(Operator::ShowTable, Childrens::None)),
            schema: vec![id],
        };

        let cases = [
            (
                Operator::CreateTable(CreateTableOperator {
                    table_name: "users".into(),
                    columns: vec![column_catalog("id"), column_catalog("name")],
                    if_not_exists: true,
                }),
                "Create users -> [id, name], If Not Exists: true",
            ),
            (
                Operator::CreateIndex(CreateIndexOperator {
                    table_name: "users".into(),
                    columns: vec![id, name],
                    index_name: "idx_users_name".to_string(),
                    if_not_exists: false,
                    ty: IndexType::Normal,
                }),
                "Create Index On users -> [id, name], If Not Exists: false",
            ),
            (
                Operator::CreateView(CreateViewOperator {
                    view,
                    or_replace: true,
                }),
                "Create View as View active_users, Or Replace: true",
            ),
            (
                Operator::DropTable(DropTableOperator {
                    table_name: "users".into(),
                    if_exists: true,
                }),
                "Drop Table users, If Exists: true",
            ),
            (
                Operator::DropView(DropViewOperator {
                    view_name: "active_users".into(),
                    if_exists: false,
                }),
                "Drop View active_users, If Exists: false",
            ),
            (
                Operator::DropColumn(DropColumnOperator {
                    table_name: "users".into(),
                    column_name: "age".to_string(),
                    if_exists: true,
                }),
                "Drop age -> users, If Exists: true",
            ),
            (
                Operator::AddColumn(AddColumnOperator {
                    table_name: "users".into(),
                    if_not_exists: true,
                    column: column_catalog("age"),
                }),
                "Add age -> users, If Not Exists: true",
            ),
            (
                Operator::ChangeColumn(ChangeColumnOperator {
                    table_name: "users".into(),
                    old_column_name: "age".to_string(),
                    new_column_name: "age_years".to_string(),
                    data_type: LogicalType::Integer,
                    default_change: DefaultChange::Drop,
                    not_null_change: NotNullChange::Set,
                }),
                "Change age -> users.age_years (Integer, Drop, Set)",
            ),
            (
                Operator::Truncate(TruncateOperator {
                    table_name: "users".into(),
                }),
                "Truncate users",
            ),
        ];

        for (operator, expected) in cases {
            assert_eq!(operator.explain(&arena).to_string(), expected);
        }
    }

    #[test]
    fn dml_values_describe_and_analyze_explain_formats_payloads() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", &mut arena);
        let index = arena.alloc_index(IndexMeta {
            id: 1,
            column_ids: vec![1],
            table_name: "users".into(),
            pk_ty: LogicalType::Integer,
            value_ty: LogicalType::Integer,
            name: "idx_users_id".to_string(),
            ty: IndexType::Normal,
        });

        let cases = [
            (
                Operator::Insert(InsertOperator {
                    table_name: "users".into(),
                    is_overwrite: true,
                    is_mapping_by_name: false,
                }),
                "Insert users, Is Overwrite: true, Is Mapping By Name: false",
            ),
            (
                Operator::Update(UpdateOperator {
                    table_name: "users".into(),
                    value_exprs: vec![(id, arena.alloc_expression(ScalarExpression::from(7_i32)))],
                }),
                "Update users set id -> 7",
            ),
            (
                Operator::Delete(DeleteOperator {
                    table_name: "users".into(),
                    primary_keys: vec![id],
                }),
                "Delete users",
            ),
            (
                Operator::Describe(DescribeOperator {
                    table_name: "users".into(),
                }),
                "Describe users",
            ),
            (
                Operator::Values(ValuesOperator::new(
                    arena.alloc_expression_rows(&[
                        vec![DataValue::Int32(1)],
                        vec![DataValue::Int32(3)],
                    ]),
                    2,
                    vec![id],
                )),
                "Values [1], [3], RowsLen: 2",
            ),
            (
                Operator::Analyze(AnalyzeOperator {
                    table_name: "users".into(),
                    index_metas: vec![index],
                    histogram_buckets: Some(128),
                    hll_precision: None,
                    statistics_relative_error: None,
                }),
                "Analyze users -> [idx_users_id]",
            ),
        ];

        for (operator, expected) in cases {
            assert_eq!(operator.explain(&arena).to_string(), expected);
        }
    }

    #[test]
    fn sort_and_top_k_explain_fields_and_build_single_child_plan() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let descending_nulls_first =
            SortField::from(arena.alloc_expression(ScalarExpression::from(9_i32)))
                .desc()
                .nulls_first();
        let ascending_nulls_last = SortField::new(
            arena.alloc_expression(ScalarExpression::from(1_i32)),
            false,
            true,
        )
        .asc()
        .nulls_last();

        let sort = Operator::Sort(SortOperator {
            sort_fields: vec![descending_nulls_first.clone(), ascending_nulls_last.clone()],
        });
        assert_eq!(
            sort.explain(&arena).to_string(),
            "Sort By 9 Desc Nulls First, 1 Asc Nulls Last"
        );

        let child = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let top_k = TopKOperator::build(vec![descending_nulls_first], 5, Some(2), child);
        assert_eq!(
            top_k.operator.explain(&arena).to_string(),
            "Top 5, Offset 2, Sort By 9 Desc Nulls First"
        );
        assert!(matches!(*top_k.childrens, Childrens::Only(_)));

        let top_k_without_offset = Operator::TopK(TopKOperator {
            sort_fields: vec![ascending_nulls_last],
            limit: 3,
            offset: None,
        });
        assert_eq!(
            top_k_without_offset.explain(&arena).to_string(),
            "Top 3, Sort By 1 Asc Nulls Last"
        );
    }

    #[test]
    fn drop_index_build_preserves_operator_payload_and_children() {
        let table_arena = TableArenaCell::default();
        let arena = PlanArena::new(&table_arena);
        let plan = DropIndexOperator::build(
            "users".into(),
            "idx_users_id".to_string(),
            true,
            Childrens::None,
        );

        assert_eq!(
            plan.operator.explain(&arena).to_string(),
            "Drop Index idx_users_id On users, If Exists: true"
        );
        assert!(matches!(*plan.childrens, Childrens::None));
    }

    #[test]
    fn function_scan_explain_and_build_preserve_table_function() {
        let table_arena = TableArenaCell::default();
        let numbers = Numbers::new();
        let mut schema = Vec::new();
        numbers.output_schema_into(table_arena.borrow_mut(), &mut schema);
        let mut arena = PlanArena::new(&table_arena);
        let table_function = TableFunction {
            args: vec![arena.alloc_expression(ScalarExpression::from(3_i32))],
            catalog: TableFunctionCatalog {
                schema,
                inner: ArcTableFunctionImpl(numbers),
            },
        };

        let plan = FunctionScanOperator::build(table_function);

        assert_eq!(
            plan.operator.explain(&arena).to_string(),
            "Function Scan: numbers"
        );
        assert!(matches!(*plan.childrens, Childrens::None));
    }

    #[test]
    fn set_membership_explain_and_build_cover_both_kinds() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left_col = column("left_id", &mut arena);
        let right_col = column("right_id", &mut arena);
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);

        let plan = SetMembershipOperator::build(
            SetMembershipKind::Intersect,
            vec![left_col],
            vec![right_col],
            left,
            right,
        );

        assert_eq!(
            plan.operator.explain(&arena).to_string(),
            "Intersect: [left_id]"
        );
        assert!(matches!(*plan.childrens, Childrens::Twins { .. }));
        assert_eq!(
            Operator::SetMembership(SetMembershipOperator {
                kind: SetMembershipKind::Except,
                left_schema_ref: vec![left_col],
                _right_schema_ref: vec![right_col],
            })
            .explain(&arena)
            .to_string(),
            "Except: [left_id]"
        );
    }

    #[test]
    fn scalar_query_init_and_subquery_build_expected_child_shapes() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);

        let id = arena.alloc_scalar_query_ref(false);
        let value = arena.alloc_expression(ScalarExpression::InitValue {
            id,
            ty: LogicalType::Integer,
        });
        let apply = ScalarQueryInitOperator::build(left.clone(), right, value, Vec::new());
        assert_eq!(
            apply.operator.explain(&arena).to_string(),
            "ScalarQueryInit InitValue(1:0)"
        );
        assert!(matches!(*apply.childrens, Childrens::Twins { .. }));

        let subquery = ScalarSubqueryOperator::build(left);
        assert_eq!(
            subquery.operator.explain(&arena).to_string(),
            "ScalarSubquery"
        );
        assert!(matches!(*subquery.childrens, Childrens::Only(_)));
    }

    #[cfg(feature = "copy")]
    #[test]
    fn copy_explain_formats_source_target_table_and_schema() {
        use crate::binder::copy::{ExtSource, FileFormat};
        use std::path::PathBuf;

        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", &mut arena);
        let name = column("name", &mut arena);

        let operator = Operator::CopyFromFile(CopyFromFileOperator {
            table: "users".into(),
            source: ExtSource {
                path: PathBuf::from("/tmp/users.csv"),
                format: FileFormat::Csv {
                    delimiter: ',',
                    quote: '"',
                    escape: None,
                    header: true,
                },
            },
            schema_ref: vec![id, name],
        });

        assert_eq!(
            operator.explain(&arena).to_string(),
            "Copy /tmp/users.csv -> users [id, name]"
        );
        assert_eq!(
            Operator::CopyToFile(CopyToFileOperator {
                target: ExtSource {
                    path: PathBuf::from("/tmp/output.csv"),
                    format: FileFormat::Csv {
                        delimiter: ',',
                        quote: '"',
                        escape: None,
                        header: false,
                    },
                },
            })
            .explain(&arena)
            .to_string(),
            "Copy To /tmp/output.csv"
        );
    }
}
// GRCOV_EXCL_STOP
