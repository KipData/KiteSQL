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
pub mod insert;
pub mod join;
pub mod limit;
pub mod mark_apply;
pub mod project;
pub mod scalar_apply;
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

use self::{
    aggregate::AggregateOperator, alter_table::add_column::AddColumnOperator,
    alter_table::change_column::ChangeColumnOperator, filter::FilterOperator, join::JoinOperator,
    limit::LimitOperator, mark_apply::MarkApplyOperator, project::ProjectOperator,
    scalar_apply::ScalarApplyOperator, scalar_subquery::ScalarSubqueryOperator, sort::SortOperator,
    table_scan::TableScanOperator,
};
use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::visitor::{walk_expr, ExprVisitor};
use crate::expression::ScalarExpression;
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
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::join::JoinCondition;
use crate::planner::operator::set_membership::SetMembershipOperator;
use crate::planner::operator::sort::SortField;
use crate::planner::operator::top_k::TopKOperator;
use crate::planner::operator::truncate::TruncateOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::update::UpdateOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::visitor::OperatorVisitor;
use crate::planner::{MetaArena, PlanArena};
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
    SetMembership(SetMembershipOperator),
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
    #[cfg(feature = "copy")]
    CopyFromFile(CopyFromFileOperator),
    #[cfg(feature = "copy")]
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
    #[cfg(feature = "copy")]
    CopyFromFile,
    #[cfg(feature = "copy")]
    CopyToFile,
    Analyze,
}

impl Operator {
    pub fn visit_referenced_columns<A: MetaArena>(
        &self,
        arena: &mut A,
        f: &mut impl FnMut(&mut A, &ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        struct ReferencedColumnVisitor<'a, A, F> {
            arena: &'a mut A,
            f: &'a mut F,
            keep_going: bool,
        }

        impl<'expr, A, F> ExprVisitor<'expr> for ReferencedColumnVisitor<'_, A, F>
        where
            F: FnMut(&mut A, &ColumnRef) -> bool,
        {
            fn visit(&mut self, expr: &'expr ScalarExpression) -> Result<(), DatabaseError> {
                if self.keep_going {
                    walk_expr(self, expr)?;
                }
                Ok(())
            }

            fn visit_column_ref(&mut self, column: &'expr ColumnRef) -> Result<(), DatabaseError> {
                if self.keep_going {
                    self.keep_going = (self.f)(self.arena, column);
                }
                Ok(())
            }
        }

        impl<'operator, A, F> OperatorVisitor<'operator> for ReferencedColumnVisitor<'_, A, F>
        where
            F: FnMut(&mut A, &ColumnRef) -> bool,
        {
            fn visit_aggregate(
                &mut self,
                op: &'operator AggregateOperator,
            ) -> Result<(), DatabaseError> {
                for expr in op.agg_calls.iter().chain(&op.groupby_exprs) {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_mark_apply(
                &mut self,
                op: &'operator MarkApplyOperator,
            ) -> Result<(), DatabaseError> {
                for expr in &op.predicates {
                    ExprVisitor::visit(self, expr)?;
                }
                if let Some(expr) = &op.parameterized_probe {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_filter(&mut self, op: &'operator FilterOperator) -> Result<(), DatabaseError> {
                ExprVisitor::visit(self, &op.predicate)
            }

            fn visit_join(&mut self, op: &'operator JoinOperator) -> Result<(), DatabaseError> {
                if let JoinCondition::On { on, filter } = &op.on {
                    for (left_expr, right_expr) in on {
                        ExprVisitor::visit(self, left_expr)?;
                        ExprVisitor::visit(self, right_expr)?;
                    }
                    if let Some(expr) = filter {
                        ExprVisitor::visit(self, expr)?;
                    }
                }
                Ok(())
            }

            fn visit_project(
                &mut self,
                op: &'operator ProjectOperator,
            ) -> Result<(), DatabaseError> {
                for expr in &op.exprs {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_table_scan(
                &mut self,
                op: &'operator TableScanOperator,
            ) -> Result<(), DatabaseError> {
                for column in &op.columns {
                    self.visit_column_ref(column)?;
                }
                Ok(())
            }

            fn visit_function_scan(
                &mut self,
                op: &'operator FunctionScanOperator,
            ) -> Result<(), DatabaseError> {
                for expr in &op.table_function.args {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_sort(&mut self, op: &'operator SortOperator) -> Result<(), DatabaseError> {
                for field in &op.sort_fields {
                    ExprVisitor::visit(self, &field.expr)?;
                }
                Ok(())
            }

            fn visit_top_k(&mut self, op: &'operator TopKOperator) -> Result<(), DatabaseError> {
                for field in &op.sort_fields {
                    ExprVisitor::visit(self, &field.expr)?;
                }
                Ok(())
            }

            fn visit_values(&mut self, op: &'operator ValuesOperator) -> Result<(), DatabaseError> {
                for column in &op.schema_ref {
                    self.visit_column_ref(column)?;
                }
                Ok(())
            }

            fn visit_union(&mut self, op: &'operator UnionOperator) -> Result<(), DatabaseError> {
                for column in op.left_schema_ref.iter().chain(&op._right_schema_ref) {
                    self.visit_column_ref(column)?;
                }
                Ok(())
            }

            fn visit_set_membership(
                &mut self,
                op: &'operator SetMembershipOperator,
            ) -> Result<(), DatabaseError> {
                for column in op.left_schema_ref.iter().chain(&op._right_schema_ref) {
                    self.visit_column_ref(column)?;
                }
                Ok(())
            }

            fn visit_delete(&mut self, op: &'operator DeleteOperator) -> Result<(), DatabaseError> {
                for column in &op.primary_keys {
                    self.visit_column_ref(column)?;
                }
                Ok(())
            }

            fn visit_update(&mut self, op: &'operator UpdateOperator) -> Result<(), DatabaseError> {
                for (_, expr) in &op.value_exprs {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_add_column(
                &mut self,
                op: &'operator AddColumnOperator,
            ) -> Result<(), DatabaseError> {
                if let Some(expr) = &op.column.desc().default {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_change_column(
                &mut self,
                op: &'operator ChangeColumnOperator,
            ) -> Result<(), DatabaseError> {
                if let ColumnDefaultChange::Set(expr) = &op.default_change {
                    ExprVisitor::visit(self, expr)?;
                }
                Ok(())
            }

            fn visit_create_table(
                &mut self,
                op: &'operator CreateTableOperator,
            ) -> Result<(), DatabaseError> {
                for column in &op.columns {
                    if let Some(expr) = &column.desc().default {
                        ExprVisitor::visit(self, expr)?;
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
        visitor.visit_operator(self)?;
        Ok(visitor.keep_going)
    }

    pub fn any_referenced_column(
        &self,
        arena: &mut PlanArena,
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
        arena: &mut PlanArena,
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
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(op) => write!(f, "{op}"),
            #[cfg(feature = "copy")]
            Operator::CopyToFile(op) => write!(f, "{op}"),
            Operator::Union(op) => write!(f, "{op}"),
            Operator::SetMembership(op) => write!(f, "{op}"),
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
            #[cfg(feature = "copy")]
            PlanImpl::CopyFromFile => write!(f, "CopyFromFile"),
            #[cfg(feature = "copy")]
            PlanImpl::CopyToFile => write!(f, "CopyToFile"),
            PlanImpl::Analyze => write!(f, "Analyze"),
        }
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
    use crate::planner::{Childrens, LogicalPlan, TableArenaCell};
    use crate::types::index::{IndexInfo, IndexMetaRef, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

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

    fn index_info() -> IndexInfo {
        IndexInfo {
            meta: IndexMetaRef::new(4),
            sort_option: SortOption::None,
            lookup: None,
            residual_predicate: None,
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_distinct_hint: None,
        }
    }

    fn column_expr(column: ColumnRef, position: usize) -> ScalarExpression {
        ScalarExpression::column_expr(column, position)
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
    fn physical_option_and_sort_option_display() {
        let sort_field = SortField::new(ScalarExpression::from(1i32), false, true);
        let sort_option = SortOption::OrderBy {
            fields: vec![sort_field],
            ignore_prefix_len: 2,
        };
        assert_eq!(
            sort_option.to_string(),
            "OrderBy: (1 Desc Nulls First) ignore_prefix_len: 2"
        );
        assert_eq!(SortOption::Follow.to_string(), "Follow");
        assert_eq!(SortOption::None.to_string(), "None");

        let physical = PhysicalOption::new(PlanImpl::TopK, sort_option.clone());
        assert_eq!(
            physical.to_string(),
            "TopK => (Sort Option: OrderBy: (1 Desc Nulls First) ignore_prefix_len: 2)"
        );
        assert_eq!(physical.sort_option(), &sort_option);
    }

    #[test]
    fn plan_impl_display_covers_physical_variants() {
        let cases = [
            (PlanImpl::Dummy, "Dummy"),
            (PlanImpl::SimpleAggregate, "SimpleAggregate"),
            (PlanImpl::HashAggregate, "HashAggregate"),
            (PlanImpl::StreamDistinct, "StreamDistinct"),
            (PlanImpl::ScalarApply, "ScalarApply"),
            (PlanImpl::MarkApply, "MarkApply"),
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
            assert_eq!(plan.to_string(), expected);
        }
        assert_eq!(
            PlanImpl::IndexScan(Box::new(index_info())).to_string(),
            "IndexScan By #4 => EMPTY"
        );
        #[cfg(feature = "copy")]
        {
            assert_eq!(PlanImpl::CopyFromFile.to_string(), "CopyFromFile");
            assert_eq!(PlanImpl::CopyToFile.to_string(), "CopyToFile");
        }
    }

    #[test]
    fn referenced_column_helpers_stop_on_predicate_result() -> Result<(), DatabaseError> {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left = column("left", &mut arena);
        let right = column("right", &mut arena);
        let values = Operator::Values(ValuesOperator {
            rows: vec![vec![DataValue::Int32(1), DataValue::Int32(2)]],
            schema_ref: vec![left, right],
        });

        assert!(values.any_referenced_column(&mut arena, |column| *column == right)?);
        assert!(!values
            .any_referenced_column(&mut arena, |column| { *column != left && *column != right })?);
        assert!(values.all_referenced_columns(&mut arena, |column| {
            *column == left || *column == right
        })?);
        assert!(!values.all_referenced_columns(&mut arena, |column| *column == left)?);

        let delete = Operator::Delete(DeleteOperator {
            table_name: "users".into(),
            primary_keys: vec![left],
        });
        assert!(delete.any_referenced_column(&mut arena, |column| *column == left)?);
        assert!(Operator::Dummy.all_referenced_columns(&mut arena, |_| false)?);
        assert!(!Operator::Dummy.any_referenced_column(&mut arena, |_| true)?);
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
            agg_calls: vec![column_expr(a, 0)],
            groupby_exprs: vec![column_expr(b, 1)],
            is_distinct: false,
        });
        assert_eq!(referenced_columns(&aggregate, &mut arena)?, vec![a, b]);

        let mut mark_apply = MarkApplyOperator::new_exists(d, vec![column_expr(c, 2)]);
        mark_apply.set_parameterized_probe(Some(column_expr(d, 3)));
        assert_eq!(
            referenced_columns(&Operator::MarkApply(mark_apply), &mut arena)?,
            vec![c, d]
        );

        let filter = Operator::Filter(FilterOperator {
            predicate: column_expr(a, 0),
            is_optimized: false,
            having: false,
        });
        assert_eq!(referenced_columns(&filter, &mut arena)?, vec![a]);

        let join = Operator::Join(JoinOperator {
            join_type: join::JoinType::Inner,
            on: JoinCondition::On {
                on: vec![(column_expr(a, 0), column_expr(b, 1))],
                filter: Some(column_expr(c, 2)),
            },
        });
        assert_eq!(referenced_columns(&join, &mut arena)?, vec![a, b, c]);
        assert!(!join.all_referenced_columns(&mut arena, |column| *column == a)?);

        let project = Operator::Project(ProjectOperator {
            exprs: vec![column_expr(b, 1), column_expr(c, 2)],
        });
        assert_eq!(referenced_columns(&project, &mut arena)?, vec![b, c]);

        let update = Operator::Update(UpdateOperator {
            table_name: "users".into(),
            value_exprs: vec![(b, column_expr(a, 0))],
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
                    Some(ScalarExpression::from(1_i32)),
                )?,
            ),
        });
        assert!(referenced_columns(&add_column, &mut arena)?.is_empty());

        let change_column = Operator::ChangeColumn(ChangeColumnOperator {
            table_name: "users".into(),
            old_column_name: "old".to_string(),
            new_column_name: "new".to_string(),
            data_type: LogicalType::Integer,
            default_change: DefaultChange::Set(column_expr(b, 1)),
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
                    Some(ScalarExpression::from(2_i32)),
                )?,
            )],
            if_not_exists: false,
        });
        assert!(referenced_columns(&create_table, &mut arena)?.is_empty());

        let table_scan = Operator::TableScan(TableScanOperator {
            table_name: "users".into(),
            columns: vec![a, d],
            limit: (None, None),
            index_infos: Vec::new(),
            with_pk: false,
        });
        assert_eq!(referenced_columns(&table_scan, &mut arena)?, vec![a, d]);

        let function_scan = Operator::FunctionScan(FunctionScanOperator {
            table_function: TableFunction {
                args: vec![column_expr(c, 2)],
                catalog: TableFunctionCatalog {
                    schema: Vec::new(),
                    inner: ArcTableFunctionImpl(Numbers::new()),
                },
            },
        });
        assert_eq!(referenced_columns(&function_scan, &mut arena)?, vec![c]);

        let sort = Operator::Sort(SortOperator {
            sort_fields: vec![SortField::from(column_expr(a, 0))],
            limit: None,
        });
        assert_eq!(referenced_columns(&sort, &mut arena)?, vec![a]);

        let top_k = Operator::TopK(TopKOperator {
            sort_fields: vec![SortField::from(column_expr(b, 1))],
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

        let no_reference_operators = [
            Operator::ScalarApply(ScalarApplyOperator),
            Operator::ScalarSubquery(ScalarSubqueryOperator),
            Operator::Analyze(AnalyzeOperator {
                table_name: "users".into(),
                index_metas: vec![IndexMetaRef::new(1)],
                histogram_buckets: Some(8),
            }),
        ];
        for operator in no_reference_operators {
            assert!(referenced_columns(&operator, &mut arena)?.is_empty());
        }
        Ok(())
    }

    #[test]
    fn mark_apply_constructors_and_accessors_cover_quantified_paths() {
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);
        let output = ColumnRef::new(10);
        let probe = ScalarExpression::from(true);

        let mut any = MarkApplyOperator::new_in(output, vec![ScalarExpression::from(1_i32)]);
        assert_eq!(any.to_string(), "MarkAnyApply");
        assert_eq!(any.predicates().len(), 1);
        any.predicates_mut().push(ScalarExpression::from(2_i32));
        assert_eq!(any.predicates().len(), 2);
        assert_eq!(*any.output_column(), output);
        assert!(any.parameterized_probe().is_none());
        any.set_parameterized_probe(Some(probe.clone()));
        assert_eq!(any.parameterized_probe(), Some(&probe));
        any.set_parameterized_probe(None);
        assert!(any.parameterized_probe().is_none());

        let all = MarkApplyOperator::new_quantified(
            MarkApplyQuantifier::All,
            output,
            vec![ScalarExpression::from(false)],
        );
        assert_eq!(all.to_string(), "MarkAllApply");

        let in_plan = MarkApplyOperator::build_in(
            left.clone(),
            right.clone(),
            output,
            vec![ScalarExpression::from(1_i32)],
        );
        assert_eq!(in_plan.operator.to_string(), "MarkAnyApply");
        assert!(matches!(*in_plan.childrens, Childrens::Twins { .. }));

        let all_plan = MarkApplyOperator::build_quantified(
            left,
            right,
            MarkApplyQuantifier::All,
            output,
            vec![ScalarExpression::from(1_i32)],
        );
        assert_eq!(all_plan.operator.to_string(), "MarkAllApply");
        assert!(matches!(*all_plan.childrens, Childrens::Twins { .. }));
    }

    #[test]
    fn ddl_operator_display_formats_table_index_and_column_actions() {
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
                "Create Index On users -> [#0, #1], If Not Exists: false",
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
            assert_eq!(operator.to_string(), expected);
        }
    }

    #[test]
    fn dml_values_describe_and_analyze_display_formats_payloads() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", &mut arena);

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
                    value_exprs: vec![(id, ScalarExpression::from(7_i32))],
                }),
                "Update users set #0 -> 7",
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
                Operator::Values(ValuesOperator {
                    rows: vec![
                        vec![DataValue::Int32(1), DataValue::Int32(2)],
                        vec![DataValue::Int32(3)],
                    ],
                    schema_ref: vec![id],
                }),
                "Values [1, 2], [3], RowsLen: 2",
            ),
            (
                Operator::Analyze(AnalyzeOperator {
                    table_name: "users".into(),
                    index_metas: vec![IndexMetaRef::new(3)],
                    histogram_buckets: Some(128),
                }),
                "Analyze users -> [#3]",
            ),
        ];

        for (operator, expected) in cases {
            assert_eq!(operator.to_string(), expected);
        }
    }

    #[test]
    fn sort_and_top_k_display_fields_and_build_single_child_plan() {
        let descending_nulls_first = SortField::from(ScalarExpression::from(9_i32))
            .desc()
            .nulls_first();
        let ascending_nulls_last = SortField::new(ScalarExpression::from(1_i32), false, true)
            .asc()
            .nulls_last();

        let sort = Operator::Sort(SortOperator {
            sort_fields: vec![descending_nulls_first.clone(), ascending_nulls_last.clone()],
            limit: Some(10),
        });
        assert_eq!(
            sort.to_string(),
            "Sort By 9 Desc Nulls First, 1 Asc Nulls Last, Limit 10"
        );

        let child = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let top_k = TopKOperator::build(vec![descending_nulls_first], 5, Some(2), child);
        assert_eq!(
            top_k.operator.to_string(),
            "Top 5, Offset 2, Sort By 9 Desc Nulls First"
        );
        assert!(matches!(*top_k.childrens, Childrens::Only(_)));

        let top_k_without_offset = Operator::TopK(TopKOperator {
            sort_fields: vec![ascending_nulls_last],
            limit: 3,
            offset: None,
        });
        assert_eq!(
            top_k_without_offset.to_string(),
            "Top 3, Sort By 1 Asc Nulls Last"
        );
    }

    #[test]
    fn drop_index_build_preserves_operator_payload_and_children() {
        let plan = DropIndexOperator::build(
            "users".into(),
            "idx_users_id".to_string(),
            true,
            Childrens::None,
        );

        assert_eq!(
            plan.operator.to_string(),
            "Drop Index idx_users_id On users, If Exists: true"
        );
        assert!(matches!(*plan.childrens, Childrens::None));
    }

    #[test]
    fn function_scan_display_and_build_preserve_table_function() {
        let table_arena = TableArenaCell::default();
        let numbers = Numbers::new();
        let mut schema = Vec::new();
        numbers.output_schema_into(table_arena.borrow_mut(), &mut schema);
        let table_function = TableFunction {
            args: vec![ScalarExpression::from(3_i32)],
            catalog: TableFunctionCatalog {
                schema,
                inner: ArcTableFunctionImpl(numbers),
            },
        };

        let plan = FunctionScanOperator::build(table_function);

        assert_eq!(plan.operator.to_string(), "Function Scan: numbers");
        assert!(matches!(*plan.childrens, Childrens::None));
    }

    #[test]
    fn set_membership_display_and_build_cover_both_kinds() {
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

        assert_eq!(plan.operator.to_string(), "Intersect: [#0]");
        assert!(matches!(*plan.childrens, Childrens::Twins { .. }));
        assert_eq!(
            Operator::SetMembership(SetMembershipOperator {
                kind: SetMembershipKind::Except,
                left_schema_ref: vec![left_col],
                _right_schema_ref: vec![right_col],
            })
            .to_string(),
            "Except: [#0]"
        );
    }

    #[test]
    fn scalar_apply_and_subquery_build_expected_child_shapes() {
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);

        let apply = ScalarApplyOperator::build(left.clone(), right);
        assert_eq!(apply.operator.to_string(), "ScalarApply");
        assert!(matches!(*apply.childrens, Childrens::Twins { .. }));

        let subquery = ScalarSubqueryOperator::build(left);
        assert_eq!(subquery.operator.to_string(), "ScalarSubquery");
        assert!(matches!(*subquery.childrens, Childrens::Only(_)));
    }

    #[cfg(feature = "copy")]
    #[test]
    fn copy_display_formats_source_target_table_and_schema() {
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
            operator.to_string(),
            "Copy /tmp/users.csv -> users [#0, #1]"
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
            .to_string(),
            "Copy To /tmp/output.csv"
        );
    }
}
// GRCOV_EXCL_STOP
