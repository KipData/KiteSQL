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

mod arena;
pub mod operator;
mod plan_keeper;
mod scalar_query_ref;

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::visitor_mut::ExprCloner;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::recursive_cte::{RecursiveCteOperator, RecursiveScanOperator};
use crate::planner::operator::set_membership::SetMembershipOperator;
use crate::planner::operator::union::UnionOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::visitor_mut::{OperatorExprVisitorMut, OperatorVisitorMut};
use crate::planner::operator::{ChildCosts, Operator, PhysicalOption, PlanCost, RowCountOption};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::hash::{Hash, Hasher};

pub(crate) use arena::{ExecMetaArena, ParamArena, PlanRef};
pub use arena::{ExprRef, MetaArena, PlanArena, TableArena, TableArenaCell};
pub(crate) use plan_keeper::{PlanInput, PlanKeeper};
pub use scalar_query_ref::ScalarQueryRef;

pub(crate) trait Explain {
    fn fmt<A: MetaArena + ?Sized>(&self, arena: &A, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    fn explain<'a, A: MetaArena + ?Sized>(&'a self, arena: &'a A) -> ExplainDisplay<'a, Self, A>
    where
        Self: Sized,
    {
        ExplainDisplay { value: self, arena }
    }
}

pub(crate) struct ExplainDisplay<'a, T: ?Sized, A: ?Sized> {
    value: &'a T,
    arena: &'a A,
}

impl<T: Explain + ?Sized, A: MetaArena + ?Sized> fmt::Display for ExplainDisplay<'_, T, A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(self.arena, f)
    }
}

pub(crate) fn fmt_explain_list<T: Explain, A: MetaArena + ?Sized>(
    values: &[T],
    separator: &str,
    arena: &A,
    f: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            f.write_str(separator)?;
        }
        value.fmt(arena, f)?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub enum Childrens {
    None,
    Only(Box<LogicalPlan>),
    Twins {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
}

impl Childrens {
    pub fn iter(&self) -> ChildrensIter<'_> {
        ChildrensIter {
            inner: self,
            pos: 0,
        }
    }

    pub(crate) fn only(&self) -> &LogicalPlan {
        match self {
            Childrens::Only(plan) => plan,
            _ => unreachable!(),
        }
    }

    pub(crate) fn twins(&self) -> (&LogicalPlan, &LogicalPlan) {
        match self {
            Childrens::Twins { left, right } => (left, right),
            _ => unreachable!(),
        }
    }

    pub fn pop_only(self) -> LogicalPlan {
        match self {
            Childrens::Only(plan) => *plan,
            _ => {
                unreachable!()
            }
        }
    }

    pub fn pop_twins(self) -> (LogicalPlan, LogicalPlan) {
        match self {
            Childrens::Twins { left, right } => (*left, *right),
            _ => unreachable!(),
        }
    }
}

pub struct ChildrensIter<'a> {
    inner: &'a Childrens,
    pos: usize,
}

impl<'a> Iterator for ChildrensIter<'a> {
    type Item = &'a LogicalPlan;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            Childrens::Only(plan) => {
                if self.pos > 0 {
                    return None;
                }
                self.pos += 1;
                Some(plan.as_ref())
            }
            Childrens::Twins { left, right } => {
                let option = match self.pos {
                    0 => Some(left.as_ref()),
                    1 => Some(right.as_ref()),
                    _ => None,
                };
                self.pos += 1;
                option
            }
            Childrens::None => None,
        }
    }
}

#[derive(Debug)]
pub struct LogicalPlan {
    pub(crate) operator: Operator,
    pub(crate) childrens: Box<Childrens>,
    pub(crate) physical_option: Option<PhysicalOption>,
    output_schema: Option<crate::types::tuple::Schema>,
    exec_capacity_hint: usize,
}

impl LogicalPlan {
    pub fn new(operator: Operator, childrens: Childrens) -> Self {
        Self {
            operator,
            childrens: Box::new(childrens),
            physical_option: None,
            output_schema: None,
            exec_capacity_hint: 0,
        }
    }

    pub(crate) fn clone_plan<A: MetaArena + ?Sized>(
        &self,
        arena: &mut A,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut plan = self.clone();
        OperatorExprVisitorMut::new(&mut ExprCloner, arena).visit_plan(&mut plan)?;
        Ok(plan)
    }

    pub(crate) fn take(&mut self) -> Self {
        std::mem::replace(self, Self::new(Operator::Dummy, Childrens::None))
    }

    pub(crate) fn row_count_option(&self) -> Option<&PhysicalOption> {
        let option = self.physical_option.as_ref()?;
        match option.row_count {
            RowCountOption::Estimated { .. } => Some(option),
            RowCountOption::Unknown => None,
            RowCountOption::Follow => match self.childrens.as_ref() {
                Childrens::Only(child) => child.row_count_option(),
                Childrens::Twins { left, .. } => left.row_count_option(),
                Childrens::None => None,
            },
        }
    }

    pub(crate) fn estimated_rows(&self) -> Option<usize> {
        match &self.physical_option.as_ref()?.row_count {
            RowCountOption::Estimated { rows, .. } => Some(*rows),
            RowCountOption::Unknown => match (&self.operator, self.childrens.as_ref()) {
                (Operator::Join(op), Childrens::Twins { left, right })
                    if op.join_type == JoinType::Cross && !op.limit_pushed =>
                {
                    Some(
                        left.estimated_rows()?
                            .saturating_mul(right.estimated_rows()?),
                    )
                }
                _ => None,
            },
            RowCountOption::Follow => match self.childrens.as_ref() {
                Childrens::Only(child) => child.estimated_rows(),
                Childrens::Twins { left, .. } => left.estimated_rows(),
                Childrens::None => None,
            },
        }
    }

    // Pass-local memoization for NLJ orientation. New physical options start
    // empty; orientation preserves child rows/startup, so their cache is reusable.
    pub(crate) fn price<A: MetaArena>(&mut self, arena: &mut A) -> Option<PlanCost> {
        self.physical_option.as_ref()?;
        let width = self.output_schema(arena).len();
        let rows = self.estimated_rows();
        let children = match self.childrens.as_mut() {
            Childrens::None => ChildCosts::None,
            Childrens::Only(child) => ChildCosts::Only(child.price(arena)),
            Childrens::Twins { left, right } => {
                ChildCosts::Twins(left.price(arena), right.price(arena))
            }
        };
        let option = self.physical_option.as_ref()?;
        let mut cost = PlanCost {
            rows,
            width,
            startup: option.calculate_startup_work(&self.operator, &children),
            total: None,
        };
        cost.total = option.calculate_work(&self.operator, &children, rows, &cost);
        Some(cost)
    }

    pub fn referenced_table(&self) -> Vec<TableName> {
        fn collect_table(plan: &LogicalPlan, results: &mut Vec<TableName>) {
            if let Operator::TableScan(op) = &plan.operator {
                results.push(op.table_name.clone());
            }
            for child in plan.childrens.iter() {
                collect_table(child, results);
            }
        }

        let mut tables = Vec::new();
        collect_table(self, &mut tables);
        tables
    }

    pub(crate) fn visit_referenced_columns<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut impl FnMut(&A, &crate::catalog::ColumnRef) -> bool,
    ) -> Result<bool, DatabaseError> {
        Operator::referenced_columns(operator::ColumnVisitRoot::Plan(self), arena, f)
    }

    pub(crate) fn visit_column_refs<A, F>(
        &self,
        arena: &mut A,
        f: &mut F,
    ) -> Result<(), DatabaseError>
    where
        A: MetaArena + ?Sized,
        F: FnMut(&crate::catalog::ColumnRef) + ?Sized,
    {
        self.visit_referenced_columns(&*arena, &mut |_, column| {
            f(column);
            true
        })?;
        Ok(())
    }

    pub fn output_schema<'plan>(
        &'plan mut self,
        arena: &mut (dyn MetaArena + '_),
    ) -> &'plan crate::types::tuple::Schema {
        let LogicalPlan {
            operator,
            childrens,
            output_schema,
            ..
        } = self;
        output_schema.get_or_insert_with(|| Self::compute_output_schema(operator, childrens, arena))
    }

    fn compute_output_schema(
        operator: &mut Operator,
        childrens: &mut Childrens,
        arena: &mut (dyn MetaArena + '_),
    ) -> crate::types::tuple::Schema {
        match operator {
            Operator::Filter(_)
            | Operator::Sort(_)
            | Operator::Limit(_)
            | Operator::TopK(_)
            | Operator::ScalarSubquery(_) => match childrens {
                Childrens::Only(child) => child.output_schema(arena).clone(),
                _ => unreachable!(),
            },
            Operator::Window(op) => match childrens {
                Childrens::Only(child) => {
                    let mut schema = child.output_schema(arena).clone();
                    schema.extend_from_slice(&op.output_columns);
                    schema
                }
                _ => unreachable!(),
            },
            Operator::ScalarQueryInit(_) => match childrens {
                Childrens::Twins { left, .. } => left.output_schema(arena).clone(),
                _ => unreachable!(),
            },
            Operator::Join(_) => match childrens {
                Childrens::Twins { left, right } => {
                    let left_schema = left.output_schema(arena);
                    let right_schema = right.output_schema(arena);
                    let mut schema = Vec::with_capacity(left_schema.len() + right_schema.len());
                    schema.extend_from_slice(left_schema);
                    schema.extend_from_slice(right_schema);
                    schema
                }
                _ => unreachable!(),
            },
            Operator::IndexProbeJoin(_) => {
                let Childrens::Twins { left, right } = childrens else {
                    unreachable!("index probe join requires two inputs")
                };
                let mut schema = left.output_schema(arena).clone();
                schema.extend_from_slice(right.output_schema(arena));
                schema
            }
            Operator::MarkApply(op) => {
                let mut schema = match childrens {
                    Childrens::Only(left) => left.output_schema(arena).clone(),
                    Childrens::Twins { left, .. } => left.output_schema(arena).clone(),
                    Childrens::None => Vec::new(),
                };
                schema.push(*op.output_column());
                schema
            }
            Operator::Aggregate(op) => op
                .agg_calls
                .iter()
                .chain(op.groupby_exprs.iter())
                .map(|expr| expr.output_column_ref(arena))
                .collect(),
            Operator::Project(op) => op
                .exprs
                .iter()
                .map(|expr| expr.output_column_ref(arena))
                .collect(),
            Operator::TableScan(op) => op.columns.clone(),
            Operator::FunctionScan(op) => {
                let mut schema = Vec::new();
                op.table_function.output_schema_into(&mut schema);
                schema
            }
            Operator::Values(ValuesOperator { schema_ref, .. })
            | Operator::Union(UnionOperator {
                left_schema_ref: schema_ref,
                ..
            })
            | Operator::SetMembership(SetMembershipOperator {
                left_schema_ref: schema_ref,
                ..
            }) => schema_ref.clone(),
            Operator::RecursiveCte(RecursiveCteOperator { schema_ref })
            | Operator::RecursiveScan(RecursiveScanOperator { schema_ref }) => schema_ref.clone(),
            Operator::Dummy => Vec::new(),
            Operator::ShowTable => Self::dummy_schema(arena, ["TABLE"]),
            Operator::ShowView => Self::dummy_schema(arena, ["VIEW"]),
            Operator::Explain => Self::dummy_schema(arena, ["PLAN"]),
            Operator::Describe(_) => {
                Self::dummy_schema(arena, ["FIELD", "TYPE", "LEN", "NULL", "Key", "DEFAULT"])
            }
            Operator::Insert(_) => Self::dummy_schema(arena, ["INSERTED"]),
            Operator::Update(_) => Self::dummy_schema(arena, ["UPDATED"]),
            Operator::Delete(_) => Self::dummy_schema(arena, ["DELETED"]),
            Operator::Analyze(_) => Self::dummy_schema(arena, ["STATISTICS_META_PATH"]),
            Operator::AddColumn(_) => Self::dummy_schema(arena, ["ADD COLUMN SUCCESS"]),
            Operator::ChangeColumn(_) => Self::dummy_schema(arena, ["CHANGE COLUMN SUCCESS"]),
            Operator::DropColumn(_) => Self::dummy_schema(arena, ["DROP COLUMN SUCCESS"]),
            Operator::CreateTable(_) => Self::dummy_schema(arena, ["CREATE TABLE SUCCESS"]),
            Operator::CreateIndex(_) => Self::dummy_schema(arena, ["CREATE INDEX SUCCESS"]),
            Operator::CreateView(_) => Self::dummy_schema(arena, ["CREATE VIEW SUCCESS"]),
            Operator::DropTable(_) => Self::dummy_schema(arena, ["DROP TABLE SUCCESS"]),
            Operator::DropView(_) => Self::dummy_schema(arena, ["DROP VIEW SUCCESS"]),
            Operator::DropIndex(_) => Self::dummy_schema(arena, ["DROP INDEX SUCCESS"]),
            Operator::Truncate(_) => Self::dummy_schema(arena, ["TRUNCATE TABLE SUCCESS"]),
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(_) => Self::dummy_schema(arena, ["COPY FROM SOURCE"]),
            #[cfg(feature = "copy")]
            Operator::CopyToFile(_) => Self::dummy_schema(arena, ["COPY TO TARGET"]),
        }
    }

    fn dummy_schema<const N: usize>(
        arena: &mut (dyn MetaArena + '_),
        names: [&str; N],
    ) -> crate::types::tuple::Schema {
        names
            .into_iter()
            .map(|name| arena.alloc_dummy(name))
            .collect()
    }

    pub(crate) fn read_schema(&self) -> &crate::types::tuple::Schema {
        self.output_schema
            .as_ref()
            .expect("output schema must be computed before it is read")
    }

    pub(crate) fn exec_capacity_hint(&self) -> usize {
        self.exec_capacity_hint
    }

    pub(crate) fn populate_output_schema_recursive(&mut self, arena: &mut (dyn MetaArena + '_)) {
        let child_nodes = match self.childrens.as_mut() {
            Childrens::Only(child) => {
                child.populate_output_schema_recursive(arena);
                child.exec_capacity_hint
            }
            Childrens::Twins { left, right } => {
                left.populate_output_schema_recursive(arena);
                right.populate_output_schema_recursive(arena);
                left.exec_capacity_hint + right.exec_capacity_hint
            }
            Childrens::None => 0,
        };
        self.exec_capacity_hint = 1 + child_nodes;
        self.output_schema(arena);
    }

    pub fn reset_output_schema_cache(&mut self) {
        self.output_schema = None;
        self.exec_capacity_hint = 0;
    }

    pub fn reset_output_schema_cache_recursive(&mut self) {
        self.reset_output_schema_cache();
        match self.childrens.as_mut() {
            Childrens::Only(child) => child.reset_output_schema_cache_recursive(),
            Childrens::Twins { left, right } => {
                left.reset_output_schema_cache_recursive();
                right.reset_output_schema_cache_recursive();
            }
            Childrens::None => (),
        }
    }

    pub fn explain(&self, arena: &mut (dyn MetaArena + '_), indentation: usize) -> String {
        format!(
            "{:indent$}{}",
            "",
            Explain::explain(self, arena),
            indent = indentation
        )
    }
}

impl Explain for LogicalPlan {
    fn fmt<A: MetaArena + ?Sized>(&self, arena: &A, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.operator.fmt(arena, f)?;

        if let Some(physical_option) = &self.physical_option {
            write!(f, " [{}]", physical_option.explain(arena))?;
        }

        for child in self.childrens.iter() {
            write!(f, " {}", Explain::explain(child, arena))?;
        }

        Ok(())
    }
}

impl Clone for LogicalPlan {
    fn clone(&self) -> Self {
        Self {
            operator: self.operator.clone(),
            childrens: self.childrens.clone(),
            physical_option: self.physical_option.clone(),
            output_schema: self.output_schema.clone(),
            exec_capacity_hint: self.exec_capacity_hint,
        }
    }
}

impl PartialEq for LogicalPlan {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator
            && self.childrens == other.childrens
            && self.physical_option == other.physical_option
    }
}

impl Eq for LogicalPlan {}

impl Hash for LogicalPlan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.operator.hash(state);
        self.childrens.hash(state);
        self.physical_option.hash(state);
    }
}

impl crate::serdes::ReferenceSerialization for LogicalPlan {
    fn encode<W: std::io::Write, A: MetaArena + ?Sized>(
        &self,
        writer: &mut W,
        is_direct: bool,
        reference_tables: &mut crate::serdes::ReferenceTables,
        arena: &A,
    ) -> Result<(), crate::errors::DatabaseError> {
        crate::serdes::ReferenceSerialization::encode(
            &self.operator,
            writer,
            is_direct,
            reference_tables,
            arena,
        )?;
        crate::serdes::ReferenceSerialization::encode(
            &self.childrens,
            writer,
            is_direct,
            reference_tables,
            arena,
        )?;
        crate::serdes::ReferenceSerialization::encode(
            &self.physical_option,
            writer,
            is_direct,
            reference_tables,
            arena,
        )
    }

    fn decode<T: crate::storage::Transaction, R: std::io::Read, A: MetaArena + ?Sized>(
        reader: &mut R,
        context: Option<&crate::serdes::ReferenceDecodeContext<'_, T>>,
        reference_tables: &crate::serdes::ReferenceTables,
        arena: &mut A,
    ) -> Result<Self, crate::errors::DatabaseError> {
        let operator = <Operator as crate::serdes::ReferenceSerialization>::decode(
            reader,
            context,
            reference_tables,
            arena,
        )?;
        let childrens = <Box<Childrens> as crate::serdes::ReferenceSerialization>::decode(
            reader,
            context,
            reference_tables,
            arena,
        )?;
        let physical_option =
            <Option<PhysicalOption> as crate::serdes::ReferenceSerialization>::decode(
                reader,
                context,
                reference_tables,
                arena,
            )?;

        Ok(Self {
            operator,
            childrens,
            physical_option,
            output_schema: None,
            exec_capacity_hint: 0,
        })
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::expression::ScalarExpression;
    use crate::optimizer::core::rule::NormalizationRule;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizerPipeline;
    use crate::optimizer::rule::normalization::NormalizationRuleImpl;
    use crate::planner::operator::describe::DescribeOperator;
    use crate::planner::operator::filter::FilterOperator;
    use crate::planner::operator::join::{JoinCondition, JoinOperator, JoinType};
    use crate::planner::operator::limit::LimitOperator;
    use crate::planner::operator::project::ProjectOperator;
    use crate::planner::operator::sort::{SortField, SortOperator};
    use crate::planner::operator::table_scan::TableScanOperator;
    use crate::planner::operator::{PlanImpl, SortOption};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn column(name: &str, ty: LogicalType, arena: &mut PlanArena) -> ColumnRef {
        arena.alloc_column(ColumnCatalog::new(
            name.to_string(),
            true,
            ColumnDesc::new(ty, None, false, None).unwrap(),
        ))
    }

    fn table_scan(table: &str, columns: Vec<ColumnRef>) -> LogicalPlan {
        LogicalPlan::new(
            Operator::TableScan(TableScanOperator {
                table_name: table.to_string().into(),
                columns,
                limit: (None, None),
                index_infos: Vec::new(),
                with_pk: false,
            }),
            Childrens::None,
        )
    }

    fn schema_names(schema: &[ColumnRef], arena: &PlanArena) -> Vec<String> {
        schema
            .iter()
            .map(|column| arena.column(*column).name().to_string())
            .collect()
    }

    #[test]
    fn row_count_follows_input_and_stops_at_unknown() {
        let mut input = table_scan("users", Vec::new());
        input.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 12,
                ndv: None,
            },
        ));
        let mut plan = LogicalPlan::new(Operator::Dummy, Childrens::Only(Box::new(input)));
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::Project,
            SortOption::Follow,
            RowCountOption::Follow,
        ));
        assert_eq!(plan.estimated_rows(), Some(12));
        plan.physical_option.as_mut().unwrap().row_count = RowCountOption::Unknown;
        assert_eq!(plan.estimated_rows(), None);
        plan.physical_option.as_mut().unwrap().row_count =
            RowCountOption::Estimated { rows: 0, ndv: None };
        assert_eq!(plan.estimated_rows(), Some(0));
        plan.physical_option = None;
        assert_eq!(plan.estimated_rows(), None);
    }

    #[test]
    fn filter_and_projection_work_preserve_input_without_blocking() -> Result<(), DatabaseError> {
        for output_rows in [0, 8, 98] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let predicate =
                arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
            let mut scan = table_scan("input", vec![]);
            scan.physical_option = Some(PhysicalOption::new(
                PlanImpl::SeqScan,
                SortOption::None,
                RowCountOption::Estimated {
                    rows: 98,
                    ndv: None,
                },
            ));
            let mut filter = FilterOperator::build(predicate, scan, false);
            filter.physical_option = Some(PhysicalOption::new(
                PlanImpl::Filter,
                SortOption::Follow,
                RowCountOption::Estimated {
                    rows: output_rows,
                    ndv: None,
                },
            ));
            assert_eq!(filter.estimated_rows(), Some(output_rows));
            assert_eq!(
                filter.price(&mut arena).and_then(|cost| cost.startup),
                Some(0)
            );
            assert_eq!(
                filter
                    .price(&mut arena)
                    .and_then(|cost| cost.input())
                    .map(|cost| cost.work),
                Some(392)
            );
            let mut project = LogicalPlan::new(
                Operator::Project(ProjectOperator::new(vec![])),
                Childrens::Only(Box::new(filter)),
            );
            project.physical_option = Some(PhysicalOption::new(
                PlanImpl::Project,
                SortOption::Follow,
                RowCountOption::Follow,
            ));
            assert_eq!(
                project.price(&mut arena).and_then(|cost| cost.startup),
                Some(0)
            );
            assert_eq!(
                project
                    .price(&mut arena)
                    .and_then(|cost| cost.input())
                    .map(|cost| cost.work),
                Some(392)
            );
        }
        Ok(())
    }

    #[test]
    fn blocking_operator_consumes_complete_filtered_input_work() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let id = column("id", LogicalType::Integer, &mut arena);
        let key = arena.alloc_expression(ScalarExpression::column_expr(id, 0));
        let predicate =
            arena.alloc_expression(ScalarExpression::Constant(DataValue::Boolean(true)));
        let mut scan = table_scan("input", vec![id]);
        scan.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Estimated {
                rows: 98,
                ndv: None,
            },
        ));
        let mut filter = FilterOperator::build(predicate, scan, false);
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Estimated { rows: 8, ndv: None },
        ));
        let mut sort = LogicalPlan::new(
            Operator::Sort(SortOperator {
                sort_fields: vec![SortField {
                    expr: key,
                    asc: true,
                    nulls_first: false,
                }],
            }),
            Childrens::Only(Box::new(filter)),
        );
        sort.physical_option = Some(PhysicalOption::new(
            PlanImpl::Sort,
            SortOption::None,
            RowCountOption::Follow,
        ));
        // 98 one-column input rows (5 each) + 24 comparisons * 2 operator units,
        // then 8 output rows.
        assert_eq!(
            sort.price(&mut arena).and_then(|cost| cost.startup),
            Some(538)
        );
        assert_eq!(
            sort.price(&mut arena)
                .and_then(|cost| cost.input())
                .map(|cost| cost.work),
            Some(578)
        );
        let mut unknown = table_scan("unknown", vec![]);
        unknown.physical_option = Some(PhysicalOption::new(
            PlanImpl::SeqScan,
            SortOption::None,
            RowCountOption::Unknown,
        ));
        let mut filter = FilterOperator::build(predicate, unknown, false);
        filter.physical_option = Some(PhysicalOption::new(
            PlanImpl::Filter,
            SortOption::Follow,
            RowCountOption::Estimated { rows: 8, ndv: None },
        ));
        assert_eq!(
            filter.price(&mut arena).and_then(|cost| cost.startup),
            Some(0)
        );
        assert_eq!(
            filter
                .price(&mut arena)
                .and_then(|cost| cost.input())
                .map(|cost| cost.work),
            None
        );
        Ok(())
    }

    #[test]
    fn childrens_iterates_and_pops_expected_plans() {
        let left = LogicalPlan::new(Operator::ShowTable, Childrens::None);
        let right = LogicalPlan::new(Operator::ShowView, Childrens::None);

        assert_eq!(Childrens::None.iter().count(), 0);

        let only = Childrens::Only(Box::new(left.clone()));
        assert_eq!(only.iter().count(), 1);
        assert!(matches!(only.pop_only().operator, Operator::ShowTable));

        let twins = Childrens::Twins {
            left: Box::new(left),
            right: Box::new(right),
        };
        let mut children = twins.iter();
        assert!(matches!(
            children.next().map(|plan| &plan.operator),
            Some(Operator::ShowTable)
        ));
        assert!(matches!(
            children.next().map(|plan| &plan.operator),
            Some(Operator::ShowView)
        ));
        assert!(children.next().is_none());

        let (left, right) = twins.pop_twins();
        assert!(matches!(left.operator, Operator::ShowTable));
        assert!(matches!(right.operator, Operator::ShowView));
    }

    #[test]
    fn logical_plan_collects_tables_explains_and_hashes_without_schema_cache() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", LogicalType::Integer, &mut arena);
        let scan = table_scan("users", vec![id]);
        let mut plan = LimitOperator::build(Some(2), Some(5), scan);
        plan.physical_option = Some(PhysicalOption::new(
            PlanImpl::Limit,
            SortOption::Follow,
            RowCountOption::Unknown,
        ));

        let tables = plan
            .referenced_table()
            .into_iter()
            .map(|table| table.to_string())
            .collect::<Vec<_>>();
        assert_eq!(tables, vec!["users"]);
        assert_eq!(
            plan.explain(&mut arena, 0),
            "Limit 5, Offset 2 [Limit => (Sort Option: Follow)] TableScan users -> [id]"
        );

        let _ = plan.output_schema(&mut arena);
        let cloned = plan.clone();
        assert_eq!(plan, cloned);

        let mut left = DefaultHasher::new();
        let mut right = DefaultHasher::new();
        plan.hash(&mut left);
        cloned.hash(&mut right);
        assert_eq!(left.finish(), right.finish());
    }

    #[test]
    fn output_schema_inherits_from_child_and_can_be_recursively_reset() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let id = column("id", LogicalType::Integer, &mut arena);
        let name = column(
            "name",
            LogicalType::Varchar(None, crate::types::CharLengthUnits::Characters),
            &mut arena,
        );
        let scan = table_scan("users", vec![id]);
        let mut plan = LimitOperator::build(None, Some(10), scan);

        let schema = plan.output_schema(&mut arena).clone();
        assert_eq!(schema_names(&schema, &arena), vec!["id"]);

        if let Childrens::Only(child) = plan.childrens.as_mut() {
            if let Operator::TableScan(scan) = &mut child.operator {
                scan.columns = vec![name];
            }
        }

        let cached = plan.output_schema(&mut arena).clone();
        assert_eq!(schema_names(&cached, &arena), vec!["id"]);

        plan.reset_output_schema_cache_recursive();
        let recomputed = plan.output_schema(&mut arena).clone();
        assert_eq!(schema_names(&recomputed, &arena), vec!["name"]);
    }

    #[test]
    fn transpose_invalidates_changed_child_before_schema_is_read() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let a = column("a", LogicalType::Integer, &mut arena);
        let b = column("b", LogicalType::Integer, &mut arena);
        let expr = arena.alloc_expression(ScalarExpression::column_expr(a, 0));
        let project = LogicalPlan::new(
            Operator::Project(ProjectOperator::new(vec![expr])),
            Childrens::Only(Box::new(table_scan("t", vec![a, b]))),
        );
        let mut plan = LimitOperator::build(None, Some(1), project);
        plan.populate_output_schema_recursive(&mut arena);
        assert_eq!(plan.childrens.only().output_schema.as_ref().unwrap(), &[a]);

        assert!(NormalizationRuleImpl::LimitProjectTranspose.apply(&mut plan, &mut arena, None)?);
        let Childrens::Only(limit) = plan.childrens.as_mut() else {
            panic!("expected a limit below the projection");
        };
        assert!(limit.output_schema.is_none());
        assert_eq!(
            limit.childrens.only().output_schema.as_ref().unwrap(),
            &[a, b]
        );
        assert_eq!(limit.output_schema(&mut arena), &[a, b]);
        assert_eq!(plan.output_schema(&mut arena), &[a]);
        Ok(())
    }

    #[test]
    fn local_rewrite_invalidates_ancestors_but_preserves_untouched_caches(
    ) -> Result<(), DatabaseError> {
        for strategy in [
            HepBatchStrategy::once_topdown(),
            HepBatchStrategy::once_bottomup(),
        ] {
            let tables = TableArenaCell::default();
            let mut arena = PlanArena::new(&tables);
            let a = column("a", LogicalType::Integer, &mut arena);
            let b = column("b", LogicalType::Integer, &mut arena);
            let c = column("c", LogicalType::Integer, &mut arena);
            let expr = arena.alloc_expression(ScalarExpression::column_expr(a, 0));
            let project = LogicalPlan::new(
                Operator::Project(ProjectOperator::new(vec![expr])),
                Childrens::Only(Box::new(table_scan("left", vec![a, b]))),
            );
            let mut plan = JoinOperator::build(
                LimitOperator::build(None, Some(1), project),
                table_scan("right", vec![c]),
                JoinCondition::None,
                JoinType::Cross,
                false,
            );
            plan.populate_output_schema_recursive(&mut arena);
            assert_eq!(plan.output_schema.as_ref().unwrap(), &[a, c]);
            let pipeline = HepOptimizerPipeline::builder()
                .before_batch(
                    "Transpose",
                    strategy,
                    &[NormalizationRuleImpl::LimitProjectTranspose],
                )
                .build();
            let mut plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;
            assert!(plan.output_schema.is_none());
            let Childrens::Twins { left, right } = plan.childrens.as_mut() else {
                panic!("expected a join");
            };
            assert!(left.output_schema.is_none());
            assert_eq!(right.output_schema.as_ref().unwrap(), &[c]);
            let Childrens::Only(limit) = left.childrens.as_mut() else {
                panic!("expected a limit below the projection");
            };
            assert!(limit.output_schema.is_none());
            assert_eq!(
                limit.childrens.only().output_schema.as_ref().unwrap(),
                &[a, b]
            );
            assert_eq!(limit.output_schema(&mut arena), &[a, b]);
            assert_eq!(plan.output_schema(&mut arena), &[a, c]);
        }
        Ok(())
    }

    #[test]
    fn wrap_child_invalidates_parent_without_clearing_input() {
        use crate::optimizer::plan_utils::wrap_child_with;

        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let a = column("a", LogicalType::Integer, &mut arena);
        let b = column("b", LogicalType::Integer, &mut arena);
        let expr = arena.alloc_expression(ScalarExpression::column_expr(a, 0));
        let mut plan = LimitOperator::build(None, Some(1), table_scan("t", vec![a, b]));
        plan.populate_output_schema_recursive(&mut arena);

        assert!(wrap_child_with(
            &mut plan,
            0,
            Operator::Project(ProjectOperator::new(vec![expr])),
        ));
        assert!(plan.output_schema.is_none());
        assert!(plan
            .childrens
            .only()
            .childrens
            .only()
            .output_schema
            .is_some());
        assert_eq!(plan.output_schema(&mut arena), &[a]);
    }

    #[test]
    fn predicate_pushdown_preserves_untouched_subtree_caches() -> Result<(), DatabaseError> {
        let tables = TableArenaCell::default();
        let mut arena = PlanArena::new(&tables);
        let a = column("a", LogicalType::Integer, &mut arena);
        let b = column("b", LogicalType::Integer, &mut arena);
        let c = column("c", LogicalType::Integer, &mut arena);
        let left = JoinOperator::build(
            table_scan("a", vec![a]),
            table_scan("b", vec![b]),
            JoinCondition::None,
            JoinType::Cross,
            false,
        );
        let join = JoinOperator::build(
            left,
            table_scan("c", vec![c]),
            JoinCondition::None,
            JoinType::Cross,
            false,
        );
        let c_expr = arena.alloc_expression(ScalarExpression::column_expr(c, 2));
        let zero = arena.alloc_expression(ScalarExpression::Constant(DataValue::Int32(0)));
        let predicate = arena.alloc_expression(ScalarExpression::Binary {
            op: crate::expression::BinaryOperator::Gt,
            left_expr: c_expr,
            right_expr: zero,
            evaluator: None,
            ty: LogicalType::Boolean,
        });
        let mut plan = FilterOperator::build(predicate, join, false);
        plan.populate_output_schema_recursive(&mut arena);
        let pipeline = HepOptimizerPipeline::builder()
            .before_batch(
                "Push Predicate",
                HepBatchStrategy::once_topdown(),
                &[NormalizationRuleImpl::PushPredicateThroughJoin],
            )
            .build();
        let mut plan = pipeline.instantiate(plan).find_best(None, &mut arena)?;
        assert!(matches!(plan.operator, Operator::Join(_)));
        assert!(plan.output_schema.is_none());
        let (left, right) = plan.childrens.twins();
        assert_eq!(left.output_schema.as_ref().unwrap(), &[a, b]);
        assert!(left.childrens.twins().0.output_schema.is_some());
        assert!(left.childrens.twins().1.output_schema.is_some());
        assert!(matches!(right.operator, Operator::Filter(_)));
        assert_eq!(right.childrens.only().output_schema.as_ref().unwrap(), &[c]);
        assert_eq!(plan.output_schema(&mut arena), &[a, b, c]);
        Ok(())
    }

    #[test]
    fn dummy_operators_produce_expected_output_schema() {
        let table_arena = TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let cases = [
            (Operator::ShowTable, vec!["TABLE"]),
            (Operator::ShowView, vec!["VIEW"]),
            (Operator::Explain, vec!["PLAN"]),
            (
                Operator::Describe(DescribeOperator {
                    table_name: "users".into(),
                }),
                vec!["FIELD", "TYPE", "LEN", "NULL", "Key", "DEFAULT"],
            ),
        ];

        for (operator, expected) in cases {
            let mut plan = LogicalPlan::new(operator, Childrens::None);
            let schema = plan.output_schema(&mut arena).clone();
            assert_eq!(schema_names(&schema, &arena), expected);
        }
    }
}
// GRCOV_EXCL_STOP

// GRCOV_EXCL_START
#[cfg(test)]
pub(crate) mod test {
    use crate::expression::ScalarExpression;
    use crate::planner::{ExprRef, PlanArena};

    pub(crate) trait PlanArenaTestExt {
        fn alloc_expressions<I, E>(&mut self, expressions: I) -> Vec<ExprRef>
        where
            I: IntoIterator<Item = E>,
            E: Into<ScalarExpression>;

        fn alloc_expression_rows<R, E>(&mut self, rows: &[R]) -> Vec<ExprRef>
        where
            R: AsRef<[E]>,
            E: Clone + Into<ScalarExpression>;
    }

    impl PlanArenaTestExt for PlanArena<'_> {
        fn alloc_expressions<I, E>(&mut self, expressions: I) -> Vec<ExprRef>
        where
            I: IntoIterator<Item = E>,
            E: Into<ScalarExpression>,
        {
            expressions
                .into_iter()
                .map(|expression| self.alloc_expression(expression.into()))
                .collect()
        }

        fn alloc_expression_rows<R, E>(&mut self, rows: &[R]) -> Vec<ExprRef>
        where
            R: AsRef<[E]>,
            E: Clone + Into<ScalarExpression>,
        {
            rows.iter()
                .flat_map(|row| row.as_ref().iter().cloned())
                .map(|expression| self.alloc_expression(expression.into()))
                .collect()
        }
    }
}
// GRCOV_EXCL_STOP
