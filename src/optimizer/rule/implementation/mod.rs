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
#[cfg(feature = "copy")]
use crate::optimizer::rule::implementation::dml::copy_from_file::CopyFromFileImplementation;
#[cfg(feature = "copy")]
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
use crate::optimizer::rule::implementation::dql::mark_apply::MarkApplyImplementation;
use crate::optimizer::rule::implementation::dql::projection::ProjectionImplementation;
use crate::optimizer::rule::implementation::dql::scalar_apply::ScalarApplyImplementation;
use crate::optimizer::rule::implementation::dql::scalar_subquery::ScalarSubqueryImplementation;
use crate::optimizer::rule::implementation::dql::sort::SortImplementation;
use crate::optimizer::rule::implementation::dql::table_scan::{
    IndexScanImplementation, SeqScanImplementation,
};
use crate::optimizer::rule::implementation::dql::top_k::TopKImplementation;
use crate::optimizer::rule::implementation::dql::values::ValuesImplementation;
use crate::optimizer::rule::implementation::dql::window::WindowImplementation;
use crate::planner::operator::Operator;

#[repr(usize)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ImplementationRuleRootTag {
    Aggregate = 0,
    Dummy,
    Filter,
    Join,
    Limit,
    MarkApply,
    Project,
    ScalarApply,
    ScalarSubquery,
    TableScan,
    FunctionScan,
    Sort,
    TopK,
    Values,
    Analyze,
    #[cfg(feature = "copy")]
    CopyFromFile,
    #[cfg(feature = "copy")]
    CopyToFile,
    Delete,
    Insert,
    Update,
    AddColumn,
    ChangeColumn,
    CreateTable,
    DropColumn,
    DropTable,
    Truncate,
    Window,
}

impl ImplementationRuleRootTag {
    pub const COUNT: usize = Self::Window as usize + 1;

    pub fn from_operator(operator: &Operator) -> Option<Self> {
        match operator {
            Operator::Aggregate(_) => Some(Self::Aggregate),
            Operator::Dummy => Some(Self::Dummy),
            Operator::Filter(_) => Some(Self::Filter),
            Operator::Join(_) => Some(Self::Join),
            Operator::Limit(_) => Some(Self::Limit),
            Operator::MarkApply(_) => Some(Self::MarkApply),
            Operator::Project(_) => Some(Self::Project),
            Operator::ScalarApply(_) => Some(Self::ScalarApply),
            Operator::ScalarSubquery(_) => Some(Self::ScalarSubquery),
            Operator::TableScan(_) => Some(Self::TableScan),
            Operator::FunctionScan(_) => Some(Self::FunctionScan),
            Operator::Sort(_) => Some(Self::Sort),
            Operator::TopK(_) => Some(Self::TopK),
            Operator::Values(_) => Some(Self::Values),
            Operator::Analyze(_) => Some(Self::Analyze),
            #[cfg(feature = "copy")]
            Operator::CopyFromFile(_) => Some(Self::CopyFromFile),
            #[cfg(feature = "copy")]
            Operator::CopyToFile(_) => Some(Self::CopyToFile),
            Operator::Delete(_) => Some(Self::Delete),
            Operator::Insert(_) => Some(Self::Insert),
            Operator::Update(_) => Some(Self::Update),
            Operator::AddColumn(_) => Some(Self::AddColumn),
            Operator::ChangeColumn(_) => Some(Self::ChangeColumn),
            Operator::CreateTable(_) => Some(Self::CreateTable),
            Operator::DropColumn(_) => Some(Self::DropColumn),
            Operator::DropTable(_) => Some(Self::DropTable),
            Operator::Truncate(_) => Some(Self::Truncate),
            Operator::Window(_) => Some(Self::Window),
            Operator::ShowTable
            | Operator::ShowView
            | Operator::Explain
            | Operator::Describe(_)
            | Operator::SetMembership(_)
            | Operator::Union(_)
            | Operator::CreateIndex(_)
            | Operator::CreateView(_)
            | Operator::DropView(_)
            | Operator::DropIndex(_) => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ImplementationRuleImpl {
    // DQL
    GroupByAggregate,
    SimpleAggregate,
    Dummy,
    Filter,
    HashJoin,
    Limit,
    MarkApply,
    Projection,
    ScalarApply,
    ScalarSubquery,
    SeqScan,
    FunctionScan,
    IndexScan,
    Sort,
    TopK,
    Values,
    // DML
    Analyze,
    #[cfg(feature = "copy")]
    CopyFromFile,
    #[cfg(feature = "copy")]
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
    Window,
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
            ImplementationRuleImpl::MarkApply => MarkApplyImplementation.pattern(),
            ImplementationRuleImpl::Projection => ProjectionImplementation.pattern(),
            ImplementationRuleImpl::ScalarApply => ScalarApplyImplementation.pattern(),
            ImplementationRuleImpl::ScalarSubquery => ScalarSubqueryImplementation.pattern(),
            ImplementationRuleImpl::SeqScan => SeqScanImplementation.pattern(),
            ImplementationRuleImpl::IndexScan => IndexScanImplementation.pattern(),
            ImplementationRuleImpl::FunctionScan => FunctionScanImplementation.pattern(),
            ImplementationRuleImpl::Sort => SortImplementation.pattern(),
            ImplementationRuleImpl::TopK => TopKImplementation.pattern(),
            ImplementationRuleImpl::Values => ValuesImplementation.pattern(),
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyFromFile => CopyFromFileImplementation.pattern(),
            #[cfg(feature = "copy")]
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
            ImplementationRuleImpl::Window => WindowImplementation.pattern(),
        }
    }
}

impl ImplementationRuleImpl {
    pub fn root_tag(&self) -> ImplementationRuleRootTag {
        match self {
            ImplementationRuleImpl::GroupByAggregate | ImplementationRuleImpl::SimpleAggregate => {
                ImplementationRuleRootTag::Aggregate
            }
            ImplementationRuleImpl::Dummy => ImplementationRuleRootTag::Dummy,
            ImplementationRuleImpl::Filter => ImplementationRuleRootTag::Filter,
            ImplementationRuleImpl::HashJoin => ImplementationRuleRootTag::Join,
            ImplementationRuleImpl::Limit => ImplementationRuleRootTag::Limit,
            ImplementationRuleImpl::MarkApply => ImplementationRuleRootTag::MarkApply,
            ImplementationRuleImpl::Projection => ImplementationRuleRootTag::Project,
            ImplementationRuleImpl::ScalarApply => ImplementationRuleRootTag::ScalarApply,
            ImplementationRuleImpl::ScalarSubquery => ImplementationRuleRootTag::ScalarSubquery,
            ImplementationRuleImpl::SeqScan | ImplementationRuleImpl::IndexScan => {
                ImplementationRuleRootTag::TableScan
            }
            ImplementationRuleImpl::FunctionScan => ImplementationRuleRootTag::FunctionScan,
            ImplementationRuleImpl::Sort => ImplementationRuleRootTag::Sort,
            ImplementationRuleImpl::TopK => ImplementationRuleRootTag::TopK,
            ImplementationRuleImpl::Values => ImplementationRuleRootTag::Values,
            ImplementationRuleImpl::Analyze => ImplementationRuleRootTag::Analyze,
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyFromFile => ImplementationRuleRootTag::CopyFromFile,
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyToFile => ImplementationRuleRootTag::CopyToFile,
            ImplementationRuleImpl::Delete => ImplementationRuleRootTag::Delete,
            ImplementationRuleImpl::Insert => ImplementationRuleRootTag::Insert,
            ImplementationRuleImpl::Update => ImplementationRuleRootTag::Update,
            ImplementationRuleImpl::AddColumn => ImplementationRuleRootTag::AddColumn,
            ImplementationRuleImpl::ChangeColumn => ImplementationRuleRootTag::ChangeColumn,
            ImplementationRuleImpl::CreateTable => ImplementationRuleRootTag::CreateTable,
            ImplementationRuleImpl::DropColumn => ImplementationRuleRootTag::DropColumn,
            ImplementationRuleImpl::DropTable => ImplementationRuleRootTag::DropTable,
            ImplementationRuleImpl::Truncate => ImplementationRuleRootTag::Truncate,
            ImplementationRuleImpl::Window => ImplementationRuleRootTag::Window,
        }
    }
}

impl ImplementationRule for ImplementationRuleImpl {
    fn update_best_option(
        &self,
        operator: &Operator,
        arena: &crate::planner::PlanArena,
        loader: &StatisticMetaLoader<'_>,
        best_physical_option: &mut BestPhysicalOption,
    ) -> Result<(), DatabaseError> {
        macro_rules! update {
            ($implementation:expr) => {
                $implementation.update_best_option(operator, arena, loader, best_physical_option)?
            };
        }

        match self {
            ImplementationRuleImpl::GroupByAggregate => update!(GroupByAggregateImplementation),
            ImplementationRuleImpl::SimpleAggregate => update!(SimpleAggregateImplementation),
            ImplementationRuleImpl::Dummy => update!(DummyImplementation),
            ImplementationRuleImpl::Filter => update!(FilterImplementation),
            ImplementationRuleImpl::HashJoin => update!(JoinImplementation),
            ImplementationRuleImpl::Limit => update!(LimitImplementation),
            ImplementationRuleImpl::MarkApply => update!(MarkApplyImplementation),
            ImplementationRuleImpl::Projection => update!(ProjectionImplementation),
            ImplementationRuleImpl::ScalarApply => update!(ScalarApplyImplementation),
            ImplementationRuleImpl::ScalarSubquery => update!(ScalarSubqueryImplementation),
            ImplementationRuleImpl::SeqScan => update!(SeqScanImplementation),
            ImplementationRuleImpl::IndexScan => update!(IndexScanImplementation),
            ImplementationRuleImpl::FunctionScan => update!(FunctionScanImplementation),
            ImplementationRuleImpl::Sort => update!(SortImplementation),
            ImplementationRuleImpl::TopK => update!(TopKImplementation),
            ImplementationRuleImpl::Values => update!(ValuesImplementation),
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyFromFile => update!(CopyFromFileImplementation),
            #[cfg(feature = "copy")]
            ImplementationRuleImpl::CopyToFile => update!(CopyToFileImplementation),
            ImplementationRuleImpl::Delete => update!(DeleteImplementation),
            ImplementationRuleImpl::Insert => update!(InsertImplementation),
            ImplementationRuleImpl::Update => update!(UpdateImplementation),
            ImplementationRuleImpl::AddColumn => update!(AddColumnImplementation),
            ImplementationRuleImpl::ChangeColumn => update!(ChangeColumnImplementation),
            ImplementationRuleImpl::CreateTable => update!(CreateTableImplementation),
            ImplementationRuleImpl::DropColumn => update!(DropColumnImplementation),
            ImplementationRuleImpl::DropTable => update!(DropTableImplementation),
            ImplementationRuleImpl::Truncate => update!(TruncateImplementation),
            ImplementationRuleImpl::Analyze => update!(AnalyzeImplementation),
            ImplementationRuleImpl::Window => update!(WindowImplementation),
        }

        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use crate::binder::test::build_t1_table;
    use crate::errors::DatabaseError;
    use crate::expression::function::table::{
        ArcTableFunctionImpl, TableFunction, TableFunctionCatalog, TableFunctionImpl,
    };
    use crate::function::numbers::Numbers;
    use crate::optimizer::core::rule::{ImplementationRule, MatchPattern};
    use crate::optimizer::core::statistics_meta::StatisticMetaLoader;
    use crate::optimizer::rule::implementation::{
        ImplementationRuleImpl, ImplementationRuleRootTag,
    };
    use crate::planner::operator::function_scan::FunctionScanOperator;
    use crate::planner::operator::top_k::TopKOperator;
    use crate::planner::operator::{Operator, PhysicalOption, PlanImpl, SortOption};
    use crate::planner::{Childrens, LogicalPlan, PlanArena, TableArenaCell};
    use crate::storage::StatisticsMetaCache;
    use crate::types::value::DataValue;

    fn find_operator<F>(plan: &LogicalPlan, predicate: F) -> Option<&Operator>
    where
        F: Fn(&Operator) -> bool + Copy,
    {
        if predicate(&plan.operator) {
            return Some(&plan.operator);
        }

        match plan.childrens.as_ref() {
            Childrens::Only(child) => find_operator(child, predicate),
            Childrens::Twins { left, right } => {
                find_operator(left, predicate).or_else(|| find_operator(right, predicate))
            }
            Childrens::None => None,
        }
    }

    fn with_operator_from_sql<F, R>(
        sql: &str,
        predicate: F,
        f: impl FnOnce(Operator, &PlanArena) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError>
    where
        F: Fn(&Operator) -> bool + Copy,
    {
        let tables = build_t1_table()?;
        let mut arena = PlanArena::new(&tables.table_arena);
        let plan = tables.plan_with_arena(sql, &mut arena)?;
        let operator = find_operator(&plan, predicate).cloned().ok_or_else(|| {
            DatabaseError::UnsupportedStmt(format!("operator not found for {sql}"))
        })?;
        f(operator, &arena)
    }

    fn best_option(
        rule: ImplementationRuleImpl,
        operator: &Operator,
        arena: &PlanArena,
    ) -> Result<PhysicalOption, DatabaseError> {
        assert!((rule.pattern().predicate)(operator));
        assert_eq!(
            ImplementationRuleRootTag::from_operator(operator),
            Some(rule.root_tag())
        );

        let statistics_cache = StatisticsMetaCache::default();
        let loader = StatisticMetaLoader::new(&statistics_cache);
        let mut best = None;
        rule.update_best_option(operator, arena, &loader, &mut best)?;

        Ok(best
            .expect("implementation rule should produce an option")
            .0)
    }

    fn assert_sql_rule(
        sql: &str,
        rule: ImplementationRuleImpl,
        predicate: impl Fn(&Operator) -> bool + Copy,
        expected_plan: PlanImpl,
    ) -> Result<(), DatabaseError> {
        let option = with_operator_from_sql(sql, predicate, |operator, arena| {
            best_option(rule, &operator, arena)
        })?;

        assert_eq!(option.plan, expected_plan);
        Ok(())
    }

    #[test]
    fn test_single_mapping_implementations() -> Result<(), DatabaseError> {
        assert_sql_rule(
            "select count(c1) from t1",
            ImplementationRuleImpl::SimpleAggregate,
            |op| matches!(op, Operator::Aggregate(agg) if agg.groupby_exprs.is_empty()),
            PlanImpl::SimpleAggregate,
        )?;
        assert_sql_rule(
            "select c1, count(c2) from t1 group by c1",
            ImplementationRuleImpl::GroupByAggregate,
            |op| matches!(op, Operator::Aggregate(agg) if !agg.groupby_exprs.is_empty()),
            PlanImpl::HashAggregate,
        )?;
        assert_sql_rule(
            "select 1",
            ImplementationRuleImpl::Dummy,
            |op| matches!(op, Operator::Dummy),
            PlanImpl::Dummy,
        )?;
        assert_sql_rule(
            "select * from t1 where c1 = 1",
            ImplementationRuleImpl::Filter,
            |op| matches!(op, Operator::Filter(_)),
            PlanImpl::Filter,
        )?;
        assert_sql_rule(
            "select * from t1 limit 1",
            ImplementationRuleImpl::Limit,
            |op| matches!(op, Operator::Limit(_)),
            PlanImpl::Limit,
        )?;
        assert_sql_rule(
            "select * from t1 where exists(select * from t2)",
            ImplementationRuleImpl::MarkApply,
            |op| matches!(op, Operator::MarkApply(_)),
            PlanImpl::MarkApply,
        )?;
        assert_sql_rule(
            "select c1 from t1",
            ImplementationRuleImpl::Projection,
            |op| matches!(op, Operator::Project(_)),
            PlanImpl::Project,
        )?;
        assert_sql_rule(
            "select (select c3 from t2 limit 1) from t1",
            ImplementationRuleImpl::ScalarApply,
            |op| matches!(op, Operator::ScalarApply(_)),
            PlanImpl::ScalarApply,
        )?;
        assert_sql_rule(
            "select (select c3 from t2 limit 1) from t1",
            ImplementationRuleImpl::ScalarSubquery,
            |op| matches!(op, Operator::ScalarSubquery(_)),
            PlanImpl::ScalarSubquery,
        )?;
        assert_sql_rule(
            "values (1, 2)",
            ImplementationRuleImpl::Values,
            |op| matches!(op, Operator::Values(_)),
            PlanImpl::Values,
        )?;
        assert_sql_rule(
            "select row_number() over (order by c1) from t1",
            ImplementationRuleImpl::Window,
            |op| matches!(op, Operator::Window(_)),
            PlanImpl::Window,
        )?;
        assert_sql_rule(
            "select * from t1",
            ImplementationRuleImpl::SeqScan,
            |op| matches!(op, Operator::TableScan(_)),
            PlanImpl::SeqScan,
        )?;

        Ok(())
    }

    #[test]
    fn test_join_sort_topk_and_function_scan_implementations() -> Result<(), DatabaseError> {
        assert_sql_rule(
            "select * from t1 join t2 on c1 = c3",
            ImplementationRuleImpl::HashJoin,
            |op| matches!(op, Operator::Join(_)),
            PlanImpl::HashJoin,
        )?;
        assert_sql_rule(
            "select * from t1 cross join t2",
            ImplementationRuleImpl::HashJoin,
            |op| matches!(op, Operator::Join(_)),
            PlanImpl::NestLoopJoin,
        )?;

        with_operator_from_sql(
            "select * from t1 order by c1",
            |op| matches!(op, Operator::Sort(_)),
            |sort, arena| {
                let sort_option = best_option(ImplementationRuleImpl::Sort, &sort, arena)?;
                assert_eq!(sort_option.plan, PlanImpl::Sort);
                assert!(matches!(
                    sort_option.sort_option(),
                    SortOption::OrderBy {
                        fields,
                        ignore_prefix_len: 0,
                    } if fields.len() == 1
                ));

                let Operator::Sort(sort_op) = sort else {
                    unreachable!("sort operator expected")
                };
                let topk = Operator::TopK(TopKOperator {
                    sort_fields: sort_op.sort_fields,
                    limit: 5,
                    offset: Some(2),
                });
                let topk_option = best_option(ImplementationRuleImpl::TopK, &topk, arena)?;
                assert_eq!(topk_option.plan, PlanImpl::TopK);
                assert!(matches!(
                    topk_option.sort_option(),
                    SortOption::OrderBy {
                        fields,
                        ignore_prefix_len: 0,
                    } if fields.len() == 1
                ));

                let function_operator = function_scan_operator();
                let function_option = best_option(
                    ImplementationRuleImpl::FunctionScan,
                    &function_operator,
                    arena,
                )?;
                assert_eq!(function_option.plan, PlanImpl::FunctionScan);

                Ok(())
            },
        )?;

        Ok(())
    }

    fn function_scan_operator() -> Operator {
        let table_arena = TableArenaCell::default();
        let numbers = Numbers::new();
        let mut schema = Vec::new();
        numbers.output_schema_into(table_arena.borrow_mut(), &mut schema);
        let table_function = TableFunction {
            args: vec![DataValue::Int32(3).into()],
            catalog: TableFunctionCatalog {
                schema,
                inner: ArcTableFunctionImpl(numbers),
            },
        };

        Operator::FunctionScan(FunctionScanOperator { table_function })
    }

    #[test]
    fn test_dml_and_ddl_implementations() -> Result<(), DatabaseError> {
        assert_sql_rule(
            "analyze table t1",
            ImplementationRuleImpl::Analyze,
            |op| matches!(op, Operator::Analyze(_)),
            PlanImpl::Analyze,
        )?;
        assert_sql_rule(
            "delete from t1 where c1 = 1",
            ImplementationRuleImpl::Delete,
            |op| matches!(op, Operator::Delete(_)),
            PlanImpl::Delete,
        )?;
        assert_sql_rule(
            "insert into t1 values (1, 2)",
            ImplementationRuleImpl::Insert,
            |op| matches!(op, Operator::Insert(_)),
            PlanImpl::Insert,
        )?;
        assert_sql_rule(
            "update t1 set c2 = 3 where c1 = 1",
            ImplementationRuleImpl::Update,
            |op| matches!(op, Operator::Update(_)),
            PlanImpl::Update,
        )?;
        #[cfg(feature = "copy")]
        assert_sql_rule(
            "copy t1 from 'in.csv'",
            ImplementationRuleImpl::CopyFromFile,
            |op| matches!(op, Operator::CopyFromFile(_)),
            PlanImpl::CopyFromFile,
        )?;
        #[cfg(feature = "copy")]
        assert_sql_rule(
            "copy t1 to 'out.csv'",
            ImplementationRuleImpl::CopyToFile,
            |op| matches!(op, Operator::CopyToFile(_)),
            PlanImpl::CopyToFile,
        )?;
        assert_sql_rule(
            "alter table t1 add column c5 int null",
            ImplementationRuleImpl::AddColumn,
            |op| matches!(op, Operator::AddColumn(_)),
            PlanImpl::AddColumn,
        )?;
        assert_sql_rule(
            "alter table t1 alter column c2 type bigint",
            ImplementationRuleImpl::ChangeColumn,
            |op| matches!(op, Operator::ChangeColumn(_)),
            PlanImpl::ChangeColumn,
        )?;
        assert_sql_rule(
            "alter table t1 drop column c2",
            ImplementationRuleImpl::DropColumn,
            |op| matches!(op, Operator::DropColumn(_)),
            PlanImpl::DropColumn,
        )?;
        assert_sql_rule(
            "create table t3 (id int primary key)",
            ImplementationRuleImpl::CreateTable,
            |op| matches!(op, Operator::CreateTable(_)),
            PlanImpl::CreateTable,
        )?;
        assert_sql_rule(
            "drop table t1",
            ImplementationRuleImpl::DropTable,
            |op| matches!(op, Operator::DropTable(_)),
            PlanImpl::DropTable,
        )?;
        assert_sql_rule(
            "truncate table t1",
            ImplementationRuleImpl::Truncate,
            |op| matches!(op, Operator::Truncate(_)),
            PlanImpl::Truncate,
        )?;

        Ok(())
    }

    #[test]
    fn test_root_tag_groups_and_unsupported_operator() {
        assert_eq!(
            ImplementationRuleImpl::SimpleAggregate.root_tag(),
            ImplementationRuleRootTag::Aggregate
        );
        assert_eq!(
            ImplementationRuleImpl::GroupByAggregate.root_tag(),
            ImplementationRuleRootTag::Aggregate
        );
        assert_eq!(
            ImplementationRuleImpl::SeqScan.root_tag(),
            ImplementationRuleRootTag::TableScan
        );
        assert_eq!(
            ImplementationRuleImpl::IndexScan.root_tag(),
            ImplementationRuleRootTag::TableScan
        );
        assert_eq!(
            ImplementationRuleRootTag::from_operator(&Operator::ShowTable),
            None
        );
    }
}
