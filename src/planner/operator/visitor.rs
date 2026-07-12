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

use super::alter_table::change_column::DefaultChange;
use super::*;
use crate::errors::DatabaseError;
use crate::expression::visitor::ExprVisitor;

pub trait OperatorVisitor<'a>: Sized {
    fn visit_operator(&mut self, operator: &'a Operator) -> Result<(), DatabaseError> {
        walk_operator(self, operator)
    }

    fn visit_dummy(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_aggregate(&mut self, _op: &'a AggregateOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_scalar_apply(&mut self, _op: &'a ScalarApplyOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_mark_apply(&mut self, _op: &'a MarkApplyOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_filter(&mut self, _op: &'a FilterOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_join(&mut self, _op: &'a JoinOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_project(&mut self, _op: &'a ProjectOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_scalar_subquery(
        &mut self,
        _op: &'a ScalarSubqueryOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_table_scan(&mut self, _op: &'a TableScanOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_function_scan(&mut self, _op: &'a FunctionScanOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_sort(&mut self, _op: &'a SortOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_window(&mut self, _op: &'a window::WindowOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_limit(&mut self, _op: &'a LimitOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_top_k(&mut self, _op: &'a TopKOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_values(&mut self, _op: &'a ValuesOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_show_table(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_show_view(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_explain(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_describe(&mut self, _op: &'a DescribeOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_set_membership(
        &mut self,
        _op: &'a SetMembershipOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_union(&mut self, _op: &'a UnionOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_insert(&mut self, _op: &'a InsertOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_update(&mut self, _op: &'a UpdateOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_delete(&mut self, _op: &'a DeleteOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_analyze(&mut self, _op: &'a AnalyzeOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_add_column(&mut self, _op: &'a AddColumnOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_change_column(&mut self, _op: &'a ChangeColumnOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_column(&mut self, _op: &'a DropColumnOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_create_table(&mut self, _op: &'a CreateTableOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_create_index(&mut self, _op: &'a CreateIndexOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_create_view(&mut self, _op: &'a CreateViewOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_table(&mut self, _op: &'a DropTableOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_view(&mut self, _op: &'a DropViewOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_index(&mut self, _op: &'a DropIndexOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_truncate(&mut self, _op: &'a TruncateOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    #[cfg(feature = "copy")]
    fn visit_copy_from_file(&mut self, _op: &'a CopyFromFileOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    #[cfg(feature = "copy")]
    fn visit_copy_to_file(&mut self, _op: &'a CopyToFileOperator) -> Result<(), DatabaseError> {
        Ok(())
    }
}

pub struct OperatorExprVisitor<'a, V> {
    visitor: &'a mut V,
}

impl<'a, V> OperatorExprVisitor<'a, V> {
    pub fn new(visitor: &'a mut V) -> Self {
        Self { visitor }
    }
}

impl<'a, V: ExprVisitor<'a>> OperatorVisitor<'a> for OperatorExprVisitor<'_, V> {
    fn visit_aggregate(&mut self, op: &'a AggregateOperator) -> Result<(), DatabaseError> {
        for expr in op.agg_calls.iter().chain(&op.groupby_exprs) {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_mark_apply(&mut self, op: &'a MarkApplyOperator) -> Result<(), DatabaseError> {
        for expr in &op.predicates {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        if let Some(expr) = &op.parameterized_probe {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_filter(&mut self, op: &'a FilterOperator) -> Result<(), DatabaseError> {
        ExprVisitor::visit(self.visitor, &op.predicate)
    }

    fn visit_join(&mut self, op: &'a JoinOperator) -> Result<(), DatabaseError> {
        if let JoinCondition::On { on, filter } = &op.on {
            for (left_expr, right_expr) in on {
                ExprVisitor::visit(self.visitor, left_expr)?;
                ExprVisitor::visit(self.visitor, right_expr)?;
            }
            if let Some(expr) = filter {
                ExprVisitor::visit(self.visitor, expr)?;
            }
        }
        Ok(())
    }

    fn visit_project(&mut self, op: &'a ProjectOperator) -> Result<(), DatabaseError> {
        for expr in &op.exprs {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_table_scan(&mut self, op: &'a TableScanOperator) -> Result<(), DatabaseError> {
        for index_info in &op.index_infos {
            if let SortOption::OrderBy { fields, .. } = &index_info.sort_option {
                for field in fields {
                    ExprVisitor::visit(self.visitor, &field.expr)?;
                }
            }
            if let Some(expr) = &index_info.residual_predicate {
                ExprVisitor::visit(self.visitor, expr)?;
            }
        }
        Ok(())
    }

    fn visit_function_scan(&mut self, op: &'a FunctionScanOperator) -> Result<(), DatabaseError> {
        for expr in &op.table_function.args {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_sort(&mut self, op: &'a SortOperator) -> Result<(), DatabaseError> {
        for field in &op.sort_fields {
            ExprVisitor::visit(self.visitor, &field.expr)?;
        }
        Ok(())
    }

    fn visit_window(&mut self, op: &'a window::WindowOperator) -> Result<(), DatabaseError> {
        for expr in op
            .partition_by
            .iter()
            .chain(op.order_by.iter().map(|field| &field.expr))
            .chain(op.functions.iter().flat_map(|function| &function.args))
        {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_top_k(&mut self, op: &'a TopKOperator) -> Result<(), DatabaseError> {
        for field in &op.sort_fields {
            ExprVisitor::visit(self.visitor, &field.expr)?;
        }
        Ok(())
    }

    fn visit_update(&mut self, op: &'a UpdateOperator) -> Result<(), DatabaseError> {
        for (_, expr) in &op.value_exprs {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_add_column(&mut self, op: &'a AddColumnOperator) -> Result<(), DatabaseError> {
        if let Some(expr) = &op.column.desc().default {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_change_column(&mut self, op: &'a ChangeColumnOperator) -> Result<(), DatabaseError> {
        if let DefaultChange::Set(expr) = &op.default_change {
            ExprVisitor::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_create_table(&mut self, op: &'a CreateTableOperator) -> Result<(), DatabaseError> {
        for column in &op.columns {
            if let Some(expr) = &column.desc().default {
                ExprVisitor::visit(self.visitor, expr)?;
            }
        }
        Ok(())
    }
}

pub fn walk_operator<'a, V: OperatorVisitor<'a>>(
    visitor: &mut V,
    operator: &'a Operator,
) -> Result<(), DatabaseError> {
    match operator {
        Operator::Dummy => visitor.visit_dummy(),
        Operator::Aggregate(op) => visitor.visit_aggregate(op),
        Operator::ScalarApply(op) => visitor.visit_scalar_apply(op),
        Operator::MarkApply(op) => visitor.visit_mark_apply(op),
        Operator::Filter(op) => visitor.visit_filter(op),
        Operator::Join(op) => visitor.visit_join(op),
        Operator::Project(op) => visitor.visit_project(op),
        Operator::ScalarSubquery(op) => visitor.visit_scalar_subquery(op),
        Operator::TableScan(op) => visitor.visit_table_scan(op),
        Operator::FunctionScan(op) => visitor.visit_function_scan(op),
        Operator::Sort(op) => visitor.visit_sort(op),
        Operator::Limit(op) => visitor.visit_limit(op),
        Operator::TopK(op) => visitor.visit_top_k(op),
        Operator::Values(op) => visitor.visit_values(op),
        Operator::ShowTable => visitor.visit_show_table(),
        Operator::ShowView => visitor.visit_show_view(),
        Operator::Explain => visitor.visit_explain(),
        Operator::Describe(op) => visitor.visit_describe(op),
        Operator::SetMembership(op) => visitor.visit_set_membership(op),
        Operator::Union(op) => visitor.visit_union(op),
        Operator::Insert(op) => visitor.visit_insert(op),
        Operator::Update(op) => visitor.visit_update(op),
        Operator::Delete(op) => visitor.visit_delete(op),
        Operator::Analyze(op) => visitor.visit_analyze(op),
        Operator::AddColumn(op) => visitor.visit_add_column(op),
        Operator::ChangeColumn(op) => visitor.visit_change_column(op),
        Operator::DropColumn(op) => visitor.visit_drop_column(op),
        Operator::CreateTable(op) => visitor.visit_create_table(op),
        Operator::CreateIndex(op) => visitor.visit_create_index(op),
        Operator::CreateView(op) => visitor.visit_create_view(op),
        Operator::DropTable(op) => visitor.visit_drop_table(op),
        Operator::DropView(op) => visitor.visit_drop_view(op),
        Operator::DropIndex(op) => visitor.visit_drop_index(op),
        Operator::Truncate(op) => visitor.visit_truncate(op),
        #[cfg(feature = "copy")]
        Operator::CopyFromFile(op) => visitor.visit_copy_from_file(op),
        #[cfg(feature = "copy")]
        Operator::CopyToFile(op) => visitor.visit_copy_to_file(op),
        Operator::Window(op) => visitor.visit_window(op),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod tests {
    use super::*;
    #[cfg(feature = "copy")]
    use crate::binder::copy::{ExtSource, FileFormat};
    use crate::catalog::view::View;
    use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnRef};
    use crate::expression::function::table::{
        ArcTableFunctionImpl, TableFunction, TableFunctionCatalog,
    };
    use crate::expression::visitor::{walk_expr, ExprVisitor};
    use crate::expression::window::{WindowFunction, WindowFunctionKind};
    use crate::expression::ScalarExpression;
    use crate::function::numbers::Numbers;
    use crate::planner::operator::alter_table::change_column::NotNullChange;
    use crate::planner::operator::join::{JoinOperator, JoinType};
    use crate::planner::operator::mark_apply::MarkApplyOperator;
    use crate::planner::operator::set_membership::SetMembershipKind;
    use crate::planner::{Childrens, LogicalPlan};
    use crate::types::index::{IndexInfo, IndexMetaRef, IndexType};
    use crate::types::value::DataValue;
    use crate::types::LogicalType;

    fn index_info() -> IndexInfo {
        IndexInfo {
            meta: IndexMetaRef::new(0),
            sort_option: SortOption::OrderBy {
                fields: vec![SortField::from(ScalarExpression::from(10_i32))],
                ignore_prefix_len: 0,
            },
            lookup: None,
            residual_predicate: Some(11_i32.into()),
            covered_deserializers: None,
            cover_mapping: None,
            sort_elimination_hint: None,
            stream_distinct_hint: None,
        }
    }

    pub(crate) fn all_operators() -> Result<Vec<Operator>, DatabaseError> {
        let column_ref = ColumnRef::new(0);
        let column = ColumnCatalog::new(
            "value".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, Some(12_i32.into()))?,
        );
        let mut mark_apply = MarkApplyOperator::new_exists(column_ref, vec![3_i32.into()]);
        mark_apply.set_parameterized_probe(Some(4_i32.into()));
        let table_function = TableFunction {
            args: vec![8_i32.into()],
            catalog: TableFunctionCatalog {
                schema: Vec::new(),
                inner: ArcTableFunctionImpl(Numbers::new()),
            },
        };
        let operators = vec![
            Operator::Dummy,
            Operator::Aggregate(AggregateOperator {
                groupby_exprs: vec![1_i32.into()],
                agg_calls: vec![2_i32.into()],
                is_distinct: false,
            }),
            Operator::ScalarApply(ScalarApplyOperator),
            Operator::MarkApply(mark_apply),
            Operator::Filter(FilterOperator {
                predicate: 5_i32.into(),
                is_optimized: false,
                having: false,
            }),
            Operator::Join(JoinOperator {
                on: JoinCondition::On {
                    on: vec![(6_i32.into(), 7_i32.into())],
                    filter: Some(8_i32.into()),
                },
                join_type: JoinType::Inner,
            }),
            Operator::Project(ProjectOperator {
                exprs: vec![9_i32.into()],
            }),
            Operator::ScalarSubquery(ScalarSubqueryOperator),
            Operator::TableScan(TableScanOperator {
                table_name: "t1".into(),
                columns: vec![column_ref],
                limit: (None, None),
                index_infos: vec![index_info()],
                with_pk: false,
            }),
            Operator::FunctionScan(FunctionScanOperator { table_function }),
            Operator::Sort(SortOperator {
                sort_fields: vec![SortField::from(ScalarExpression::from(13_i32))],
                limit: None,
            }),
            Operator::Limit(LimitOperator {
                offset: None,
                limit: Some(1),
            }),
            Operator::TopK(TopKOperator {
                sort_fields: vec![SortField::from(ScalarExpression::from(14_i32))],
                limit: 1,
                offset: None,
            }),
            Operator::Values(ValuesOperator {
                rows: vec![vec![DataValue::Int32(1)]],
                schema_ref: vec![column_ref],
            }),
            Operator::Window(window::WindowOperator {
                partition_by: vec![17_i32.into()],
                order_by: vec![SortField::from(ScalarExpression::from(18_i32))],
                functions: vec![WindowFunction {
                    kind: WindowFunctionKind::RowNumber,
                    args: Vec::new(),
                    ty: LogicalType::Bigint,
                }],
                output_columns: vec![column_ref],
            }),
            Operator::ShowTable,
            Operator::ShowView,
            Operator::Explain,
            Operator::Describe(DescribeOperator {
                table_name: "t1".into(),
            }),
            Operator::SetMembership(SetMembershipOperator {
                kind: SetMembershipKind::Intersect,
                left_schema_ref: vec![column_ref],
                _right_schema_ref: vec![column_ref],
            }),
            Operator::Union(UnionOperator {
                left_schema_ref: vec![column_ref],
                _right_schema_ref: vec![column_ref],
            }),
            Operator::Insert(InsertOperator {
                table_name: "t1".into(),
                is_overwrite: false,
                is_mapping_by_name: false,
            }),
            Operator::Update(UpdateOperator {
                table_name: "t1".into(),
                value_exprs: vec![(column_ref, 15_i32.into())],
            }),
            Operator::Delete(DeleteOperator {
                table_name: "t1".into(),
                primary_keys: vec![column_ref],
            }),
            Operator::Analyze(AnalyzeOperator {
                table_name: "t1".into(),
                index_metas: vec![IndexMetaRef::new(0)],
                histogram_buckets: Some(1),
            }),
            Operator::AddColumn(AddColumnOperator {
                table_name: "t1".into(),
                if_not_exists: false,
                column: column.clone(),
            }),
            Operator::ChangeColumn(ChangeColumnOperator {
                table_name: "t1".into(),
                old_column_name: "value".to_string(),
                new_column_name: "value".to_string(),
                data_type: LogicalType::Integer,
                default_change: DefaultChange::Set(16_i32.into()),
                not_null_change: NotNullChange::NoChange,
            }),
            Operator::DropColumn(DropColumnOperator {
                table_name: "t1".into(),
                column_name: "value".to_string(),
                if_exists: false,
            }),
            Operator::CreateTable(CreateTableOperator {
                table_name: "t1".into(),
                columns: vec![column],
                if_not_exists: false,
            }),
            Operator::CreateIndex(CreateIndexOperator {
                table_name: "t1".into(),
                columns: vec![column_ref],
                index_name: "idx".to_string(),
                if_not_exists: false,
                ty: IndexType::Normal,
            }),
            Operator::CreateView(CreateViewOperator {
                view: View {
                    name: "v1".into(),
                    plan: Box::new(LogicalPlan::new(Operator::Dummy, Childrens::None)),
                    schema: vec![column_ref],
                },
                or_replace: false,
            }),
            Operator::DropTable(DropTableOperator {
                table_name: "t1".into(),
                if_exists: false,
            }),
            Operator::DropView(DropViewOperator {
                view_name: "v1".into(),
                if_exists: false,
            }),
            Operator::DropIndex(DropIndexOperator {
                table_name: "t1".into(),
                index_name: "idx".to_string(),
                if_exists: false,
            }),
            Operator::Truncate(TruncateOperator {
                table_name: "t1".into(),
            }),
        ];
        #[cfg(feature = "copy")]
        let operators = {
            let mut with_copy_operators = operators;
            let source = ExtSource {
                path: "data.csv".into(),
                format: FileFormat::Csv {
                    delimiter: ',',
                    quote: '"',
                    escape: None,
                    header: false,
                },
            };
            with_copy_operators.push(Operator::CopyFromFile(CopyFromFileOperator {
                table: "t1".into(),
                source: source.clone(),
                schema_ref: vec![column_ref],
            }));
            with_copy_operators.push(Operator::CopyToFile(CopyToFileOperator { target: source }));
            with_copy_operators
        };
        Ok(operators)
    }

    #[derive(Default)]
    struct ExpressionCounter(usize);

    impl<'a> ExprVisitor<'a> for ExpressionCounter {
        fn visit(&mut self, expr: &'a ScalarExpression) -> Result<(), DatabaseError> {
            self.0 += 1;
            walk_expr(self, expr)
        }
    }

    #[test]
    fn dispatches_all_variants_and_visits_expressions() -> Result<(), DatabaseError> {
        struct NoopVisitor;
        impl OperatorVisitor<'_> for NoopVisitor {}

        let operators = all_operators()?;
        for operator in &operators {
            NoopVisitor.visit_operator(operator)?;
        }

        let mut counter = ExpressionCounter::default();
        let mut visitor = OperatorExprVisitor::new(&mut counter);
        for operator in &operators {
            visitor.visit_operator(operator)?;
        }
        assert_eq!(counter.0, 20);

        Ok(())
    }
}
