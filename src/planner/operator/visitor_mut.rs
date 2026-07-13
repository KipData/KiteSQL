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
use crate::expression::visitor_mut::ExprVisitorMut;

pub trait OperatorVisitorMut<'a>: Sized {
    fn visit_operator(&mut self, operator: &'a mut Operator) -> Result<(), DatabaseError> {
        walk_mut_operator(self, operator)
    }

    fn visit_dummy(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_aggregate(&mut self, _op: &'a mut AggregateOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_scalar_apply(
        &mut self,
        _op: &'a mut ScalarApplyOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_mark_apply(&mut self, _op: &'a mut MarkApplyOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_filter(&mut self, _op: &'a mut FilterOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_join(&mut self, _op: &'a mut JoinOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_project(&mut self, _op: &'a mut ProjectOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_scalar_subquery(
        &mut self,
        _op: &'a mut ScalarSubqueryOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_table_scan(&mut self, _op: &'a mut TableScanOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_function_scan(
        &mut self,
        _op: &'a mut FunctionScanOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_sort(&mut self, _op: &'a mut SortOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_window(&mut self, _op: &'a mut window::WindowOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_limit(&mut self, _op: &'a mut LimitOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_top_k(&mut self, _op: &'a mut TopKOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_values(&mut self, _op: &'a mut ValuesOperator) -> Result<(), DatabaseError> {
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

    fn visit_describe(&mut self, _op: &'a mut DescribeOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_set_membership(
        &mut self,
        _op: &'a mut SetMembershipOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_union(&mut self, _op: &'a mut UnionOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_insert(&mut self, _op: &'a mut InsertOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_update(&mut self, _op: &'a mut UpdateOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_delete(&mut self, _op: &'a mut DeleteOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_analyze(&mut self, _op: &'a mut AnalyzeOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_add_column(&mut self, _op: &'a mut AddColumnOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_change_column(
        &mut self,
        _op: &'a mut ChangeColumnOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_column(&mut self, _op: &'a mut DropColumnOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_create_table(
        &mut self,
        _op: &'a mut CreateTableOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_create_index(
        &mut self,
        _op: &'a mut CreateIndexOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_create_view(&mut self, _op: &'a mut CreateViewOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_table(&mut self, _op: &'a mut DropTableOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_view(&mut self, _op: &'a mut DropViewOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_drop_index(&mut self, _op: &'a mut DropIndexOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn visit_truncate(&mut self, _op: &'a mut TruncateOperator) -> Result<(), DatabaseError> {
        Ok(())
    }

    #[cfg(feature = "copy")]
    fn visit_copy_from_file(
        &mut self,
        _op: &'a mut CopyFromFileOperator,
    ) -> Result<(), DatabaseError> {
        Ok(())
    }

    #[cfg(feature = "copy")]
    fn visit_copy_to_file(&mut self, _op: &'a mut CopyToFileOperator) -> Result<(), DatabaseError> {
        Ok(())
    }
}

pub struct OperatorExprVisitorMut<'a, V> {
    visitor: &'a mut V,
}

impl<'a, V> OperatorExprVisitorMut<'a, V> {
    pub fn new(visitor: &'a mut V) -> Self {
        Self { visitor }
    }
}

impl<'a, V: ExprVisitorMut<'a>> OperatorVisitorMut<'a> for OperatorExprVisitorMut<'_, V> {
    fn visit_aggregate(&mut self, op: &'a mut AggregateOperator) -> Result<(), DatabaseError> {
        for expr in op.agg_calls.iter_mut().chain(&mut op.groupby_exprs) {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_mark_apply(&mut self, op: &'a mut MarkApplyOperator) -> Result<(), DatabaseError> {
        for expr in &mut op.predicates {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        if let Some(expr) = &mut op.parameterized_probe {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_filter(&mut self, op: &'a mut FilterOperator) -> Result<(), DatabaseError> {
        ExprVisitorMut::visit(self.visitor, &mut op.predicate)
    }

    fn visit_join(&mut self, op: &'a mut JoinOperator) -> Result<(), DatabaseError> {
        if let JoinCondition::On { on, filter } = &mut op.on {
            for (left_expr, right_expr) in on {
                ExprVisitorMut::visit(self.visitor, left_expr)?;
                ExprVisitorMut::visit(self.visitor, right_expr)?;
            }
            if let Some(expr) = filter {
                ExprVisitorMut::visit(self.visitor, expr)?;
            }
        }
        Ok(())
    }

    fn visit_project(&mut self, op: &'a mut ProjectOperator) -> Result<(), DatabaseError> {
        for expr in &mut op.exprs {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_table_scan(&mut self, op: &'a mut TableScanOperator) -> Result<(), DatabaseError> {
        for index_info in &mut op.index_infos {
            if let SortOption::OrderBy { fields, .. } = &mut index_info.sort_option {
                for field in fields {
                    ExprVisitorMut::visit(self.visitor, &mut field.expr)?;
                }
            }
            if let Some(expr) = &mut index_info.residual_predicate {
                ExprVisitorMut::visit(self.visitor, expr)?;
            }
        }
        Ok(())
    }

    fn visit_function_scan(
        &mut self,
        op: &'a mut FunctionScanOperator,
    ) -> Result<(), DatabaseError> {
        for expr in &mut op.table_function.args {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_sort(&mut self, op: &'a mut SortOperator) -> Result<(), DatabaseError> {
        for field in &mut op.sort_fields {
            ExprVisitorMut::visit(self.visitor, &mut field.expr)?;
        }
        Ok(())
    }

    fn visit_window(&mut self, op: &'a mut window::WindowOperator) -> Result<(), DatabaseError> {
        for expr in op
            .sort_fields
            .iter_mut()
            .map(|field| &mut field.expr)
            .chain(
                op.functions
                    .iter_mut()
                    .flat_map(|function| &mut function.args),
            )
        {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_top_k(&mut self, op: &'a mut TopKOperator) -> Result<(), DatabaseError> {
        for field in &mut op.sort_fields {
            ExprVisitorMut::visit(self.visitor, &mut field.expr)?;
        }
        Ok(())
    }

    fn visit_update(&mut self, op: &'a mut UpdateOperator) -> Result<(), DatabaseError> {
        for (_, expr) in &mut op.value_exprs {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_add_column(&mut self, op: &'a mut AddColumnOperator) -> Result<(), DatabaseError> {
        if let Some(expr) = &mut op.column.desc_mut().default {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_change_column(
        &mut self,
        op: &'a mut ChangeColumnOperator,
    ) -> Result<(), DatabaseError> {
        if let DefaultChange::Set(expr) = &mut op.default_change {
            ExprVisitorMut::visit(self.visitor, expr)?;
        }
        Ok(())
    }

    fn visit_create_table(&mut self, op: &'a mut CreateTableOperator) -> Result<(), DatabaseError> {
        for column in &mut op.columns {
            if let Some(expr) = &mut column.desc_mut().default {
                ExprVisitorMut::visit(self.visitor, expr)?;
            }
        }
        Ok(())
    }
}

pub fn walk_mut_operator<'a, V: OperatorVisitorMut<'a>>(
    visitor: &mut V,
    operator: &'a mut Operator,
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
mod tests {
    use super::*;
    use crate::planner::operator::visitor::tests::all_operators;
    use crate::types::value::DataValue;

    struct IncrementConstants(usize);

    impl ExprVisitorMut<'_> for IncrementConstants {
        fn visit_constant(&mut self, value: &mut DataValue) -> Result<(), DatabaseError> {
            if let DataValue::Int32(value) = value {
                *value += 1;
            }
            self.0 += 1;
            Ok(())
        }
    }

    #[test]
    fn dispatches_all_variants_and_mutates_expressions() -> Result<(), DatabaseError> {
        struct NoopVisitor;
        impl OperatorVisitorMut<'_> for NoopVisitor {}

        let mut operators = all_operators()?;
        for operator in &mut operators {
            NoopVisitor.visit_operator(operator)?;
        }

        let mut counter = IncrementConstants(0);
        {
            let mut visitor = OperatorExprVisitorMut::new(&mut counter);
            for operator in &mut operators {
                visitor.visit_operator(operator)?;
            }
        }
        assert_eq!(counter.0, 20);

        Ok(())
    }
}
