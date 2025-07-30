use crate::binder::{lower_case_name, lower_ident, Binder};
use crate::catalog::view::View;
use crate::catalog::{ColumnCatalog, ColumnRef};
use crate::errors::DatabaseError;
use crate::expression::{AliasType, ScalarExpression};
use crate::planner::operator::create_view::CreateViewOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use itertools::Itertools;
use sqlparser::ast::{Ident, ObjectName, Query};
use std::sync::Arc;
use ulid::Ulid;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_create_view(
        &mut self,
        or_replace: &bool,
        name: &ObjectName,
        columns: &[Ident],
        query: &Query,
    ) -> Result<LogicalPlan, DatabaseError> {
        let view_name = Arc::new(lower_case_name(name)?);
        let mut plan = self.bind_query(query)?;

        let mapping_schema = plan.output_schema();

        let exprs = if columns.is_empty() {
            Box::new(
                mapping_schema
                    .iter()
                    .map(|column| column.name().to_string()),
            ) as Box<dyn Iterator<Item = String>>
        } else {
            Box::new(columns.iter().map(lower_ident)) as Box<dyn Iterator<Item = String>>
        }
        .enumerate()
        .map(|(i, column_name)| {
            let mapping_column = &mapping_schema[i];
            let mut column = ColumnCatalog::new(
                column_name,
                mapping_column.nullable(),
                mapping_column.desc().clone(),
            );
            column.set_ref_table(view_name.clone(), Ulid::new(), true);

            ScalarExpression::Alias {
                expr: Box::new(ScalarExpression::ColumnRef(mapping_column.clone())),
                alias: AliasType::Expr(Box::new(ScalarExpression::ColumnRef(ColumnRef::from(
                    column,
                )))),
            }
        })
        .collect_vec();
        plan = self.bind_project(plan, exprs)?;

        Ok(LogicalPlan::new(
            Operator::CreateView(CreateViewOperator {
                view: View {
                    name: view_name,
                    plan: Box::new(plan),
                },
                or_replace: *or_replace,
            }),
            Childrens::None,
        ))
    }
}
