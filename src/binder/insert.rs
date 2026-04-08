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

use crate::binder::{
    attach_span_from_sqlparser_span_if_absent, attach_span_if_absent, lower_case_name, lower_ident,
    Binder,
};
use crate::errors::DatabaseError;
use crate::expression::simplify::ConstantCalculator;
use crate::expression::visitor_mut::VisitorMut;
use crate::expression::AliasType;
use crate::expression::ScalarExpression;
use crate::planner::operator::insert::InsertOperator;
use crate::planner::operator::values::ValuesOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::tuple::SchemaRef;
use crate::types::value::DataValue;
use sqlparser::ast::{Expr, Ident, ObjectName, Query};
use std::borrow::Cow;
use std::slice;
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_insert(
        &mut self,
        name: &ObjectName,
        idents: &[Ident],
        expr_rows: &Vec<Vec<Expr>>,
        is_overwrite: bool,
        is_mapping_by_name: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        // FIXME: Make it better to detect the current BindStep
        self.context.allow_default = true;
        let table_name: Arc<str> = lower_case_name(name)?.into();

        let source = self
            .context
            .source_and_bind(table_name.clone(), None, None, false)?
            .ok_or(DatabaseError::TableNotFound)?;
        let mut _schema_ref = None;
        let values_len = expr_rows[0].len();

        if idents.is_empty() {
            let schema_buf = self.table_schema_buf.entry(table_name.clone()).or_default();
            let temp_schema_ref = source.schema_ref(schema_buf);
            if values_len > temp_schema_ref.len() {
                return Err(DatabaseError::ValuesLenMismatch(
                    temp_schema_ref.len(),
                    values_len,
                ));
            }
            _schema_ref = Some(temp_schema_ref);
        } else {
            let mut columns = Vec::with_capacity(idents.len());
            for ident in idents {
                match self.bind_column_ref_from_identifiers(
                    slice::from_ref(ident),
                    Some(table_name.to_string()),
                )? {
                    ScalarExpression::ColumnRef { column, .. } => columns.push(column),
                    _ => return Err(DatabaseError::UnsupportedStmt(ident.to_string())),
                }
            }
            if values_len != columns.len() {
                return Err(DatabaseError::ValuesLenMismatch(columns.len(), values_len));
            }
            _schema_ref = Some(Arc::new(columns));
        }
        let schema_ref = _schema_ref.ok_or(DatabaseError::ColumnsEmpty)?;
        let mut rows = Vec::with_capacity(expr_rows.len());

        for expr_row in expr_rows {
            if expr_row.len() != values_len {
                return Err(DatabaseError::ValuesLenMismatch(expr_row.len(), values_len));
            }
            let mut row = Vec::with_capacity(expr_row.len());

            for (i, expr) in expr_row.iter().enumerate() {
                let mut expression = self.bind_expr(expr)?;

                ConstantCalculator.visit(&mut expression)?;
                match expression {
                    ScalarExpression::Constant(mut value) => {
                        let ty = schema_ref[i].datatype();

                        value = value.cast(ty)?;
                        // Check if the value length is too long
                        value.check_len(ty)?;
                        if value.is_null() && !schema_ref[i].nullable() {
                            return Err(attach_span_if_absent(
                                DatabaseError::not_null_column(schema_ref[i].name().to_string()),
                                expr,
                            ));
                        }

                        row.push(value);
                    }
                    ScalarExpression::Empty => {
                        let default_value = schema_ref[i]
                            .default_value()?
                            .ok_or(DatabaseError::DefaultNotExist)?;
                        if default_value.is_null() && !schema_ref[i].nullable() {
                            return Err(attach_span_if_absent(
                                DatabaseError::not_null_column(schema_ref[i].name().to_string()),
                                expr,
                            ));
                        }
                        row.push(default_value);
                    }
                    _ => return Err(DatabaseError::UnsupportedStmt(expr.to_string())),
                }
            }
            rows.push(row);
        }
        self.context.allow_default = false;
        let values_plan = self.bind_values(rows, schema_ref);

        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name,
            }),
            Childrens::Only(Box::new(values_plan)),
        ))
    }

    pub(crate) fn bind_insert_query(
        &mut self,
        name: &ObjectName,
        idents: &[Ident],
        query: &Query,
        is_overwrite: bool,
    ) -> Result<LogicalPlan, DatabaseError> {
        let table_name: Arc<str> = lower_case_name(name)?.into();
        let table_schema = {
            let source = self
                .context
                .source(&table_name)?
                .ok_or(DatabaseError::TableNotFound)?;
            let mut schema_buf = None;
            source.schema_ref(&mut schema_buf)
        };

        let mut input_plan = self.bind_query(query)?;
        let input_schema = input_plan.output_schema().clone();
        let input_len = input_schema.len();

        let target_columns = if idents.is_empty() {
            if input_len > table_schema.len() {
                return Err(DatabaseError::ValuesLenMismatch(
                    table_schema.len(),
                    input_len,
                ));
            }
            Cow::Borrowed(&table_schema[..input_len])
        } else {
            let mut columns = Vec::with_capacity(idents.len());
            let source = self
                .context
                .source(&table_name)?
                .ok_or(DatabaseError::TableNotFound)?;
            let mut schema_buf = None;
            for ident in idents {
                let column_name = lower_ident(ident);
                let column = source
                    .column(&column_name, &mut schema_buf)
                    .ok_or_else(|| {
                        attach_span_from_sqlparser_span_if_absent(
                            DatabaseError::column_not_found(column_name),
                            ident.span,
                        )
                    })?;
                columns.push(column);
            }
            if input_len != columns.len() {
                return Err(DatabaseError::ValuesLenMismatch(columns.len(), input_len));
            }
            Cow::Owned(columns)
        };

        let projection = input_schema
            .iter()
            .enumerate()
            .zip(target_columns.iter())
            .map(
                |((position, input_column), target_column)| ScalarExpression::Alias {
                    expr: Box::new(ScalarExpression::column_expr(
                        input_column.clone(),
                        position,
                    )),
                    alias: AliasType::Name(target_column.name().to_string()),
                },
            )
            .collect::<Vec<_>>();
        input_plan = self.bind_project(input_plan, projection)?;

        Ok(LogicalPlan::new(
            Operator::Insert(InsertOperator {
                table_name,
                is_overwrite,
                is_mapping_by_name: true,
            }),
            Childrens::Only(Box::new(input_plan)),
        ))
    }

    pub(crate) fn bind_values(
        &mut self,
        rows: Vec<Vec<DataValue>>,
        schema_ref: SchemaRef,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::Values(ValuesOperator { rows, schema_ref }),
            Childrens::None,
        )
    }
}
