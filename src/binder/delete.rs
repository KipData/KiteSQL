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

use crate::binder::{lower_case_name, Binder};
use crate::errors::DatabaseError;
use crate::planner::operator::delete::DeleteOperator;
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use crate::storage::Transaction;
use crate::types::value::DataValue;
use itertools::Itertools;
use sqlparser::ast::{Expr, TableFactor, TableWithJoins};
use std::sync::Arc;

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(crate) fn bind_delete(
        &mut self,
        from: &TableWithJoins,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan, DatabaseError> {
        if let TableFactor::Table { name, .. } = &from.relation {
            let table_name: Arc<str> = lower_case_name(name)?.into();
            let table = self
                .context
                .table(table_name.clone())?
                .ok_or(DatabaseError::TableNotFound)?;
            let primary_keys = table
                .primary_keys()
                .iter()
                .map(|(_, column)| column.clone())
                .collect_vec();
            self.with_pk(table_name.clone());
            let mut plan = self.bind_table_ref(from)?;

            if let Some(predicate) = selection {
                plan = self.bind_where(plan, predicate)?;
            }

            Ok(LogicalPlan::new(
                Operator::Delete(DeleteOperator {
                    table_name,
                    primary_keys,
                }),
                Childrens::Only(Box::new(plan)),
            ))
        } else {
            unreachable!("only table")
        }
    }
}
