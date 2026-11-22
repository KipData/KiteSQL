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

use super::Operator;
use crate::catalog::{ColumnRef, TableId};
use crate::errors::DatabaseError;
use crate::planner::{fmt_table, Childrens, Explain, LogicalPlan, MetaArena};
use crate::types::index::{IndexType, IndexValType};
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CreateIndexOperator {
    pub table_id: TableId,
    pub columns: Vec<ColumnRef>,
    pub index_val_ty: Vec<IndexValType>,
    pub index_name: String,
    pub if_not_exists: bool,
    pub ty: IndexType,
}

impl CreateIndexOperator {
    pub fn build(
        table_id: TableId,
        index_name: String,
        key_parts: impl ExactSizeIterator<Item = Result<(ColumnRef, IndexValType), DatabaseError>>,
        if_not_exists: bool,
        is_unique: bool,
        input: LogicalPlan,
    ) -> Result<LogicalPlan, DatabaseError> {
        let mut columns = Vec::with_capacity(key_parts.len());
        let mut index_val_ty = Vec::with_capacity(key_parts.len());
        for part in key_parts {
            let (column, key_ty) = part?;
            columns.push(column);
            index_val_ty.push(key_ty);
        }
        let ty = if is_unique {
            IndexType::Unique
        } else if columns.len() == 1 {
            IndexType::Normal
        } else {
            IndexType::Composite
        };

        Ok(LogicalPlan::new(
            Operator::CreateIndex(Self {
                table_id,
                columns,
                index_val_ty,
                index_name,
                if_not_exists,
                ty,
            }),
            Childrens::Only(Box::new(input)),
        ))
    }
}

impl Explain for CreateIndexOperator {
    fn fmt<A: MetaArena + ?Sized>(
        &self,
        arena: &A,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.write_str("Create Index On ")?;
        fmt_table(self.table_id, arena, f)?;
        f.write_str(" -> [")?;
        for (i, (column, key_ty)) in self.columns.iter().zip(&self.index_val_ty).enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}", column.explain(arena))?;
            if let Some(len) = key_ty.prefix_len {
                write!(f, "({len})")?;
            }
        }
        write!(f, "], If Not Exists: {}", self.if_not_exists)
    }
}
