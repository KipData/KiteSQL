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

use crate::catalog::TableName;
use crate::errors::DatabaseError;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::CharLengthUnits;
use crate::types::{ColumnId, LogicalType};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::hash::Hash;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct ColumnRef {
    pos: usize,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, ReferenceSerialization)]
pub struct ColumnCatalog {
    summary: ColumnSummary,
    nullable: bool,
    desc: ColumnDesc,
    in_join: bool,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum ColumnRelation {
    None,
    Table {
        column_id: ColumnId,
        table_name: TableName,
        is_temp: bool,
    },
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, ReferenceSerialization)]
pub struct ColumnSummary {
    pub name: String,
    pub relation: ColumnRelation,
}

impl ColumnRef {
    pub(crate) fn new(pos: usize) -> Self {
        Self { pos }
    }

    pub(crate) fn pos(self) -> usize {
        self.pos
    }
}

impl fmt::Display for ColumnRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{}", self.pos)
    }
}

impl ColumnCatalog {
    pub fn new(column_name: String, nullable: bool, column_desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                name: column_name,
                relation: ColumnRelation::None,
            },
            nullable,
            desc: column_desc,
            in_join: false,
        }
    }

    pub(crate) fn direct_new(
        summary: ColumnSummary,
        nullable: bool,
        column_desc: ColumnDesc,
        in_join: bool,
    ) -> ColumnCatalog {
        ColumnCatalog {
            summary,
            nullable,
            desc: column_desc,
            in_join,
        }
    }

    pub(crate) fn new_dummy(column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            summary: ColumnSummary {
                name: column_name,
                relation: ColumnRelation::None,
            },
            nullable: true,
            // SAFETY: default expr must not be [`ScalarExpression::ColumnRef`]
            desc: ColumnDesc::new(
                LogicalType::Varchar(None, CharLengthUnits::Characters),
                None,
                false,
                None,
            )
            .unwrap(),
            in_join: false,
        }
    }

    pub(crate) fn summary(&self) -> &ColumnSummary {
        &self.summary
    }

    pub fn summary_mut(&mut self) -> &mut ColumnSummary {
        &mut self.summary
    }

    pub fn id(&self) -> Option<ColumnId> {
        match &self.summary.relation {
            ColumnRelation::None => None,
            ColumnRelation::Table { column_id, .. } => Some(*column_id),
        }
    }

    pub fn name(&self) -> &str {
        &self.summary.name
    }

    pub fn full_name(&self) -> String {
        if let Some(table_name) = self.table_name() {
            return format!("{}.{}", table_name, self.name());
        }
        self.name().to_string()
    }

    pub fn table_name(&self) -> Option<&TableName> {
        match &self.summary.relation {
            ColumnRelation::None => None,
            ColumnRelation::Table { table_name, .. } => Some(table_name),
        }
    }

    pub(crate) fn is_persistent_table_column(&self) -> bool {
        matches!(
            self.summary.relation,
            ColumnRelation::Table { is_temp: false, .. }
        )
    }

    pub fn set_name(&mut self, name: String) {
        self.summary.name = name;
    }

    pub fn set_ref_table(&mut self, table_name: TableName, column_id: ColumnId, is_temp: bool) {
        self.summary.relation = ColumnRelation::Table {
            column_id,
            table_name,
            is_temp,
        };
    }

    pub fn in_join(&self) -> bool {
        self.in_join
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub(crate) fn set_nullable(&mut self, nullable: bool) {
        self.nullable = nullable;
    }

    pub(crate) fn set_in_join(&mut self, in_join: bool) {
        self.in_join = in_join;
    }

    pub fn datatype(&self) -> &LogicalType {
        &self.desc.column_datatype
    }

    pub(crate) fn default_value(&self) -> Result<Option<DataValue>, DatabaseError> {
        self.desc
            .default
            .as_ref()
            .map(|expr| expr.eval::<&Tuple>(None))
            .transpose()
    }

    pub(crate) fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub(crate) fn desc_mut(&mut self) -> &mut ColumnDesc {
        &mut self.desc
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::{ColumnCatalog, ColumnDesc};
    use crate::errors::DatabaseError;
    use crate::planner::PlanArena;
    use crate::types::LogicalType;

    #[test]
    fn test_same_column_ignores_nullable_and_desc() -> Result<(), DatabaseError> {
        let mut left = ColumnCatalog::new(
            "c1".to_string(),
            false,
            ColumnDesc::new(LogicalType::Integer, None, false, None)?,
        );
        let mut right = ColumnCatalog::new(
            "c1".to_string(),
            true,
            ColumnDesc::new(LogicalType::Bigint, None, false, None)?,
        );
        let table_arena = crate::planner::TableArenaCell::default();
        let mut arena = PlanArena::new(&table_arena);
        let left_ref = arena.alloc_column(left.clone());
        let right_ref = arena.alloc_column(right.clone());

        assert_ne!(left_ref, right_ref);
        assert!(arena.same_column(left_ref, right_ref));

        left.set_name("c2".to_string());
        right.set_name("c3".to_string());
        let left_ref = arena.alloc_column(left);
        let right_ref = arena.alloc_column(right);
        assert!(!arena.same_column(left_ref, right_ref));
        Ok(())
    }
}

/// The descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, Hash, ReferenceSerialization)]
pub struct ColumnDesc {
    pub(crate) column_datatype: LogicalType,
    primary: Option<usize>,
    is_unique: bool,
    pub(crate) default: Option<ScalarExpression>,
}

impl ColumnDesc {
    pub fn new(
        column_datatype: LogicalType,
        primary: Option<usize>,
        is_unique: bool,
        default: Option<ScalarExpression>,
    ) -> Result<ColumnDesc, DatabaseError> {
        if let Some(expr) = &default {
            let table_arena = crate::planner::TableArenaCell::default();
            let plan_arena = crate::planner::PlanArena::new(&table_arena);
            if expr.has_table_ref_column(&plan_arena) {
                return Err(DatabaseError::DefaultNotColumnRef);
            }
        }

        Ok(ColumnDesc {
            column_datatype,
            primary,
            is_unique,
            default,
        })
    }

    pub(crate) fn primary(&self) -> Option<usize> {
        self.primary
    }

    pub(crate) fn is_primary(&self) -> bool {
        self.primary.is_some()
    }

    pub(crate) fn set_primary(&mut self, is_primary: Option<usize>) {
        self.primary = is_primary
    }

    pub(crate) fn is_unique(&self) -> bool {
        self.is_unique
    }

    pub(crate) fn set_unique(&mut self) {
        self.is_unique = true
    }
}
