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

use std::path::PathBuf;
use std::str::FromStr;

use super::*;
use crate::catalog::TableName;
use crate::planner::operator::copy_from_file::CopyFromFileOperator;
use crate::planner::operator::copy_to_file::CopyToFileOperator;
use crate::planner::operator::table_scan::TableScanOperator;
use crate::planner::operator::Operator;
use crate::planner::Childrens;
use kite_sql_serde_macros::ReferenceSerialization;

#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, ReferenceSerialization)]
pub struct ExtSource {
    pub path: PathBuf,
    pub format: FileFormat,
}

/// File format.
#[derive(Debug, PartialEq, PartialOrd, Ord, Hash, Eq, Clone, ReferenceSerialization)]
pub enum FileFormat {
    Csv {
        /// Delimiter to parse.
        delimiter: char,
        /// Quote to use.
        quote: char,
        /// Escape character to use.
        escape: Option<char>,
        /// Whether or not the file has a header line.
        header: bool,
    },
}

impl std::fmt::Display for ExtSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl FromStr for ExtSource {
    type Err = ();
    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        Err(())
    }
}

impl<T: Transaction, A: AsRef<[(&'static str, DataValue)]>> Binder<'_, '_, T, A> {
    pub(super) fn bind_copy_to_file(
        &mut self,
        target: ExtSource,
        input: LogicalPlan,
    ) -> Result<LogicalPlan, DatabaseError> {
        Ok(LogicalPlan::new(
            Operator::CopyToFile(CopyToFileOperator { target }),
            Childrens::Only(Box::new(input)),
        ))
    }

    pub(super) fn bind_copy_table(
        &mut self,
        table_name: TableName,
        to: bool,
        ext_source: ExtSource,
        arena: &crate::planner::PlanArena,
    ) -> Result<LogicalPlan, DatabaseError> {
        if let Some(table) = self.context.table(table_name.clone())?.cloned() {
            if to {
                Ok(LogicalPlan::new(
                    Operator::CopyToFile(CopyToFileOperator { target: ext_source }),
                    Childrens::Only(Box::new(TableScanOperator::build(
                        table_name, &table, false, arena,
                    )?)),
                ))
            } else {
                let schema_ref = table.columns().copied().collect();
                Ok(LogicalPlan::new(
                    Operator::CopyFromFile(CopyFromFileOperator {
                        source: ext_source,
                        schema_ref,
                        table: table_name,
                    }),
                    Childrens::None,
                ))
            }
        } else {
            Err(DatabaseError::TableNotFound)
        }
    }
}
