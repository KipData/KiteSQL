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
use crate::planner::operator::Operator;
use crate::planner::{Childrens, LogicalPlan};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct DropIndexOperator {
    pub table_name: TableName,
    pub index_name: String,
    pub if_exists: bool,
}

impl DropIndexOperator {
    pub fn build(
        table_name: TableName,
        index_name: String,
        if_exists: bool,
        childrens: Childrens,
    ) -> LogicalPlan {
        LogicalPlan::new(
            Operator::DropIndex(DropIndexOperator {
                table_name,
                index_name,
                if_exists,
            }),
            childrens,
        )
    }
}

impl fmt::Display for DropIndexOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Drop Index {} On {}, If Exists: {}",
            self.index_name, self.table_name, self.if_exists
        )?;

        Ok(())
    }
}
