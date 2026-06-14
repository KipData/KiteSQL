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

use crate::binder::copy::ExtSource;
use crate::catalog::TableName;
use crate::iter_ext::Itertools;
use crate::types::tuple::Schema;
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug, PartialEq, Eq, Clone, Hash, ReferenceSerialization)]
pub struct CopyFromFileOperator {
    pub table: TableName,
    pub source: ExtSource,
    pub schema_ref: Schema,
}

impl fmt::Display for CopyFromFileOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let columns = self.schema_ref.iter().join(", ");
        write!(
            f,
            "Copy {} -> {} [{}]",
            self.source.path.display(),
            self.table,
            columns
        )?;

        Ok(())
    }
}
