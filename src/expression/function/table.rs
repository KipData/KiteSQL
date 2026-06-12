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
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::planner::TableArena;
use crate::types::tuple::{Schema, Tuple};
use kite_sql_serde_macros::ReferenceSerialization;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArcTableFunctionImpl(pub Arc<dyn TableFunctionImpl>);

impl Deref for ArcTableFunctionImpl {
    type Target = dyn TableFunctionImpl;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct TableFunction {
    pub(crate) args: Vec<ScalarExpression>,
    pub(crate) catalog: TableFunctionCatalog,
}

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct TableFunctionCatalog {
    pub(crate) schema: Schema,
    pub(crate) inner: ArcTableFunctionImpl,
}

impl PartialEq for TableFunction {
    fn eq(&self, other: &Self) -> bool {
        self.summary() == other.summary()
    }
}

impl Eq for TableFunction {}

impl Hash for TableFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.summary().hash(state);
    }
}
pub trait TableFunctionImpl: Debug + Send + Sync {
    fn eval(
        &self,
        args: &[ScalarExpression],
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>, DatabaseError>;

    fn summary(&self) -> &FunctionSummary;

    fn output_schema_into(
        &self,
        table_name: &TableName,
        table_arena: &mut TableArena,
        schema: &mut Schema,
    );
}

impl TableFunction {
    pub fn summary(&self) -> &FunctionSummary {
        self.catalog.inner.summary()
    }

    pub fn output_schema_into(&self, schema: &mut Schema) {
        schema.clear();
        schema.extend(self.catalog.schema.iter().copied());
    }

    pub fn inner(&self) -> &ArcTableFunctionImpl {
        &self.catalog.inner
    }
}
