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

use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use kite_sql_serde_macros::ReferenceSerialization;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

/// for `datafusion`
/// - `None` unknown monotonicity or non-monotonicity
/// - `Some(true)` monotonically increasing
/// - `Some(false)` monotonically decreasing
pub type FuncMonotonicity = Vec<Option<bool>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArcScalarFunctionImpl(pub Arc<dyn ScalarFunctionImpl>);

impl Deref for ArcScalarFunctionImpl {
    type Target = dyn ScalarFunctionImpl;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

#[derive(Debug, Clone, ReferenceSerialization)]
pub struct ScalarFunction {
    pub(crate) args: Vec<ScalarExpression>,
    pub(crate) inner: ArcScalarFunctionImpl,
}

impl PartialEq for ScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.summary() == other.summary()
    }
}

impl Eq for ScalarFunction {}

impl Hash for ScalarFunction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.summary().hash(state);
    }
}

#[typetag::serde(tag = "scala")]
pub trait ScalarFunctionImpl: Debug + Send + Sync {
    fn eval(
        &self,
        args: &[ScalarExpression],
        tuple: Option<(&[DataValue], &[ColumnRef])>,
    ) -> Result<DataValue, DatabaseError>;

    // TODO: Exploiting monotonicity when optimizing `ScalarFunctionImpl::monotonicity()`
    fn monotonicity(&self) -> Option<FuncMonotonicity>;

    fn return_type(&self) -> &LogicalType;

    fn summary(&self) -> &FunctionSummary;
}

impl ScalarFunction {
    pub fn summary(&self) -> &FunctionSummary {
        self.inner.summary()
    }
}
