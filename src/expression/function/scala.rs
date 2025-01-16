use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
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
        tuple: Option<(&Tuple, &[ColumnRef])>,
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
