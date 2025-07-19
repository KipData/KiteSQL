use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::Utc;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CurrentTimeStamp {
    summary: FunctionSummary,
}

impl CurrentTimeStamp {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "current_timestamp".to_lowercase();

        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types: Vec::new(),
            },
        })
    }
}

#[typetag::serde]
impl ScalarFunctionImpl for CurrentTimeStamp {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        _: &[ScalarExpression],
        _: Option<(&Tuple, &[ColumnRef])>,
    ) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Time64(Utc::now().timestamp(), 0, false))
    }

    fn monotonicity(&self) -> Option<FuncMonotonicity> {
        todo!()
    }

    fn return_type(&self) -> &LogicalType {
        &LogicalType::TimeStamp(None, false)
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }
}
