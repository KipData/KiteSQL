use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use chrono::{Datelike, Local};
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CurrentDate {
    summary: FunctionSummary,
}

impl CurrentDate {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "current_date".to_lowercase();

        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types: Vec::new(),
            },
        })
    }
}

#[typetag::serde]
impl ScalarFunctionImpl for CurrentDate {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        _: &[ScalarExpression],
        _: Option<(&[DataValue], &[ColumnRef])>,
    ) -> Result<DataValue, DatabaseError> {
        Ok(DataValue::Date32(Local::now().num_days_from_ce()))
    }

    fn monotonicity(&self) -> Option<FuncMonotonicity> {
        todo!()
    }

    fn return_type(&self) -> &LogicalType {
        &LogicalType::Date
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }
}
