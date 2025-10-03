use crate::catalog::ColumnRef;
use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use serde::Deserialize;
use serde::Serialize;
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CharLength {
    summary: FunctionSummary,
}

impl CharLength {
    pub(crate) fn new(function_name: String) -> Arc<Self> {
        let arg_types = vec![LogicalType::Varchar(None, CharLengthUnits::Characters)];
        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name,
                arg_types,
            },
        })
    }
}

#[typetag::serde]
impl ScalarFunctionImpl for CharLength {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        exprs: &[ScalarExpression],
        tuples: Option<(&[DataValue], &[ColumnRef])>,
    ) -> Result<DataValue, DatabaseError> {
        let mut value = exprs[0].eval(tuples)?;
        if !matches!(value.logical_type(), LogicalType::Varchar(_, _)) {
            value = value.cast(&LogicalType::Varchar(None, CharLengthUnits::Characters))?;
        }
        let mut length: u64 = 0;
        if let DataValue::Utf8 { value, ty, unit } = &mut value {
            length = value.chars().count() as u64;
        }
        Ok(DataValue::UInt64(length))
    }

    fn monotonicity(&self) -> Option<FuncMonotonicity> {
        todo!()
    }

    fn return_type(&self) -> &LogicalType {
        &LogicalType::Varchar(None, CharLengthUnits::Characters)
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }
}
