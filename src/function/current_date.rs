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
        let function_name = "current_date".to_string();

        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name.into(),
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
