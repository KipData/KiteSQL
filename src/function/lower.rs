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
use serde::Deserialize;
use serde::Serialize;
use sqlparser::ast::CharLengthUnits;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Lower {
    summary: FunctionSummary,
}

impl Lower {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "lower".to_string();
        let arg_types = vec![LogicalType::Varchar(None, CharLengthUnits::Characters)];
        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name.into(),
                arg_types,
            },
        })
    }
}

#[typetag::serde]
impl ScalarFunctionImpl for Lower {
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
        if let DataValue::Utf8 { value, ty, unit } = &mut value {
            *value = value.to_lowercase();
        }
        Ok(value)
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
