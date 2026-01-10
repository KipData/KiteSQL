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

use crate::catalog::ColumnCatalog;
use crate::catalog::ColumnDesc;
use crate::catalog::TableCatalog;
use crate::errors::DatabaseError;
use crate::expression::function::table::TableFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::expression::ScalarExpression;
use crate::types::tuple::SchemaRef;
use crate::types::tuple::Tuple;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use std::sync::LazyLock;

static NUMBERS: LazyLock<TableCatalog> = LazyLock::new(|| {
    TableCatalog::new(
        "numbers".to_string().into(),
        vec![ColumnCatalog::new(
            "number".to_string(),
            true,
            ColumnDesc::new(LogicalType::Integer, None, false, None).unwrap(),
        )],
    )
    .unwrap()
});

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Numbers {
    summary: FunctionSummary,
}

impl Numbers {
    #[allow(unused_mut)]
    pub(crate) fn new() -> Arc<Self> {
        let function_name = "numbers".to_string();

        Arc::new(Self {
            summary: FunctionSummary {
                name: function_name.into(),
                arg_types: vec![LogicalType::Integer],
            },
        })
    }
}

#[typetag::serde]
impl TableFunctionImpl for Numbers {
    #[allow(unused_variables, clippy::redundant_closure_call)]
    fn eval(
        &self,
        args: &[ScalarExpression],
    ) -> Result<Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>, DatabaseError> {
        let mut value = args[0].eval::<&Tuple>(None)?;

        if value.logical_type() != LogicalType::Integer {
            value = value.cast(&LogicalType::Integer)?;
        }
        let num = value.i32().ok_or(DatabaseError::NotNull)?;

        Ok(
            Box::new((0..num).map(|i| Ok(Tuple::new(None, vec![DataValue::Int32(i)]))))
                as Box<dyn Iterator<Item = Result<Tuple, DatabaseError>>>,
        )
    }

    fn output_schema(&self) -> &SchemaRef {
        NUMBERS.schema_ref()
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }

    fn table(&self) -> &'static TableCatalog {
        &NUMBERS
    }
}
