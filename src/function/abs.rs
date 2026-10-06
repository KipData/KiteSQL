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

use crate::errors::DatabaseError;
use crate::expression::function::scala::FuncMonotonicity;
use crate::expression::function::scala::ScalarFunctionImpl;
use crate::expression::function::FunctionSummary;
use crate::planner::ExprRef;
use crate::planner::MetaArena;
use crate::types::tuple::TupleLike;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use ordered_float::OrderedFloat;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Abs {
    summary: FunctionSummary,
    return_type: LogicalType,
}

impl Abs {
    pub(crate) fn new(ty: LogicalType) -> Arc<Self> {
        Arc::new(Self {
            summary: FunctionSummary {
                name: "abs".into(),
                arg_types: vec![ty.clone()],
            },
            return_type: ty,
        })
    }

    pub(crate) fn types() -> [LogicalType; 10] {
        [
            LogicalType::Tinyint,
            LogicalType::Smallint,
            LogicalType::Integer,
            LogicalType::Bigint,
            LogicalType::UTinyint,
            LogicalType::USmallint,
            LogicalType::UInteger,
            LogicalType::UBigint,
            LogicalType::Float,
            LogicalType::Double,
        ]
    }
}

impl ScalarFunctionImpl for Abs {
    fn eval(
        &self,
        exprs: &[ExprRef],
        arena: &(dyn MetaArena + '_),
        tuples: Option<&dyn TupleLike>,
    ) -> Result<DataValue, DatabaseError> {
        let value = arena.expression(exprs[0]).eval(arena, tuples)?;
        let overflow = || DatabaseError::OverFlow;
        Ok(match value.as_ref() {
            DataValue::Int8(v) => DataValue::Int8(v.checked_abs().ok_or_else(overflow)?),
            DataValue::Int16(v) => DataValue::Int16(v.checked_abs().ok_or_else(overflow)?),
            DataValue::Int32(v) => DataValue::Int32(v.checked_abs().ok_or_else(overflow)?),
            DataValue::Int64(v) => DataValue::Int64(v.checked_abs().ok_or_else(overflow)?),
            DataValue::Float32(v) => DataValue::Float32(OrderedFloat(v.0.abs())),
            DataValue::Float64(v) => DataValue::Float64(OrderedFloat(v.0.abs())),
            _ => value.into_owned(),
        })
    }

    fn monotonicity(&self) -> Option<FuncMonotonicity> {
        None
    }

    fn return_type(&self) -> &LogicalType {
        &self.return_type
    }

    fn summary(&self) -> &FunctionSummary {
        &self.summary
    }
}
