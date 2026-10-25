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
use crate::execution::dql::aggregate::sum::text_as_number;
use crate::execution::dql::aggregate::Accumulator;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::collections::HashSet;

/// SQLite `total()`: always a double, and 0.0 rather than NULL when nothing is summed.
pub struct TotalAccumulator {
    distinct_values: Option<HashSet<DataValue>>,
    result: DataValue,
}

impl TotalAccumulator {
    pub fn new(distinct: bool) -> Self {
        Self {
            distinct_values: distinct.then(HashSet::new),
            result: DataValue::Float64(0.0.into()),
        }
    }
}

impl Accumulator for TotalAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if value.is_null() {
            return Ok(());
        }
        if let Some(values) = &mut self.distinct_values {
            if !values.insert(value.clone()) {
                return Ok(());
            }
        }
        let DataValue::Float64(value) = text_as_number(value)
            .into_owned()
            .cast(&LogicalType::Double)?
        else {
            unreachable!("cast to Double yields Float64");
        };
        let DataValue::Float64(total) = &mut self.result else {
            unreachable!("total is always Float64");
        };
        *total = (total.0 + value.0).into();
        Ok(())
    }

    fn result(&self) -> &DataValue {
        &self.result
    }

    fn result_owned(self: Box<Self>) -> DataValue {
        self.result
    }
}
