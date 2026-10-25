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
use crate::execution::dql::aggregate::Accumulator;
use crate::types::value::{DataValue, Utf8Type};
use crate::types::CharLengthUnits;
use std::collections::HashSet;
use std::fmt::Write;

/// SQLite `group_concat()`: non-NULL values in input order joined with `,`; NULL when there are none.
pub struct GroupConcatAccumulator {
    distinct_values: Option<HashSet<DataValue>>,
    result: DataValue,
}

impl GroupConcatAccumulator {
    pub fn new(distinct: bool) -> Self {
        Self {
            distinct_values: distinct.then(HashSet::new),
            result: DataValue::Null,
        }
    }
}

impl Accumulator for GroupConcatAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if value.is_null() {
            return Ok(());
        }
        if let Some(values) = &mut self.distinct_values {
            if !values.insert(value.clone()) {
                return Ok(());
            }
        }
        match &mut self.result {
            DataValue::Utf8 { value: text, .. } => {
                text.push(',');
                let _ = write!(text, "{value}");
            }
            result => {
                *result = DataValue::Utf8 {
                    value: value.to_string(),
                    ty: Utf8Type::Variable(None),
                    unit: CharLengthUnits::Characters,
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<(), DatabaseError> {
        Ok(())
    }

    fn result(&self) -> &DataValue {
        &self.result
    }

    fn result_owned(self: Box<Self>) -> DataValue {
        self.result
    }
}
