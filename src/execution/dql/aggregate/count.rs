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
use crate::types::value::DataValue;
use std::collections::HashSet;

pub struct CountAccumulator {
    result: DataValue,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self {
            result: DataValue::Int32(0),
        }
    }
}

impl Accumulator for CountAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            let DataValue::Int32(result) = &mut self.result else {
                unreachable!()
            };
            *result += 1;
        }

        Ok(())
    }

    fn result(&self) -> &DataValue {
        &self.result
    }

    fn result_owned(self: Box<Self>) -> DataValue {
        self.result
    }
}

pub struct DistinctCountAccumulator {
    distinct_values: HashSet<DataValue>,
    result: DataValue,
}

impl DistinctCountAccumulator {
    pub fn new() -> Self {
        Self {
            distinct_values: HashSet::default(),
            result: DataValue::Int32(0),
        }
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() && self.distinct_values.insert(value.clone()) {
            self.result = DataValue::Int32(self.distinct_values.len() as i32);
        }

        Ok(())
    }

    fn result(&self) -> &DataValue {
        &self.result
    }

    fn result_owned(self: Box<Self>) -> DataValue {
        self.result
    }
}

// GRCOV_EXCL_START
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn count_results() -> Result<(), DatabaseError> {
        let mut accumulator = CountAccumulator::new();
        for value in [DataValue::Null, 1.into(), 1.into()] {
            accumulator.update_value(&value)?;
        }
        assert_eq!(accumulator.result(), &DataValue::Int32(2));
        assert_eq!(Box::new(accumulator).result_owned(), DataValue::Int32(2));
        Ok(())
    }

    #[test]
    fn distinct_count_results() -> Result<(), DatabaseError> {
        let mut accumulator = DistinctCountAccumulator::new();
        for value in [DataValue::Null, 1.into(), 1.into(), 2.into()] {
            accumulator.update_value(&value)?;
        }
        assert_eq!(accumulator.result(), &DataValue::Int32(2));
        assert_eq!(Box::new(accumulator).result_owned(), DataValue::Int32(2));
        Ok(())
    }
}
// GRCOV_EXCL_STOP
