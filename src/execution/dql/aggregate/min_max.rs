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
use crate::expression::BinaryOperator;
use crate::types::evaluator::binary_create;
use crate::types::value::DataValue;
use std::borrow::Cow;

pub struct MinMaxAccumulator {
    result: DataValue,
    op: BinaryOperator,
}

impl MinMaxAccumulator {
    pub fn new(is_max: bool) -> Self {
        let op = if is_max {
            BinaryOperator::Lt
        } else {
            BinaryOperator::Gt
        };

        Self {
            result: DataValue::Null,
            op,
        }
    }
}

impl Accumulator for MinMaxAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            if !self.result.is_null() {
                let evaluator = binary_create(Cow::Owned(value.logical_type()), self.op)?;
                if let DataValue::Boolean(result) = evaluator.binary_eval(&self.result, value)? {
                    result
                } else {
                    return Err(DatabaseError::InvalidType);
                }
            } else {
                true
            }
            .then(|| self.result = value.clone());
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
    fn min_max_results() -> Result<(), DatabaseError> {
        for (is_max, expected) in [(false, 1), (true, 3)] {
            let mut accumulator = MinMaxAccumulator::new(is_max);
            for value in [DataValue::Null, 3.into(), 1.into(), 2.into()] {
                accumulator.update_value(&value)?;
            }
            assert_eq!(accumulator.result(), &DataValue::Int32(expected));
            assert_eq!(
                Box::new(accumulator).result_owned(),
                DataValue::Int32(expected)
            );
        }
        Ok(())
    }
}
// GRCOV_EXCL_STOP
