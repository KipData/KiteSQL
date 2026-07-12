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
use crate::execution::dql::aggregate::sum::SumAccumulator;
use crate::execution::dql::aggregate::Accumulator;
use crate::expression::BinaryOperator;
use crate::types::evaluator::binary_create;
use crate::types::value::DataValue;
use std::borrow::Cow;

pub struct AvgAccumulator {
    inner: Option<SumAccumulator>,
    count: usize,
    result: DataValue,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self {
            inner: None,
            count: 0,
            result: DataValue::Null,
        }
    }
}

impl Accumulator for AvgAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            let acc = if let Some(ref mut inner) = self.inner {
                inner
            } else {
                self.inner
                    .get_or_insert(SumAccumulator::new(Cow::Owned(value.logical_type()))?)
            };
            acc.update_value(value)?;
            self.count += 1;
            self.result = DataValue::Null;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<(), DatabaseError> {
        if !self.result.is_null() {
            return Ok(());
        }
        let Some(acc) = &self.inner else {
            return Ok(());
        };
        let mut value = Cow::Borrowed(acc.result());
        let value_ty = value.logical_type();

        if self.count == 0 {
            return Ok(());
        }
        let quantity = if value_ty.is_signed_numeric() {
            DataValue::Int64(self.count as i64)
        } else {
            DataValue::UInt32(self.count as u32)
        };
        let quantity_ty = quantity.logical_type();

        if value_ty != quantity_ty {
            value = Cow::Owned(value.into_owned().cast(&quantity_ty)?)
        }
        let evaluator = binary_create(Cow::Owned(quantity_ty), BinaryOperator::Divide)?;
        self.result = evaluator.binary_eval(value.as_ref(), &quantity)?;
        Ok(())
    }

    fn result(&self) -> &DataValue {
        &self.result
    }

    fn result_owned(self: Box<Self>) -> DataValue {
        self.result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalidate_cached_result_after_update() -> Result<(), DatabaseError> {
        let mut accumulator = AvgAccumulator::new();
        accumulator.update_value(&DataValue::Int32(2))?;
        accumulator.evaluate()?;
        let first = accumulator.result().clone();

        accumulator.update_value(&DataValue::Int32(4))?;
        accumulator.evaluate()?;
        let second = accumulator.result().clone();

        assert_ne!(first, second);
        accumulator.evaluate()?;
        assert_eq!(&second, accumulator.result());
        assert_eq!(Box::new(accumulator).result_owned(), second);
        Ok(())
    }

    #[test]
    fn empty_and_unsigned_results() -> Result<(), DatabaseError> {
        let mut empty = AvgAccumulator::new();
        empty.evaluate()?;
        assert_eq!(empty.result(), &DataValue::Null);
        assert_eq!(Box::new(empty).result_owned(), DataValue::Null);

        let mut accumulator = AvgAccumulator::new();
        accumulator.update_value(&DataValue::UInt32(2))?;
        accumulator.update_value(&DataValue::UInt32(4))?;
        accumulator.evaluate()?;
        assert_eq!(
            Box::new(accumulator).result_owned(),
            DataValue::Float64(3.0.into())
        );
        Ok(())
    }
}
