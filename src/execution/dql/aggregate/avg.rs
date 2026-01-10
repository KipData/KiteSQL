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
use crate::types::evaluator::EvaluatorFactory;
use crate::types::value::DataValue;

pub struct AvgAccumulator {
    inner: Option<SumAccumulator>,
    count: usize,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self {
            inner: None,
            count: 0,
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
                    .get_or_insert(SumAccumulator::new(&value.logical_type())?)
            };
            acc.update_value(value)?;
            self.count += 1;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, DatabaseError> {
        let Some(acc) = &self.inner else {
            return Ok(DataValue::Null);
        };
        let mut value = acc.evaluate()?;
        let value_ty = value.logical_type();

        if self.count == 0 {
            return Ok(DataValue::Null);
        }
        let quantity = if value_ty.is_signed_numeric() {
            DataValue::Int64(self.count as i64)
        } else {
            DataValue::UInt32(self.count as u32)
        };
        let quantity_ty = quantity.logical_type();

        if value_ty != quantity_ty {
            value = value.cast(&quantity_ty)?
        }
        let evaluator = EvaluatorFactory::binary_create(quantity_ty, BinaryOperator::Divide)?;
        evaluator.0.binary_eval(&value, &quantity)
    }
}
