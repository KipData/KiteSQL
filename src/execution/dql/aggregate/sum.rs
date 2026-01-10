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
use crate::types::evaluator::{BinaryEvaluatorBox, EvaluatorFactory};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use ahash::RandomState;
use std::collections::HashSet;

pub struct SumAccumulator {
    result: DataValue,
    evaluator: BinaryEvaluatorBox,
}

impl SumAccumulator {
    pub fn new(ty: &LogicalType) -> Result<Self, DatabaseError> {
        debug_assert!(ty.is_numeric());

        Ok(Self {
            result: DataValue::Null,
            evaluator: EvaluatorFactory::binary_create(ty.clone(), BinaryOperator::Plus)?,
        })
    }
}

impl Accumulator for SumAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            if self.result.is_null() {
                self.result = value.clone();
            } else {
                self.result = self.evaluator.0.binary_eval(&self.result, value)?;
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, DatabaseError> {
        Ok(self.result.clone())
    }
}

pub struct DistinctSumAccumulator {
    distinct_values: HashSet<DataValue, RandomState>,
    inner: SumAccumulator,
}

impl DistinctSumAccumulator {
    pub fn new(ty: &LogicalType) -> Result<Self, DatabaseError> {
        Ok(Self {
            distinct_values: HashSet::default(),
            inner: SumAccumulator::new(ty)?,
        })
    }
}

impl Accumulator for DistinctSumAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !self.distinct_values.contains(value) {
            self.distinct_values.insert(value.clone());
            self.inner.update_value(value)?;
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, DatabaseError> {
        self.inner.evaluate()
    }
}
