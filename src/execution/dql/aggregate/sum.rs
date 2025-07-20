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
