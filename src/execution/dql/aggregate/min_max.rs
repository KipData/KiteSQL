use crate::errors::DatabaseError;
use crate::execution::dql::aggregate::Accumulator;
use crate::expression::BinaryOperator;
use crate::types::evaluator::EvaluatorFactory;
use crate::types::value::DataValue;

pub struct MinMaxAccumulator {
    inner: Option<DataValue>,
    op: BinaryOperator,
}

impl MinMaxAccumulator {
    pub fn new(is_max: bool) -> Self {
        let op = if is_max {
            BinaryOperator::Lt
        } else {
            BinaryOperator::Gt
        };

        Self { inner: None, op }
    }
}

impl Accumulator for MinMaxAccumulator {
    fn update_value(&mut self, value: &DataValue) -> Result<(), DatabaseError> {
        if !value.is_null() {
            if let Some(inner_value) = &self.inner {
                let evaluator = EvaluatorFactory::binary_create(value.logical_type(), self.op)?;
                if let DataValue::Boolean(result) = evaluator.0.binary_eval(inner_value, value)? {
                    result
                } else {
                    return Err(DatabaseError::InvalidType);
                }
            } else {
                true
            }
            .then(|| self.inner = Some(value.clone()));
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<DataValue, DatabaseError> {
        Ok(self.inner.clone().unwrap_or(DataValue::Null))
    }
}
