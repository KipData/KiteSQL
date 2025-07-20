use crate::numeric_binary_evaluator_definition;
use crate::types::evaluator::BinaryEvaluator;
use crate::types::evaluator::DataValue;
use crate::types::DatabaseError;
use paste::paste;
use serde::{Deserialize, Serialize};
use std::hint;

numeric_binary_evaluator_definition!(Date, DataValue::Date32);
