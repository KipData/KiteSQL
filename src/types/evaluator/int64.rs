use crate::types::evaluator::DataValue;
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use crate::types::DatabaseError;
use crate::{numeric_binary_evaluator_definition, numeric_unary_evaluator_definition};
use paste::paste;
use serde::{Deserialize, Serialize};
use std::hint;

numeric_unary_evaluator_definition!(Int64, DataValue::Int64);
numeric_binary_evaluator_definition!(Int64, DataValue::Int64);
