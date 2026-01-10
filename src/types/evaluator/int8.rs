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

use crate::types::evaluator::DataValue;
use crate::types::evaluator::{BinaryEvaluator, UnaryEvaluator};
use crate::types::DatabaseError;
use crate::{numeric_binary_evaluator_definition, numeric_unary_evaluator_definition};
use paste::paste;
use serde::{Deserialize, Serialize};
use std::hint;

numeric_unary_evaluator_definition!(Int8, DataValue::Int8);
numeric_binary_evaluator_definition!(Int8, DataValue::Int8);
