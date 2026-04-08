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

pub mod binary;
pub mod boolean;
pub mod cast;
pub mod date;
pub mod datetime;
pub mod decimal;
pub mod float32;
pub mod float64;
pub mod int16;
pub mod int32;
pub mod int64;
pub mod int8;
pub mod null;
pub mod time32;
pub mod time64;
pub mod tuple;
pub mod uint16;
pub mod uint32;
pub mod uint64;
pub mod uint8;
pub mod unary;
pub mod utf8;

use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, UnaryOperator};
use crate::types::evaluator::binary::create_binary_evaluator;
use crate::types::evaluator::cast::create_cast_evaluator;
use crate::types::evaluator::unary::create_unary_evaluator;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

#[typetag::serde(tag = "binary")]
pub trait BinaryEvaluator: Send + Sync + Debug {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError>;
}

#[typetag::serde(tag = "unary")]
pub trait UnaryEvaluator: Send + Sync + Debug {
    fn unary_eval(&self, value: &DataValue) -> DataValue;
}

#[typetag::serde(tag = "cast")]
pub trait CastEvaluator: Send + Sync + Debug {
    fn eval_cast(&self, value: &DataValue) -> Result<DataValue, DatabaseError>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BinaryEvaluatorBox(pub Arc<dyn BinaryEvaluator>);

impl Deref for BinaryEvaluatorBox {
    type Target = Arc<dyn BinaryEvaluator>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl BinaryEvaluatorBox {
    pub fn binary_eval(
        &self,
        left: &DataValue,
        right: &DataValue,
    ) -> Result<DataValue, DatabaseError> {
        self.0.binary_eval(left, right)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnaryEvaluatorBox(pub Arc<dyn UnaryEvaluator>);

impl UnaryEvaluatorBox {
    pub fn unary_eval(&self, value: &DataValue) -> DataValue {
        self.0.unary_eval(value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CastEvaluatorBox(pub Arc<dyn CastEvaluator>);

impl Deref for CastEvaluatorBox {
    type Target = Arc<dyn CastEvaluator>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl CastEvaluatorBox {
    pub fn eval_cast(&self, value: &DataValue) -> Result<DataValue, DatabaseError> {
        self.0.eval_cast(value)
    }
}

impl PartialEq for BinaryEvaluatorBox {
    fn eq(&self, _: &Self) -> bool {
        // FIXME
        true
    }
}

impl Eq for BinaryEvaluatorBox {}

impl Hash for BinaryEvaluatorBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i8(42)
    }
}

impl PartialEq for UnaryEvaluatorBox {
    fn eq(&self, _: &Self) -> bool {
        // FIXME
        true
    }
}

impl Eq for UnaryEvaluatorBox {}

impl Hash for UnaryEvaluatorBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i8(42)
    }
}

impl PartialEq for CastEvaluatorBox {
    fn eq(&self, _: &Self) -> bool {
        // FIXME
        true
    }
}

impl Eq for CastEvaluatorBox {}

impl Hash for CastEvaluatorBox {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_i8(42)
    }
}

pub struct EvaluatorFactory;

impl EvaluatorFactory {
    pub fn cast_create(
        from: Cow<'_, LogicalType>,
        to: Cow<'_, LogicalType>,
    ) -> Result<CastEvaluatorBox, DatabaseError> {
        create_cast_evaluator(from, to)
    }

    pub fn unary_create(
        ty: Cow<'_, LogicalType>,
        op: UnaryOperator,
    ) -> Result<UnaryEvaluatorBox, DatabaseError> {
        create_unary_evaluator(ty, op)
    }

    pub fn binary_create(
        ty: Cow<'_, LogicalType>,
        op: BinaryOperator,
    ) -> Result<BinaryEvaluatorBox, DatabaseError> {
        create_binary_evaluator(ty, op)
    }
}
