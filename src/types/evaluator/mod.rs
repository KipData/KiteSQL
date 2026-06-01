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

pub use self::binary::binary_create;
pub use self::cast::cast_create;
pub use self::unary::unary_create;

use crate::errors::DatabaseError;
use crate::expression::{BinaryOperator, UnaryOperator};
use crate::types::value::DataValue;
use crate::types::LogicalType;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

pub trait BinaryEvaluator: Send + Sync + Debug {
    fn binary_eval(&self, left: &DataValue, right: &DataValue) -> Result<DataValue, DatabaseError>;
}

pub trait UnaryEvaluator: Send + Sync + Debug {
    fn unary_eval(&self, value: &DataValue) -> DataValue;
}

pub trait CastEvaluator: Send + Sync + Debug {
    fn eval_cast(&self, value: &DataValue) -> Result<DataValue, DatabaseError>;
}

#[derive(Clone, Debug)]
pub struct BinaryEvaluatorBox {
    pub evaluator: Arc<dyn BinaryEvaluator>,
    pub ty: LogicalType,
    pub op: BinaryOperator,
}

impl Deref for BinaryEvaluatorBox {
    type Target = dyn BinaryEvaluator;

    fn deref(&self) -> &Self::Target {
        self.evaluator.as_ref()
    }
}

impl BinaryEvaluatorBox {
    pub fn new(evaluator: Arc<dyn BinaryEvaluator>, ty: LogicalType, op: BinaryOperator) -> Self {
        Self { evaluator, ty, op }
    }

    pub fn binary_eval(
        &self,
        left: &DataValue,
        right: &DataValue,
    ) -> Result<DataValue, DatabaseError> {
        self.evaluator.binary_eval(left, right)
    }
}

#[derive(Clone, Debug)]
pub struct UnaryEvaluatorBox {
    pub evaluator: Arc<dyn UnaryEvaluator>,
    pub ty: LogicalType,
    pub op: UnaryOperator,
}

impl UnaryEvaluatorBox {
    pub fn new(evaluator: Arc<dyn UnaryEvaluator>, ty: LogicalType, op: UnaryOperator) -> Self {
        Self { evaluator, ty, op }
    }

    pub fn unary_eval(&self, value: &DataValue) -> DataValue {
        self.evaluator.unary_eval(value)
    }
}

#[derive(Clone, Debug)]
pub struct CastEvaluatorBox {
    pub evaluator: Arc<dyn CastEvaluator>,
    pub from: LogicalType,
    pub to: LogicalType,
}

impl Deref for CastEvaluatorBox {
    type Target = dyn CastEvaluator;

    fn deref(&self) -> &Self::Target {
        self.evaluator.as_ref()
    }
}

impl CastEvaluatorBox {
    pub fn new(evaluator: Arc<dyn CastEvaluator>, from: LogicalType, to: LogicalType) -> Self {
        Self {
            evaluator,
            from,
            to,
        }
    }

    pub fn eval_cast(&self, value: &DataValue) -> Result<DataValue, DatabaseError> {
        self.evaluator.eval_cast(value)
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
