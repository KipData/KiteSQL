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
#[cfg(feature = "time")]
pub mod date;
#[cfg(feature = "time")]
pub mod datetime;
#[cfg(feature = "decimal")]
pub mod decimal;
pub mod float32;
pub mod float64;
pub mod int16;
pub mod int32;
pub mod int64;
pub mod int8;
pub mod null;
#[cfg(feature = "time")]
pub mod time32;
#[cfg(feature = "time")]
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
use crate::types::value::DataValue;
use crate::types::CharLengthUnits;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BinaryEvaluatorParams {
    Unit,
    Like { escape_char: Option<char> },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BinaryEvaluatorRef {
    pub pos: u16,
    pub params: BinaryEvaluatorParams,
}

impl BinaryEvaluatorRef {
    pub fn new(pos: u16, params: BinaryEvaluatorParams) -> Self {
        Self { pos, params }
    }

    pub fn binary_eval(
        &self,
        left: &DataValue,
        right: &DataValue,
    ) -> Result<DataValue, DatabaseError> {
        binary::eval_binary(self.pos, &self.params, left, right)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UnaryEvaluatorRef {
    pub pos: u16,
}

impl UnaryEvaluatorRef {
    pub fn new(pos: u16) -> Self {
        Self { pos }
    }

    pub fn unary_eval(&self, value: &DataValue) -> DataValue {
        unary::eval_unary(self.pos, value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CastEvaluatorRef {
    pub pos: u16,
    pub params: CastEvaluatorParams,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CastEvaluatorParams {
    Identity,
    Unit,
    String {
        len: Option<u32>,
        unit: CharLengthUnits,
    },
    #[cfg(feature = "decimal")]
    Decimal {
        precision: Option<u8>,
        scale: Option<u8>,
    },
    Precision {
        precision: Option<u64>,
    },
    Timestamp {
        precision: Option<u64>,
        zone: bool,
    },
    Tuple {
        evaluators: Vec<CastEvaluatorRef>,
    },
}

impl CastEvaluatorRef {
    pub fn new(pos: u16, params: CastEvaluatorParams) -> Self {
        Self { pos, params }
    }

    pub fn pos(&self) -> u16 {
        self.pos
    }
}
